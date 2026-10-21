package postgres

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/workflow"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) AuthorizeSandbox(ctx context.Context, worker string, instance, id, request uuid.UUID, generation int64) (execution.Attempt, error) {
	if request != sandbox.RequestID(id) {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	defer rollback(tx)
	_, a, live, err := withAttempt(ctx, tx, worker, instance, id, generation)
	if err != nil {
		return a, err
	}
	if !live {
		return a, execution.ErrStale
	}
	if a.Kind != "sandbox_go" || a.Sandbox == nil || !a.Sandbox.Valid() {
		return a, workflow.ErrInvalid
	}
	_, err = tx.Exec(ctx, `INSERT INTO sandbox_commands(attempt_id,request_id,workflow_id,task_id,generation,input) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(attempt_id) DO NOTHING`, id, request, a.WorkflowID, a.TaskID, generation, a.Sandbox)
	if err != nil {
		return a, classify(err)
	}
	var matches bool
	err = tx.QueryRow(ctx, `SELECT request_id=$2 AND generation=$3 AND input=$4::jsonb FROM sandbox_commands WHERE attempt_id=$1`, id, request, generation, a.Sandbox).Scan(&matches)
	if err != nil {
		return a, classify(err)
	}
	if !matches {
		return a, execution.ErrOutcome
	}
	_, err = tx.Exec(ctx, `UPDATE task_attempts SET cleanup_required=true WHERE id=$1`, id)
	if err != nil {
		return a, classify(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return a, classify(err)
	}
	a.CleanupRequired = true
	return a, nil
}

type ArtifactRecord = sandbox.ArtifactRecord

func artifactReferences(id uuid.UUID, r sandbox.Result) []map[string]any {
	refs := []map[string]any{}
	for _, a := range artifactRecords(id, r) {
		refs = append(refs, map[string]any{"id": a.ID, "kind": a.Kind, "sha256": a.SHA256, "content_length": a.ContentLength, "truncated": a.Truncated})
	}
	return refs
}

func artifactRecords(id uuid.UUID, r sandbox.Result) []ArtifactRecord {
	items := []ArtifactRecord{}
	for _, a := range r.Artifacts {
		h := sha256.Sum256(a.Body)
		items = append(items, ArtifactRecord{ID: uuid.NewSHA1(id, []byte("artifact-v1:"+a.Kind)), AttemptID: id, Kind: a.Kind, MediaType: a.MediaType, BaseRevision: r.BaseRevision, SHA256: hex.EncodeToString(h[:]), ContentLength: len(a.Body), Truncated: a.Truncated})
	}
	return items
}
func (s *Store) FinishSandbox(ctx context.Context, worker string, instance, id, request uuid.UUID, generation int64, r sandbox.Result) (execution.Attempt, error) {
	if request != sandbox.RequestID(id) || !r.Valid() {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	// Hash every metadata field and artifact byte, not just IDs. A lost reply
	// can be retried, but altered output cannot reuse an accepted receipt.
	encoded, _ := json.Marshal(r)
	hash := sha256.Sum256(encoded)
	body, _ := json.Marshal(map[string]any{"schema_version": 1, "sandbox_request_id": request, "command_sha256": hex.EncodeToString(hash[:]), "status": r.Status, "exit_code": r.ExitCode, "artifacts": artifactReferences(id, r)})
	success := r.Status == "exited" && r.ExitCode == 0
	class := execution.Diagnostic
	if success {
		class = ""
	} else if r.Status == "infrastructure_error" {
		class = execution.Transient
	} else if r.Status == "policy_denied" {
		class = execution.Permission
	}
	return s.finishAttempt(ctx, worker, instance, id, generation, success, class, body, &r)
}
func (s *Store) acceptSandbox(ctx context.Context, tx pgx.Tx, a execution.Attempt, r sandbox.Result) error {
	var exists bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM sandbox_commands WHERE attempt_id=$1 AND request_id=$2 AND input=$3::jsonb AND generation=$4)`, a.ID, sandbox.RequestID(a.ID), a.Sandbox, a.Generation).Scan(&exists)
	if err != nil {
		return classify(err)
	}
	if !exists {
		return execution.ErrStale
	}
	metadata := r
	metadata.Artifacts = nil
	body, _ := json.Marshal(metadata)
	_, err = tx.Exec(ctx, `INSERT INTO sandbox_executions(attempt_id,metadata) VALUES($1,$2)`, a.ID, body)
	if err != nil {
		return classify(err)
	}
	items := artifactRecords(a.ID, r)
	for i, item := range items {
		hash, _ := hex.DecodeString(item.SHA256)
		content := r.Artifacts[i].Body
		if content == nil {
			content = []byte{}
		}
		_, err = tx.Exec(ctx, `INSERT INTO sandbox_artifacts(id,attempt_id,workflow_id,task_id,generation,kind,media_type,base_revision,sha256,content_length,body,truncated) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)`, item.ID, a.ID, a.WorkflowID, a.TaskID, a.Generation, item.Kind, item.MediaType, item.BaseRevision, hash, item.ContentLength, content, item.Truncated)
		if err != nil {
			return classify(err)
		}
	}
	_, err = tx.Exec(ctx, `UPDATE task_attempts SET cleanup_required=false WHERE id=$1`, a.ID)
	return classify(err)
}

func (s *Store) Artifact(ctx context.Context, wf, id uuid.UUID) (ArtifactRecord, error) {
	var r ArtifactRecord
	err := s.pool.QueryRow(ctx, `SELECT id,attempt_id,workflow_id,task_id,generation,kind,media_type,base_revision,encode(sha256,'hex'),content_length,body,truncated FROM sandbox_artifacts WHERE workflow_id=$1 AND id=$2`, wf, id).Scan(&r.ID, &r.AttemptID, &r.WorkflowID, &r.TaskID, &r.Generation, &r.Kind, &r.MediaType, &r.BaseRevision, &r.SHA256, &r.ContentLength, &r.Body, &r.Truncated)
	return r, classify(err)
}
func (s *Store) SandboxCleanup(ctx context.Context, worker string, instance uuid.UUID) ([]execution.Attempt, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, classify(err)
	}
	defer rollback(tx)
	if _, err = lockWorker(ctx, tx, worker, instance); err != nil {
		return nil, err
	}
	rows, err := tx.Query(ctx, `SELECT a.id FROM task_attempts a JOIN sandbox_commands c ON c.attempt_id=a.id WHERE a.worker_id=$1 AND a.cleanup_required AND (a.worker_instance_id<>$2 OR a.state<>'RUNNING') ORDER BY a.started_at LIMIT 100`, worker, instance)
	if err != nil {
		return nil, classify(err)
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return nil, classify(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, classify(err)
	}
	items := []execution.Attempt{}
	for _, id := range ids {
		a, e := readAttempt(ctx, tx, id)
		if e != nil {
			return nil, e
		}
		items = append(items, a)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, classify(err)
	}
	return items, nil
}

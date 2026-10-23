package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/operator"
	"forgeswarm/internal/publication"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"time"
)

// Delivery identity and preparation/project admission commit together. Preparation
// performs only bounded remote reads before this transaction, never Git commands.
func (s *Store) RequiresOperator(ctx context.Context) (bool, error) {
	var required bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM github_imports)`).Scan(&required)
	return required, classify(err)
}
func (s *Store) Imported(ctx context.Context, delivery, digest string) (workflow.Receipt, error) {
	var r workflow.Receipt
	var old string
	err := s.pool.QueryRow(ctx, `SELECT i.root_id,w.created_at,i.payload_sha256 FROM github_imports i JOIN workflows w ON w.id=i.root_id WHERE delivery_id=$1`, delivery).Scan(&r.ID, &r.CreatedAt, &old)
	if err == nil && old != digest {
		return r, workflow.ErrConflict
	}
	return r, classify(err)
}
func (s *Store) ImportGitHub(ctx context.Context, delivery, digest, actor string, p publication.Prepared) (workflow.Receipt, error) {
	if len(delivery) < 1 || len(delivery) > 128 || !agent.ValidDigest(digest) || len(actor) < 1 || len(actor) > 100 || !p.Valid() {
		return workflow.Receipt{}, workflow.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return workflow.Receipt{}, classify(err)
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,81))`, delivery); err != nil {
		return workflow.Receipt{}, classify(err)
	}
	var r workflow.Receipt
	var old string
	err = tx.QueryRow(ctx, `SELECT i.root_id,w.created_at,i.payload_sha256 FROM github_imports i JOIN workflows w ON w.id=i.root_id WHERE delivery_id=$1`, delivery).Scan(&r.ID, &r.CreatedAt, &old)
	if err == nil {
		if old != digest {
			return r, workflow.ErrConflict
		}
		return r, classify(tx.Commit(ctx))
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return r, classify(err)
	}
	job := agent.Job{SchemaVersion: 1, RepositoryID: agent.SnapshotDigest(p.Snapshot), BaseRevision: p.Snapshot.BaseRevision, TaskDescription: p.Description, Policy: "controlled-go-v1", Budget: "small-v1", TestProfile: agent.MathProfile}
	r, err = s.createProjectTx(ctx, tx, "github:"+delivery, job, p.Snapshot, 2)
	if err != nil {
		return r, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO github_imports(root_id,delivery_id,payload_sha256,actor,prepared) VALUES($1,$2,$3,$4,$5)`, r.ID, delivery, digest, actor, p)
	if err != nil {
		return r, classify(err)
	}
	return r, classify(tx.Commit(ctx))
}

// Reconstruct from immutable accepted records and actual artifact bodies. No
// mutable "latest patch", issue instructions or user-supplied passed flag enters.
func bindingTx(ctx context.Context, tx pgx.Tx, root uuid.UUID, version int) (publication.Binding, publication.Operation, error) {
	var p publication.Prepared
	var b publication.Binding
	var op publication.Operation
	err := tx.QueryRow(ctx, `SELECT prepared FROM github_imports WHERE root_id=$1`, root).Scan(&p)
	if err != nil {
		return b, op, classify(err)
	}
	if !p.Valid() {
		return b, op, workflow.ErrState
	}
	b = publication.Binding{Root: root, PlanVersion: version, Repository: p.Repository, BaseCommit: p.Snapshot.BaseRevision, BaseTree: p.BaseTree, Action: publication.Action, Policy: p.Policy, TestDemo: p.TestDemo}
	err = tx.QueryRow(ctx, `SELECT e.child_id,e.plan_digest FROM collab_executions e JOIN workflows w ON w.id=e.child_id WHERE e.root_id=$1 AND e.version=$2 AND w.status='SUCCEEDED'`, root, version).Scan(&b.Workflow, &b.PlanDigest)
	if err != nil {
		return b, op, classify(err)
	}
	var integrated, reviewed agent.RoleOutcome
	var integrationID uuid.UUID
	var integrationSHA, reviewSHA, candidate, test string
	err = tx.QueryRow(ctx, `SELECT i.attempt_id,i.body,i.artifact_sha256,i.candidate_sha256,i.test_sha256,r.body,r.artifact_sha256 FROM collab_results i JOIN task_attempts a ON a.id=i.attempt_id JOIN tasks it ON it.id=a.task_id JOIN collab_results r ON r.role='reviewer' AND r.accepted AND r.candidate_sha256=i.candidate_sha256 JOIN task_attempts ra ON ra.id=r.attempt_id JOIN tasks rt ON rt.id=ra.task_id WHERE a.workflow_id=$1 AND ra.workflow_id=$1 AND i.role='integration' AND i.accepted AND a.state='SUCCEEDED' AND ra.state='SUCCEEDED' AND it.current_attempt_id=a.id AND rt.current_attempt_id=ra.id`, b.Workflow).Scan(&integrationID, &integrated, &integrationSHA, &candidate, &test, &reviewed, &reviewSHA)
	if err != nil {
		return b, op, classify(err)
	}
	if agent.JSONSHA(integrated) != integrationSHA || agent.JSONSHA(reviewed) != reviewSHA || agent.SnapshotDigest(integrated.Snapshot) != candidate || agent.SnapshotDigest(reviewed.Snapshot) != candidate || reviewed.Review == nil || !reviewed.Review.Valid(candidate) || reviewed.Review.Decision != "pass" || !agent.ValidDigest(test) || agent.SnapshotDigest(integrated.Work.Origin) != agent.SnapshotDigest(p.Snapshot) {
		return b, op, workflow.ErrState
	}
	b.Candidate = candidate
	b.CandidateTree = publication.TreeSHA(integrated.Snapshot)
	b.Review = reviewSHA
	paths := []string{}
	for n, body := range p.Snapshot.Files {
		changed := !bytes.Equal(body, integrated.Snapshot.Files[n])
		if changed {
			paths = append(paths, n)
		}
	}
	// Final candidates must still be within the original scoped preparation.
	if !agent.ScopedCandidate(p.Snapshot, integrated.Snapshot, agent.MathFiles) || len(paths) == 0 {
		return b, op, workflow.ErrState
	}
	b.Flags = publication.Flags(paths)
	var patch, proof []byte
	var patchHash, proofHash string
	var truncated bool
	for _, kind := range []string{"patch", "test_result"} {
		var body []byte
		var hash string
		err = tx.QueryRow(ctx, `SELECT body,encode(sha256,'hex'),truncated FROM sandbox_artifacts WHERE attempt_id=$1 AND kind=$2 AND base_revision=$3`, integrationID, kind, b.BaseCommit).Scan(&body, &hash, &truncated)
		if err != nil {
			return b, op, classify(err)
		}
		if truncated || agent.SHA(body) != hash {
			return b, op, workflow.ErrState
		}
		if kind == "patch" {
			patch, patchHash = body, hash
		} else {
			proof, proofHash = body, hash
		}
	}
	if !bytes.Equal(patch, agent.MathPatch(p.Snapshot, integrated.Snapshot)) {
		return b, op, workflow.ErrState
	}
	var testProof struct {
		Candidate string `json:"candidate_sha256"`
		Base      string `json:"base_revision"`
		Profile   string `json:"profile"`
		Success   bool   `json:"success"`
		Authority string `json:"authority"`
		Cases     int    `json:"cases"`
		Passed    int    `json:"passed"`
		Schema    int    `json:"schema_version"`
	}
	if json.Unmarshal(proof, &testProof) != nil || !testProof.Success || testProof.Profile != agent.MathProfile || testProof.Authority != "immutable-helper" || testProof.Candidate != candidate || testProof.Base != b.BaseCommit || testProof.Cases != 6 || testProof.Passed != 6 {
		return b, op, workflow.ErrState
	}
	b.Patch = patchHash
	b.Test = proofHash
	op.Binding = b
	op.Snapshot = integrated.Snapshot
	return b, op, nil
}
func (s *Store) requestApprovalTx(ctx context.Context, tx pgx.Tx, root uuid.UUID, version int) (bool, error) {
	var imported bool
	if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM github_imports WHERE root_id=$1)`, root).Scan(&imported); err != nil {
		return false, classify(err)
	}
	if !imported {
		return false, nil
	}
	b, _, err := bindingTx(ctx, tx, root, version)
	if err != nil {
		return false, err
	}
	ttl := s.approvalTTL
	if ttl == 0 {
		ttl = 24 * time.Hour
	}
	_, err = tx.Exec(ctx, `INSERT INTO approvals(id,root_id,plan_version,binding,binding_sha256,status,expires_at) VALUES($1,$2,$3,$4,$5,'PENDING',clock_timestamp()+($6::bigint*interval '1 millisecond')) ON CONFLICT(root_id,plan_version) DO NOTHING`, uuid.New(), root, version, b, b.Digest(), ttl.Milliseconds())
	return true, classify(err)
}

// Startup-only expiry policy; the request pins the resulting DB-clock deadline.
func (s *Store) ConfigureApprovalTTL(ttl time.Duration) error {
	if ttl < time.Second || ttl > 24*time.Hour {
		return workflow.ErrInvalid
	}
	s.approvalTTL = ttl
	return nil
}

type Approval struct {
	ID        uuid.UUID              `json:"id"`
	Root      uuid.UUID              `json:"root_id"`
	Version   int                    `json:"version"`
	Binding   publication.Binding    `json:"binding"`
	Digest    string                 `json:"binding_sha256"`
	Status    string                 `json:"status"`
	Expires   time.Time              `json:"expires_at"`
	Actor     string                 `json:"actor"`
	Decided   *time.Time             `json:"decided_at"`
	Operation *publication.Operation `json:"operation,omitempty"`
}

const approvalColumns = `id,root_id,version,binding,binding_sha256,status,expires_at,actor,decided_at`

func scanApproval(row pgx.Row) (Approval, error) {
	var a Approval
	err := row.Scan(&a.ID, &a.Root, &a.Version, &a.Binding, &a.Digest, &a.Status, &a.Expires, &a.Actor, &a.Decided)
	if err == nil && a.Binding.Digest() != a.Digest {
		return a, workflow.ErrState
	}
	return a, classify(err)
}
func (s *Store) Approval(ctx context.Context, id uuid.UUID) (Approval, error) {
	a, err := scanApproval(s.pool.QueryRow(ctx, `SELECT `+approvalColumns+` FROM approvals WHERE id=$1`, id))
	if err != nil {
		return a, err
	}
	op, err := s.operation(ctx, id, true)
	if errors.Is(err, workflow.ErrNotFound) {
		return a, nil
	}
	if err == nil {
		a.Operation = &op
	}
	return a, err
}
func (s *Store) Approvals(ctx context.Context, root, after uuid.UUID) ([]Approval, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+approvalColumns+` FROM approvals WHERE ($1::uuid='00000000-0000-0000-0000-000000000000' OR root_id=$1) AND id>$2 ORDER BY id LIMIT 20`, root, after)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	items := []Approval{}
	for rows.Next() {
		a, e := scanApproval(rows)
		if e != nil {
			return nil, e
		}
		items = append(items, a)
	}
	if err = rows.Err(); err != nil {
		return nil, classify(err)
	}
	for i := range items {
		op, e := s.operation(ctx, items[i].ID, true)
		if e == nil {
			items[i].Operation = &op
		} else if !errors.Is(e, workflow.ErrNotFound) {
			return nil, e
		}
	}
	return items, nil
}
func (s *Store) DecideApproval(ctx context.Context, id uuid.UUID, actor operator.Principal, d publication.Decision) (Approval, error) {
	if !actor.IsOperator() {
		return Approval{}, workflow.ErrInvalid
	}
	if d.Decision != "approve" && d.Decision != "reject" {
		return Approval{}, workflow.ErrInvalid
	}
	var root uuid.UUID
	if err := s.pool.QueryRow(ctx, `SELECT root_id FROM approvals WHERE id=$1`, id).Scan(&root); err != nil {
		return Approval{}, classify(err)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return Approval{}, classify(err)
	}
	defer rollback(tx)
	parent, err := lockParent(ctx, tx, root)
	if err != nil {
		return Approval{}, err
	}
	var state string
	if err = tx.QueryRow(ctx, `SELECT status FROM collab_projects WHERE root_id=$1 FOR UPDATE`, root).Scan(&state); err != nil {
		return Approval{}, classify(err)
	}
	a, err := scanApproval(tx.QueryRow(ctx, `SELECT `+approvalColumns+` FROM approvals WHERE id=$1 FOR UPDATE`, id))
	if err != nil {
		return a, err
	}
	if d.ExpectedVersion != a.Version || d.Binding != a.Digest || d.Candidate != a.Binding.Candidate || len(a.Binding.Flags) > 0 && !d.AcknowledgeSensitive {
		return a, workflow.ErrConflict
	}
	want := "APPROVED"
	if d.Decision == "reject" {
		want = "REJECTED"
	}
	if a.Status == want && a.Actor == actor.Actor {
		if err = tx.Commit(ctx); err != nil {
			return a, classify(err)
		}
		return s.Approval(ctx, id)
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return a, err
	}
	if a.Status != "PENDING" || state != "WAITING_FOR_APPROVAL" || !now.Before(a.Expires) {
		return a, workflow.ErrConflict
	}
	current, _, err := bindingTx(ctx, tx, root, a.Binding.PlanVersion)
	if err != nil {
		return a, err
	}
	if current.Digest() != a.Digest {
		return a, workflow.ErrConflict
	}
	_, err = tx.Exec(ctx, `UPDATE approvals SET status=$2,actor=$3,decided_at=clock_timestamp() WHERE id=$1`, id, want, actor.Actor)
	if err != nil {
		return a, classify(err)
	}
	next := "NEEDS_HUMAN"
	reason := "approval_rejected"
	if want == "APPROVED" {
		next = "PUBLISHING"
		reason = "approved_exact_candidate"
		opID := uuid.NewSHA1(id, []byte("github-publication-v1"))
		_, err = tx.Exec(ctx, `INSERT INTO external_operations(id,approval_id,root_id,binding_sha256,branch,status) VALUES($1,$2,$3,$4,$5,'prepared') ON CONFLICT(approval_id) DO NOTHING`, opID, id, root, a.Digest, "forgeswarm/op-"+opID.String())
		if err != nil {
			return a, classify(err)
		}
	}
	_, err = tx.Exec(ctx, `UPDATE collab_projects SET status=$2,reason=$3 WHERE root_id=$1`, root, next, reason)
	if err == nil {
		err = mutation(ctx, tx, parent, parent.status, []eventWrite{{"project.state", map[string]any{"status": next, "reason": reason, "approval_id": id, "actor": actor.Actor}}})
	}
	if err != nil {
		return a, classify(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return a, classify(err)
	}
	return s.Approval(ctx, id)
}
func (s *Store) expireApprovals(ctx context.Context, batch int) error {
	rows, err := s.pool.Query(ctx, `SELECT id,root_id FROM approvals WHERE status IN ('PENDING','APPROVED') AND expires_at<=clock_timestamp() ORDER BY expires_at LIMIT $1`, batch)
	if err != nil {
		return classify(err)
	}
	type item struct{ id, root uuid.UUID }
	items := []item{}
	for rows.Next() {
		var i item
		if err = rows.Scan(&i.id, &i.root); err != nil {
			rows.Close()
			return classify(err)
		}
		items = append(items, i)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return classify(err)
	}
	for _, i := range items {
		err = func() error {
			tx, e := s.pool.Begin(ctx)
			if e != nil {
				return classify(e)
			}
			defer rollback(tx)
			parent, e := lockParent(ctx, tx, i.root)
			if e != nil {
				return e
			}
			var state string
			if e = tx.QueryRow(ctx, `SELECT status FROM collab_projects WHERE root_id=$1 FOR UPDATE`, i.root).Scan(&state); e != nil {
				return classify(e)
			}
			result, e := tx.Exec(ctx, `UPDATE approvals SET status='EXPIRED' WHERE id=$1 AND status IN ('PENDING','APPROVED') AND expires_at<=clock_timestamp()`, i.id)
			if e != nil {
				return classify(e)
			}
			if result.RowsAffected() > 0 && (state == "WAITING_FOR_APPROVAL" || state == "PUBLISHING") {
				if _, e = tx.Exec(ctx, `UPDATE collab_projects SET status='NEEDS_HUMAN',reason='approval_expired' WHERE root_id=$1`, i.root); e != nil {
					return classify(e)
				}
				if e = mutation(ctx, tx, parent, parent.status, []eventWrite{{"project.state", map[string]string{"status": "NEEDS_HUMAN", "reason": "approval_expired"}}}); e != nil {
					return e
				}
			}
			return classify(tx.Commit(ctx))
		}()
		if err != nil {
			return err
		}
	}
	return nil
}

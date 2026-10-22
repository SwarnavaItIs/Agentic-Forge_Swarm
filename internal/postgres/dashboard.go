package postgres

import (
	"context"
	"encoding/json"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"time"
)

type WorkflowRow struct {
	ID         uuid.UUID  `json:"id"`
	Name       string     `json:"name"`
	Status     string     `json:"status"`
	CreatedAt  time.Time  `json:"created_at"`
	Repository string     `json:"repository_id"`
	Root       *uuid.UUID `json:"root_id,omitempty"`
}
type WorkflowPage struct {
	Items     []WorkflowRow `json:"items"`
	NextAfter string        `json:"next_after"`
}

func (s *Store) ListWorkflows(ctx context.Context, after uuid.UUID, limit int) (WorkflowPage, error) {
	if limit < 1 || limit > 25 {
		return WorkflowPage{}, workflow.ErrInvalid
	}
	rows, err := s.pool.Query(ctx, `SELECT w.id,w.name,COALESCE(p.status,w.status),w.created_at,COALESCE((SELECT agent_input->>'repository_id' FROM tasks WHERE workflow_id=w.id AND agent_input IS NOT NULL ORDER BY position LIMIT 1),''),f.root_id FROM workflows w LEFT JOIN collab_projects p ON p.root_id=w.id LEFT JOIN collab_workflows f ON f.workflow_id=w.id WHERE w.id>$1 ORDER BY w.id LIMIT $2`, after, limit)
	if err != nil {
		return WorkflowPage{}, classify(err)
	}
	defer rows.Close()
	page := WorkflowPage{Items: []WorkflowRow{}, NextAfter: after.String()}
	for rows.Next() {
		var v WorkflowRow
		if err = rows.Scan(&v.ID, &v.Name, &v.Status, &v.CreatedAt, &v.Repository, &v.Root); err != nil {
			return page, classify(err)
		}
		page.Items = append(page.Items, v)
		page.NextAfter = v.ID.String()
	}
	return page, classify(rows.Err())
}

type EventBounds struct {
	First int64 `json:"first"`
	Last  int64 `json:"last"`
}

func (s *Store) EventBounds(ctx context.Context, id uuid.UUID) (EventBounds, error) {
	var v EventBounds
	err := s.pool.QueryRow(ctx, `SELECT COALESCE((SELECT min(sequence) FROM workflow_events WHERE workflow_id=w.id),w.last_event_sequence+1),w.last_event_sequence FROM workflows w WHERE w.id=$1`, id).Scan(&v.First, &v.Last)
	return v, classify(err)
}

type UsageSummary struct {
	Root              uuid.UUID   `json:"root_id"`
	Calls             int         `json:"calls"`
	Tokens            int         `json:"tokens"`
	EstimatedMicrousd int64       `json:"estimated_microusd"`
	MaximumMicrousd   *int64      `json:"maximum_microusd"`
	UnknownUsageCalls int         `json:"unknown_usage_calls"`
	EstimateOnly      bool        `json:"estimate_only"`
	Bounds            EventBounds `json:"event_bounds"`
}

func (s *Store) Usage(ctx context.Context, id uuid.UUID) (UsageSummary, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return UsageSummary{}, classify(err)
	}
	defer rollback(tx)
	v := UsageSummary{Root: id, EstimateOnly: true}
	err = tx.QueryRow(ctx, `SELECT COALESCE(f.root_id,w.id),p.maximum_microusd,COALESCE((SELECT min(sequence) FROM workflow_events WHERE workflow_id=w.id),w.last_event_sequence+1),w.last_event_sequence FROM workflows w LEFT JOIN collab_workflows f ON f.workflow_id=w.id LEFT JOIN collab_projects p ON p.root_id=f.root_id WHERE w.id=$1`, id).Scan(&v.Root, &v.MaximumMicrousd, &v.Bounds.First, &v.Bounds.Last)
	if err != nil {
		return v, classify(err)
	}
	err = tx.QueryRow(ctx, `SELECT COALESCE(sum(s.turns),0),COALESCE(sum(s.tokens),0),COALESCE(sum(s.estimated_microusd),0) FROM agent_sessions s JOIN task_attempts a ON a.id=s.attempt_id LEFT JOIN collab_workflows f ON f.workflow_id=a.workflow_id WHERE a.workflow_id=$1 OR f.root_id=$1`, v.Root).Scan(&v.Calls, &v.Tokens, &v.EstimatedMicrousd)
	if err != nil {
		return v, classify(err)
	}
	err = tx.QueryRow(ctx, `SELECT count(*) FROM model_calls b JOIN task_attempts a ON a.id=b.attempt_id LEFT JOIN collab_workflows f ON f.workflow_id=a.workflow_id LEFT JOIN model_calls e ON e.attempt_id=b.attempt_id AND e.request_id=b.request_id AND e.phase='end' WHERE b.phase='begin' AND (a.workflow_id=$1 OR f.root_id=$1) AND (e.request_id IS NULL OR e.metadata->>'usage'='unknown_retained_reservation')`, v.Root).Scan(&v.UnknownUsageCalls)
	if err != nil {
		return v, classify(err)
	}
	return v, classify(tx.Commit(ctx))
}

type CandidateEvidence struct {
	Attempt  uuid.UUID       `json:"attempt_id"`
	Role     string          `json:"role"`
	Accepted bool            `json:"accepted"`
	Digest   string          `json:"candidate_sha256"`
	Artifact string          `json:"artifact_sha256"`
	Test     string          `json:"test_sha256"`
	Body     json.RawMessage `json:"body"`
}

func (s *Store) Candidates(ctx context.Context, id uuid.UUID) ([]CandidateEvidence, error) {
	// At most one accepted terminal result per role task, with bounded attempts.
	rows, err := s.pool.Query(ctx, `SELECT r.attempt_id,r.role,r.accepted,r.candidate_sha256,r.artifact_sha256,r.test_sha256,r.body FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id WHERE a.workflow_id=$1 ORDER BY r.recorded_at,r.attempt_id LIMIT 12`, id)
	if err != nil {
		return nil, classify(err)
	}
	defer rows.Close()
	v := []CandidateEvidence{}
	for rows.Next() {
		var item CandidateEvidence
		if err = rows.Scan(&item.Attempt, &item.Role, &item.Accepted, &item.Digest, &item.Artifact, &item.Test, &item.Body); err != nil {
			return nil, classify(err)
		}
		var body agent.RoleOutcome
		if json.Unmarshal(item.Body, &body) != nil || agent.JSONSHA(body) != item.Artifact || agent.SnapshotDigest(body.Snapshot) != item.Digest {
			return nil, workflow.ErrState
		}
		v = append(v, item)
	}
	return v, classify(rows.Err())
}

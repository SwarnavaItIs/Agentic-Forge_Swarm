package postgres

import (
	"context"
	"errors"
	"fmt"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"time"
)

// Recovery scans durable versions/results. No in-memory plan, leader, mutable
// checkout, task reset, or edge insertion is part of correction/finalization.
func (s *Store) ReconcileCollaboration(ctx context.Context, batch int) error {
	if batch < 1 || batch > 100 {
		return workflow.ErrInvalid
	}
	rows, err := s.pool.Query(ctx, `SELECT root_id FROM collab_projects WHERE status IN ('PLANNING','EXECUTING','CANCELLING') ORDER BY created_at,root_id LIMIT $1`, batch)
	if err != nil {
		return classify(err)
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return classify(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return classify(err)
	}
	for _, id := range ids {
		if err = s.reconcileProject(ctx, id); err != nil {
			return err
		}
	}
	return s.expireApprovals(ctx, batch)
}
func (s *Store) reconcileProject(ctx context.Context, root uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return classify(err)
	}
	defer rollback(tx)
	if err = admission(ctx, tx); err != nil {
		return err
	}
	parent, err := lockParent(ctx, tx, root)
	if err != nil {
		return err
	}
	var job agent.Job
	var state string
	var round, maximum int
	var deadline time.Time
	err = tx.QueryRow(ctx, `SELECT job,status,round,max_rounds,deadline_at FROM collab_projects WHERE root_id=$1 FOR UPDATE`, root).Scan(&job, &state, &round, &maximum, &deadline)
	if err != nil {
		return classify(err)
	}
	next, reason := state, ""
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if state == "CANCELLING" || !now.Before(deadline) {
		if state != "CANCELLING" {
			next = "CANCELLING"
			reason = "workflow_budget_deadline"
		}
		var settled bool
		err = tx.QueryRow(ctx, `SELECT NOT EXISTS(SELECT 1 FROM collab_workflows f JOIN workflows w ON w.id=f.workflow_id WHERE f.root_id=$1 AND w.status IN ('PENDING','RUNNING','CANCELLING')) AND NOT EXISTS(SELECT 1 FROM collab_workflows f JOIN task_attempts a ON a.workflow_id=f.workflow_id WHERE f.root_id=$1 AND a.cleanup_required)`, root).Scan(&settled)
		if err != nil {
			return classify(err)
		}
		if settled {
			next = "CANCELLED"
		}
	} else if state == "PLANNING" {
		var digest string
		err = tx.QueryRow(ctx, `SELECT digest FROM collab_plans WHERE root_id=$1 AND version=$2`, root, round+1).Scan(&digest)
		if err == nil {
			if err = tx.Commit(ctx); err != nil {
				return classify(err)
			}
			_, err = s.FinalizePlan(ctx, root, round+1, digest)
			if err != nil {
				// An inadmissible plan cannot keep the parent spinning forever.
				if errors.Is(err, workflow.ErrUnavailable) {
					return err
				}
				_, e := s.pool.Exec(ctx, `UPDATE collab_projects SET status='NEEDS_HUMAN',reason='plan_budget_or_finalization_rejected' WHERE root_id=$1 AND status='PLANNING'`, root)
				return classify(e)
			}
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return classify(err)
		}
		var planningState string
		err = tx.QueryRow(ctx, `SELECT w.status FROM collab_workflows f JOIN workflows w ON w.id=f.workflow_id WHERE f.root_id=$1 AND f.version=$2 AND f.phase='planning'`, root, round+1).Scan(&planningState)
		if err != nil {
			return classify(err)
		}
		if planningState == workflow.Failed || planningState == workflow.Cancelled {
			next = "FAILED"
			reason = "planner_failed_or_budget_exhausted"
		}
	} else if state == "EXECUTING" {
		var child uuid.UUID
		var childState string
		err = tx.QueryRow(ctx, `SELECT e.child_id,w.status FROM collab_executions e JOIN workflows w ON w.id=e.child_id WHERE e.root_id=$1 AND e.version=$2`, root, round+1).Scan(&child, &childState)
		if err != nil {
			return classify(err)
		}
		if childState == workflow.Failed || childState == workflow.Cancelled {
			next = "NEEDS_HUMAN"
			reason = "integration_conflict_test_failure_or_execution_failure"
			var detail string
			err = tx.QueryRow(ctx, `SELECT r.body->>'reason' FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id WHERE a.workflow_id=$1 AND NOT r.accepted AND COALESCE(r.body->>'reason','')<>'' ORDER BY r.recorded_at LIMIT 1`, child).Scan(&detail)
			if err == nil {
				reason = detail
			} else if !errors.Is(err, pgx.ErrNoRows) {
				return classify(err)
			}
		}
		if childState == workflow.Succeeded {
			var reviewed agent.RoleOutcome
			err = tx.QueryRow(ctx, `SELECT r.body FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id WHERE a.workflow_id=$1 AND r.role='reviewer' AND r.accepted AND a.state='SUCCEEDED'`, child).Scan(&reviewed)
			if err != nil {
				return classify(err)
			}
			if reviewed.Review == nil || !reviewed.Review.Valid(agent.SnapshotDigest(reviewed.Snapshot)) {
				return workflow.ErrState
			}
			if reviewed.Review.Decision == "pass" {
				waiting, e := s.requestApprovalTx(ctx, tx, root, round+1)
				if e != nil {
					return e
				}
				next = "SUCCEEDED"
				if waiting {
					next = "WAITING_FOR_APPROVAL"
				}
			} else if round >= maximum {
				next = "NEEDS_HUMAN"
				reason = "correction_rounds_exhausted"
			} else {
				// A fresh immutable preparation/child family starts from the rejected,
				// still-preserved integrated candidate. All earlier artifacts stay intact.
				version := round + 2
				role := agent.RoleSpec{SchemaVersion: 1, Name: "planner", Version: version, TaskKey: "plan", Scope: []string{}, Profile: agent.MathProfile, MaxCalls: 4}
				receipt, e := s.createTx(ctx, tx, fmt.Sprintf("collab:%s:v%d:plan", root, version), workflow.CreateInput{Name: fmt.Sprintf("Correction planning v%d", version), TimeoutMS: max(100, deadline.Sub(now).Milliseconds()), Tasks: []workflow.TaskDefinition{roleTask(job, role, job.TaskDescription, nil)}})
				if e != nil {
					return e
				}
				_, err = tx.Exec(ctx, `INSERT INTO collab_workflows(workflow_id,root_id,version,phase,parent_version,base_snapshot,base_sha256) VALUES($1,$2,$3,'planning',$4,$5,$6)`, receipt.ID, root, version, version-1, reviewed.Snapshot, agent.SnapshotDigest(reviewed.Snapshot))
				if err != nil {
					return classify(err)
				}
				round++
				next = "PLANNING"
				reason = "review_rejected_new_immutable_version"
			}
		}
	}
	if next != state {
		_, err = tx.Exec(ctx, `UPDATE collab_projects SET status=$2,reason=$3,round=$4 WHERE root_id=$1`, root, next, reason, round)
		if err == nil {
			err = mutation(ctx, tx, parent, parent.status, []eventWrite{{"project.state", map[string]any{"status": next, "reason": reason, "round": round}}})
		}
		if err != nil {
			return classify(err)
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return classify(err)
	}
	if next == "CANCELLING" {
		return s.cancelProjectChildren(ctx, root)
	}
	return nil
}
func (s *Store) cancelProjectChildren(ctx context.Context, root uuid.UUID) error {
	rows, err := s.pool.Query(ctx, `SELECT workflow_id FROM collab_workflows WHERE root_id=$1 ORDER BY version,phase`, root)
	if err != nil {
		return classify(err)
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return classify(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return classify(err)
	}
	for _, id := range ids {
		if _, err = s.cancelWorkflow(ctx, id); err != nil {
			return err
		}
	}
	return nil
}
func (s *Store) cancelProject(ctx context.Context, root uuid.UUID) (workflow.CancellationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	defer rollback(tx)
	p, err := lockParent(ctx, tx, root)
	if err != nil {
		return workflow.CancellationResult{}, err
	}
	var state string
	err = tx.QueryRow(ctx, `SELECT status FROM collab_projects WHERE root_id=$1 FOR UPDATE`, root).Scan(&state)
	if err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	if state == "PLANNING" || state == "EXECUTING" || state == "WAITING_FOR_APPROVAL" || state == "PUBLISHING" {
		state = "CANCELLING"
		_, err = tx.Exec(ctx, `UPDATE collab_projects SET status='CANCELLING',reason='explicit_cancel' WHERE root_id=$1`, root)
		if err == nil {
			_, err = tx.Exec(ctx, `UPDATE approvals SET status='CANCELLED' WHERE root_id=$1 AND status IN ('PENDING','APPROVED')`, root)
		}
		if err == nil {
			err = mutation(ctx, tx, p, p.status, []eventWrite{{"project.state", map[string]string{"status": state, "reason": "explicit_cancel"}}})
			p.version++
		}
	}
	if err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	if state == "CANCELLING" {
		err = s.cancelProjectChildren(ctx, root)
	}
	return workflow.CancellationResult{ID: root, Status: state, Version: p.version}, err
}

// Kept here for read-side provenance helpers without exposing DB internals.
type ProjectVersion struct {
	Version  int       `json:"version"`
	Phase    string    `json:"phase"`
	Workflow uuid.UUID `json:"workflow_id"`
	Status   string    `json:"status"`
	Base     string    `json:"base_sha256"`
}
type ProjectPlan struct {
	Version int        `json:"version"`
	Digest  string     `json:"digest"`
	Plan    agent.Plan `json:"plan"`
	Planner uuid.UUID  `json:"planner_attempt"`
}
type ProjectDetails struct {
	ID        uuid.UUID        `json:"id"`
	Status    string           `json:"status"`
	Reason    string           `json:"reason"`
	Round     int              `json:"round"`
	MaxRounds int              `json:"max_rounds"`
	Job       agent.Job        `json:"job"`
	Versions  []ProjectVersion `json:"versions"`
	Plans     []ProjectPlan    `json:"plans"`
}

func (s *Store) Project(ctx context.Context, root uuid.UUID) (ProjectDetails, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return ProjectDetails{}, classify(err)
	}
	defer rollback(tx)
	var p ProjectDetails
	err = tx.QueryRow(ctx, `SELECT root_id,status,reason,round,max_rounds,job FROM collab_projects WHERE root_id=$1`, root).Scan(&p.ID, &p.Status, &p.Reason, &p.Round, &p.MaxRounds, &p.Job)
	if err != nil {
		return p, classify(err)
	}
	p.Versions = []ProjectVersion{}
	p.Plans = []ProjectPlan{}
	rows, err := tx.Query(ctx, `SELECT f.version,f.phase,f.workflow_id,w.status,f.base_sha256 FROM collab_workflows f JOIN workflows w ON w.id=f.workflow_id WHERE f.root_id=$1 ORDER BY f.version,f.phase`, root)
	if err != nil {
		return p, classify(err)
	}
	for rows.Next() {
		var v ProjectVersion
		if err = rows.Scan(&v.Version, &v.Phase, &v.Workflow, &v.Status, &v.Base); err != nil {
			rows.Close()
			return p, classify(err)
		}
		p.Versions = append(p.Versions, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, classify(err)
	}
	rows, err = tx.Query(ctx, `SELECT version,digest,body,planner_attempt FROM collab_plans WHERE root_id=$1 ORDER BY version`, root)
	if err != nil {
		return p, classify(err)
	}
	for rows.Next() {
		var v ProjectPlan
		if err = rows.Scan(&v.Version, &v.Digest, &v.Plan, &v.Planner); err != nil {
			rows.Close()
			return p, classify(err)
		}
		p.Plans = append(p.Plans, v)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return p, classify(err)
	}
	return p, classify(tx.Commit(ctx))
}

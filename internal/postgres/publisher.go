package postgres

import (
	"context"
	"errors"
	"forgeswarm/internal/github"
	"forgeswarm/internal/publication"
	"forgeswarm/internal/telemetry"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"time"
)

func (s *Store) operation(ctx context.Context, id uuid.UUID, byApproval bool) (publication.Operation, error) {
	var op publication.Operation
	column := "o.id"
	if byApproval {
		column = "o.approval_id"
	}
	err := s.pool.QueryRow(ctx, `SELECT o.id,o.approval_id,a.binding,o.created_at,o.status,o.attempts,o.branch,o.head_commit,o.pr_number,o.pr_url,o.reason,o.next_attempt_at FROM external_operations o JOIN approvals a ON a.id=o.approval_id WHERE `+column+`=$1`, id).Scan(&op.ID, &op.Approval, &op.Binding, &op.CreatedAt, &op.Status, &op.Attempts, &op.Branch, &op.Head, &op.PRNumber, &op.URL, &op.Reason, &op.NextAttempt)
	return op, classify(err)
}

// Approval/cancellation and exact provenance are checked in a short transaction
// immediately before every external write. No row lock spans an HTTP call.
func (s *Store) checkPublication(ctx context.Context, id uuid.UUID, client *github.Client) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return classify(err)
	}
	defer rollback(tx)
	var root uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT root_id FROM external_operations WHERE id=$1`, id).Scan(&root); err != nil {
		return classify(err)
	}
	if _, err = lockParent(ctx, tx, root); err != nil {
		return err
	}
	var state string
	if err = tx.QueryRow(ctx, `SELECT status FROM collab_projects WHERE root_id=$1 FOR UPDATE`, root).Scan(&state); err != nil {
		return classify(err)
	}
	var a Approval
	a, err = scanApproval(tx.QueryRow(ctx, `SELECT `+approvalColumns+` FROM approvals WHERE id=(SELECT approval_id FROM external_operations WHERE id=$1) FOR UPDATE`, id))
	if err != nil {
		return err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if a.Status != "APPROVED" || state != "PUBLISHING" || !now.Before(a.Expires) || a.Binding.Repository != client.Repository() || a.Binding.Policy != client.Policy() || a.Binding.TestDemo != client.TestDemo() {
		return workflow.ErrConflict
	}
	b, _, err := bindingTx(ctx, tx, root, a.Binding.PlanVersion)
	if err != nil {
		return err
	}
	var matches bool
	if err = tx.QueryRow(ctx, `SELECT binding_sha256=$2 AND status='in_flight' FROM external_operations WHERE id=$1`, id, a.Digest).Scan(&matches); err != nil {
		return classify(err)
	}
	if !matches || b.Digest() != a.Digest {
		return workflow.ErrConflict
	}
	return classify(tx.Commit(ctx))
}

// A session advisory lock serializes the logical operation across publishers.
// Losing a session releases the lock; deterministic remote identity and discovery
// remain necessary because a lost HTTP outcome cannot be rolled back by SQL.
func (s *Store) PublishOne(ctx context.Context, client *github.Client, id uuid.UUID, readOnly bool) (progressed bool, err error) {
	ctx, span := telemetry.Start(ctx, "publication.run")
	started := time.Now()
	defer func() { telemetry.End(ctx, span, "publication.run", started, err) }()
	if id == uuid.Nil {
		err := s.pool.QueryRow(ctx, `SELECT id FROM external_operations WHERE status IN ('prepared','in_flight','uncertain') AND next_attempt_at<=clock_timestamp() AND attempts<6 ORDER BY next_attempt_at,id LIMIT 1`).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return false, nil
		}
		if err != nil {
			return false, classify(err)
		}
	}
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return false, classify(err)
	}
	// Dedicated connection must never return to the pool holding a session lock.
	// Hijack+Close also releases the lock if cleanup is interrupted.
	raw := conn.Hijack()
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = raw.Close(cleanup)
	}()
	var locked bool
	err = raw.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,82))`, id.String()).Scan(&locked)
	if err != nil {
		return false, classify(err)
	}
	if !locked {
		return false, nil
	}
	op, err := s.operation(ctx, id, false)
	if err != nil {
		return false, err
	}
	if op.Status == "confirmed" || op.Status == "failed" || op.Status == "needs_review" || op.Status == "cancelled" {
		return false, nil
	}
	if op.Attempts >= 6 && !readOnly {
		return false, workflow.ErrConflict
	}
	var databaseNow time.Time
	if err = raw.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&databaseNow); err != nil {
		return false, classify(err)
	}
	if !readOnly && databaseNow.Before(op.NextAttempt) {
		return false, nil
	}
	// Reconstruct exact snapshot rather than trusting a branch/head cached in RAM.
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, classify(err)
	}
	b, proved, err := bindingTx(ctx, tx, op.Binding.Root, op.Binding.PlanVersion)
	rollback(tx)
	if err != nil {
		return false, err
	}
	if b.Digest() != op.Binding.Digest() {
		return false, workflow.ErrState
	}
	op.Snapshot = proved.Snapshot
	if !readOnly {
		_, err = s.pool.Exec(ctx, `UPDATE external_operations SET status='in_flight',attempts=attempts+1,updated_at=clock_timestamp() WHERE id=$1 AND attempts<6`, id)
		if err != nil {
			return false, classify(err)
		}
		op.Attempts++
	}
	check := func(call context.Context) error {
		if readOnly {
			return workflow.ErrConflict
		}
		return s.checkPublication(call, id, client)
	}
	// Even when the approval expired/cancelled, first discover any uncertain
	// branch/PR. New writes are denied by check; observed effects are retained.
	result, remoteErr := client.Publish(ctx, op, check)
	status, reason := result.Status, result.Reason
	next := time.Now()
	if remoteErr != nil {
		status = "failed"
		reason = "github_failure"
		var apiErr *github.Error
		if errors.As(remoteErr, &apiErr) {
			reason = apiErr.Error()
			if apiErr.Class == "approval_not_current" {
				status = "cancelled"
			} else if apiErr.Class == "rate_limit" || apiErr.Class == "server" || apiErr.Class == "transport" || apiErr.Uncertain {
				status = "uncertain"
				next = time.Now().Add(time.Duration(1<<min(op.Attempts, 6)) * time.Second)
				if apiErr.RetryAt.After(next) {
					next = apiErr.RetryAt
				}
				if op.Attempts >= 6 {
					status = "manual_reconciliation"
					reason = "retry_budget_exhausted_external_state_unknown"
				}
			} else if apiErr.Class == "repository_conflict" || apiErr.Class == "branch_conflict" || apiErr.Class == "pull_conflict" || apiErr.Class == "ref_conflict" || apiErr.Class == "commit_conflict" || apiErr.Class == "tree_conflict" || apiErr.Class == "intent_conflict" {
				status = "needs_review"
			}
		}
	}
	if readOnly && remoteErr == nil && status == "cancelled" {
		status = "manual_reconciliation"
		reason = "read_only_reconciliation_no_matching_pull"
	}
	// Independent cleanup context records observed effects even if HTTP timed out.
	recordCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	tx, err = s.pool.Begin(recordCtx)
	if err != nil {
		return true, classify(err)
	}
	defer rollback(tx)
	parent, err := lockParent(recordCtx, tx, op.Binding.Root)
	if err != nil {
		return true, err
	}
	var state string
	if err = tx.QueryRow(recordCtx, `SELECT status FROM collab_projects WHERE root_id=$1 FOR UPDATE`, op.Binding.Root).Scan(&state); err != nil {
		return true, classify(err)
	}
	var expired bool
	if err = tx.QueryRow(recordCtx, `SELECT expires_at<=clock_timestamp() FROM approvals WHERE id=$1`, op.Approval).Scan(&expired); err != nil {
		return true, classify(err)
	}
	if expired && status == "confirmed" {
		status = "needs_review"
		reason = "approval_expired_during_external_write"
	}
	if state == "CANCELLING" || state == "CANCELLED" {
		reason = "cancelled_external_effect_may_exist"
	}
	_, err = tx.Exec(recordCtx, `UPDATE external_operations SET status=$2,reason=$3,next_attempt_at=$4,head_commit=CASE WHEN $5<>'' THEN $5 ELSE head_commit END,pr_number=CASE WHEN $6>0 THEN $6 ELSE pr_number END,pr_url=CASE WHEN $7<>'' THEN $7 ELSE pr_url END,updated_at=clock_timestamp() WHERE id=$1`, id, status, reason, next, result.Head, result.PRNumber, result.URL)
	if err != nil {
		return true, classify(err)
	}
	if state == "PUBLISHING" && (status == "confirmed" || status == "failed" || status == "needs_review" || status == "cancelled" || status == "manual_reconciliation") {
		nextState := "NEEDS_HUMAN"
		if status == "confirmed" {
			nextState = "PUBLISHED"
		}
		if _, err = tx.Exec(recordCtx, `UPDATE collab_projects SET status=$2,reason=$3 WHERE root_id=$1`, op.Binding.Root, nextState, reason); err != nil {
			return true, classify(err)
		}
		if err = mutation(recordCtx, tx, parent, parent.status, []eventWrite{{"project.state", map[string]any{"status": nextState, "reason": reason, "operation_id": id, "pr_number": result.PRNumber}}}); err != nil {
			return true, err
		}
	}
	if status == "needs_review" {
		if _, err = tx.Exec(recordCtx, `UPDATE approvals SET status='INVALIDATED' WHERE id=$1 AND status='APPROVED'`, op.Approval); err != nil {
			return true, classify(err)
		}
	}
	return true, classify(tx.Commit(recordCtx))
}

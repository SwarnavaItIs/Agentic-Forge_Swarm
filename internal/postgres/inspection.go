package postgres

import (
	"context"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) InspectTask(ctx context.Context, wf, task uuid.UUID) (execution.TaskInspection, error) {
	var t execution.TaskInspection
	err := s.pool.QueryRow(ctx, `SELECT id::text,workflow_id::text,status,kind,delay_ms,dispatch_generation,attempt_count,max_attempts,current_attempt_id::text,next_attempt_at,deadline_at,last_error_class FROM tasks WHERE workflow_id=$1 AND id=$2`, wf, task).Scan(&t.ID, &t.WorkflowID, &t.Status, &t.Kind, &t.DelayMS, &t.Generation, &t.AttemptCount, &t.MaxAttempts, &t.CurrentAttempt, &t.NextAttemptAt, &t.DeadlineAt, &t.LastError)
	t.ManualRedrive = "deferred_read_only"
	return t, classify(err)
}
func (s *Store) Attempts(ctx context.Context, wf, task uuid.UUID, after int64, limit int) (execution.AttemptPage, error) {
	if after < 0 || limit < 1 || limit > 10 {
		return execution.AttemptPage{}, workflow.ErrInvalid
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return execution.AttemptPage{}, classify(err)
	}
	defer rollback(tx)
	var exists bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tasks WHERE id=$1 AND workflow_id=$2)`, task, wf).Scan(&exists); err != nil {
		return execution.AttemptPage{}, classify(err)
	}
	if !exists {
		return execution.AttemptPage{}, workflow.ErrNotFound
	}
	rows, err := tx.Query(ctx, `SELECT id FROM task_attempts WHERE task_id=$1 AND attempt_number>$2 ORDER BY attempt_number LIMIT $3`, task, after, limit)
	if err != nil {
		return execution.AttemptPage{}, classify(err)
	}
	ids := []uuid.UUID{}
	for rows.Next() {
		var id uuid.UUID
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return execution.AttemptPage{}, classify(err)
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return execution.AttemptPage{}, classify(err)
	}
	page := execution.AttemptPage{Attempts: []execution.Attempt{}, NextAfter: after}
	for _, id := range ids {
		a, e := readAttempt(ctx, tx, id)
		if e != nil {
			return page, e
		}
		page.Attempts = append(page.Attempts, a)
		page.NextAfter = int64(a.Number)
	}
	if err = tx.Commit(ctx); err != nil {
		return page, classify(err)
	}
	return page, nil
}

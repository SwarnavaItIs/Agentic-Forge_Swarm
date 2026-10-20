package postgres

import (
	"context"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
)

// AuthorizeTool authorizes only this stage's bounded diagnostic actions. Future
// sandbox tools must also enforce ownership at their side-effect boundary.
func (s *Store) AuthorizeTool(ctx context.Context, worker string, instance, id uuid.UUID, generation int64) (execution.Attempt, error) {
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
	if err = tx.Commit(ctx); err != nil {
		return a, classify(err)
	}
	return a, nil
}

// AcknowledgeStop attests that this diagnostic has stopped. It never grants
// ownership. An old LOST attempt may clear only its residual cleanup flag;
// accepted outcomes and any replacement attempt remain untouched.
func (s *Store) AcknowledgeStop(ctx context.Context, worker string, instance, id uuid.UUID, generation int64) (execution.Attempt, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	defer rollback(tx)
	var wf uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT workflow_id FROM task_attempts WHERE id=$1`, id).Scan(&wf); err != nil {
		return execution.Attempt{}, classify(err)
	}
	p, err := lockParent(ctx, tx, wf)
	if err != nil {
		return execution.Attempt{}, err
	}
	a, err := readAttempt(ctx, tx, id)
	if err != nil {
		return a, err
	}
	if a.WorkerID != worker || a.Instance != instance || a.Generation != generation {
		return a, execution.ErrStale
	}
	var current *uuid.UUID
	var gen int64
	if err = tx.QueryRow(ctx, `SELECT current_attempt_id,dispatch_generation FROM tasks WHERE id=$1 FOR UPDATE`, a.TaskID).Scan(&current, &gen); err != nil {
		return a, classify(err)
	}
	if a.State == workflow.Running {
		if _, err = lockWorker(ctx, tx, worker, instance); err != nil {
			return a, err
		}
		if current == nil || *current != id || gen != generation {
			return a, execution.ErrInvariant
		}
		now, e := dbNow(ctx, tx)
		if e != nil {
			return a, e
		}
		class := execution.WorkerLost
		state := "LOST"
		if a.StopRequested || p.status == workflow.Cancelling || !now.Before(a.DeadlineAt) || !now.Before(p.deadline) {
			class = execution.Deadline
			if a.StopReason == execution.ExplicitCancel || p.reason == execution.ExplicitCancel {
				class = execution.ExplicitCancel
			}
			state = workflow.Cancelled
		}
		_, err = tx.Exec(ctx, `UPDATE task_attempts SET state=$2,finished_at=clock_timestamp(),cleanup_required=false,last_error_class=$3 WHERE id=$1`, id, state, class)
		if err != nil {
			return a, classify(err)
		}
		if class == execution.ExplicitCancel {
			_, err = tx.Exec(ctx, `UPDATE tasks SET status='CANCELLED',last_error_class=$2,updated_at=clock_timestamp() WHERE id=$1`, a.TaskID, class)
			if err != nil {
				return a, classify(err)
			}
		} else if err = s.scheduleFailure(ctx, tx, &p, a.TaskID, class, now); err != nil {
			return a, err
		}
	} else if !a.CleanupRequired {
		if err = tx.Commit(ctx); err != nil {
			return a, classify(err)
		}
		return a, nil
	} else {
		if _, err = tx.Exec(ctx, `UPDATE task_attempts SET cleanup_required=false WHERE id=$1`, id); err != nil {
			return a, classify(err)
		}
	}
	if err = mutation(ctx, tx, p, p.status, []eventWrite{{"attempt.stopped", map[string]any{"attempt_id": id, "generation": generation, "diagnostic_stopped_acknowledged": true}}}); err != nil {
		return a, err
	}
	a, err = readAttempt(ctx, tx, id)
	if err != nil {
		return a, err
	}
	if err = tx.Commit(ctx); err != nil {
		return a, classify(err)
	}
	return a, nil
}

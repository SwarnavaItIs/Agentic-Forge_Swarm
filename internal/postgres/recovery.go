package postgres

import (
	"context"
	"encoding/json"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"time"
)

func dbNow(ctx context.Context, tx pgx.Tx) (time.Time, error) {
	var now time.Time
	err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now)
	return now, classify(err)
}
func appendHistory(ctx context.Context, tx pgx.Tx, p *lockedWorkflow, kind string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	p.sequence++
	at := p.at
	if at.IsZero() {
		at, err = dbNow(ctx, tx)
		if err != nil {
			return err
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5)`, p.id, p.sequence, kind, body, at)
	return classify(err)
}

// A contradictory current owner is an incident, not a lease to be erased.
// Called while the workflow is locked before recovery changes any evidence.
func consistentLocked(ctx context.Context, tx pgx.Tx, p lockedWorkflow) error {
	var bad bool
	err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM tasks t LEFT JOIN task_attempts a ON a.id=t.current_attempt_id WHERE t.workflow_id=$1 AND t.status='RUNNING' AND (a.id IS NULL OR a.task_id<>t.id OR a.generation<>t.dispatch_generation OR a.state<>'RUNNING' OR t.attempt_count<>a.attempt_number)) OR EXISTS(SELECT 1 FROM task_attempts a JOIN tasks t ON t.id=a.task_id JOIN workers w ON w.id=a.worker_id WHERE a.workflow_id=$1 AND a.state='RUNNING' AND (t.status<>'RUNNING' OR t.current_attempt_id IS DISTINCT FROM a.id OR t.dispatch_generation<>a.generation OR w.instance_id<>a.worker_instance_id))`, p.id).Scan(&bad)
	if err != nil {
		return classify(err)
	}
	if bad {
		return execution.ErrInvariant
	}
	return nil
}
func (s *Store) CheckConsistency(ctx context.Context) error {
	return s.checkCapacity(ctx, s.pool)
}
func (s *Store) checkCapacity(ctx context.Context, q interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}) error {
	var bad bool
	err := q.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workers w WHERE (SELECT count(*) FROM task_attempts a WHERE a.worker_id=w.id AND a.state='RUNNING')>w.capacity) OR (SELECT count(*) FROM task_attempts WHERE state='RUNNING')>$1 OR EXISTS(SELECT 1 FROM task_attempts a JOIN workflows w ON w.id=a.workflow_id JOIN tasks t ON t.id=a.task_id JOIN workers r ON r.id=a.worker_id WHERE a.state='RUNNING' AND (w.status NOT IN ('RUNNING','CANCELLING') OR t.status<>'RUNNING' OR t.current_attempt_id IS DISTINCT FROM a.id OR t.dispatch_generation<>a.generation OR t.attempt_count<>a.attempt_number OR r.instance_id<>a.worker_instance_id)) OR EXISTS(SELECT 1 FROM tasks t LEFT JOIN task_attempts a ON a.id=t.current_attempt_id WHERE t.status='RUNNING' AND (a.id IS NULL OR a.state<>'RUNNING' OR a.generation<>t.dispatch_generation))`, s.limits.TotalCapacity).Scan(&bad)
	if err != nil {
		return classify(err)
	}
	if bad {
		return execution.ErrInvariant
	}
	return nil
}

// scheduleFailure never waits for retry. Its selected due time is durable and
// the generation is fenced immediately; a later bounded scan publishes it.
func (s *Store) scheduleFailure(ctx context.Context, tx pgx.Tx, p *lockedWorkflow, task uuid.UUID, class string, now time.Time) error {
	var count, maxAttempts int
	var deadline time.Time
	var gen int64
	err := tx.QueryRow(ctx, `SELECT attempt_count,max_attempts,deadline_at,dispatch_generation FROM tasks WHERE id=$1 FOR UPDATE`, task).Scan(&count, &maxAttempts, &deadline, &gen)
	if err != nil {
		return classify(err)
	}
	state, event := workflow.Failed, "task.failed"
	var due *time.Time
	if p.status == workflow.Cancelling {
		if p.reason == execution.ExplicitCancel {
			state, event, class = workflow.Cancelled, "task.cancelled", execution.ExplicitCancel
		} else {
			class = execution.Deadline
		}
	} else if !now.Before(deadline) || !now.Before(p.deadline) {
		class = execution.Deadline
	} else if execution.Retryable(class) && count < maxAttempts {
		next := s.policy.RetryAt(now, count, s.random())
		if next.Before(deadline) && next.Before(p.deadline) {
			state, event, due = workflow.RetryWait, "task.retry_wait", &next
		} else {
			class = execution.Deadline
		}
	}
	_, err = tx.Exec(ctx, `UPDATE tasks SET status=$2,current_attempt_id=NULL,dispatch_generation=dispatch_generation+1,next_attempt_at=$3,last_error_class=$4,ready_since=NULL,updated_at=clock_timestamp() WHERE id=$1`, task, state, due, class)
	if err != nil {
		return classify(err)
	}
	if err = obsoleteDispatch(ctx, tx, task); err != nil {
		return err
	}
	return appendHistory(ctx, tx, p, event, map[string]any{"task_id": task, "generation": gen + 1, "error_class": class, "attempt_count": count, "max_attempts": maxAttempts, "next_attempt_at": due})
}
func obsoleteDispatch(ctx context.Context, tx pgx.Tx, task uuid.UUID) error {
	_, err := tx.Exec(ctx, `UPDATE outbox SET state='OBSOLETE',relay_id=NULL,relay_lease_expires_at=NULL WHERE task_id=$1 AND state='PENDING'`, task)
	return classify(err)
}

func requestStops(ctx context.Context, tx pgx.Tx, p *lockedWorkflow, reason string) error {
	rows, err := tx.Query(ctx, `UPDATE task_attempts SET cancel_requested_at=clock_timestamp(),stop_reason=$2,cleanup_required=true WHERE workflow_id=$1 AND state='RUNNING' AND cancel_requested_at IS NULL RETURNING id`, p.id, reason)
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
		if err = appendHistory(ctx, tx, p, "attempt.stop_requested", map[string]any{"attempt_id": id, "reason": reason}); err != nil {
			return err
		}
	}
	return nil
}
func beginCancellation(ctx context.Context, tx pgx.Tx, p *lockedWorkflow, reason string) error {
	if p.status == workflow.Cancelled || p.status == workflow.Succeeded || p.status == workflow.Failed || p.status == workflow.Cancelling {
		return nil
	}
	var active int
	now, clockErr := dbNow(ctx, tx)
	if clockErr != nil {
		return clockErr
	}
	p.at = now
	if err := tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE workflow_id=$1 AND state='RUNNING'`, p.id).Scan(&active); err != nil {
		return classify(err)
	}
	state := workflow.Cancelled
	if reason == execution.Deadline {
		state = workflow.Failed
	}
	if active > 0 {
		state = workflow.Cancelling
	}
	taskState := workflow.Cancelled
	if reason == execution.Deadline {
		taskState = workflow.Failed
	}
	tag, err := tx.Exec(ctx, `UPDATE tasks SET status=$2,next_attempt_at=NULL,last_error_class=$3,updated_at=$4 WHERE workflow_id=$1 AND status IN ('PENDING','READY','RETRY_WAIT')`, p.id, taskState, reason, now)
	if err != nil {
		return classify(err)
	}
	if _, err = tx.Exec(ctx, `UPDATE outbox o SET state='OBSOLETE',relay_id=NULL,relay_lease_expires_at=NULL FROM tasks t WHERE t.id=o.task_id AND t.workflow_id=$1 AND o.state='PENDING'`, p.id); err != nil {
		return classify(err)
	}
	p.status, p.reason = state, reason
	if active > 0 {
		if err = requestStops(ctx, tx, p, reason); err != nil {
			return err
		}
	}
	event := "workflow.cancelled"
	if state == workflow.Cancelling {
		event = "workflow.cancelling"
	} else if state == workflow.Failed {
		event = "workflow.failed"
	}
	if active == 0 && reason == execution.ExplicitCancel {
		return appendHistory(ctx, tx, p, event, map[string]int64{"cancelled_task_count": tag.RowsAffected()})
	}
	return appendHistory(ctx, tx, p, event, map[string]any{"status": state, "reason": reason, "active_attempts": active, "physical_termination_proven": false})
}

// recoverLocked evaluates the DB clock only after the parent lock. All rows
// belong to a <=100-task immutable DAG. No network waits occur here.
func (s *Store) recoverLocked(ctx context.Context, tx pgx.Tx, p *lockedWorkflow) error {
	if err := consistentLocked(ctx, tx, *p); err != nil {
		return err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return err
	}
	if p.status != workflow.Cancelling && !now.Before(p.deadline) {
		if err = beginCancellation(ctx, tx, p, execution.Deadline); err != nil {
			return err
		}
	}
	rows, err := tx.Query(ctx, `SELECT t.id,a.id,t.deadline_at,a.lease_expires_at,a.cancel_requested_at IS NOT NULL FROM tasks t JOIN task_attempts a ON a.id=t.current_attempt_id WHERE t.workflow_id=$1 AND t.status='RUNNING' ORDER BY t.position FOR UPDATE OF t,a`, p.id)
	if err != nil {
		return classify(err)
	}
	type active struct {
		task, attempt   uuid.UUID
		deadline, lease time.Time
		stop            bool
	}
	items := []active{}
	for rows.Next() {
		var a active
		if err = rows.Scan(&a.task, &a.attempt, &a.deadline, &a.lease, &a.stop); err != nil {
			rows.Close()
			return classify(err)
		}
		items = append(items, a)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return classify(err)
	}
	// Re-sample after all row locks; a blocked transaction-start timestamp is
	// never used as evidence that a lease or deadline remains live.
	now, err = dbNow(ctx, tx)
	if err != nil {
		return err
	}
	for _, a := range items {
		if !now.Before(a.lease) {
			_, err = tx.Exec(ctx, `UPDATE task_attempts SET state='LOST',finished_at=clock_timestamp(),last_error_class='worker_lost',cleanup_required=true WHERE id=$1 AND state='RUNNING'`, a.attempt)
			if err != nil {
				return classify(err)
			}
			if err = appendHistory(ctx, tx, p, "attempt.lost", map[string]any{"attempt_id": a.attempt, "task_id": a.task, "physical_termination_proven": false}); err != nil {
				return err
			}
			if err = s.scheduleFailure(ctx, tx, p, a.task, execution.WorkerLost, now); err != nil {
				return err
			}
		} else if !a.stop && (!now.Before(a.deadline) || p.status == workflow.Cancelling) {
			reason := execution.Deadline
			if p.status == workflow.Cancelling {
				reason = p.reason
			}
			_, err = tx.Exec(ctx, `UPDATE task_attempts SET cancel_requested_at=clock_timestamp(),stop_reason=$2,cleanup_required=true WHERE id=$1`, a.attempt, reason)
			if err != nil {
				return classify(err)
			}
			if err = appendHistory(ctx, tx, p, "attempt.stop_requested", map[string]any{"attempt_id": a.attempt, "reason": reason}); err != nil {
				return err
			}
		}
	}
	if p.status == workflow.Cancelling {
		var active int
		if err = tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE workflow_id=$1 AND state='RUNNING'`, p.id).Scan(&active); err != nil {
			return classify(err)
		}
		if active == 0 {
			p.status = workflow.Cancelled
			event := "workflow.cancelled"
			if p.reason == execution.Deadline {
				p.status, event = workflow.Failed, "workflow.failed"
			}
			return appendHistory(ctx, tx, p, event, map[string]any{"status": p.status, "reason": p.reason, "settled_by": "stop_ack_or_lease_fence"})
		}
		return nil
	}
	rows, err = tx.Query(ctx, `SELECT id,status,dispatch_generation,deadline_at,ready_since,kind FROM tasks WHERE workflow_id=$1 AND (status IN ('PENDING','READY') OR (status='RETRY_WAIT' AND next_attempt_at<=$2)) ORDER BY position FOR UPDATE`, p.id, now)
	if err != nil {
		return classify(err)
	}
	type waiting struct {
		id       uuid.UUID
		state    string
		gen      int64
		deadline time.Time
		ready    *time.Time
		kind     string
	}
	waiters := []waiting{}
	for rows.Next() {
		var w waiting
		if err = rows.Scan(&w.id, &w.state, &w.gen, &w.deadline, &w.ready, &w.kind); err != nil {
			rows.Close()
			return classify(err)
		}
		waiters = append(waiters, w)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return classify(err)
	}
	for _, w := range waiters {
		class := ""
		if !now.Before(w.deadline) {
			class = execution.Deadline
		}
		if class == "" && w.state == workflow.Ready && w.ready != nil && now.Sub(*w.ready) >= s.policy.CapabilityWait {
			var capable bool
			err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM workers WHERE NOT draining AND $1=ANY(capabilities) AND heartbeat_at>clock_timestamp()-$2*interval '1 millisecond')`, w.kind, s.policy.WorkerFreshness.Milliseconds()).Scan(&capable)
			if err != nil {
				return classify(err)
			}
			if !capable {
				class = execution.Unsupported
			}
		}
		if class != "" {
			_, err = tx.Exec(ctx, `UPDATE tasks SET status='FAILED',next_attempt_at=NULL,last_error_class=$2,updated_at=clock_timestamp() WHERE id=$1`, w.id, class)
			if err != nil {
				return classify(err)
			}
			if err = obsoleteDispatch(ctx, tx, w.id); err != nil {
				return err
			}
			if err = appendHistory(ctx, tx, p, "task.failed", map[string]any{"task_id": w.id, "error_class": class}); err != nil {
				return err
			}
			continue
		}
		if w.state == workflow.RetryWait {
			var due bool
			err = tx.QueryRow(ctx, `SELECT next_attempt_at<=clock_timestamp() FROM tasks WHERE id=$1`, w.id).Scan(&due)
			if err != nil {
				return classify(err)
			}
			if due {
				room, e := s.readyRoom(ctx, tx)
				if e != nil {
					return e
				}
				if room {
					if err = s.makeReady(ctx, tx, p, w.id, false); err != nil {
						return err
					}
				}
			}
		} else if w.state == workflow.Ready {
			var revision int
			err = tx.QueryRow(ctx, `UPDATE outbox SET state='PENDING',sent_at=NULL,publish_revision=publish_revision+1,available_at=clock_timestamp(),relay_id=NULL,relay_lease_expires_at=NULL WHERE task_id=$1 AND dispatch_generation=$2 AND state='SENT' AND sent_at<=clock_timestamp()-$3*interval '1 millisecond' RETURNING publish_revision`, w.id, w.gen, s.policy.ReadyResend.Milliseconds()).Scan(&revision)
			if err == nil {
				if err = appendHistory(ctx, tx, p, "task.dispatch_requeued", map[string]any{"task_id": w.id, "generation": w.gen, "publish_revision": revision}); err != nil {
					return err
				}
			} else if err != pgx.ErrNoRows {
				return classify(err)
			}
		}
	}
	return nil
}
func (s *Store) readyRoom(ctx context.Context, tx pgx.Tx) (bool, error) {
	var n int
	err := tx.QueryRow(ctx, `SELECT count(*) FROM tasks WHERE status='READY'`).Scan(&n)
	return n < s.policy.MaxReady, classify(err)
}
func (s *Store) makeReady(ctx context.Context, tx pgx.Tx, p *lockedWorkflow, task uuid.UUID, increment bool) error {
	var gen int64
	err := tx.QueryRow(ctx, `UPDATE tasks SET status='READY',next_attempt_at=NULL,ready_since=clock_timestamp(),updated_at=clock_timestamp(),dispatch_generation=dispatch_generation+CASE WHEN $2 THEN 1 ELSE 0 END WHERE id=$1 RETURNING dispatch_generation`, task, increment).Scan(&gen)
	if err != nil {
		return classify(err)
	}
	if err = dispatch(ctx, tx, task, gen); err != nil {
		return err
	}
	return appendHistory(ctx, tx, p, "task.ready", map[string]any{"task_id": task, "generation": gen})
}

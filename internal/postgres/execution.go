package postgres

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/telemetry"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"slices"
	"time"
)

// Admission lock -> workflow -> task -> worker -> attempt. Register takes only
// admission/worker locks; heartbeat/drain take only worker locks. No network
// broker/RPC waits occur in these transactions. Global admission serialization
// is a deliberate small-platform throughput tradeoff, not a distributed leader.
const admissionLock int64 = 734192801

func admission(ctx context.Context, tx pgx.Tx) error {
	_, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock($1)`, admissionLock)
	return classify(err)
}
func (s *Store) RegisterWorker(ctx context.Context, id string, instance uuid.UUID, caps []string, capacity int) (execution.Worker, error) {
	if !workflow.ValidCreationKey(id) || instance == uuid.Nil || capacity < 1 || capacity > s.limits.WorkerCapacity || len(caps) < 1 || len(caps) > 4 {
		return execution.Worker{}, workflow.ErrInvalid
	}
	caps = append([]string(nil), caps...)
	slices.Sort(caps)
	for i, c := range caps {
		if (c != "noop" && c != "delay" && c != "sandbox_go" && c != "agent_go") || (i > 0 && caps[i-1] == c) {
			return execution.Worker{}, workflow.ErrInvalid
		}
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return execution.Worker{}, classify(err)
	}
	defer rollback(tx)
	if err = admission(ctx, tx); err != nil {
		return execution.Worker{}, err
	}
	var old uuid.UUID
	var draining bool
	err = tx.QueryRow(ctx, `SELECT instance_id,draining FROM workers WHERE id=$1 FOR UPDATE`, id).Scan(&old, &draining)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return execution.Worker{}, classify(err)
	}
	if err == nil {
		var active int
		err = tx.QueryRow(ctx, `SELECT count(*) FROM task_attempts WHERE worker_id=$1 AND state='RUNNING'`, id).Scan(&active)
		if err != nil {
			return execution.Worker{}, classify(err)
		}
		if old != instance && active > 0 {
			return execution.Worker{}, execution.ErrClaimed
		}
		if active > capacity {
			return execution.Worker{}, execution.ErrCapacity
		}
		if old == instance && draining {
			return execution.Worker{}, execution.ErrDraining
		}
	}
	w := execution.Worker{ID: id, Instance: instance, Capabilities: caps, Capacity: capacity}
	err = tx.QueryRow(ctx, `INSERT INTO workers(id,instance_id,capabilities,capacity,heartbeat_at) VALUES($1,$2,$3,$4,clock_timestamp())
 ON CONFLICT(id) DO UPDATE SET instance_id=$2,capabilities=$3,capacity=$4,heartbeat_at=clock_timestamp(),draining=false RETURNING heartbeat_at`, id, instance, caps, capacity).Scan(&w.Heartbeat)
	if err != nil {
		return w, classify(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return execution.Worker{}, classify(err)
	}
	return w, nil
}
func (s *Store) HeartbeatWorker(ctx context.Context, id string, instance uuid.UUID, drain *bool) (execution.Worker, error) {
	w := execution.Worker{ID: id, Instance: instance}
	err := s.pool.QueryRow(ctx, `UPDATE workers SET heartbeat_at=clock_timestamp(),draining=COALESCE($3,draining) WHERE id=$1 AND instance_id=$2 RETURNING capabilities,capacity,heartbeat_at,draining`, id, instance, drain).Scan(&w.Capabilities, &w.Capacity, &w.Heartbeat, &w.Draining)
	if errors.Is(err, pgx.ErrNoRows) {
		return w, execution.ErrWorker
	}
	return w, classify(err)
}
func lockParent(ctx context.Context, tx pgx.Tx, id uuid.UUID) (lockedWorkflow, error) {
	p := lockedWorkflow{id: id}
	err := tx.QueryRow(ctx, `SELECT status,version,last_event_sequence,deadline_at,terminal_reason FROM workflows WHERE id=$1 FOR UPDATE`, id).Scan(&p.status, &p.version, &p.sequence, &p.deadline, &p.reason)
	return p, classify(err)
}
func lockWorker(ctx context.Context, tx pgx.Tx, id string, instance uuid.UUID) (execution.Worker, error) {
	w := execution.Worker{ID: id}
	err := tx.QueryRow(ctx, `SELECT instance_id,capabilities,capacity,draining,heartbeat_at FROM workers WHERE id=$1 FOR UPDATE`, id).Scan(&w.Instance, &w.Capabilities, &w.Capacity, &w.Draining, &w.Heartbeat)
	if errors.Is(err, pgx.ErrNoRows) || err == nil && w.Instance != instance {
		return w, execution.ErrWorker
	}
	return w, classify(err)
}

const attemptColumns = `a.id,a.task_id,a.attempt_number,a.generation,a.state,t.kind,t.delay_ms,a.started_at,a.lease_expires_at,a.finished_at,a.outcome_digest,a.result_metadata,a.worker_id,a.worker_instance_id,t.deadline_at,a.cancel_requested_at IS NOT NULL,a.stop_reason,a.cleanup_required,a.last_error_class,a.workflow_id,t.sandbox_input,(SELECT metadata FROM sandbox_executions WHERE attempt_id=a.id),t.agent_input`

func readAttempt(ctx context.Context, tx pgx.Tx, id uuid.UUID) (execution.Attempt, error) {
	var a execution.Attempt
	err := tx.QueryRow(ctx, `SELECT `+attemptColumns+` FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE a.id=$1`, id).Scan(&a.ID, &a.TaskID, &a.Number, &a.Generation, &a.State, &a.Kind, &a.DelayMS, &a.StartedAt, &a.LeaseExpiresAt, &a.FinishedAt, &a.Digest, &a.Result, &a.WorkerID, &a.Instance, &a.DeadlineAt, &a.StopRequested, &a.StopReason, &a.CleanupRequired, &a.LastError, &a.WorkflowID, &a.Sandbox, &a.SandboxExecution, &a.Agent)
	return a, classify(err)
}

type eventWrite struct {
	kind    string
	payload any
}

func mutation(ctx context.Context, tx pgx.Tx, p lockedWorkflow, status string, events []eventWrite) error {
	var at time.Time
	if err := tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&at); err != nil {
		return classify(err)
	}
	for _, e := range events {
		body, err := json.Marshal(e.payload)
		if err != nil {
			return err
		}
		p.sequence++
		if _, err = tx.Exec(ctx, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload,occurred_at) VALUES($1,$2,$3,$4,$5)`, p.id, p.sequence, e.kind, body, at); err != nil {
			return classify(err)
		}
	}
	_, err := tx.Exec(ctx, `UPDATE workflows SET status=$2,version=version+1,last_event_sequence=$3,updated_at=$4 WHERE id=$1`, p.id, status, p.sequence, at)
	return classify(err)
}
func (s *Store) ClaimTask(ctx context.Context, worker string, instance, task, request uuid.UUID, generation int64) (out execution.Attempt, err error) {
	ctx, span := telemetry.Start(ctx, "attempt.claim")
	started := time.Now()
	defer func() {
		span.SetAttributes(telemetry.IDs(out.WorkflowID.String(), out.TaskID.String(), out.ID.String())...)
		telemetry.End(ctx, span, "attempt.claim", started, err)
	}()
	if instance == uuid.Nil || task == uuid.Nil || request == uuid.Nil || generation < 1 {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	defer rollback(tx)
	if err = admission(ctx, tx); err != nil {
		return execution.Attempt{}, err
	}
	var workflowID uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT workflow_id FROM tasks WHERE id=$1`, task).Scan(&workflowID); err != nil {
		return execution.Attempt{}, classify(err)
	}
	p, err := lockParent(ctx, tx, workflowID)
	if err != nil {
		return execution.Attempt{}, err
	}
	var state, kind string
	var gen int64
	var current *uuid.UUID
	var count int
	var maxAttempts int
	var deadline time.Time
	err = tx.QueryRow(ctx, `SELECT status,kind,dispatch_generation,current_attempt_id,attempt_count,max_attempts,deadline_at FROM tasks WHERE id=$1 FOR UPDATE`, task).Scan(&state, &kind, &gen, &current, &count, &maxAttempts, &deadline)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	w, err := lockWorker(ctx, tx, worker, instance)
	if err != nil {
		return execution.Attempt{}, err
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return execution.Attempt{}, err
	}
	if (p.status != workflow.Pending && p.status != workflow.Running) || !now.Before(deadline) || !now.Before(p.deadline) {
		return execution.Attempt{}, execution.ErrObsolete
	}
	var retry uuid.UUID
	err = tx.QueryRow(ctx, `SELECT id FROM task_attempts WHERE worker_id=$1 AND claim_request_id=$2`, worker, request).Scan(&retry)
	if err == nil {
		a, err := readAttempt(ctx, tx, retry)
		if err != nil {
			return a, err
		}
		if a.TaskID != task || a.Generation != generation || a.Instance != instance {
			return execution.Attempt{}, execution.ErrOutcome
		}
		var live bool
		err = tx.QueryRow(ctx, `SELECT lease_expires_at>clock_timestamp() FROM task_attempts WHERE id=$1`, retry).Scan(&live)
		if err != nil {
			return a, classify(err)
		}
		if current == nil || *current != a.ID || state != workflow.Running || a.State != workflow.Running || gen != generation || !live || a.StopRequested {
			return execution.Attempt{}, execution.ErrObsolete
		}
		if err = tx.Commit(ctx); err != nil {
			return execution.Attempt{}, classify(err)
		}
		return a, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return execution.Attempt{}, classify(err)
	}
	if gen != generation || (p.status != workflow.Pending && p.status != workflow.Running) {
		return execution.Attempt{}, execution.ErrObsolete
	}
	if state != workflow.Ready {
		if state == workflow.Running {
			return execution.Attempt{}, execution.ErrClaimed
		}
		return execution.Attempt{}, execution.ErrObsolete
	}
	if w.Draining {
		return execution.Attempt{}, execution.ErrDraining
	}
	if !slices.Contains(w.Capabilities, kind) {
		return execution.Attempt{}, execution.ErrCapability
	}
	var own, total int
	err = tx.QueryRow(ctx, `SELECT count(*) FILTER(WHERE worker_id=$1),count(*) FROM task_attempts WHERE state='RUNNING'`, worker).Scan(&own, &total)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	if own >= w.Capacity || total >= s.limits.TotalCapacity {
		return execution.Attempt{}, execution.ErrCapacity
	}
	if count >= maxAttempts {
		return execution.Attempt{}, execution.ErrObsolete
	}
	attemptID, err := uuid.NewRandom()
	if err != nil {
		return execution.Attempt{}, err
	}
	lease := now.Add(s.limits.Lease)
	if deadline.Before(lease) {
		lease = deadline
	}
	if p.deadline.Before(lease) {
		lease = p.deadline
	}
	_, err = tx.Exec(ctx, `INSERT INTO task_attempts(id,workflow_id,task_id,attempt_number,worker_id,worker_instance_id,claim_request_id,generation,started_at,lease_expires_at,state)
 VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'RUNNING')`, attemptID, p.id, task, count+1, worker, instance, request, generation, now, lease)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	_, err = tx.Exec(ctx, `UPDATE tasks SET status='RUNNING',current_attempt_id=$2,attempt_count=attempt_count+1,updated_at=clock_timestamp() WHERE id=$1`, task, attemptID)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	events := []eventWrite{}
	if p.status == workflow.Pending {
		events = append(events, eventWrite{"workflow.running", map[string]string{"status": workflow.Running}})
	}
	events = append(events, eventWrite{"task.claimed", map[string]any{"task_id": task, "attempt_id": attemptID, "generation": generation}})
	if err = mutation(ctx, tx, p, workflow.Running, events); err != nil {
		return execution.Attempt{}, err
	}
	a, err := readAttempt(ctx, tx, attemptID)
	if err != nil {
		return a, err
	}
	if err = tx.Commit(ctx); err != nil {
		return execution.Attempt{}, classify(err)
	}
	return a, nil
}

// withAttempt locks the parent's ownership boundary before reading child state.
func withAttempt(ctx context.Context, tx pgx.Tx, worker string, instance, id uuid.UUID, generation int64) (lockedWorkflow, execution.Attempt, bool, error) {
	var workflowID uuid.UUID
	err := tx.QueryRow(ctx, `SELECT workflow_id FROM task_attempts WHERE id=$1`, id).Scan(&workflowID)
	if err != nil {
		return lockedWorkflow{}, execution.Attempt{}, false, classify(err)
	}
	p, err := lockParent(ctx, tx, workflowID)
	if err != nil {
		return p, execution.Attempt{}, false, err
	}
	var task uuid.UUID
	if err = tx.QueryRow(ctx, `SELECT task_id FROM task_attempts WHERE id=$1`, id).Scan(&task); err != nil {
		return p, execution.Attempt{}, false, classify(err)
	}
	var current *uuid.UUID
	var gen int64
	var status string
	if err = tx.QueryRow(ctx, `SELECT current_attempt_id,dispatch_generation,status FROM tasks WHERE id=$1 FOR UPDATE`, task).Scan(&current, &gen, &status); err != nil {
		return p, execution.Attempt{}, false, classify(err)
	}
	a, err := readAttempt(ctx, tx, id)
	if err != nil {
		return p, a, false, err
	}
	if _, err = lockWorker(ctx, tx, worker, instance); err != nil {
		// A terminal receipt belongs to the authenticated original owner even
		// after it registers a replacement instance. This permits receipt-only
		// retries; the original attempt instance/generation are still checked.
		if !errors.Is(err, execution.ErrWorker) || (a.State != workflow.Succeeded && a.State != workflow.Failed) {
			return p, a, false, err
		}
	}
	accepted := a.State == workflow.Succeeded || a.State == workflow.Failed
	if a.WorkerID != worker || a.Instance != instance || a.Generation != generation || (!accepted && (current == nil || *current != id || gen != generation)) {
		return p, a, false, execution.ErrStale
	}
	var leaseLive, deadlineLive bool
	err = tx.QueryRow(ctx, `SELECT lease_expires_at>clock_timestamp(),$2::timestamptz>clock_timestamp() AND $3::timestamptz>clock_timestamp() FROM task_attempts WHERE id=$1`, id, a.DeadlineAt, p.deadline).Scan(&leaseLive, &deadlineLive)
	if p.status == workflow.Cancelling {
		a.StopRequested = true
		a.StopReason = p.reason
	}
	if !deadlineLive && a.State == workflow.Running {
		a.StopRequested = true
		if a.StopReason == "" {
			a.StopReason = execution.Deadline
		}
	}
	return p, a, leaseLive && deadlineLive && !a.StopRequested && a.State == workflow.Running && status == workflow.Running && p.status == workflow.Running, classify(err)
}
func (s *Store) RenewLease(ctx context.Context, worker string, instance, id uuid.UUID, generation int64) (execution.Attempt, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	defer rollback(tx)
	p, a, live, err := withAttempt(ctx, tx, worker, instance, id, generation)
	if err != nil {
		return a, err
	}
	if a.StopRequested && a.State == workflow.Running {
		// Signal cancellation without extending ownership; DB predicates below
		// remain the authority (the application clock never grants a lease).
		var unexpired bool
		if err = tx.QueryRow(ctx, `SELECT lease_expires_at>clock_timestamp() FROM task_attempts WHERE id=$1`, id).Scan(&unexpired); err != nil {
			return a, classify(err)
		}
		if unexpired {
			if err = tx.Commit(ctx); err != nil {
				return a, classify(err)
			}
			return a, nil
		}
	}
	if !live {
		return execution.Attempt{}, execution.ErrStale
	}
	// Recheck in the UPDATE too: time may pass after the previous statement.
	err = tx.QueryRow(ctx, `UPDATE task_attempts SET lease_expires_at=LEAST(clock_timestamp()+$2*interval '1 millisecond',$3,$4) WHERE id=$1 AND state='RUNNING' AND lease_expires_at>clock_timestamp() AND cancel_requested_at IS NULL AND $3::timestamptz>clock_timestamp() AND $4::timestamptz>clock_timestamp() RETURNING lease_expires_at`, id, s.limits.Lease.Milliseconds(), a.DeadlineAt, p.deadline).Scan(&a.LeaseExpiresAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return execution.Attempt{}, execution.ErrStale
	}
	if err != nil {
		return a, classify(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return execution.Attempt{}, classify(err)
	}
	return a, nil
}
func (s *Store) FinishAttempt(ctx context.Context, worker string, instance, id uuid.UUID, generation int64, success bool, result []byte) (execution.Attempt, error) {
	class := ""
	if !success {
		class = execution.Diagnostic
	}
	return s.finishAttempt(ctx, worker, instance, id, generation, success, class, result, nil)
}
func (s *Store) FailAttempt(ctx context.Context, worker string, instance, id uuid.UUID, generation int64, class string, result []byte) (execution.Attempt, error) {
	if class == "" {
		class = execution.Diagnostic
	}
	if !execution.FailureClass(class) {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	return s.finishAttempt(ctx, worker, instance, id, generation, false, class, result, nil)
}
func (s *Store) finishAttempt(ctx context.Context, worker string, instance, id uuid.UUID, generation int64, success bool, class string, result []byte, command *sandbox.Result, proof ...*agent.Outcome) (execution.Attempt, error) {
	body, digest, err := execution.Outcome(result)
	if err != nil {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	state, kind := workflow.Succeeded, "task.succeeded"
	if !success {
		state, kind = workflow.Failed, "task.failed"
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	defer rollback(tx)
	p, a, live, err := withAttempt(ctx, tx, worker, instance, id, generation)
	if err != nil {
		return a, err
	}
	if a.Kind == "agent_go" {
		if len(proof) != 1 || proof[0] == nil {
			return a, workflow.ErrInvalid
		}
	} else if a.Kind == "sandbox_go" && command == nil || a.Kind != "sandbox_go" && command != nil {
		return a, workflow.ErrInvalid
	}
	if a.State == workflow.Succeeded || a.State == workflow.Failed {
		equal := bytes.Equal(a.Digest, digest[:])
		if !equal {
			if err = tx.QueryRow(ctx, `SELECT result_metadata=$2::jsonb FROM task_attempts WHERE id=$1`, id, body).Scan(&equal); err != nil {
				return execution.Attempt{}, classify(err)
			}
		}
		if a.State != state || !equal || a.LastError != class {
			return execution.Attempt{}, execution.ErrOutcome
		}
		if err = tx.Commit(ctx); err != nil {
			return execution.Attempt{}, classify(err)
		}
		return a, nil
	}
	if !live {
		return execution.Attempt{}, execution.ErrStale
	}
	if a.Kind == "agent_go" {
		if err = s.validateAgent(ctx, tx, a, *proof[0]); err != nil {
			return a, err
		}
	}
	var bounded bool
	if err = tx.QueryRow(ctx, `SELECT octet_length($1::jsonb::text)<=4096`, body).Scan(&bounded); err != nil {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	if !bounded {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	tag, err := tx.Exec(ctx, `UPDATE task_attempts SET state=$2,finished_at=clock_timestamp(),outcome_digest=$3,result_metadata=$4,last_error_class=$5 WHERE id=$1 AND state='RUNNING' AND lease_expires_at>clock_timestamp() AND cancel_requested_at IS NULL AND $6::timestamptz>clock_timestamp() AND $7::timestamptz>clock_timestamp()`, id, state, digest[:], body, class, a.DeadlineAt, p.deadline)
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	if tag.RowsAffected() != 1 {
		return execution.Attempt{}, execution.ErrStale
	}
	if command != nil {
		if err = s.acceptSandbox(ctx, tx, a, *command); err != nil {
			return a, err
		}
	}
	if !success && execution.Retryable(class) {
		now, e := dbNow(ctx, tx)
		if e != nil {
			return a, e
		}
		err = s.scheduleFailure(ctx, tx, &p, a.TaskID, class, now)
	} else {
		_, err = tx.Exec(ctx, `UPDATE tasks SET status=$2,last_error_class=$3,updated_at=clock_timestamp() WHERE id=$1`, a.TaskID, state, class)
	}
	if err != nil {
		return execution.Attempt{}, classify(err)
	}
	if err = obsoleteDispatch(ctx, tx, a.TaskID); err != nil {
		return a, err
	}
	if err = mutation(ctx, tx, p, p.status, []eventWrite{{kind, map[string]any{"task_id": a.TaskID, "attempt_id": id, "generation": generation}}}); err != nil {
		return execution.Attempt{}, err
	}
	a, err = readAttempt(ctx, tx, id)
	if err != nil {
		return a, err
	}
	if err = tx.Commit(ctx); err != nil {
		return execution.Attempt{}, classify(err)
	}
	return a, nil
}
func dispatch(ctx context.Context, tx pgx.Tx, task uuid.UUID, generation int64) error {
	id, err := uuid.NewRandom()
	if err != nil {
		return err
	}
	body, err := json.Marshal(execution.Notification{SchemaVersion: 1, TaskID: task, Generation: generation})
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO outbox(id,task_id,dispatch_generation,schema_version,payload,traceparent) SELECT $1,$2,$3,1,$4,w.traceparent FROM tasks t JOIN workflows w ON w.id=t.workflow_id WHERE t.id=$2`, id, task, generation, body)
	return classify(err)
}

type Dispatch struct {
	ID          uuid.UUID
	Payload     []byte
	MessageID   string
	Traceparent string
}

func (s *Store) ClaimOutbox(ctx context.Context, relay uuid.UUID, batch int, lease time.Duration) ([]Dispatch, error) {
	if relay == uuid.Nil || batch < 1 || batch > 100 || lease < time.Second || lease > time.Minute {
		return nil, workflow.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, classify(err)
	}
	defer rollback(tx)
	rows, err := tx.Query(ctx, `WITH picked AS (SELECT id FROM outbox WHERE state='PENDING' AND available_at<=clock_timestamp() AND (relay_lease_expires_at IS NULL OR relay_lease_expires_at<=clock_timestamp()) ORDER BY available_at,id LIMIT $2 FOR UPDATE SKIP LOCKED)
 UPDATE outbox o SET relay_id=$1,relay_lease_expires_at=clock_timestamp()+$3*interval '1 millisecond' FROM picked p WHERE o.id=p.id RETURNING o.id,o.payload,o.id::text||':'||o.publish_revision::text,o.traceparent`, relay, batch, lease.Milliseconds())
	if err != nil {
		return nil, classify(err)
	}
	result := []Dispatch{}
	for rows.Next() {
		var d Dispatch
		if err = rows.Scan(&d.ID, &d.Payload, &d.MessageID, &d.Traceparent); err != nil {
			rows.Close()
			return nil, classify(err)
		}
		result = append(result, d)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, classify(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, classify(err)
	}
	return result, nil
}
func (s *Store) MarkSent(ctx context.Context, relay, id uuid.UUID) error {
	tag, err := s.pool.Exec(ctx, `UPDATE outbox SET state='SENT',sent_at=clock_timestamp(),relay_id=NULL,relay_lease_expires_at=NULL WHERE id=$1 AND state='PENDING' AND relay_id=$2 AND relay_lease_expires_at>clock_timestamp()`, id, relay)
	if err != nil {
		return classify(err)
	}
	if tag.RowsAffected() != 1 {
		return execution.ErrStale
	}
	return nil
}

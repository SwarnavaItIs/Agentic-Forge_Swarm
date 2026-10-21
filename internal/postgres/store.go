package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"time"

	"forgeswarm/internal/execution"
	"forgeswarm/internal/workflow"
	"forgeswarm/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool   *pgxpool.Pool
	limits execution.Limits
	policy execution.Policy
	random func() float64
}

func New(pool *pgxpool.Pool) *Store {
	s, _ := NewWithPolicy(pool, execution.DefaultLimits(), execution.DefaultPolicy(), rand.Float64)
	return s
}
func NewWithLimits(pool *pgxpool.Pool, l execution.Limits) (*Store, error) {
	return NewWithPolicy(pool, l, execution.DefaultPolicy(), rand.Float64)
}
func NewConfigured(pool *pgxpool.Pool, l execution.Limits, p execution.Policy) (*Store, error) {
	return NewWithPolicy(pool, l, p, rand.Float64)
}
func NewWithPolicy(pool *pgxpool.Pool, l execution.Limits, p execution.Policy, rng func() float64) (*Store, error) {
	if !l.Valid() || !p.Valid() || rng == nil {
		return nil, workflow.ErrInvalid
	}
	return &Store{pool: pool, limits: l, policy: p, random: rng}, nil
}

// Cleanup has its own bounded context: request cancellation does not prevent a
// rollback from releasing transaction locks and returning/discarding the connection.
func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}

func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return workflow.ErrNotFound
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("%w: query interrupted", workflow.ErrUnavailable)
	}
	var netErr net.Error
	var pgErr *pgconn.PgError
	var connectErr *pgconn.ConnectError
	if errors.As(err, &netErr) || errors.As(err, &connectErr) || pgconn.SafeToRetry(err) || pgconn.Timeout(err) {
		return fmt.Errorf("%w: connection failure", workflow.ErrUnavailable)
	}
	if errors.As(err, &pgErr) {
		switch pgErr.Code {
		case "40001", "40P01", "53300", "53400", "55000", "55P03", "57014", "57P01", "57P02", "57P03", "42P01", "42703":
			return fmt.Errorf("%w: database temporarily unavailable", workflow.ErrUnavailable)
		}
		if len(pgErr.Code) >= 2 && pgErr.Code[:2] == "08" {
			return workflow.ErrUnavailable
		}
	}
	return fmt.Errorf("database operation: %w", err)
}

func (s *Store) Create(ctx context.Context, key string, in workflow.CreateInput) (workflow.Receipt, error) {
	if !workflow.ValidCreationKey(key) {
		return workflow.Receipt{}, workflow.ErrInvalid
	}
	in, err := workflow.Normalize(in)
	if err != nil {
		return workflow.Receipt{}, err
	}
	hash, err := workflow.HashForVersion(in, workflow.RequestSchemaVersion)
	if err != nil {
		return workflow.Receipt{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return workflow.Receipt{}, classify(err)
	}
	defer rollback(tx)
	if err = admission(ctx, tx); err != nil {
		return workflow.Receipt{}, err
	}
	// Resolve immutable creation receipts before checking new-admission limits.
	var prior workflow.Receipt
	var stored []byte
	var oldVersion int
	err = tx.QueryRow(ctx, `SELECT id,created_at,request_hash,request_schema_version FROM workflows WHERE creation_key=$1`, key).Scan(&prior.ID, &prior.CreatedAt, &stored, &oldVersion)
	if err == nil {
		h, e := workflow.HashForVersion(in, oldVersion)
		if e != nil {
			return prior, e
		}
		if !bytes.Equal(h[:], stored) {
			return prior, workflow.ErrConflict
		}
		if e = tx.Commit(ctx); e != nil {
			return workflow.Receipt{}, classify(e)
		}
		return prior, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return workflow.Receipt{}, classify(err)
	}
	if len(in.Tasks) > s.policy.MaxTasks {
		return workflow.Receipt{}, workflow.ErrInvalid
	}
	var workflows, queued int
	err = tx.QueryRow(ctx, `SELECT (SELECT count(*) FROM workflows WHERE status IN ('PENDING','RUNNING','CANCELLING')),(SELECT count(*) FROM tasks WHERE status IN ('PENDING','READY','RUNNING','RETRY_WAIT'))`).Scan(&workflows, &queued)
	if err != nil {
		return workflow.Receipt{}, classify(err)
	}
	if workflows >= s.policy.MaxWorkflows || queued+len(in.Tasks) > s.policy.MaxQueued {
		return workflow.Receipt{}, workflow.ErrSaturated
	}
	var now time.Time
	if err = tx.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return workflow.Receipt{}, classify(err)
	}
	timeout := s.policy.WorkflowTimeout
	if in.TimeoutMS > 0 {
		timeout = time.Duration(in.TimeoutMS) * time.Millisecond
	}
	deadline := now.Add(timeout)
	id, err := uuid.NewRandom()
	if err != nil {
		return workflow.Receipt{}, err
	}
	var receipt workflow.Receipt
	err = tx.QueryRow(ctx, `INSERT INTO workflows(id,name,status,version,creation_key,request_hash,request_schema_version,deadline_at)
		VALUES($1,$2,'PENDING',1,$3,$4,$5,$6) ON CONFLICT (creation_key) DO NOTHING RETURNING id,created_at`,
		id, in.Name, key, hash[:], workflow.RequestSchemaVersion, deadline).Scan(&receipt.ID, &receipt.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		// This MUST be a separate Read Committed statement: the winning row may
		// not have been visible to the INSERT's original snapshot.
		var storedHash []byte
		var version int
		err = tx.QueryRow(ctx, `SELECT id,created_at,request_hash,request_schema_version FROM workflows WHERE creation_key=$1`, key).
			Scan(&receipt.ID, &receipt.CreatedAt, &storedHash, &version)
		if err != nil {
			return workflow.Receipt{}, classify(err)
		}
		retryHash, err := workflow.HashForVersion(in, version)
		if err != nil {
			return workflow.Receipt{}, err
		}
		if !bytes.Equal(storedHash, retryHash[:]) {
			return workflow.Receipt{}, workflow.ErrConflict
		}
	} else {
		if err != nil {
			return workflow.Receipt{}, classify(err)
		}
		taskIDs := make(map[string]uuid.UUID, len(in.Tasks))
		for position, task := range in.Tasks {
			taskTimeout := s.policy.TaskTimeout
			if task.TimeoutMS > 0 {
				taskTimeout = time.Duration(task.TimeoutMS) * time.Millisecond
			}
			taskDeadline := now.Add(taskTimeout)
			if deadline.Before(taskDeadline) {
				taskDeadline = deadline
			}
			maxAttempts := task.MaxAttempts
			if maxAttempts == 0 {
				maxAttempts = s.limits.MaxAttempts
			}
			taskID, err := uuid.NewRandom()
			if err != nil {
				return workflow.Receipt{}, err
			}
			taskIDs[task.Key] = taskID
			_, err = tx.Exec(ctx, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,delay_ms,status,max_attempts,deadline_at,sandbox_input) VALUES($1,$2,$3,$4,$5,$6,'PENDING',$7,$8,$9)`, taskID, id, task.Key, position, task.Kind, task.DelayMS, maxAttempts, taskDeadline, task.Sandbox)
			if err != nil {
				return workflow.Receipt{}, classify(err)
			}
		}
		for _, task := range in.Tasks {
			for _, predecessor := range task.DependsOn {
				_, err = tx.Exec(ctx, `INSERT INTO task_dependencies(workflow_id,task_id,depends_on_task_id) VALUES($1,$2,$3)`, id, taskIDs[task.Key], taskIDs[predecessor])
				if err != nil {
					return workflow.Receipt{}, classify(err)
				}
			}
		}
		_, err = tx.Exec(ctx, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created',jsonb_build_object('task_count',$2::integer))`, id, len(in.Tasks))
		if err != nil {
			return workflow.Receipt{}, classify(err)
		}
	}
	// Commit failure may be ambiguous. Never return a receipt on error; retrying
	// the creation key resolves whether the server committed or rolled back.
	if err := tx.Commit(ctx); err != nil {
		return workflow.Receipt{}, classify(err)
	}
	return receipt, nil
}

func (s *Store) Cancel(ctx context.Context, id uuid.UUID) (workflow.CancellationResult, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	defer rollback(tx)
	p, err := lockParent(ctx, tx, id)
	if err != nil {
		return workflow.CancellationResult{}, err
	}
	before := p.sequence
	if err = beginCancellation(ctx, tx, &p, execution.ExplicitCancel); err != nil {
		return workflow.CancellationResult{}, err
	}
	if p.sequence != before {
		if _, err = tx.Exec(ctx, `UPDATE workflows SET status=$2,terminal_reason=$3,version=version+1,last_event_sequence=$4,updated_at=$5 WHERE id=$1`, id, p.status, p.reason, p.sequence, p.at); err != nil {
			return workflow.CancellationResult{}, classify(err)
		}
		p.version++
	}
	if err = tx.Commit(ctx); err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	return workflow.CancellationResult{ID: id, Status: p.status, Version: p.version}, nil
}

func (s *Store) Details(ctx context.Context, id uuid.UUID) (workflow.Details, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return workflow.Details{}, classify(err)
	}
	defer rollback(tx)
	var result workflow.Details
	err = tx.QueryRow(ctx, `SELECT id,name,status,version,created_at,updated_at,deadline_at,terminal_reason FROM workflows WHERE id=$1`, id).
		Scan(&result.ID, &result.Name, &result.Status, &result.Version, &result.CreatedAt, &result.UpdatedAt, &result.DeadlineAt, &result.TerminalReason)
	if err != nil {
		return workflow.Details{}, classify(err)
	}
	rows, err := tx.Query(ctx, `SELECT id,task_key,position,kind,status,created_at,updated_at FROM tasks WHERE workflow_id=$1 ORDER BY position`, id)
	if err != nil {
		return workflow.Details{}, classify(err)
	}
	result.Tasks = make([]workflow.Task, 0)
	for rows.Next() {
		var task workflow.Task
		if err := rows.Scan(&task.ID, &task.Key, &task.Position, &task.Kind, &task.Status, &task.CreatedAt, &task.UpdatedAt); err != nil {
			rows.Close()
			return workflow.Details{}, classify(err)
		}
		result.Tasks = append(result.Tasks, task)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return workflow.Details{}, classify(err)
	}
	if err := tx.Commit(ctx); err != nil {
		return workflow.Details{}, classify(err)
	}
	return result, nil
}

func (s *Store) Events(ctx context.Context, id uuid.UUID, after int64, limit int) (workflow.EventPage, error) {
	if after < 0 || limit < 1 || limit > 100 {
		return workflow.EventPage{}, workflow.ErrInvalid
	}
	// One statement snapshot also distinguishes a missing workflow from an empty
	// page. LEFT JOIN gives one NULL sequence for an existing, exhausted workflow.
	rows, err := s.pool.Query(ctx, `SELECT e.sequence,e.event_type,e.payload,e.occurred_at FROM workflows w
		LEFT JOIN LATERAL (SELECT sequence,event_type,payload,occurred_at FROM workflow_events
		WHERE workflow_id=w.id AND sequence>$2 ORDER BY sequence LIMIT $3) e ON true
		WHERE w.id=$1 ORDER BY e.sequence`, id, after, limit)
	if err != nil {
		return workflow.EventPage{}, classify(err)
	}
	defer rows.Close()
	result := workflow.EventPage{Events: make([]workflow.Event, 0), NextAfter: after}
	exists := false
	for rows.Next() {
		exists = true
		var sequence *int64
		var eventType *string
		var payload []byte
		var occurredAt *time.Time
		if err := rows.Scan(&sequence, &eventType, &payload, &occurredAt); err != nil {
			return workflow.EventPage{}, classify(err)
		}
		if sequence != nil {
			result.Events = append(result.Events, workflow.Event{Sequence: *sequence, EventType: *eventType, Payload: payload, OccurredAt: *occurredAt})
			result.NextAfter = *sequence
		}
	}
	if err := rows.Err(); err != nil {
		return workflow.EventPage{}, classify(err)
	}
	if !exists {
		return workflow.EventPage{}, workflow.ErrNotFound
	}
	return result, nil
}

func (s *Store) Ready(ctx context.Context) error {
	var version int64
	// Read-only schema probe; no automatic migrations or cached readiness flag.
	err := s.pool.QueryRow(ctx, `SELECT COALESCE(max(version_id),0) FROM goose_db_version WHERE is_applied`).Scan(&version)
	if err != nil {
		return classify(err)
	}
	if version != migrations.Version {
		return workflow.ErrUnavailable
	}
	rows, err := s.pool.Query(ctx, `SELECT w.id,w.name,w.status,w.version,w.creation_key,w.request_hash,w.request_schema_version,w.created_at,w.updated_at,
		w.last_event_sequence,w.scheduler_checked_at,t.id,t.workflow_id,t.task_key,t.position,t.kind,t.status,t.created_at,t.updated_at,e.workflow_id,e.sequence,e.event_type,e.payload,e.occurred_at,d.task_id,d.depends_on_task_id
		FROM workflows w LEFT JOIN tasks t ON t.workflow_id=w.id LEFT JOIN workflow_events e ON e.workflow_id=w.id LEFT JOIN task_dependencies d ON d.workflow_id=w.id
		LEFT JOIN task_attempts a ON a.id=t.current_attempt_id LEFT JOIN outbox o ON o.task_id=t.id
		LEFT JOIN sandbox_commands sc ON sc.attempt_id=a.id LEFT JOIN sandbox_executions se ON se.attempt_id=a.id
		LEFT JOIN sandbox_artifacts sa ON sa.attempt_id=a.id WHERE false`)
	if err != nil {
		return classify(err)
	}
	rows.Close()
	if err = rows.Err(); err != nil {
		return classify(err)
	}
	return s.CheckConsistency(ctx)
}

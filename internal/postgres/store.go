package postgres

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool, limits: execution.DefaultLimits()} }
func NewWithLimits(pool *pgxpool.Pool, l execution.Limits) (*Store, error) {
	if !l.Valid() {
		return nil, workflow.ErrInvalid
	}
	return &Store{pool: pool, limits: l}, nil
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
	id, err := uuid.NewRandom()
	if err != nil {
		return workflow.Receipt{}, err
	}
	var receipt workflow.Receipt
	err = tx.QueryRow(ctx, `INSERT INTO workflows(id,name,status,version,creation_key,request_hash,request_schema_version)
		VALUES($1,$2,'PENDING',1,$3,$4,$5) ON CONFLICT (creation_key) DO NOTHING RETURNING id,created_at`,
		id, in.Name, key, hash[:], workflow.RequestSchemaVersion).Scan(&receipt.ID, &receipt.CreatedAt)
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
			taskID, err := uuid.NewRandom()
			if err != nil {
				return workflow.Receipt{}, err
			}
			taskIDs[task.Key] = taskID
			_, err = tx.Exec(ctx, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,delay_ms,status) VALUES($1,$2,$3,$4,$5,$6,'PENDING')`, taskID, id, task.Key, position, task.Kind, task.DelayMS)
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
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	defer rollback(tx)
	result := workflow.CancellationResult{ID: id}
	var sequence int64
	err = tx.QueryRow(ctx, `SELECT status,version,last_event_sequence FROM workflows WHERE id=$1 FOR UPDATE`, id).Scan(&result.Status, &result.Version, &sequence)
	if err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	change, err := workflow.Cancellation(result.Status, result.Version)
	if err != nil {
		return workflow.CancellationResult{}, err
	}
	if change {
		_, err = tx.Exec(ctx, `UPDATE task_attempts SET state='CANCELLED',finished_at=clock_timestamp() WHERE workflow_id=$1 AND state='RUNNING'`, id)
		if err != nil {
			return workflow.CancellationResult{}, classify(err)
		}
		// Parent lock serializes all cancellation writers across every process.
		// clock_timestamp avoids moving updated_at backwards after a lock wait.
		var updatedAt time.Time
		err = tx.QueryRow(ctx, `UPDATE workflows SET status='CANCELLED',version=version+1,last_event_sequence=last_event_sequence+1,updated_at=clock_timestamp() WHERE id=$1 RETURNING status,version,updated_at`, id).
			Scan(&result.Status, &result.Version, &updatedAt)
		if err != nil {
			return workflow.CancellationResult{}, classify(err)
		}
		tag, err := tx.Exec(ctx, `UPDATE tasks SET status='CANCELLED',updated_at=$2 WHERE workflow_id=$1 AND status IN ('PENDING','READY','RUNNING')`, id, updatedAt)
		if err != nil {
			return workflow.CancellationResult{}, classify(err)
		}
		_, err = tx.Exec(ctx, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload,occurred_at)
			VALUES($1,$2,'workflow.cancelled',jsonb_build_object('cancelled_task_count',$3::integer),$4)`, id, sequence+1, int(tag.RowsAffected()), updatedAt)
		if err != nil {
			return workflow.CancellationResult{}, classify(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return workflow.CancellationResult{}, classify(err)
	}
	return result, nil
}

func (s *Store) Details(ctx context.Context, id uuid.UUID) (workflow.Details, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return workflow.Details{}, classify(err)
	}
	defer rollback(tx)
	var result workflow.Details
	err = tx.QueryRow(ctx, `SELECT id,name,status,version,created_at,updated_at FROM workflows WHERE id=$1`, id).
		Scan(&result.ID, &result.Name, &result.Status, &result.Version, &result.CreatedAt, &result.UpdatedAt)
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
		LEFT JOIN task_attempts a ON a.id=t.current_attempt_id LEFT JOIN outbox o ON o.task_id=t.id WHERE false`)
	if err != nil {
		return classify(err)
	}
	rows.Close()
	return classify(rows.Err())
}

package postgres

import (
	"context"
	"time"

	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Lock order for every state/history producer: workflow row first, then child
// tasks (stable topological order), then append-only history. A batch holds at
// most 100 parent locks. Short per-workflow serialization trades throughput for
// cancellation/history correctness, without any in-process leader mutex.
func readGraph(ctx context.Context, tx pgx.Tx, graph *workflow.Graph) error {
	rows, err := tx.Query(ctx, `SELECT id,task_key,position,kind,status,created_at,updated_at FROM tasks WHERE workflow_id=$1 ORDER BY position LIMIT $2`, graph.ID, workflow.MaxTasks+1)
	if err != nil {
		return classify(err)
	}
	graph.Nodes = make([]workflow.Task, 0)
	for rows.Next() {
		var task workflow.Task
		if err := rows.Scan(&task.ID, &task.Key, &task.Position, &task.Kind, &task.Status, &task.CreatedAt, &task.UpdatedAt); err != nil {
			rows.Close()
			return classify(err)
		}
		graph.Nodes = append(graph.Nodes, task)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return classify(err)
	}
	if len(graph.Nodes) > workflow.MaxTasks {
		return workflow.ErrState
	}
	rows, err = tx.Query(ctx, `SELECT d.task_id,d.depends_on_task_id FROM task_dependencies d JOIN tasks t ON t.id=d.task_id AND t.workflow_id=d.workflow_id JOIN tasks p ON p.id=d.depends_on_task_id AND p.workflow_id=d.workflow_id WHERE d.workflow_id=$1 ORDER BY t.position,p.position LIMIT $2`, graph.ID, workflow.MaxEdges+1)
	if err != nil {
		return classify(err)
	}
	graph.Edges = make([]workflow.Edge, 0)
	for rows.Next() {
		var edge workflow.Edge
		if err := rows.Scan(&edge.TaskID, &edge.DependsOnTaskID); err != nil {
			rows.Close()
			return classify(err)
		}
		graph.Edges = append(graph.Edges, edge)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return classify(err)
	}
	if len(graph.Edges) > workflow.MaxEdges {
		return workflow.ErrState
	}
	return nil
}

// Graph returns a bounded consistent snapshot, never node/edge projections from
// different revisions. Cancellation and scans may commit while this read runs.
func (s *Store) Graph(ctx context.Context, id uuid.UUID) (workflow.Graph, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return workflow.Graph{}, classify(err)
	}
	defer rollback(tx)
	graph := workflow.Graph{ID: id}
	err = tx.QueryRow(ctx, `SELECT status,version FROM workflows WHERE id=$1`, id).Scan(&graph.Status, &graph.Version)
	if err != nil {
		return workflow.Graph{}, classify(err)
	}
	if err = readGraph(ctx, tx, &graph); err != nil {
		return workflow.Graph{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return workflow.Graph{}, classify(err)
	}
	return graph, nil
}

type lockedWorkflow struct {
	id                uuid.UUID
	status            string
	version, sequence int64
	deadline          time.Time
	reason            string
	at                time.Time
}
type ScanResult struct {
	Workflows   int
	Transitions int
}

func (s *Store) reconcileLocked(ctx context.Context, tx pgx.Tx, parent lockedWorkflow) (int, error) {
	if parent.status != workflow.Pending && parent.status != workflow.Running && parent.status != workflow.Cancelling {
		return 0, nil
	}
	initialSequence := parent.sequence
	if err := s.recoverLocked(ctx, tx, &parent); err != nil {
		return 0, err
	}
	if parent.status == workflow.Pending || parent.status == workflow.Running {
		graph := workflow.Graph{ID: parent.id, Status: parent.status, Version: parent.version}
		if err := readGraph(ctx, tx, &graph); err != nil {
			return 0, err
		}
		decision, err := workflow.Reconcile(graph)
		if err != nil {
			return 0, err
		}
		for _, transition := range decision.Transitions {
			if transition.To == workflow.Ready {
				room, e := s.readyRoom(ctx, tx)
				if e != nil {
					return 0, e
				}
				if !room {
					continue
				}
				if err = s.makeReady(ctx, tx, &parent, transition.TaskID, true); err != nil {
					return 0, err
				}
			} else {
				tag, e := tx.Exec(ctx, `UPDATE tasks SET status=$3,updated_at=clock_timestamp() WHERE workflow_id=$1 AND id=$2 AND status=$4`, parent.id, transition.TaskID, transition.To, transition.From)
				if e != nil {
					return 0, classify(e)
				}
				if tag.RowsAffected() != 1 {
					return 0, workflow.ErrState
				}
				if err = obsoleteDispatch(ctx, tx, transition.TaskID); err != nil {
					return 0, err
				}
				if err = appendHistory(ctx, tx, &parent, "task.blocked", map[string]any{"task_id": transition.TaskID, "from": transition.From, "to": transition.To}); err != nil {
					return 0, err
				}
			}
		}
		if decision.Status != parent.status {
			parent.status = decision.Status
			event := "workflow.succeeded"
			if parent.status == workflow.Failed {
				event = "workflow.failed"
			}
			if err = appendHistory(ctx, tx, &parent, event, map[string]string{"status": parent.status}); err != nil {
				return 0, err
			}
		}
	}
	var err error
	if parent.sequence != initialSequence {
		_, err = tx.Exec(ctx, `UPDATE workflows SET status=$2,terminal_reason=$3,version=version+1,last_event_sequence=$4,updated_at=clock_timestamp(),scheduler_checked_at=clock_timestamp() WHERE id=$1`, parent.id, parent.status, parent.reason, parent.sequence)
	} else {
		_, err = tx.Exec(ctx, `UPDATE workflows SET scheduler_checked_at=clock_timestamp() WHERE id=$1`, parent.id)
	}
	return int(parent.sequence - initialSequence), classify(err)
}

func (s *Store) Reconcile(ctx context.Context, id uuid.UUID) (int, error) {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return 0, classify(err)
	}
	defer rollback(tx)
	if err = admission(ctx, tx); err != nil {
		return 0, err
	}
	var parent lockedWorkflow
	err = tx.QueryRow(ctx, `SELECT id,status,version,last_event_sequence,deadline_at,terminal_reason FROM workflows WHERE id=$1 FOR UPDATE`, id).Scan(&parent.id, &parent.status, &parent.version, &parent.sequence, &parent.deadline, &parent.reason)
	if err != nil {
		return 0, classify(err)
	}
	count, err := s.reconcileLocked(ctx, tx, parent)
	if err != nil {
		return 0, err
	}
	if err = tx.Commit(ctx); err != nil {
		return 0, classify(err)
	}
	return count, nil
}

// SKIP LOCKED avoids a competing scheduler/cancellation holding a parent.
// It gives no strict fairness guarantee for a perpetually locked workflow.
// Oldest checked-at first prevents unchanged early READY workflows starving
// later roots; every bounded scan persists its check time even on a no-op.
func (s *Store) ReconcileBatch(ctx context.Context, batch int) (ScanResult, error) {
	if batch < 1 || batch > 100 {
		return ScanResult{}, workflow.ErrInvalid
	}
	if err := s.Ready(ctx); err != nil {
		return ScanResult{}, err
	}
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return ScanResult{}, classify(err)
	}
	defer rollback(tx)
	if err = admission(ctx, tx); err != nil {
		return ScanResult{}, err
	}
	if err = s.checkCapacity(ctx, tx); err != nil {
		return ScanResult{}, err
	}
	rows, err := tx.Query(ctx, `SELECT id,status,version,last_event_sequence,deadline_at,terminal_reason FROM workflows WHERE status IN ('PENDING','RUNNING','CANCELLING') ORDER BY scheduler_checked_at NULLS FIRST,created_at,id LIMIT $1 FOR UPDATE SKIP LOCKED`, batch)
	if err != nil {
		return ScanResult{}, classify(err)
	}
	parents := make([]lockedWorkflow, 0, batch)
	for rows.Next() {
		var parent lockedWorkflow
		if err = rows.Scan(&parent.id, &parent.status, &parent.version, &parent.sequence, &parent.deadline, &parent.reason); err != nil {
			rows.Close()
			return ScanResult{}, classify(err)
		}
		parents = append(parents, parent)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ScanResult{}, classify(err)
	}
	result := ScanResult{Workflows: len(parents)}
	for _, parent := range parents {
		count, err := s.reconcileLocked(ctx, tx, parent)
		if err != nil {
			return ScanResult{}, err
		}
		result.Transitions += count
	}
	if err = tx.Commit(ctx); err != nil {
		return ScanResult{}, classify(err)
	}
	return result, nil
}

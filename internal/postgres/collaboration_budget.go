package postgres

import (
	"context"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/execution"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func (s *Store) roleLive(ctx context.Context, tx pgx.Tx, a execution.Attempt) error {
	if a.Agent == nil || a.Agent.Role == nil {
		return nil
	}
	var live bool
	err := tx.QueryRow(ctx, `SELECT p.status IN ('PLANNING','EXECUTING') AND p.deadline_at>clock_timestamp() FROM collab_projects p JOIN collab_workflows f ON f.root_id=p.root_id WHERE f.workflow_id=$1`, a.WorkflowID).Scan(&live)
	if err != nil {
		return classify(err)
	}
	if !live {
		return execution.ErrStale
	}
	return nil
}
func (s *Store) reserveRoleBudget(ctx context.Context, tx pgx.Tx, a execution.Attempt, tokens int, cost int64) error {
	var root uuid.UUID
	var maximum int64
	var live bool
	err := tx.QueryRow(ctx, `SELECT p.root_id,p.maximum_microusd,p.status IN ('PLANNING','EXECUTING') AND p.deadline_at>clock_timestamp() FROM collab_projects p JOIN collab_workflows f ON f.root_id=p.root_id WHERE f.workflow_id=$1 FOR UPDATE OF p`, a.WorkflowID).Scan(&root, &maximum, &live)
	if err != nil {
		return classify(err)
	}
	if !live {
		return execution.ErrStale
	}
	var usedTokens, turns int
	var usedCost int64
	err = tx.QueryRow(ctx, `SELECT COALESCE(sum(s.tokens),0),COALESCE(sum(s.turns),0),COALESCE(sum(s.estimated_microusd),0) FROM agent_sessions s JOIN task_attempts a ON a.id=s.attempt_id JOIN collab_workflows f ON f.workflow_id=a.workflow_id WHERE f.root_id=$1`, root).Scan(&usedTokens, &turns, &usedCost)
	if err != nil {
		return classify(err)
	}
	if usedTokens+tokens > agent.WorkflowTokens || turns >= agent.WorkflowTurns || usedCost+cost > maximum {
		return execution.ErrCapacity
	}
	return nil
}

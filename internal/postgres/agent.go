package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"slices"
	"time"
)

// ConfigureAgent is startup-only; all coordinators sharing a database must use
// the same operator pricing policy. An attempt pins its policy on first use.
func (s *Store) ConfigureAgent(p agent.Pricing) error {
	if !p.Valid() {
		return workflow.ErrInvalid
	}
	s.agentPricing = &p
	return nil
}

func (s *Store) AgentEvent(ctx context.Context, worker string, instance, id uuid.UUID, generation int64, e agent.Event) (execution.Attempt, error) {
	request, err := uuid.Parse(e.ID)
	if err != nil || request == uuid.Nil || !slices.Contains([]string{"model", "tool"}, e.Kind) || !slices.Contains([]string{"begin", "end"}, e.Phase) || !agent.ValidDigest(e.InputDigest) || !agent.ValidDigest(e.Workspace) || len(e.Name) < 1 || len(e.Name) > 64 || len(e.Status) > 32 || s.agentPricing == nil {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	if e.Phase == "end" && (!agent.ValidDigest(e.OutputDigest) || !agent.ValidDigest(e.AfterWorkspace)) {
		return execution.Attempt{}, workflow.ErrInvalid
	}
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
	if a.Kind != "agent_go" || a.Agent == nil {
		return a, workflow.ErrInvalid
	}
	_, err = tx.Exec(ctx, `INSERT INTO agent_sessions(attempt_id,generation,base_sha256,workspace_sha256,deadline_at,pricing) VALUES($1,$2,$3,$3,LEAST($4,clock_timestamp()+interval '90 seconds'),$5) ON CONFLICT DO NOTHING`, id, generation, a.Agent.RepositoryID, a.DeadlineAt, s.agentPricing.Bytes())
	if err != nil {
		return a, classify(err)
	}
	var workspace string
	var deadline time.Time
	var turns, tools, denials, tokens int
	var cost int64
	var pendingModel, pendingTool *uuid.UUID
	var pricing agent.Pricing
	err = tx.QueryRow(ctx, `SELECT workspace_sha256,deadline_at,turns,tools,denials,tokens,estimated_microusd,pending_model,pending_tool,pricing FROM agent_sessions WHERE attempt_id=$1 FOR UPDATE`, id).Scan(&workspace, &deadline, &turns, &tools, &denials, &tokens, &cost, &pendingModel, &pendingTool, &pricing)
	if err != nil {
		return a, classify(err)
	}
	// Compare duplicate identity before counters. A recorded call is never an
	// invitation to repeat an uncertain provider/edit side effect.
	table := "agent_steps"
	if e.Kind == "model" {
		table = "model_calls"
	}
	var oldInput, oldOutput string
	err = tx.QueryRow(ctx, `SELECT input_sha256,output_sha256 FROM `+table+` WHERE attempt_id=$1 AND request_id=$2 AND phase=$3`, id, request, e.Phase).Scan(&oldInput, &oldOutput)
	if err == nil {
		if oldInput != e.InputDigest || e.Phase == "end" && oldOutput != e.OutputDigest {
			return a, execution.ErrOutcome
		}
		return a, execution.ErrOutcome
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return a, classify(err)
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return a, err
	}
	if !now.Before(deadline) {
		return a, execution.ErrStale
	}
	if e.Kind == "model" {
		if e.Phase == "begin" {
			if pendingModel != nil || pendingTool != nil || turns >= agent.MaxTurns || denials >= agent.MaxDenials || e.PromptUpper < 1 || e.PromptUpper > 30000 || e.Workspace != workspace {
				return a, workflow.ErrInvalid
			}
			reserve := e.PromptUpper + agent.MaxOutput
			money := pricing.Estimate(e.PromptUpper, agent.MaxOutput)
			// Parent ownership lock also serializes reservations across retries.
			// Uncertain calls on an old attempt retain their charge; a fresh
			// workspace never grants a fresh task spending/token allowance.
			var taskTokens, taskTurns int
			var taskCost, taskMaximum int64
			err = tx.QueryRow(ctx, `SELECT COALESCE(sum(s.tokens),0),COALESCE(sum(s.turns),0),COALESCE(sum(s.estimated_microusd),0),min((s.pricing->>'maximum_estimate_microusd')::bigint) FROM agent_sessions s JOIN task_attempts a ON a.id=s.attempt_id WHERE a.task_id=$1`, a.TaskID).Scan(&taskTokens, &taskTurns, &taskCost, &taskMaximum)
			if err != nil {
				return a, classify(err)
			}
			if tokens+reserve > agent.MaxTokens || cost+money > pricing.Maximum || taskTokens+reserve > agent.MaxTokens || taskCost+money > taskMaximum || taskTurns >= agent.MaxTurns {
				// Keep the pinned session even when its first reservation is
				// denied, so the worker can durably finish budget_exhausted.
				// No provider call, pending reservation or usage is created.
				if err = tx.Commit(ctx); err != nil {
					return a, classify(err)
				}
				return a, execution.ErrCapacity
			}
			_, err = tx.Exec(ctx, `INSERT INTO model_calls(attempt_id,request_id,phase,generation,input_sha256,output_sha256,prompt_token_upper,reserved_tokens,estimated_microusd,metadata) VALUES($1,$2,'begin',$3,$4,'',$5,$6,$7,'{"usage":"reserved_estimate"}')`, id, request, generation, e.InputDigest, e.PromptUpper, reserve, money)
			if err == nil {
				_, err = tx.Exec(ctx, `UPDATE agent_sessions SET pending_model=$2,turns=turns+1,tokens=tokens+$3,estimated_microusd=estimated_microusd+$4 WHERE attempt_id=$1`, id, request, reserve, money)
			}
		} else {
			if pendingModel == nil || *pendingModel != request || e.Reply == nil || !agent.ValidReply(*e.Reply) {
				return a, workflow.ErrInvalid
			}
			var prompt, reserve int
			var money int64
			var input string
			err = tx.QueryRow(ctx, `SELECT prompt_token_upper,reserved_tokens,estimated_microusd,input_sha256 FROM model_calls WHERE attempt_id=$1 AND request_id=$2 AND phase='begin'`, id, request).Scan(&prompt, &reserve, &money, &input)
			if err != nil {
				return a, classify(err)
			}
			if input != e.InputDigest {
				return a, execution.ErrOutcome
			}
			reply := *e.Reply
			reply.Action = nil // Never log model text/source or reasoning.
			used, charged := reserve, money
			usage := "unknown_retained_reservation"
			if reply.InputTokens != nil && reply.OutputTokens != nil {
				used = *reply.InputTokens + *reply.OutputTokens
				charged = pricing.Estimate(*reply.InputTokens, *reply.OutputTokens)
				usage = "provider_reported_estimate"
				if *reply.InputTokens > prompt || *reply.OutputTokens > agent.MaxOutput || used > reserve || charged > money {
					return a, execution.ErrCapacity
				}
			}
			metadata, _ := json.Marshal(map[string]any{"usage": usage, "provider": reply.Provider, "model": reply.Model, "request_id": reply.RequestID, "status": reply.Status, "input_tokens": reply.InputTokens, "output_tokens": reply.OutputTokens, "latency_ms": reply.LatencyMS, "pricing_date": pricing.VerifiedAt})
			if reply.Provider == "openai" && (reply.Model != pricing.Model) {
				return a, workflow.ErrInvalid
			}
			_, err = tx.Exec(ctx, `INSERT INTO model_calls(attempt_id,request_id,phase,generation,input_sha256,output_sha256,prompt_token_upper,reserved_tokens,estimated_microusd,metadata) VALUES($1,$2,'end',$3,$4,$5,$6,$7,$8,$9)`, id, request, generation, input, e.OutputDigest, prompt, used, charged, metadata)
			if err == nil {
				_, err = tx.Exec(ctx, `UPDATE agent_sessions SET pending_model=NULL,tokens=tokens-$2+$3,estimated_microusd=estimated_microusd-$4+$5 WHERE attempt_id=$1`, id, reserve, used, money, charged)
			}
		}
	} else {
		if e.Phase == "begin" {
			if pendingModel != nil || pendingTool != nil || tools >= agent.MaxTools || denials >= agent.MaxDenials || e.Workspace != workspace {
				return a, execution.ErrCapacity
			}
			_, err = tx.Exec(ctx, `UPDATE agent_sessions SET pending_tool=$2,tools=tools+1 WHERE attempt_id=$1`, id, request)
		} else {
			if pendingTool == nil || *pendingTool != request {
				return a, execution.ErrOutcome
			}
			var input, name, previous string
			err = tx.QueryRow(ctx, `SELECT input_sha256,name,workspace_sha256 FROM agent_steps WHERE attempt_id=$1 AND request_id=$2 AND phase='begin'`, id, request).Scan(&input, &name, &previous)
			if err != nil {
				return a, classify(err)
			}
			if input != e.InputDigest || name != e.Name || previous != e.Workspace || e.Name != "apply_patch" && e.AfterWorkspace != e.Workspace {
				return a, execution.ErrOutcome
			}
			denied := 0
			if e.Status == "denied" || e.Status == "conflict" {
				denied = 1
			}
			_, err = tx.Exec(ctx, `UPDATE agent_sessions SET pending_tool=NULL,workspace_sha256=$2,denials=denials+$3 WHERE attempt_id=$1`, id, e.AfterWorkspace, denied)
		}
		if err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO agent_steps(attempt_id,request_id,phase,generation,name,input_sha256,output_sha256,workspace_sha256,after_workspace_sha256,status) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)`, id, request, e.Phase, generation, e.Name, e.InputDigest, e.OutputDigest, e.Workspace, e.AfterWorkspace, e.Status)
		}
	}
	if err != nil {
		return a, classify(err)
	}
	if err = tx.Commit(ctx); err != nil {
		return a, classify(err)
	}
	return a, nil
}

func (s *Store) FinishAgent(ctx context.Context, worker string, instance, id uuid.UUID, generation int64, o agent.Outcome) (execution.Attempt, error) {
	if !slices.Contains([]string{"accepted", "failed", "budget_exhausted", "provider_error", "denied"}, o.Status) || !agent.ValidDigest(o.Base) || !agent.ValidDigest(o.Candidate) || len(o.Patch) > sandbox.MaxArtifact || o.Status == "accepted" && !o.Success() {
		return execution.Attempt{}, workflow.ErrInvalid
	}
	var command *sandbox.Result
	if o.Test != nil {
		if !o.Test.Valid() {
			return execution.Attempt{}, workflow.ErrInvalid
		}
		copyResult := *o.Test
		copyResult.Artifacts = append([]sandbox.Artifact(nil), o.Test.Artifacts...)
		for i := range copyResult.Artifacts {
			if copyResult.Artifacts[i].Kind == "patch" {
				copyResult.Artifacts[i].Body = o.Patch
				copyResult.Artifacts[i].Truncated = false
			}
		}
		command = &copyResult
	}
	refs := []map[string]any{}
	if command != nil {
		refs = artifactReferences(id, *command)
	}
	body, _ := json.Marshal(map[string]any{"schema_version": 1, "agent_status": o.Status, "candidate_sha256": o.Candidate, "base_revision": o.BaseRevision, "patch_sha256": agent.SHA(o.Patch), "test_sha256": o.TestDigest, "outcome_sha256": agent.JSONSHA(o), "artifacts": refs})
	class := ""
	if !o.Success() {
		class = execution.Diagnostic
	}
	return s.finishAttempt(ctx, worker, instance, id, generation, o.Success(), class, body, command, &o)
}
func (s *Store) validateAgent(ctx context.Context, tx pgx.Tx, a execution.Attempt, o agent.Outcome) error {
	if a.Agent == nil || o.Base != a.Agent.RepositoryID || o.BaseRevision != a.Agent.BaseRevision {
		return workflow.ErrInvalid
	}
	var current string
	var pendingModel, pendingTool *uuid.UUID
	var deadline time.Time
	err := tx.QueryRow(ctx, `SELECT workspace_sha256,pending_model,pending_tool,deadline_at FROM agent_sessions WHERE attempt_id=$1`, a.ID).Scan(&current, &pendingModel, &pendingTool, &deadline)
	if err != nil {
		return classify(err)
	}
	if current != o.Candidate || pendingTool != nil {
		return execution.ErrOutcome
	}
	if o.Status == "accepted" {
		if pendingModel != nil {
			return execution.ErrOutcome
		}
		now, e := dbNow(ctx, tx)
		if e != nil {
			return e
		}
		if !now.Before(deadline) {
			return execution.ErrStale
		}
		var proved bool
		err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_steps WHERE attempt_id=$1 AND request_id=$2 AND phase='end' AND name='run_test_profile' AND status='passed' AND workspace_sha256=$3 AND after_workspace_sha256=$3 AND output_sha256=$4)`, a.ID, o.TestID, o.Candidate, o.TestDigest).Scan(&proved)
		if err != nil {
			return classify(err)
		}
		if !proved {
			return execution.ErrOutcome
		}
	}
	return nil
}

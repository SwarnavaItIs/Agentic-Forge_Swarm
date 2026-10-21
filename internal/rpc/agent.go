package rpc

import (
	"context"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type agentStore interface {
	AgentEvent(context.Context, string, uuid.UUID, uuid.UUID, int64, agent.Event) (execution.Attempt, error)
	FinishAgent(context.Context, string, uuid.UUID, uuid.UUID, int64, agent.Outcome) (execution.Attempt, error)
}

func agentIDs(r *workerv1.AttemptRequest) (uuid.UUID, uuid.UUID, error) {
	if r == nil || r.Generation < 1 {
		return uuid.Nil, uuid.Nil, status.Error(codes.InvalidArgument, "attempt identity")
	}
	i, e := id(r.InstanceId)
	if e != nil {
		return i, uuid.Nil, e
	}
	a, e := id(r.AttemptId)
	return i, a, e
}
func (s *Server) AgentEvent(ctx context.Context, r *workerv1.AgentEventRequest) (*workerv1.Attempt, error) {
	i, a, e := agentIDs(r.Attempt)
	if e != nil {
		return nil, e
	}
	var event agent.Event
	if sandbox.Decode(r.EventJson, &event, 4096) != nil {
		return nil, status.Error(codes.InvalidArgument, "event schema")
	}
	store, ok := s.store.(agentStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "agent unavailable")
	}
	value, e := store.AgentEvent(ctx, Identity(ctx), i, a, r.Attempt.Generation, event)
	if e != nil {
		return nil, safe(e)
	}
	return attempt(value), nil
}
func (s *Server) CompleteAgent(ctx context.Context, r *workerv1.AgentOutcome) (*workerv1.Attempt, error) {
	i, a, e := agentIDs(r.Attempt)
	if e != nil {
		return nil, e
	}
	var outcome agent.Outcome
	if sandbox.Decode(r.OutcomeJson, &outcome, 56<<10) != nil {
		return nil, status.Error(codes.InvalidArgument, "outcome schema")
	}
	store, ok := s.store.(agentStore)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "agent unavailable")
	}
	value, e := store.FinishAgent(ctx, Identity(ctx), i, a, r.Attempt.Generation, outcome)
	if e != nil {
		return nil, safe(e)
	}
	return attempt(value), nil
}

package worker

import (
	"context"
	"encoding/json"
	"forgeswarm/api/agent/v1"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/config"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type AgentRuntime struct{ Client agentv1.RuntimeClient }

func ExecuteAgent(ctx context.Context, client workerv1.CoordinatorClient, c config.Distributed, driver *sandbox.Docker, runtime *AgentRuntime, a *workerv1.Attempt) (agent.Outcome, error) {
	b, err := binding(driver, a)
	if err != nil {
		return agent.Outcome{}, err
	}
	var in agent.Input
	if sandbox.Decode(a.AgentInputJson, &in, 4096) != nil || !in.Valid() {
		return agent.Outcome{}, sandbox.ErrPolicy
	}
	base, err := sandbox.LoadSnapshot(driver.Registry, in.RepositoryID)
	if err != nil {
		return agent.Outcome{}, err
	}
	owned := &workerv1.AttemptRequest{InstanceId: b.Instance.String(), AttemptId: b.Attempt.String(), Generation: b.Generation}
	authorize := func(ctx context.Context) error {
		call, done := context.WithTimeout(ctx, c.RPCTimeout)
		defer done()
		_, e := client.AuthorizeTool(call, owned)
		return e
	}
	gateway := agent.Gateway{Next: agent.NextAction(runtime.Client), Authorize: authorize,
		Journal: func(ctx context.Context, event agent.Event) error {
			wire, _ := json.Marshal(event)
			call, done := context.WithTimeout(ctx, c.RPCTimeout)
			defer done()
			_, e := client.AgentEvent(call, &workerv1.AgentEventRequest{Attempt: owned, EventJson: wire})
			if status.Code(e) == codes.ResourceExhausted {
				return agent.ErrBudget
			}
			return e
		},
		Test: func(ctx context.Context, s sandbox.Snapshot) (sandbox.Result, error) {
			_, id, e := sandbox.SnapshotBytes(s)
			if e != nil {
				return sandbox.Result{}, e
			}
			authorizeSandbox := func(ctx context.Context) error {
				call, done := context.WithTimeout(ctx, c.RPCTimeout)
				defer done()
				_, e := client.AuthorizeSandbox(call, &workerv1.SandboxRequest{Attempt: owned, RequestId: sandbox.RequestID(uuid.MustParse(a.Id)).String()})
				return e
			}
			profile := agent.Profile
			if in.Role != nil {
				profile = in.Role.Profile
			}
			return driver.RunSnapshot(ctx, driver.Request(b, sandbox.Input{SchemaVersion: 1, FixtureID: id, Command: "test"}, sandbox.DefaultPolicy()), s, profile, authorizeSandbox)
		},
	}
	if in.Role != nil {
		call, done := context.WithTimeout(ctx, c.RPCTimeout)
		wire, e := client.PrepareRole(call, owned)
		done()
		if e != nil {
			return agent.Outcome{}, e
		}
		var work agent.RoleWork
		if sandbox.Decode(wire.WorkJson, &work, 32<<10) != nil {
			return agent.Outcome{}, sandbox.ErrPolicy
		}
		return agent.RunRole(ctx, in, b.Attempt, b.Generation, work, agent.RoleGateway{Gateway: gateway, NextRole: func(call context.Context, n agent.RoleNext) (agent.Reply, error) {
			body, _ := json.Marshal(n)
			wire, e := runtime.Client.Collaborate(call, &agentv1.NextRequest{ContextJson: body})
			if e != nil {
				return agent.Reply{}, e
			}
			var reply agent.Reply
			if sandbox.Decode(wire.ResponseJson, &reply, 32<<10) != nil || !agent.ValidReply(reply) {
				return reply, sandbox.ErrPolicy
			}
			return reply, nil
		}})
	}
	return agent.Run(ctx, in, b.Attempt, b.Generation, base, gateway)
}

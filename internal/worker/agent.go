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
			return driver.RunSnapshot(ctx, driver.Request(b, sandbox.Input{SchemaVersion: 1, FixtureID: id, Command: "test"}, sandbox.DefaultPolicy()), s, agent.Profile, authorizeSandbox)
		},
	}
	return agent.Run(ctx, in, b.Attempt, b.Generation, base, gateway)
}

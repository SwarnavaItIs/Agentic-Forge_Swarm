package agent

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeFailureJournalKeepsOnlyBoundedRPCCode(t *testing.T) {
	for _, role := range []bool{false, true} {
		var recorded []Event
		g := Gateway{
			Authorize: func(context.Context) error { return nil },
			Journal:   func(_ context.Context, event Event) error { recorded = append(recorded, event); return nil },
			Test: func(context.Context, sandbox.Snapshot) (sandbox.Result, error) {
				t.Fatal("unexpected test execution")
				return sandbox.Result{}, nil
			},
			Next: func(context.Context, Next) (Reply, error) {
				return Reply{}, status.Error(codes.ResourceExhausted, "private-source-and-credential")
			},
		}
		var out Outcome
		var err error
		if role {
			base, job := mathFixture()
			in := Input{Job: job, Role: &RoleSpec{1, "planner", 1, "plan", []string{}, MathProfile, 4}}
			out, err = RunRole(context.Background(), in, uuid.New(), 1, RoleWork{Origin: base, Snapshot: base}, RoleGateway{Gateway: g, NextRole: func(context.Context, RoleNext) (Reply, error) { return g.Next(context.Background(), Next{}) }})
		} else {
			base, in := fixture()
			out, err = Run(context.Background(), in, uuid.New(), 1, base, g)
		}
		if err != nil || out.Status != "provider_error" || len(recorded) != 2 || recorded[1].Reply == nil || recorded[1].Reply.Model != "runtime_rpc_ResourceExhausted" {
			t.Fatalf("role=%v outcome=%s journal=%v err=%v", role, out.Status, recorded, err)
		}
		wire, _ := json.Marshal(recorded)
		if strings.Contains(string(wire), "private-source-and-credential") {
			t.Fatal("RPC error content leaked into journal")
		}
	}
}

package evaluation

import (
	"context"
	"encoding/json"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/evalcatalog"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"strings"
	"testing"
	"time"
)

func unitAuthority(task evalcatalog.Task, wrongBase bool) Test {
	return func(ctx context.Context, s sandbox.Snapshot) (sandbox.Result, error) {
		passing := agent.JSONSHA(s.Files) == agent.JSONSHA(task.FixtureCandidate())
		base := task.BaseCommit()
		if wrongBase {
			base = strings.Repeat("f", 40)
		}
		body, _ := json.Marshal(map[string]any{"success": passing, "authority": "immutable-helper", "profile": "eval:" + task.ID, "catalog_sha256": evalcatalog.CatalogDigest(), "candidate_sha256": agent.SnapshotDigest(s), "base_revision": base, "cases": len(task.ExpectedTests()), "passed": len(task.ExpectedTests())})
		result := sandbox.Result{SchemaVersion: 1, Status: "nonzero_exit", ExitCode: 1, Image: "sha256:" + strings.Repeat("a", 64), HelperSHA256: strings.Repeat("b", 64), ContainerID: strings.Repeat("c", 64), IsolationMode: "rootful_linux", BaseRevision: s.BaseRevision, Policy: sandbox.DefaultPolicy(), CleanupConfirmed: true, Artifacts: []sandbox.Artifact{{Kind: "test_result", MediaType: "text/plain; charset=utf-8", Body: body}}}
		if passing {
			result.Status = "exited"
			result.ExitCode = 0
		}
		return result, ctx.Err()
	}
}
func fixtureEngine(task evalcatalog.Task) Engine {
	return Engine{Provider: "scripted_TEST_DEMO", Model: "fixture-v1", Next: Fixture(task), Test: unitAuthority(task, false)}
}
func TestEqualAllRoleBudgetsAndFixtureRepeatability(t *testing.T) {
	for _, task := range evalcatalog.All() {
		for _, config := range Configurations {
			e := fixtureEngine(task)
			first := e.Run(context.Background(), task, config, 0, 1, DefaultBudget())
			second := e.Run(context.Background(), task, config, 0, 1, DefaultBudget())
			if first.Outcome != "passed" {
				t.Fatalf("%s/%s: %s %v", task.ID, config, first.Outcome, first.PolicyViolations)
			}
			if Normalized(first) != Normalized(second) {
				t.Fatal("scripted plumbing changed across identical fixtures")
			}
			minimum := 3
			if config != "developer" {
				minimum++
			}
			if config == "planner_developer_reviewer" || config == "full_bounded" {
				minimum++
			}
			if first.ModelCallsAttempted < minimum || first.Budget != DefaultBudget() {
				t.Fatal("planning/review calls escaped shared budgets")
			}
			if first.EstimatedMicrousd != nil {
				t.Fatal("fixture cost is unknown, not zero")
			}
		}
	}
	task, _ := evalcatalog.Get("add")
	e := fixtureEngine(task)
	budget := DefaultBudget()
	budget.Calls = 3
	if value := e.Run(context.Background(), task, "planner_developer_reviewer", 0, 1, budget); value.Outcome != "budget_exhausted" || value.ModelCallsAttempted != 3 {
		t.Fatal("all-role ceiling not enforced")
	}
}
func TestFakeSuccessWrongBaseTimeoutAndRoleEnforcement(t *testing.T) {
	task, _ := evalcatalog.Get("add")
	e := fixtureEngine(task)
	e.Next = func(context.Context, agent.Next) (agent.Reply, error) {
		return agent.Reply{Status: "ok", Provider: "scripted_TEST_DEMO", Model: "fixture-v1", Action: &agent.Action{SchemaVersion: 1, ID: uuid.NewString(), Tool: "final", Args: json.RawMessage(`{"decision":"pass","notes":"trust me"}`)}}, nil
	}
	if value := e.Run(context.Background(), task, "developer", 0, 1, DefaultBudget()); value.Outcome != "task_failure" {
		t.Fatal("fake model success accepted")
	}
	e = fixtureEngine(task)
	e.Test = unitAuthority(task, true)
	if value := e.Run(context.Background(), task, "developer", 0, 1, DefaultBudget()); value.Outcome == "passed" {
		t.Fatal("wrong-base evidence accepted")
	}
	e = fixtureEngine(task)
	e.Next = func(ctx context.Context, _ agent.Next) (agent.Reply, error) {
		<-ctx.Done()
		return agent.Reply{}, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if value := e.Run(ctx, task, "developer", 0, 1, DefaultBudget()); value.Outcome == "passed" || value.InputTokens != nil {
		t.Fatal("timeout/unknown usage lost")
	}
	e = fixtureEngine(task)
	next := e.Next
	e.Next = func(ctx context.Context, n agent.Next) (agent.Reply, error) {
		n.EvaluationRole = "developer"
		return next(ctx, n)
	}
	if value := e.Run(context.Background(), task, "planner_developer", 0, 1, DefaultBudget()); value.Outcome != "policy_violation" {
		t.Fatal("planner obtained edit capability")
	}
}

func TestSingleDeveloperCanUseTheEntireEqualTotalCallBudget(t *testing.T) {
	task, _ := evalcatalog.Get("add")
	engine := fixtureEngine(task)
	engine.Next = func(context.Context, agent.Next) (agent.Reply, error) {
		input, output := 1, 1
		return agent.Reply{Status: "ok", Provider: "scripted_TEST_DEMO", Model: "fixture-v1", InputTokens: &input, OutputTokens: &output, Action: &agent.Action{SchemaVersion: 1, ID: uuid.NewString(), Tool: "status", Args: json.RawMessage("{}")}}, nil
	}
	value := engine.Run(context.Background(), task, "developer", 0, 1, DefaultBudget())
	if value.ModelCallsAttempted != DefaultBudget().Calls || value.Outcome != "budget_exhausted" {
		t.Fatalf("single-agent effective budget differed: calls=%d outcome=%s", value.ModelCallsAttempted, value.Outcome)
	}
}
func TestSpendingReservationPersistsThroughRestartAndAllRoles(t *testing.T) {
	pricing := agent.Pricing{SchemaVersion: 1, Provider: "openai", Model: "test-pinned-model", VerifiedAt: "2026-10-23", Input: 1000000, Output: 2000000, Maximum: 100}
	auth := Authorization{uuid.NewString(), "test-operator", 12, 6, agent.SHA(pricing.Bytes()), evalcatalog.CatalogDigest()}
	root := t.TempDir()
	ledger, err := OpenLedger(auth, pricing, root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = OpenLedger(auth, pricing, root); err == nil {
		t.Fatal("two spending writers")
	}
	reserve := ledger.Reservation(pricing)
	for range 2 {
		if err = reserve(1, 1); err != nil {
			t.Fatal(err)
		}
	}
	if err = reserve(1, 1); err != ErrSpendingCap {
		t.Fatal("per-run cap")
	}
	ledger.Close()
	ledger, err = OpenLedger(auth, pricing, root)
	if err != nil {
		t.Fatal(err)
	}
	defer ledger.Close()
	reserve = ledger.Reservation(pricing)
	for range 2 {
		if err = reserve(1, 1); err != nil {
			t.Fatal(err)
		}
	}
	if ledger.Reservation(pricing)(1, 1) != ErrSpendingCap {
		t.Fatal("restart reset total spending cap")
	}
}

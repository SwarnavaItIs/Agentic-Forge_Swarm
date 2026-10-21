//go:build integration && stage03 && stage04 && stage05 && stage06

package integration

import (
	"encoding/json"
	"errors"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/httpapi"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"io"
	"log/slog"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func agentInput() agent.Input {
	return agent.Input{Job: agent.Job{SchemaVersion: 1, RepositoryID: strings.Repeat("a", 64), BaseRevision: strings.Repeat("b", 40), TaskDescription: "Fix Add", Policy: "controlled-go-v1", Budget: "small-v1", TestProfile: agent.Profile}}
}
func agentPricing(t *testing.T, f *fixture) {
	t.Helper()
	p, e := agent.LoadPricing("../../config/agent-pricing.json")
	if e != nil {
		t.Fatal(e)
	}
	if e = f.s1.ConfigureAgent(p); e != nil {
		t.Fatal(e)
	}
	if e = f.s2.ConfigureAgent(p); e != nil {
		t.Fatal(e)
	}
}
func agentTask(t *testing.T, f *fixture, in agent.Input) (uuid.UUID, uuid.UUID, uuid.UUID, execution.Attempt) {
	t.Helper()
	agentPricing(t, f)
	si := in.Sandbox()
	receipt := newGraph(t, f, "agent-"+uuid.NewString(), workflow.CreateInput{Name: "controlled", Tasks: []workflow.TaskDefinition{{Key: "code", Kind: "agent_go", Sandbox: &si, Agent: &in}}})
	reconcile(t, f, receipt.ID)
	task := snapshot(t, f, receipt.ID).Nodes[0].ID
	instance := uuid.New()
	if _, e := f.s1.RegisterWorker(f.ctx, "worker_a", instance, []string{"agent_go"}, 1); e != nil {
		t.Fatal(e)
	}
	a := claimTask(t, f, "worker_a", instance, task, 1)
	return receipt.ID, task, instance, a
}
func modelEvent(in agent.Input) agent.Event {
	return agent.Event{ID: uuid.NewString(), Phase: "begin", Kind: "model", Name: "next_action", InputDigest: agent.SHA([]byte("bounded request")), Workspace: in.RepositoryID, PromptUpper: 1000, Status: "reserved"}
}

func TestAgentRetryRetainsUncertainBudget(t *testing.T) {
	f := setup(t)
	in := agentInput()
	wf, task, instance, a := agentTask(t, f, in)
	event := modelEvent(in)
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, event); e != nil {
		t.Fatal(e)
	}
	exec(t, f, `UPDATE agent_sessions SET tokens=199999 WHERE attempt_id=$1`, a.ID)
	expire(t, f, a)
	reconcile(t, f, wf)
	awaitRetryReady(t, f, wf, task)
	if _, e := f.s1.HeartbeatWorker(f.ctx, "worker_a", instance, nil); e != nil {
		t.Fatal(e)
	}
	replacement := claimTask(t, f, "worker_a", instance, task, 2)
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, replacement.ID, 2, modelEvent(in)); !errors.Is(e, execution.ErrCapacity) {
		t.Fatal("retry reset task budget", e)
	}
	if scalar(t, f, `SELECT count(*) FROM model_calls WHERE attempt_id=$1`, replacement.ID) != 0 {
		t.Fatal("unreserved replacement call")
	}
}

func TestAgentRetryCannotIncreasePinnedMonetaryAllowance(t *testing.T) {
	f := setup(t)
	in := agentInput()
	wf, task, instance, a := agentTask(t, f, in)
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, modelEvent(in)); e != nil {
		t.Fatal(e)
	}
	exec(t, f, `UPDATE agent_sessions SET estimated_microusd=500000 WHERE attempt_id=$1`, a.ID)
	expire(t, f, a)
	reconcile(t, f, wf)
	awaitRetryReady(t, f, wf, task)
	if _, e := f.s1.HeartbeatWorker(f.ctx, "worker_a", instance, nil); e != nil {
		t.Fatal(e)
	}
	replacement := claimTask(t, f, "worker_a", instance, task, 2)
	p, e := agent.LoadPricing("../../config/agent-pricing.json")
	if e != nil {
		t.Fatal(e)
	}
	p.Maximum = 1000000
	if e = f.s1.ConfigureAgent(p); e != nil {
		t.Fatal(e)
	}
	if _, e = f.s1.AgentEvent(f.ctx, "worker_a", instance, replacement.ID, 2, modelEvent(in)); !errors.Is(e, execution.ErrCapacity) {
		t.Fatal("configuration upgrade increased old task budget", e)
	}
}

func TestAgentJSONTypesAlsoProtectLegacySandbox(t *testing.T) {
	f := setup(t)
	wf := uuid.New()
	exec(t, f, `INSERT INTO workflows(id,name,status,version,last_event_sequence,creation_key,request_hash,request_schema_version) VALUES($1,'types','PENDING',1,1,'types',$2,6)`, wf, make([]byte, 32))
	valid := `{"schema_version":1,"fixture_id":"` + strings.Repeat("a", 64) + `","command":"test"}`
	exec(t, f, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,status,delay_ms,sandbox_input) VALUES($1,$2,'valid',0,'sandbox_go','PENDING',0,$3)`, uuid.New(), wf, valid)
	for _, schema := range []string{`null`, `"1"`} {
		_, e := f.p1.Exec(f.ctx, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,status,delay_ms,sandbox_input) VALUES($1,$2,'invalid',99,'sandbox_go','PENDING',0,$3)`, uuid.New(), wf, `{"schema_version":`+schema+`,"fixture_id":"`+strings.Repeat("a", 64)+`","command":"test"}`)
		var pg *pgconn.PgError
		if !errors.As(e, &pg) || pg.ConstraintName != "tasks_execution_json_types" {
			t.Fatal("SQL JSON type coercion accepted", schema)
		}
	}
}

func TestAgentFirstReservationDenialCanFinish(t *testing.T) {
	f := setup(t)
	in := agentInput()
	_, _, instance, a := agentTask(t, f, in)
	p, e := agent.LoadPricing("../../config/agent-pricing.json")
	if e != nil {
		t.Fatal(e)
	}
	p.Maximum = 1
	if e = f.s1.ConfigureAgent(p); e != nil {
		t.Fatal(e)
	}
	if _, e = f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, modelEvent(in)); !errors.Is(e, execution.ErrCapacity) {
		t.Fatal("first reservation", e)
	}
	out := agent.Outcome{Status: "budget_exhausted", Base: in.RepositoryID, Candidate: in.RepositoryID, BaseRevision: in.BaseRevision}
	done, e := f.s1.FinishAgent(f.ctx, "worker_a", instance, a.ID, 1, out)
	if e != nil || done.State != "FAILED" {
		t.Fatal("budget stop could not finish", e)
	}
	if scalar(t, f, `SELECT count(*) FROM model_calls WHERE attempt_id=$1`, a.ID) != 0 {
		t.Fatal("overbudget call was created")
	}
}
func TestAgentReservationsUsageUnknownAndStaleFence(t *testing.T) {
	f := setup(t)
	in := agentInput()
	wf, _, instance, a := agentTask(t, f, in)
	event := modelEvent(in)
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, event); e != nil {
		t.Fatal(e)
	}
	if scalar(t, f, `SELECT tokens FROM agent_sessions WHERE attempt_id=$1`, a.ID) != 2024 {
		t.Fatal("reservation absent")
	}
	other := modelEvent(in)
	if _, e := f.s2.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, other); e == nil {
		t.Fatal("simultaneous reservation")
	}
	event.Phase = "end"
	event.OutputDigest = agent.SHA([]byte("failed response"))
	event.AfterWorkspace = in.RepositoryID
	event.Reply = &agent.Reply{Status: "timeout", Provider: "openai", Model: "gpt-5.4-mini-2026-03-17"}
	event.Status = "timeout"
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, event); e != nil {
		t.Fatal(e)
	}
	if scalar(t, f, `SELECT tokens FROM agent_sessions WHERE attempt_id=$1`, a.ID) != 2024 || scalar(t, f, `SELECT count(*) FROM model_calls WHERE attempt_id=$1 AND phase='end' AND metadata->>'usage'='unknown_retained_reservation'`, a.ID) != 1 {
		t.Fatal("unknown charged as zero")
	}
	other = modelEvent(in)
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, other); e != nil {
		t.Fatal(e)
	}
	i, o := 100, 50
	other.Phase = "end"
	other.AfterWorkspace = in.RepositoryID
	other.OutputDigest = agent.SHA([]byte("valid"))
	other.Status = "invalid_response"
	other.Reply = &agent.Reply{Status: "invalid_response", Provider: "openai", Model: "gpt-5.4-mini-2026-03-17", InputTokens: &i, OutputTokens: &o}
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, other); e != nil {
		t.Fatal(e)
	}
	if scalar(t, f, `SELECT tokens FROM agent_sessions WHERE attempt_id=$1`, a.ID) != 2174 {
		t.Fatal("actual usage not reconciled")
	}
	if _, e := f.s1.FinishAttempt(f.ctx, "worker_a", instance, a.ID, 1, true, []byte(`{}`)); !errors.Is(e, workflow.ErrInvalid) {
		t.Fatal("legacy success bypass", e)
	}
	if _, e := f.s1.Cancel(f.ctx, wf); e != nil {
		t.Fatal(e)
	}
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, modelEvent(in)); !errors.Is(e, execution.ErrStale) {
		t.Fatal("cancelled call accepted", e)
	}
}
func TestAgentToolIdentityAndBudgetLimits(t *testing.T) {
	f := setup(t)
	in := agentInput()
	_, _, instance, a := agentTask(t, f, in)
	event := modelEvent(in)
	event.Kind = "tool"
	event.Name = "apply_patch"
	event.PromptUpper = 0
	event.Status = "started"
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, event); e != nil {
		t.Fatal(e)
	}
	event.InputDigest = agent.SHA([]byte("different args"))
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, event); !errors.Is(e, execution.ErrOutcome) {
		t.Fatal("changed args reused", e)
	}
	event.InputDigest = agent.SHA([]byte("bounded request"))
	event.Phase = "end"
	event.OutputDigest = agent.SHA([]byte("result"))
	event.AfterWorkspace = strings.Repeat("c", 64)
	event.Status = "ok"
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, event); e != nil {
		t.Fatal(e)
	}
	next := modelEvent(in)
	next.Workspace = event.AfterWorkspace
	exec(t, f, `UPDATE agent_sessions SET tokens=199999 WHERE attempt_id=$1`, a.ID)
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, next); !errors.Is(e, execution.ErrCapacity) {
		t.Fatal("token budget", e)
	}
	exec(t, f, `UPDATE agent_sessions SET tokens=0,estimated_microusd=500000 WHERE attempt_id=$1`, a.ID)
	if _, e := f.s1.AgentEvent(f.ctx, "worker_a", instance, a.ID, 1, next); !errors.Is(e, execution.ErrCapacity) {
		t.Fatal("monetary budget", e)
	}
	if scalar(t, f, `SELECT count(*) FROM model_calls WHERE attempt_id=$1`, a.ID) != 0 {
		t.Fatal("overbudget side effect")
	}
}
func TestAgentRegisteredJobsStrictInputAndReplay(t *testing.T) {
	f := setup(t)
	in := agentInput()
	catalog := t.TempDir()
	r := agent.Registration{RepositoryID: in.RepositoryID, BaseRevision: in.BaseRevision, TestProfile: agent.Profile}
	b, _ := json.Marshal(r)
	if e := os.WriteFile(filepath.Join(catalog, in.RepositoryID+".json"), b, 0600); e != nil {
		t.Fatal(e)
	}
	api := httpapi.New(f.s1, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, time.Second).WithAgentCatalog(catalog)
	send := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest("POST", "/v1/jobs", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Idempotency-Key", "same-job")
		w := httptest.NewRecorder()
		api.Handler().ServeHTTP(w, req)
		return w
	}
	data, _ := json.Marshal(in.Job)
	first := send(string(data))
	if first.Code != 202 {
		t.Fatal(first.Code, first.Body.String())
	}
	again := send(string(data))
	if again.Code != 202 || again.Body.String() != first.Body.String() {
		t.Fatal("replay drift", again.Code)
	}
	for _, body := range []string{strings.Replace(string(data), "Fix Add", "Changed task", 1), strings.Replace(string(data), `"small-v1"`, `"unbounded"`, 1), strings.TrimSuffix(string(data), "}") + `,"path":"C:/outside"}`, strings.Replace(string(data), `"schema_version"`, `"Schema_Version"`, 1)} {
		r := send(body)
		if r.Code != 400 && r.Code != 409 {
			t.Fatal("invalid admission", r.Code, r.Body.String())
		}
	}
	if scalar(t, f, `SELECT count(*) FROM workflows`) != 1 {
		t.Fatal("duplicate workflow")
	}
}

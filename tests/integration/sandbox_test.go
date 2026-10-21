//go:build integration && stage03 && stage04 && stage05

package integration

import (
	"encoding/json"
	"errors"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/httpapi"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/workflow"
	"forgeswarm/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/stdlib"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func sandboxTask(t *testing.T, f *fixture, key string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	r := newGraph(t, f, key, workflow.CreateInput{Name: key, Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "sandbox_go", Sandbox: &sandbox.Input{SchemaVersion: 1, FixtureID: strings.Repeat("a", 64), Command: "test"}}}})
	reconcile(t, f, r.ID)
	return r.ID, snapshot(t, f, r.ID).Nodes[0].ID
}
func sandboxResult() sandbox.Result {
	return sandbox.Result{SchemaVersion: 1, Status: "exited", ExitCode: 0, Image: "sha256:" + strings.Repeat("b", 64), HelperSHA256: strings.Repeat("c", 64), ContainerID: strings.Repeat("d", 64), IsolationMode: "rootful_linux", BaseRevision: strings.Repeat("e", 40), Policy: sandbox.DefaultPolicy(), CleanupConfirmed: true, Artifacts: []sandbox.Artifact{{Kind: "stdout", MediaType: "text/plain; charset=utf-8", Body: []byte("fixture output"), Truncated: true}, {Kind: "patch", MediaType: "text/plain; charset=utf-8", Body: []byte{}}}}
}
func registerSandbox(t *testing.T, f *fixture, name string) uuid.UUID {
	t.Helper()
	instance := uuid.New()
	if _, err := f.s1.RegisterWorker(f.ctx, name, instance, []string{"sandbox_go"}, 1); err != nil {
		t.Fatal(err)
	}
	return instance
}

func TestSandboxAuthorizationPublicationAtomicityAndReadOnlyIDs(t *testing.T) {
	f := setup(t)
	wf, task := sandboxTask(t, f, "sandbox-result")
	if _, err := f.p1.Exec(f.ctx, `UPDATE tasks SET sandbox_input=jsonb_set(sandbox_input,'{command}','"build"') WHERE id=$1`, task); err == nil {
		t.Fatal("sandbox task input mutable")
	}
	instance := registerSandbox(t, f, "worker_a")
	a := claimTask(t, f, "worker_a", instance, task, 1)
	request := sandbox.RequestID(a.ID)
	if _, err := f.s1.AuthorizeSandbox(f.ctx, "worker_a", instance, a.ID, uuid.New(), 1); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatal("unstable tool ID", err)
	}
	if _, err := f.s1.FinishSandbox(f.ctx, "worker_a", instance, a.ID, request, 1, sandboxResult()); !errors.Is(err, execution.ErrStale) {
		t.Fatal("unauthorized publication", err)
	}
	if _, err := f.s1.AuthorizeSandbox(f.ctx, "worker_a", instance, a.ID, request, 1); err != nil {
		t.Fatal(err)
	}
	if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND cleanup_required`, a.ID) != 1 {
		t.Fatal("physical cleanup uncertainty not recorded")
	}
	if _, err := f.s1.FinishAttempt(f.ctx, "worker_a", instance, a.ID, 1, true, []byte(`{}`)); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatal("legacy completion bypass", err)
	}
	// Force the second artifact insert to fail: accepted state, metadata and all
	// earlier artifact rows must roll back together.
	exec(t, f, `CREATE FUNCTION reject_patch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='patch' THEN RAISE EXCEPTION 'injected'; END IF; RETURN NEW; END $$`)
	exec(t, f, `CREATE TRIGGER reject_patch BEFORE INSERT ON sandbox_artifacts FOR EACH ROW EXECUTE FUNCTION reject_patch()`)
	if _, err := f.s1.FinishSandbox(f.ctx, "worker_a", instance, a.ID, request, 1, sandboxResult()); err == nil {
		t.Fatal("injected artifact failure accepted")
	}
	if scalar(t, f, `SELECT count(*) FROM sandbox_artifacts`) != 0 || scalar(t, f, `SELECT count(*) FROM sandbox_executions`) != 0 || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND state='RUNNING'`, a.ID) != 1 {
		t.Fatal("partial canonical artifact commit")
	}
	exec(t, f, `DROP TRIGGER reject_patch ON sandbox_artifacts`)
	if _, err := f.s1.FinishSandbox(f.ctx, "worker_a", instance, a.ID, request, 1, sandboxResult()); err != nil {
		t.Fatal(err)
	}
	if scalar(t, f, `SELECT count(*) FROM sandbox_artifacts`) != 2 || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND cleanup_required`, a.ID) != 0 {
		t.Fatal("missing references or cleanup attestation")
	}
	if _, err := f.s1.FinishSandbox(f.ctx, "worker_a", instance, a.ID, request, 1, sandboxResult()); err != nil {
		t.Fatal("lost-reply retry", err)
	}
	changed := sandboxResult()
	changed.Artifacts[0].Body = []byte("changed")
	if _, err := f.s1.FinishSandbox(f.ctx, "worker_a", instance, a.ID, request, 1, changed); !errors.Is(err, execution.ErrOutcome) {
		t.Fatal("receipt accepted changed bytes", err)
	}
	artifact := uuid.NewSHA1(a.ID, []byte("artifact-v1:stdout"))
	value, err := f.s1.Artifact(f.ctx, wf, artifact)
	if err != nil || string(value.Body) != "fixture output" || value.ContentLength != 14 || !value.Truncated {
		t.Fatal(value, err)
	}
	if _, err = f.s1.Artifact(f.ctx, uuid.New(), artifact); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatal("cross-workflow retrieval", err)
	}
	api := httpapi.New(f.s1, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, time.Second)
	r := httptest.NewRequest("GET", "/v1/workflows/"+wf.String()+"/artifacts/"+artifact.String(), nil)
	w := httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"sha256"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	r = httptest.NewRequest("GET", "/v1/workflows/"+wf.String()+"/artifacts/"+artifact.String()+"?path=/etc/passwd", nil)
	w = httptest.NewRecorder()
	api.Handler().ServeHTTP(w, r)
	if w.Code != 400 {
		t.Fatal("path query accepted")
	}
	if _, err = f.p1.Exec(f.ctx, `UPDATE sandbox_artifacts SET body='altered' WHERE id=$1`, artifact); err == nil {
		t.Fatal("accepted artifact mutable")
	}
}

func TestSandboxV4MigrationRetainsHashReceiptAndActiveLease(t *testing.T) {
	f := setupAt(t, 4)
	wf, task, attempt, instance := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	in := workflow.CreateInput{Name: "v4 fixture upgrade", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop"}}}
	hash, err := workflow.HashForVersion(in, 4)
	if err != nil {
		t.Fatal(err)
	}
	var receipt workflow.Receipt
	if err = f.p1.QueryRow(f.ctx, `INSERT INTO workflows(id,name,status,version,last_event_sequence,creation_key,request_hash,request_schema_version) VALUES($1,$2,'RUNNING',3,4,'v4-upgrade',$3,4) RETURNING id,created_at`, wf, in.Name, hash[:]).Scan(&receipt.ID, &receipt.CreatedAt); err != nil {
		t.Fatal(err)
	}
	exec(t, f, `INSERT INTO workers(id,instance_id,capabilities,capacity,heartbeat_at) VALUES('worker_a',$1,ARRAY['noop'],1,clock_timestamp())`, instance)
	exec(t, f, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,status,dispatch_generation) VALUES($1,$2,'a',0,'noop','READY',1)`, task, wf)
	exec(t, f, `INSERT INTO task_attempts(id,workflow_id,task_id,attempt_number,worker_id,worker_instance_id,claim_request_id,generation,started_at,lease_expires_at,state) VALUES($1,$2,$3,1,'worker_a',$4,$5,1,clock_timestamp(),clock_timestamp()+interval '3 seconds','RUNNING')`, attempt, wf, task, instance, uuid.New())
	exec(t, f, `UPDATE tasks SET status='RUNNING',attempt_count=1,current_attempt_id=$2 WHERE id=$1`, task, attempt)
	exec(t, f, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created','{}'),($1,2,'task.ready','{}'),($1,3,'workflow.running','{}'),($1,4,'task.claimed','{}')`, wf)
	db := stdlib.OpenDB(*f.cfg2.ConnConfig)
	defer db.Close()
	if err = migrations.Up(f.ctx, db); err != nil {
		t.Fatal(err)
	}
	if replay, e := f.s1.Create(f.ctx, "v4-upgrade", in); e != nil || replay.ID != receipt.ID || !replay.CreatedAt.Equal(receipt.CreatedAt) {
		t.Fatal("v4 receipt changed", e)
	}
	if _, err = f.s1.RenewLease(f.ctx, "worker_a", instance, attempt, 1); err != nil {
		t.Fatal("v4 active lease changed", err)
	}
	if scalar(t, f, `SELECT request_schema_version FROM workflows WHERE id=$1`, wf) != 4 {
		t.Fatal("v4 normalization rewritten")
	}
	in.Tasks[0].Kind = "sandbox_go"
	in.Tasks[0].Sandbox = &sandbox.Input{SchemaVersion: 1, FixtureID: strings.Repeat("a", 64), Command: "test"}
	if _, err = f.s1.Create(f.ctx, "v4-upgrade", in); !errors.Is(err, workflow.ErrConflict) {
		t.Fatal("sandbox projected onto v4 replay", err)
	}
}

func TestSandboxLateAttemptArtifactsCannotBecomeCanonical(t *testing.T) {
	f := setup(t)
	wf, task := sandboxTask(t, f, "sandbox-fence")
	instance := registerSandbox(t, f, "worker_a")
	a := claimTask(t, f, "worker_a", instance, task, 1)
	if _, err := f.s1.AuthorizeSandbox(f.ctx, "worker_a", instance, a.ID, sandbox.RequestID(a.ID), 1); err != nil {
		t.Fatal(err)
	}
	expire(t, f, a)
	reconcile(t, f, wf)
	if _, err := f.s1.FinishSandbox(f.ctx, "worker_a", instance, a.ID, sandbox.RequestID(a.ID), 1, sandboxResult()); !errors.Is(err, execution.ErrStale) {
		t.Fatal("late A artifacts accepted", err)
	}
	if scalar(t, f, `SELECT count(*) FROM sandbox_artifacts`) != 0 {
		t.Fatal("stale artifact persisted")
	}
	page, err := f.s1.SandboxCleanup(f.ctx, "worker_a", instance)
	if err != nil || len(page) != 1 || page[0].ID != a.ID {
		t.Fatal("cleanup lost", page, err)
	}
	if _, err = f.s1.AcknowledgeStop(f.ctx, "worker_a", instance, a.ID, 1); err != nil {
		t.Fatal(err)
	}
	// Make the already-persisted backoff due without changing definition fields.
	exec(t, f, `UPDATE tasks SET next_attempt_at=clock_timestamp()-interval '1 millisecond' WHERE id=$1`, task)
	reconcile(t, f, wf)
	b := claimTask(t, f, "worker_a", instance, task, 2)
	if b.ID == a.ID {
		t.Fatal("attempt reused")
	}
	if _, err = f.s1.AuthorizeSandbox(f.ctx, "worker_a", instance, b.ID, sandbox.RequestID(b.ID), 2); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s1.FinishSandbox(f.ctx, "worker_a", instance, b.ID, sandbox.RequestID(b.ID), 2, sandboxResult()); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s1.FinishSandbox(f.ctx, "worker_a", instance, a.ID, sandbox.RequestID(a.ID), 1, sandboxResult()); !errors.Is(err, execution.ErrStale) {
		t.Fatal("late A after B", err)
	}
	if scalar(t, f, `SELECT count(*) FROM sandbox_artifacts WHERE attempt_id=$1`, b.ID) != 2 || scalar(t, f, `SELECT count(*) FROM sandbox_artifacts WHERE attempt_id=$1`, a.ID) != 0 {
		t.Fatal("canonical ownership mismatch")
	}
	// Database-level FK/ownership guards cannot be bypassed by mixing identities.
	_, err = f.p1.Exec(f.ctx, `INSERT INTO sandbox_artifacts(id,attempt_id,workflow_id,task_id,generation,kind,media_type,base_revision,sha256,content_length,body,truncated) SELECT $1,attempt_id,workflow_id,task_id,999,'stderr',media_type,base_revision,sha256,content_length,body,truncated FROM sandbox_artifacts WHERE attempt_id=$2 LIMIT 1`, uuid.New(), b.ID)
	if err == nil {
		t.Fatal("artifact generation mismatch accepted")
	}
	data, _ := json.Marshal(sandboxResult())
	if len(data) > sandbox.MaxWire {
		t.Fatal("test result exceeds RPC policy")
	}
}

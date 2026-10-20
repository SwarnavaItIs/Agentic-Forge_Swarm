//go:build integration && stage03 && stage04

package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	workerv1 "forgeswarm/api/worker/v1"
	"forgeswarm/internal/broker"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/httpapi"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/relay"
	"forgeswarm/internal/scheduler"
	"forgeswarm/internal/worker"
	"forgeswarm/internal/workflow"
	"forgeswarm/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func recoveryPolicy(t *testing.T, f *fixture, p execution.Policy) {
	t.Helper()
	var err error
	f.s1, err = postgres.NewWithPolicy(f.p1, execution.DefaultLimits(), p, func() float64 { return 0.5 })
	if err != nil {
		t.Fatal(err)
	}
	f.s2, err = postgres.NewWithPolicy(f.p2, execution.DefaultLimits(), p, func() float64 { return 0.5 })
	if err != nil {
		t.Fatal(err)
	}
}
func TestCancelledAndSupersededOutboxCannotAccumulateQueuedWork(t *testing.T) {
	f := setup(t)
	p := execution.DefaultPolicy()
	p.MaxWorkflows = 1
	p.MaxQueued = 1
	p.MaxReady = 1
	recoveryPolicy(t, f, p)
	for i := 0; i < 20; i++ {
		wf, _ := runnable(t, f, fmt.Sprintf("cancel-outbox-%d", i))
		if _, err := f.s1.Cancel(f.ctx, wf); err != nil {
			t.Fatal(err)
		}
		if scalar(t, f, `SELECT count(*) FROM outbox WHERE state='PENDING'`) != 0 {
			t.Fatal("cancelled notification still queued during broker outage")
		}
	}
	if scalar(t, f, `SELECT count(*) FROM outbox WHERE state='OBSOLETE'`) != 20 {
		t.Fatal("obsolete dispatch audit evidence erased")
	}
	instance := register(t, f, "worker_a", 1)
	wf, task := runnable(t, f, "superseded-outbox")
	a := claimTask(t, f, "worker_a", instance, task, 1)
	expire(t, f, a)
	reconcile(t, f, wf)
	if scalar(t, f, `SELECT count(*) FROM outbox WHERE task_id=$1 AND state='PENDING'`, task) != 0 {
		t.Fatal("old generation remained queued during RETRY_WAIT")
	}
	awaitRetryReady(t, f, wf, task)
	if scalar(t, f, `SELECT count(*) FROM outbox WHERE task_id=$1 AND state='PENDING' AND dispatch_generation=2`, task) != 1 {
		t.Fatal("fresh generation missing")
	}
}

type heldRenewal struct {
	workerv1.CoordinatorClient
	entered chan struct{}
}

func TestRunningWorkerRecreatesLostBrokerDefinitions(t *testing.T) {
	f := setup(t)
	b := testBroker(t, f)
	rpc := rpcServer(t, f, "")
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx, rpc.ca, b, rpc.cfg, slog.New(slog.NewJSONHandler(io.Discard, nil))) }()
	waitFor(t, f, `SELECT count(*) FROM workers WHERE id='worker_a'`)
	if err := b.JS.DeleteStream(f.ctx, broker.Quarantine); err != nil {
		t.Fatal(err)
	}
	if err := b.JS.DeleteStream(f.ctx, broker.Stream); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(4 * time.Second)
	restored := false
	for time.Now().Before(deadline) {
		_, a := b.JS.Stream(f.ctx, broker.Stream)
		_, q := b.JS.Stream(f.ctx, broker.Quarantine)
		if a == nil && q == nil {
			restored = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if !restored {
		t.Fatal("running worker did not restore lost stream/consumer definitions")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("worker failed to drain after topology recovery")
	}
}

func (c heldRenewal) RenewLease(ctx context.Context, r *workerv1.AttemptRequest, options ...grpc.CallOption) (*workerv1.Attempt, error) {
	_, err := c.CoordinatorClient.RenewLease(ctx, r, options...)
	if err != nil {
		return nil, err
	}
	close(c.entered)
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestFinishingDiagnosticDoesNotInterpretOwnRenewalCancellationAsLeaseLoss(t *testing.T) {
	f := setup(t)
	b := testBroker(t, f)
	rpc := rpcServer(t, f, "")
	instance := uuid.New()
	if _, err := rpc.ca.RegisterWorker(f.ctx, &workerv1.RegisterRequest{InstanceId: instance.String(), Capabilities: []string{"delay"}, Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	r := newGraph(t, f, "renewal-finish-race", workflow.CreateInput{Name: "renewal race", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "delay", DelayMS: 1300}}})
	reconcile(t, f, r.ID)
	task := snapshot(t, f, r.ID).Nodes[0].ID
	if _, err := relay.Step(f.ctx, f.s1, b, uuid.New(), 1, time.Second); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	client := heldRenewal{rpc.ca, entered}
	worker.Handle(f.ctx, client, b, rpc.cfg, instance, message(t, b), slog.New(slog.NewJSONHandler(io.Discard, nil)))
	select {
	case <-entered:
	default:
		t.Fatal("did not control in-flight renewal")
	}
	if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND state='SUCCEEDED'`, task) != 1 || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1`, task) != 1 {
		t.Fatal("normal completion cancellation lost ownership")
	}
}

func TestStage03ActiveUpgradeKeepsV3HashAndClaim(t *testing.T) {
	f := setupAt(t, 3)
	wf, task, attempt, instance := uuid.New(), uuid.New(), uuid.New(), uuid.New()
	in := workflow.CreateInput{Name: "v3 active", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "delay", DelayMS: 1000}}}
	hash, err := workflow.HashForVersion(in, 3)
	if err != nil {
		t.Fatal(err)
	}
	var original workflow.Receipt
	if err = f.p1.QueryRow(f.ctx, `INSERT INTO workflows(id,name,status,version,last_event_sequence,creation_key,request_hash,request_schema_version) VALUES($1,$2,'RUNNING',3,4,'v3-active',$3,3) RETURNING id,created_at`, wf, in.Name, hash[:]).Scan(&original.ID, &original.CreatedAt); err != nil {
		t.Fatal(err)
	}
	exec(t, f, `INSERT INTO workers(id,instance_id,capabilities,capacity,heartbeat_at) VALUES('worker_a',$1,ARRAY['delay'],1,clock_timestamp())`, instance)
	exec(t, f, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,delay_ms,status,dispatch_generation) VALUES($1,$2,'a',0,'delay',1000,'READY',1)`, task, wf)
	exec(t, f, `INSERT INTO task_attempts(id,workflow_id,task_id,attempt_number,worker_id,worker_instance_id,claim_request_id,generation,started_at,lease_expires_at,state) VALUES($1,$2,$3,1,'worker_a',$4,$5,1,clock_timestamp(),clock_timestamp()+interval '3 seconds','RUNNING')`, attempt, wf, task, instance, uuid.New())
	exec(t, f, `UPDATE tasks SET status='RUNNING',attempt_count=1,current_attempt_id=$2 WHERE id=$1`, task, attempt)
	exec(t, f, `INSERT INTO outbox(id,task_id,dispatch_generation,schema_version,payload) VALUES($1,$2,1,1,'{}')`, uuid.New(), task)
	exec(t, f, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created','{}'),($1,2,'task.ready','{}'),($1,3,'workflow.running','{}'),($1,4,'task.claimed','{}')`, wf)
	db := stdlib.OpenDB(*f.cfg2.ConnConfig)
	defer db.Close()
	if err = migrations.Up(f.ctx, db); err != nil {
		t.Fatal(err)
	}
	if err = f.s1.Ready(f.ctx); err != nil {
		t.Fatal(err)
	}
	if scalar(t, f, `SELECT count(*) FROM outbox WHERE task_id=$1 AND state='OBSOLETE'`, task) != 1 {
		t.Fatal("upgrade left obsolete legacy dispatch queued")
	}
	if replay, err := f.s1.Create(f.ctx, "v3-active", in); err != nil || replay.ID != original.ID || !replay.CreatedAt.Equal(original.CreatedAt) {
		t.Fatal("upgrade changed v3 receipt", err)
	}
	if scalar(t, f, `SELECT request_schema_version FROM workflows WHERE id=$1`, wf) != 3 || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND state='RUNNING'`, attempt) != 1 || len(history(t, f, wf)) != 4 {
		t.Fatal("upgrade rewrote claim/history")
	}
	in.Tasks[0].MaxAttempts = 4
	if _, err = f.s1.Create(f.ctx, "v3-active", in); !errors.Is(err, workflow.ErrConflict) {
		t.Fatal("old normalization ignored supplied retry policy", err)
	}
	if _, err = f.s1.RenewLease(f.ctx, "worker_a", instance, attempt, 1); err != nil {
		t.Fatal("upgrade lost current ownership", err)
	}
}
func TestTwoReapersOneRecoveryAndStaleToolFence(t *testing.T) {
	f := setup(t)
	ia, ib := register(t, f, "worker_a", 1), register(t, f, "worker_b", 1)
	wf, task := runnable(t, f, "two-reapers")
	a := claimTask(t, f, "worker_a", ia, task, 1)
	expire(t, f, a)
	for _, err := range parallel(2, func(i int) error {
		s := f.s1
		if i == 1 {
			s = f.s2
		}
		_, e := s.Reconcile(f.ctx, wf)
		return e
	}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if scalar(t, f, `SELECT count(*) FROM workflow_events WHERE workflow_id=$1 AND event_type='attempt.lost'`, wf) != 1 || scalar(t, f, `SELECT dispatch_generation FROM tasks WHERE id=$1`, task) != 2 || scalar(t, f, `SELECT count(*) FROM tasks WHERE id=$1 AND status='RETRY_WAIT' AND current_attempt_id IS NULL`, task) != 1 {
		t.Fatal("multiple recovery mutation or generation")
	}
	if err := f.s1.CheckConsistency(f.ctx); err != nil {
		t.Fatal(err)
	}
	awaitRetryReady(t, f, wf, task)
	b := claimTask(t, f, "worker_b", ib, task, 2)
	if _, err := f.s2.FinishAttempt(f.ctx, "worker_b", ib, b.ID, 2, true, []byte(`{"accepted":true}`)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, wf)
	for _, fn := range []func() error{
		func() error { _, e := f.s1.RenewLease(f.ctx, "worker_a", ia, a.ID, 1); return e },
		func() error { _, e := f.s1.AuthorizeTool(f.ctx, "worker_a", ia, a.ID, 1); return e },
		func() error { _, e := f.s1.FinishAttempt(f.ctx, "worker_a", ia, a.ID, 1, true, []byte(`{}`)); return e },
	} {
		if err := fn(); !errors.Is(err, execution.ErrStale) {
			t.Fatal("stale physical worker bypassed fence", err)
		}
	}
	if _, err := f.s1.AcknowledgeStop(f.ctx, "worker_a", ia, a.ID, 1); err != nil {
		t.Fatal(err)
	}
	if snapshot(t, f, wf).Status != workflow.Succeeded || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND state='SUCCEEDED'`, task) != 1 {
		t.Fatal("cleanup attestation rewrote accepted result")
	}
}

func TestRetryWaitRestartPreservesDueDeadlineAndStopsAtBudget(t *testing.T) {
	f := setup(t)
	p := execution.DefaultPolicy()
	p.BackoffBase = time.Second
	p.BackoffCap = time.Second
	recoveryPolicy(t, f, p)
	ia := register(t, f, "worker_a", 1)
	r := newGraph(t, f, "persist-retry", workflow.CreateInput{Name: "persist", TimeoutMS: 10000, Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop", MaxAttempts: 2, TimeoutMS: 5000}}})
	reconcile(t, f, r.ID)
	task := snapshot(t, f, r.ID).Nodes[0].ID
	a := claimTask(t, f, "worker_a", ia, task, 1)
	first, err := f.s1.FailAttempt(f.ctx, "worker_a", ia, a.ID, 1, execution.Transient, []byte(`{"class":"network"}`))
	if err != nil {
		t.Fatal(err)
	}
	before, err := f.s1.InspectTask(f.ctx, r.ID, task)
	if err != nil || before.Status != workflow.RetryWait || before.NextAttemptAt == nil {
		t.Fatal(before, err)
	}
	pool, err := pgxpool.NewWithConfig(f.ctx, f.cfg2.Copy())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	restarted, err := postgres.NewWithPolicy(pool, execution.DefaultLimits(), p, func() float64 { return 0 })
	if err != nil {
		t.Fatal(err)
	}
	if _, err = restarted.Reconcile(f.ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	after, err := restarted.InspectTask(f.ctx, r.ID, task)
	if err != nil || !after.NextAttemptAt.Equal(*before.NextAttemptAt) || !after.DeadlineAt.Equal(before.DeadlineAt) {
		t.Fatal("restart reset budget", err)
	}
	awaitRetryReady(t, f, r.ID, task)
	b := claimTask(t, f, "worker_a", ia, task, 2)
	if _, err = f.s1.FailAttempt(f.ctx, "worker_a", ia, b.ID, 2, execution.Transient, []byte(`{"class":"network"}`)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, r.ID)
	if snapshot(t, f, r.ID).Status != workflow.Failed || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1`, task) != 2 {
		t.Fatal("retry budget exceeded")
	}
	old, err := f.s2.FailAttempt(f.ctx, "worker_a", ia, a.ID, 1, execution.Transient, []byte(`{"class":"network"}`))
	if err != nil || old.ID != first.ID || !old.FinishedAt.Equal(*first.FinishedAt) {
		t.Fatal("old accepted failure receipt changed", err)
	}
	if _, err = f.s2.FailAttempt(f.ctx, "worker_a", ia, a.ID, 1, execution.Permission, []byte(`{"class":"network"}`)); !errors.Is(err, execution.ErrOutcome) {
		t.Fatal("conflicting classification accepted", err)
	}
}

func TestPermanentDiagnosticFailuresNeverRetry(t *testing.T) {
	f := setup(t)
	instance := register(t, f, "worker_a", 1)
	for _, class := range []string{execution.Permission, execution.Configuration, execution.Unsupported, execution.Diagnostic} {
		wf, task := runnable(t, f, class)
		a := claimTask(t, f, "worker_a", instance, task, 1)
		if _, err := f.s1.FailAttempt(f.ctx, "worker_a", instance, a.ID, 1, class, []byte(`{}`)); err != nil {
			t.Fatal(err)
		}
		reconcile(t, f, wf)
		reconcile(t, f, wf)
		view, err := f.s1.InspectTask(f.ctx, wf, task)
		if err != nil || view.Status != workflow.Failed || view.LastError != class || view.AttemptCount != 1 || view.NextAttemptAt != nil {
			t.Fatal("permanent error spun", view, err)
		}
	}
}

func TestCancellationHandshakePreservesAcceptedResultsAndCapacity(t *testing.T) {
	f := setup(t)
	instance := register(t, f, "worker_a", 2)
	r := newGraph(t, f, "stop-handshake", workflow.CreateInput{Name: "stop", Tasks: []workflow.TaskDefinition{{Key: "done", Kind: "noop"}, {Key: "active", Kind: "delay", DelayMS: 10000}, {Key: "pending", Kind: "noop", DependsOn: []string{"active"}}}})
	reconcile(t, f, r.ID)
	g := snapshot(t, f, r.ID)
	done := claimTask(t, f, "worker_a", instance, g.Nodes[0].ID, 1)
	receipt, err := f.s1.FinishAttempt(f.ctx, "worker_a", instance, done.ID, 1, true, []byte(`{"retained":true}`))
	if err != nil {
		t.Fatal(err)
	}
	active := claimTask(t, f, "worker_a", instance, g.Nodes[1].ID, 1)
	cancel, err := f.s2.Cancel(f.ctx, r.ID)
	if err != nil || cancel.Status != workflow.Cancelling {
		t.Fatal("active cancellation settled too early", err)
	}
	seq := scalar(t, f, `SELECT last_event_sequence FROM workflows WHERE id=$1`, r.ID)
	if repeated, err := f.s1.Cancel(f.ctx, r.ID); err != nil || repeated != cancel || scalar(t, f, `SELECT last_event_sequence FROM workflows WHERE id=$1`, r.ID) != seq {
		t.Fatal("repeated cancellation mutated state", err)
	}
	if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE state='RUNNING'`) != 1 {
		t.Fatal("capacity released before stop/fence")
	}
	if _, err = f.s1.FinishAttempt(f.ctx, "worker_a", instance, active.ID, 1, true, []byte(`{}`)); !errors.Is(err, execution.ErrStale) {
		t.Fatal("late success accepted", err)
	}
	if _, err = f.s1.AuthorizeTool(f.ctx, "worker_a", instance, active.ID, 1); !errors.Is(err, execution.ErrStale) {
		t.Fatal("tool allowed during cancellation", err)
	}
	signal, err := f.s1.RenewLease(f.ctx, "worker_a", instance, active.ID, 1)
	if err != nil || !signal.StopRequested || !signal.LeaseExpiresAt.Equal(active.LeaseExpiresAt) {
		t.Fatal("stop signal renewed lease", err)
	}
	if _, err = f.s1.AcknowledgeStop(f.ctx, "worker_a", instance, active.ID, 1); err != nil {
		t.Fatal(err)
	}
	seq = scalar(t, f, `SELECT last_event_sequence FROM workflows WHERE id=$1`, r.ID)
	if _, err = f.s1.AcknowledgeStop(f.ctx, "worker_a", instance, active.ID, 1); err != nil || seq != scalar(t, f, `SELECT last_event_sequence FROM workflows WHERE id=$1`, r.ID) {
		t.Fatal("stop released twice", err)
	}
	reconcile(t, f, r.ID)
	g = snapshot(t, f, r.ID)
	if g.Status != workflow.Cancelled || g.Nodes[0].Status != workflow.Succeeded || g.Nodes[2].Status != workflow.Cancelled || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE state='RUNNING'`) != 0 {
		t.Fatal("bad cancellation convergence")
	}
	again, err := f.s1.FinishAttempt(f.ctx, "worker_a", instance, done.ID, 1, true, []byte(`{"retained":true}`))
	if err != nil || !again.FinishedAt.Equal(*receipt.FinishedAt) {
		t.Fatal("cancellation changed completed outcome", err)
	}
	if _, err = f.p1.Exec(f.ctx, `UPDATE task_attempts SET result_metadata='{}' WHERE id=$1`, done.ID); err == nil {
		t.Fatal("accepted metadata was mutable")
	}
}

func TestDeadlinesCannotBeExtendedByRenewalOrRestart(t *testing.T) {
	for _, overall := range []bool{false, true} {
		t.Run(fmt.Sprint(overall), func(t *testing.T) {
			f := setup(t)
			instance := register(t, f, "worker_a", 1)
			input := workflow.CreateInput{Name: "deadline", TimeoutMS: 5000, Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "delay", DelayMS: 10000, TimeoutMS: 650}}}
			if overall {
				input.TimeoutMS = 650
				input.Tasks[0].TimeoutMS = 5000
			}
			r := newGraph(t, f, "deadline", input)
			reconcile(t, f, r.ID)
			task := snapshot(t, f, r.ID).Nodes[0].ID
			a := claimTask(t, f, "worker_a", instance, task, 1)
			before, err := f.s1.InspectTask(f.ctx, r.ID, task)
			if err != nil {
				t.Fatal(err)
			}
			if _, err = f.s1.RenewLease(f.ctx, "worker_a", instance, a.ID, 1); err != nil {
				t.Fatal(err)
			}
			if _, err = f.p1.Exec(f.ctx, `UPDATE tasks SET deadline_at=deadline_at+interval '1 second' WHERE id=$1`, task); err == nil {
				t.Fatal("deadline mutable")
			}
			waitFor(t, f, `SELECT count(*) FROM tasks WHERE id=$1 AND deadline_at<=clock_timestamp()`, task)
			if _, err = f.s1.AuthorizeTool(f.ctx, "worker_a", instance, a.ID, 1); !errors.Is(err, execution.ErrStale) {
				t.Fatal("expired deadline authorized tool", err)
			}
			if _, err = f.s1.FinishAttempt(f.ctx, "worker_a", instance, a.ID, 1, true, []byte(`{}`)); !errors.Is(err, execution.ErrStale) {
				t.Fatal("deadline accepted success", err)
			}
			reconcile(t, f, r.ID)
			after, err := f.s2.InspectTask(f.ctx, r.ID, task)
			if err != nil || !after.DeadlineAt.Equal(before.DeadlineAt) || after.LastError != execution.Deadline || snapshot(t, f, r.ID).Status != workflow.Failed {
				t.Fatal("deadline did not converge", after, err)
			}
		})
	}
}

func TestLeaseBoundaryBlockedRenewCompleteReapAndCancel(t *testing.T) {
	f := setup(t)
	limits := execution.DefaultLimits()
	limits.Lease = 500 * time.Millisecond
	var err error
	f.s1, err = postgres.NewWithLimits(f.p1, limits)
	if err != nil {
		t.Fatal(err)
	}
	instance := register(t, f, "worker_a", 1)
	wf, task := runnable(t, f, "lease-barrier")
	a := claimTask(t, f, "worker_a", instance, task, 1)
	gate, err := f.p1.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Rollback(context.Background())
	if _, err = gate.Exec(f.ctx, `SELECT id FROM workflows WHERE id=$1 FOR UPDATE`, wf); err != nil {
		t.Fatal(err)
	}
	results := make(chan error, 2)
	reaper := make(chan error, 1)
	cancel := make(chan error, 1)
	go func() { _, e := f.s2.RenewLease(f.ctx, "worker_a", instance, a.ID, 1); results <- e }()
	go func() {
		_, e := f.s2.FinishAttempt(f.ctx, "worker_a", instance, a.ID, 1, true, []byte(`{}`))
		results <- e
	}()
	go func() { _, e := f.s2.Reconcile(f.ctx, wf); reaper <- e }()
	go func() { _, e := f.s1.Cancel(f.ctx, wf); cancel <- e }()
	waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND lease_expires_at<=clock_timestamp()`, a.ID)
	if err = gate.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if e := <-results; !errors.Is(e, execution.ErrStale) {
			t.Fatal("lock wait revived lease or accepted result", e)
		}
	}
	if e := <-reaper; e != nil {
		t.Fatal(e)
	}
	if e := <-cancel; e != nil {
		t.Fatal(e)
	}
	reconcile(t, f, wf)
	if snapshot(t, f, wf).Status != workflow.Cancelled || scalar(t, f, `SELECT count(*) FROM workflow_events WHERE workflow_id=$1 AND event_type='attempt.lost'`, wf) != 1 || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE state='RUNNING'`) != 0 {
		t.Fatal("lease/cancellation boundary inconsistent")
	}
}

func TestHardAdmissionAcrossTwoHTTPInstancesAndDrainingPaths(t *testing.T) {
	for _, queuedLimit := range []bool{false, true} {
		t.Run(fmt.Sprint(queuedLimit), func(t *testing.T) {
			f := setup(t)
			p := execution.DefaultPolicy()
			p.MaxWorkflows = 6
			p.MaxQueued = 8
			p.MaxReady = 2
			p.MaxTasks = 3
			expected := 6
			if queuedLimit {
				p.MaxWorkflows = 100
				p.MaxQueued = 3
				expected = 3
			}
			recoveryPolicy(t, f, p)
			logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
			if _, err := f.s1.Create(f.ctx, "configured-task-bound", workflow.CreateInput{Name: "too many", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop"}, {Key: "b", Kind: "noop"}, {Key: "c", Kind: "noop"}, {Key: "d", Kind: "noop"}}}); !errors.Is(err, workflow.ErrInvalid) || scalar(t, f, `SELECT count(*) FROM workflows`) != 0 {
				t.Fatal("configured per-workflow task bound ignored", err)
			}
			servers := []*httptest.Server{httptest.NewServer(httpapi.New(f.s1, logger, 10*time.Second, time.Second).Handler()), httptest.NewServer(httpapi.New(f.s2, logger, 10*time.Second, time.Second).Handler())}
			for _, server := range servers {
				defer server.Close()
			}
			type admitted struct {
				key     string
				receipt workflow.Receipt
			}
			accepted := []admitted{}
			var lock sync.Mutex
			client := &http.Client{Timeout: 15 * time.Second}
			for _, err := range parallel(120, func(i int) error {
				key := fmt.Sprintf("admit-%d", i)
				req, e := http.NewRequestWithContext(f.ctx, http.MethodPost, servers[i%2].URL+"/v1/workflows", strings.NewReader(`{"name":"admission","tasks":[{"key":"a","kind":"noop"}]}`))
				if e != nil {
					return e
				}
				req.Header.Set("Content-Type", "application/json")
				req.Header.Set("Idempotency-Key", key)
				response, e := client.Do(req)
				if e != nil {
					return e
				}
				defer response.Body.Close()
				if response.StatusCode == 201 {
					var receipt workflow.Receipt
					if e = json.NewDecoder(response.Body).Decode(&receipt); e != nil {
						return e
					}
					lock.Lock()
					accepted = append(accepted, admitted{key, receipt})
					lock.Unlock()
					return nil
				}
				if response.StatusCode != 429 || response.Header.Get("Retry-After") != "1" {
					return fmt.Errorf("unexpected saturation HTTP %d", response.StatusCode)
				}
				return nil
			}) {
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(accepted) != expected || scalar(t, f, `SELECT count(*) FROM workflows`) != int64(expected) {
				t.Fatal("admission exceeded hard bound", len(accepted))
			}
			first := accepted[0]
			if replay, err := f.s2.Create(f.ctx, first.key, workflow.CreateInput{Name: "admission", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop"}}}); err != nil || replay.ID != first.receipt.ID {
				t.Fatal("saturation blocked replay", err)
			}
			if _, err := f.s1.ReconcileBatch(f.ctx, 100); err != nil {
				t.Fatal(err)
			}
			if _, err := f.s2.ReconcileBatch(f.ctx, 100); err != nil {
				t.Fatal(err)
			}
			if scalar(t, f, `SELECT count(*) FROM tasks WHERE status='READY'`) != 2 {
				t.Fatal("READY backpressure failed")
			}
			// Result reporting is deliberately outside new-work admission controls.
			var wf, task uuid.UUID
			if err := f.p1.QueryRow(f.ctx, `SELECT workflow_id,id FROM tasks WHERE status='READY' ORDER BY created_at,id LIMIT 1`).Scan(&wf, &task); err != nil {
				t.Fatal(err)
			}
			rpc := rpcServer(t, f, "")
			instance := uuid.NewString()
			if _, err := rpc.ca.RegisterWorker(f.ctx, &workerv1.RegisterRequest{InstanceId: instance, Capabilities: []string{"noop"}, Capacity: 1}); err != nil {
				t.Fatal(err)
			}
			a, err := rpc.ca.ClaimTask(f.ctx, &workerv1.ClaimRequest{InstanceId: instance, TaskId: task.String(), DispatchGeneration: 1, RequestId: uuid.NewString()})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = rpc.ca.CompleteTask(f.ctx, &workerv1.OutcomeRequest{InstanceId: instance, AttemptId: a.Id, Generation: 1, ResultJson: []byte(`{}`)}); err != nil {
				t.Fatal("saturation blocked result", err)
			}
			other := accepted[len(accepted)-1].receipt.ID
			if other == wf {
				other = accepted[1].receipt.ID
			}
			request, _ := http.NewRequestWithContext(f.ctx, http.MethodPost, servers[0].URL+"/v1/workflows/"+other.String()+"/cancel", nil)
			response, err := client.Do(request)
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal("saturation blocked cancellation")
			}
			response, err = client.Get(servers[1].URL + "/health/ready")
			if err != nil {
				t.Fatal(err)
			}
			response.Body.Close()
			if response.StatusCode != 200 {
				t.Fatal("saturation blocked health")
			}
		})
	}
}

func TestCapabilityBoundAndVisibleContradiction(t *testing.T) {
	f := setup(t)
	p := execution.DefaultPolicy()
	p.CapabilityWait = 150 * time.Millisecond
	recoveryPolicy(t, f, p)
	instance := uuid.New()
	if _, err := f.s1.RegisterWorker(f.ctx, "worker_a", instance, []string{"noop"}, 1); err != nil {
		t.Fatal(err)
	}
	r := newGraph(t, f, "capability", workflow.CreateInput{Name: "capability", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "delay", DelayMS: 100}}})
	reconcile(t, f, r.ID)
	task := snapshot(t, f, r.ID).Nodes[0].ID
	if _, err := f.s1.ClaimTask(f.ctx, "worker_a", instance, task, uuid.New(), 1); !errors.Is(err, execution.ErrCapability) {
		t.Fatal("mismatch not classified", err)
	}
	waitFor(t, f, `SELECT count(*) FROM tasks WHERE id=$1 AND ready_since<=clock_timestamp()-interval '150 milliseconds'`, task)
	reconcile(t, f, r.ID)
	view, err := f.s1.InspectTask(f.ctx, r.ID, task)
	if err != nil || view.LastError != execution.Unsupported || view.AttemptCount != 0 || snapshot(t, f, r.ID).Status != workflow.Failed {
		t.Fatal("unsupported task hot-looped", err)
	}
	wf, task := runnable(t, f, "contradiction")
	a := claimTask(t, f, "worker_a", instance, task, 1)
	exec(t, f, `UPDATE tasks SET dispatch_generation=dispatch_generation+1 WHERE id=$1`, task)
	if _, err = f.s1.Reconcile(f.ctx, wf); !errors.Is(err, execution.ErrInvariant) {
		t.Fatal("contradiction silently repaired", err)
	}
	if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND state='RUNNING'`, a.ID) != 1 {
		t.Fatal("contradictory evidence erased")
	}
	exec(t, f, `UPDATE workflows SET status='CANCELLED' WHERE id=$1`, wf)
	if err = f.s1.CheckConsistency(f.ctx); !errors.Is(err, execution.ErrInvariant) {
		t.Fatal("capacity contradiction invisible", err)
	}
	if err = scheduler.Run(f.ctx, f.s1, scheduler.Settings{BatchSize: 2, Interval: time.Second, ScanTimeout: time.Second}, slog.New(slog.NewJSONHandler(io.Discard, nil))); !errors.Is(err, execution.ErrInvariant) {
		t.Fatal("scheduler continued after contradiction", err)
	}
}

func TestLostNotificationRepublishSameGenerationNewTransportID(t *testing.T) {
	f := setup(t)
	p := execution.DefaultPolicy()
	p.ReadyResend = time.Second
	recoveryPolicy(t, f, p)
	b := testBroker(t, f)
	wf, task := runnable(t, f, "lost-notification")
	items, err := f.s1.ClaimOutbox(f.ctx, uuid.New(), 1, time.Second)
	if err != nil || len(items) != 1 {
		t.Fatal(err)
	}
	// Let this relay claim lapse, then use production relay for publish/sent.
	waitFor(t, f, `SELECT count(*) FROM outbox WHERE relay_lease_expires_at<=clock_timestamp()`)
	if _, err = relay.Step(f.ctx, f.s1, b, uuid.New(), 1, time.Second); err != nil {
		t.Fatal(err)
	}
	// Remove messages AND stream/consumer definitions, modeling a broker
	// storage reset. Existing clients must rebuild topology before resend.
	if err = b.JS.DeleteStream(f.ctx, broker.Stream); err != nil {
		t.Fatal(err)
	}
	if err = b.JS.DeleteStream(f.ctx, broker.Quarantine); err != nil {
		t.Fatal(err)
	}
	waitFor(t, f, `SELECT count(*) FROM outbox WHERE sent_at<=clock_timestamp()-interval '1 second'`)
	for _, e := range parallel(2, func(i int) error {
		s := f.s1
		if i == 1 {
			s = f.s2
		}
		_, e := s.Reconcile(f.ctx, wf)
		return e
	}) {
		if e != nil {
			t.Fatal(e)
		}
	}
	var revision int
	if err = f.p1.QueryRow(f.ctx, `SELECT publish_revision FROM outbox WHERE task_id=$1`, task).Scan(&revision); err != nil || revision != 1 {
		t.Fatal("resend frequency or logical uniqueness", revision, err)
	}
	if scalar(t, f, `SELECT dispatch_generation FROM tasks WHERE id=$1`, task) != 1 || scalar(t, f, `SELECT count(*) FROM outbox WHERE task_id=$1`, task) != 1 {
		t.Fatal("notification loss changed execution generation")
	}
	if _, err = relay.Step(f.ctx, f.s1, b, uuid.New(), 1, time.Second); err != nil {
		t.Fatal(err)
	}
	notification := message(t, b)
	decoded, err := execution.DecodeNotification(notification.Data())
	if err != nil || decoded.Generation != 1 {
		t.Fatal("lost dispatch not restored", err)
	}
	instance := register(t, f, "worker_a", 1)
	a := claimTask(t, f, "worker_a", instance, task, 1)
	if err = notification.DoubleAck(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s1.FinishAttempt(f.ctx, "worker_a", instance, a.ID, 1, true, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, wf)
	page, err := f.s1.Attempts(f.ctx, wf, task, 0, 10)
	if err != nil || len(page.Attempts) != 1 || page.NextAfter != 1 {
		t.Fatal("bounded inspection", err)
	}
	if _, err = f.s1.Attempts(f.ctx, wf, task, 0, 11); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatal("attempt page unbounded", err)
	}
}
func TestReadOnlyInspectionHTTPAndTransientRPCClassification(t *testing.T) {
	f := setup(t)
	r := rpcServer(t, f, "")
	instance := uuid.NewString()
	if _, err := r.ca.RegisterWorker(f.ctx, &workerv1.RegisterRequest{InstanceId: instance, Capabilities: []string{"noop"}, Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	wf, task := runnable(t, f, "inspection")
	a, err := r.ca.ClaimTask(f.ctx, &workerv1.ClaimRequest{InstanceId: instance, TaskId: task.String(), DispatchGeneration: 1, RequestId: uuid.NewString()})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = r.ca.FailTask(f.ctx, &workerv1.OutcomeRequest{InstanceId: instance, AttemptId: a.Id, Generation: 1, FailureClass: execution.Transient, ResultJson: []byte(`{"bounded":true}`)}); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(httpapi.New(f.s1, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, time.Second).Handler())
	defer server.Close()
	path := server.URL + "/v1/workflows/" + wf.String() + "/tasks/" + task.String()
	for _, suffix := range []string{"", "/attempts?after=0&limit=1"} {
		response, e := http.Get(path + suffix)
		if e != nil {
			t.Fatal(e)
		}
		body, e := io.ReadAll(io.LimitReader(response.Body, 65537))
		response.Body.Close()
		if e != nil || response.StatusCode != 200 || len(body) > 65536 || strings.Contains(string(body), r.token) {
			t.Fatal("inspection boundedness or credential leak", e)
		}
		if suffix == "" {
			var view execution.TaskInspection
			if e = json.Unmarshal(body, &view); e != nil || view.Status != workflow.RetryWait || view.LastError != execution.Transient || view.ManualRedrive != "deferred_read_only" {
				t.Fatal("missing recovery diagnostics", e)
			}
		}
	}
	response, err := http.Get(path + "/attempts?limit=11")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 400 {
		t.Fatal("unbounded attempt limit accepted")
	}
	req, _ := http.NewRequest(http.MethodPost, path, nil)
	response, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 405 {
		t.Fatal("manual redrive unexpectedly exposed")
	}
	// Record the actual bounded eligibility scan plan without asserting speed or
	// forcing an index choice on a one-task fixture.
	rows, err := f.p1.Query(f.ctx, `EXPLAIN (ANALYZE,BUFFERS) SELECT id,status,dispatch_generation,deadline_at,ready_since,kind FROM tasks WHERE workflow_id=$1 AND (status IN ('PENDING','READY') OR (status='RETRY_WAIT' AND next_attempt_at<=clock_timestamp())) ORDER BY position LIMIT 100`, wf)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var line string
		if err = rows.Scan(&line); err != nil {
			t.Fatal(err)
		}
		t.Log("small-fixture eligibility plan:", line)
	}
	if err = rows.Err(); err != nil {
		t.Fatal(err)
	}
}

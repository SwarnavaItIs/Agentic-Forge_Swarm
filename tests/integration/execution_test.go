//go:build integration && stage03

package integration

import (
	"bytes"
	"context"
	"errors"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/broker"
	"forgeswarm/internal/config"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/relay"
	"forgeswarm/internal/rpc"
	"forgeswarm/internal/worker"
	"forgeswarm/internal/workflow"
	"forgeswarm/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"io"
	"log/slog"
	"net"
	"os"
	osExec "os/exec"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func register(t *testing.T, f *fixture, name string, capacity int) uuid.UUID {
	t.Helper()
	id := uuid.New()
	if _, err := f.s1.RegisterWorker(f.ctx, name, id, []string{"noop", "delay"}, capacity); err != nil {
		t.Fatal(err)
	}
	return id
}
func runnable(t *testing.T, f *fixture, key string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	r := newGraph(t, f, key, workflow.CreateInput{Name: key, Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop"}}})
	reconcile(t, f, r.ID)
	return r.ID, snapshot(t, f, r.ID).Nodes[0].ID
}
func claimTask(t *testing.T, f *fixture, name string, instance, task uuid.UUID, gen int64) execution.Attempt {
	t.Helper()
	a, err := f.s1.ClaimTask(f.ctx, name, instance, task, uuid.New(), gen)
	if err != nil {
		t.Fatal(err)
	}
	return a
}
func expire(t *testing.T, f *fixture, a execution.Attempt) {
	t.Helper()
	tx, err := f.p1.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(f.ctx, `SELECT id FROM workflows WHERE id=(SELECT workflow_id FROM task_attempts WHERE id=$1) FOR UPDATE`, a.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(f.ctx, `UPDATE task_attempts SET lease_expires_at=started_at+interval '1 millisecond' WHERE id=$1`, a.ID); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND lease_expires_at<=clock_timestamp()`, a.ID)
}

// Stage 04 intentionally replaces immediate lease-loss requeue with persisted
// RETRY_WAIT. Earlier ownership checks still require the next eligible claim.
func awaitRetryReady(t *testing.T, f *fixture, wf, task uuid.UUID) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		reconcile(t, f, wf)
		g := snapshot(t, f, wf)
		for _, n := range g.Nodes {
			if n.ID == task && (n.Status == workflow.Ready || n.Status == workflow.Failed) {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("retry did not become durably eligible")
}
func TestCompetingClaimsRetryAndTransactionalCapacity(t *testing.T) {
	f := setup(t)
	ia, ib := register(t, f, "worker_a", 1), register(t, f, "worker_b", 1)
	wf, task := runnable(t, f, "competing")
	keys := []uuid.UUID{uuid.New(), uuid.New()}
	attempts := make([]execution.Attempt, 2)
	errs := parallel(2, func(i int) error {
		name, instance, store := "worker_a", ia, f.s1
		if i == 1 {
			name, instance, store = "worker_b", ib, f.s2
		}
		a, err := store.ClaimTask(f.ctx, name, instance, task, keys[i], 1)
		attempts[i] = a
		return err
	})
	winners := 0
	winner := 0
	for i, err := range errs {
		if err == nil {
			winners++
			winner = i
		} else if !errors.Is(err, execution.ErrClaimed) {
			t.Fatal(err)
		}
	}
	if winners != 1 || scalar(t, f, `SELECT count(*) FROM task_attempts`) != 1 {
		t.Fatal("multiple claims")
	}
	name, instance := "worker_a", ia
	if winner == 1 {
		name, instance = "worker_b", ib
	}
	retry, err := f.s2.ClaimTask(f.ctx, name, instance, task, keys[winner], 1)
	if err != nil || retry.ID != attempts[winner].ID || !retry.LeaseExpiresAt.Equal(attempts[winner].LeaseExpiresAt) {
		t.Fatal("claim reply retry changed attempt", err)
	}
	if snapshot(t, f, wf).Status != workflow.Running {
		t.Fatal("accepted claim did not start workflow")
	}
	_, other := runnable(t, f, "capacity")
	if _, err = f.s1.ClaimTask(f.ctx, name, instance, other, uuid.New(), 1); !errors.Is(err, execution.ErrCapacity) {
		t.Fatal("worker capacity bypass", err)
	}
	tight, err := postgres.NewWithLimits(f.p2, execution.Limits{WorkerCapacity: 1, TotalCapacity: 1, MaxAttempts: 3, Lease: 3 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	otherName, otherInstance := "worker_b", ib
	if winner == 1 {
		otherName, otherInstance = "worker_a", ia
	}
	if _, err = tight.ClaimTask(f.ctx, otherName, otherInstance, other, uuid.New(), 1); !errors.Is(err, execution.ErrCapacity) {
		t.Fatal("total capacity bypass", err)
	}
	drain := true
	if _, err = f.s1.HeartbeatWorker(f.ctx, otherName, otherInstance, &drain); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s2.ClaimTask(f.ctx, otherName, otherInstance, other, uuid.New(), 1); !errors.Is(err, execution.ErrDraining) {
		t.Fatal("draining accepted new claim", err)
	}
	if _, err = f.s1.ClaimTask(f.ctx, otherName, otherInstance, task, uuid.New(), 99); !errors.Is(err, execution.ErrObsolete) {
		t.Fatal("old generation claimed", err)
	}
}
func TestReadyOutboxRollbackAndAcceptedFailurePropagation(t *testing.T) {
	f := setup(t)
	r := newGraph(t, f, "outbox-rollback", workflow.CreateInput{Name: "rollback", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop"}}})
	exec(t, f, `CREATE FUNCTION reject_dispatch() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test dispatch failure'; END $$`)
	exec(t, f, `CREATE TRIGGER reject_dispatch BEFORE INSERT ON outbox FOR EACH ROW EXECUTE FUNCTION reject_dispatch()`)
	if _, err := f.s1.Reconcile(f.ctx, r.ID); err == nil {
		t.Fatal("outbox insertion did not fail")
	}
	g := snapshot(t, f, r.ID)
	if g.Version != 1 || g.Nodes[0].Status != workflow.Pending || len(history(t, f, r.ID)) != 1 || scalar(t, f, `SELECT sum(dispatch_generation) FROM tasks`) != 0 || scalar(t, f, `SELECT count(*) FROM outbox`) != 0 {
		t.Fatal("READY/outbox rollback was partial")
	}
	exec(t, f, `DROP TRIGGER reject_dispatch ON outbox`)
	reconcile(t, f, r.ID)
	if scalar(t, f, `SELECT count(*) FROM outbox`) != 1 {
		t.Fatal("READY lacked outbox")
	}
	instance := register(t, f, "worker_a", 2)
	root := claimTask(t, f, "worker_a", instance, g.Nodes[0].ID, 1)
	if _, err := f.s1.FinishAttempt(f.ctx, "worker_a", instance, root.ID, 1, true, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, r.ID)
	r = newGraph(t, f, "accepted-failure", workflow.CreateInput{Name: "failure", Tasks: []workflow.TaskDefinition{{Key: "root", Kind: "noop"}, {Key: "child", Kind: "noop", DependsOn: []string{"root"}}, {Key: "independent", Kind: "noop"}}})
	reconcile(t, f, r.ID)
	g = snapshot(t, f, r.ID)
	a := claimTask(t, f, "worker_a", instance, g.Nodes[0].ID, 1)
	independent := claimTask(t, f, "worker_a", instance, g.Nodes[2].ID, 1)
	if _, err := f.s1.FinishAttempt(f.ctx, "worker_a", instance, a.ID, 1, false, []byte(`{"failed":true}`)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, r.ID)
	g = snapshot(t, f, r.ID)
	if g.Status != workflow.Running || g.Nodes[1].Status != workflow.Blocked || g.Nodes[2].Status != workflow.Running {
		t.Fatal("accepted failure did not preserve independent progress")
	}
	if _, err := f.s1.FinishAttempt(f.ctx, "worker_a", instance, independent.ID, 1, true, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, r.ID)
	if snapshot(t, f, r.ID).Status != workflow.Failed {
		t.Fatal("accepted failure did not aggregate")
	}
}
func TestStage02ReadyMigrationAndNormalizationCompatibility(t *testing.T) {
	f := setupAt(t, 2)
	in := workflow.CreateInput{Name: "stage2 ready", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop"}}}
	hash, err := workflow.HashForVersion(in, 2)
	if err != nil {
		t.Fatal(err)
	}
	wf, task := uuid.New(), uuid.New()
	var receipt workflow.Receipt
	var updated time.Time
	err = f.p1.QueryRow(f.ctx, `INSERT INTO workflows(id,name,status,version,last_event_sequence,creation_key,request_hash,request_schema_version) VALUES($1,$2,'PENDING',2,2,'stage2-ready',$3,2) RETURNING id,created_at,updated_at`, wf, in.Name, hash[:]).Scan(&receipt.ID, &receipt.CreatedAt, &updated)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, f, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,'a',0,'noop','READY')`, task, wf)
	exec(t, f, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created','{}'),($1,2,'task.ready','{}')`, wf)
	db := stdlib.OpenDB(*f.cfg2.ConnConfig)
	defer db.Close()
	if err = migrations.Up(f.ctx, db); err != nil {
		t.Fatal(err)
	}
	if err = f.s1.Ready(f.ctx); err != nil {
		t.Fatal(err)
	}
	var stored []byte
	var version int
	var after time.Time
	err = f.p1.QueryRow(f.ctx, `SELECT request_hash,request_schema_version,updated_at FROM workflows WHERE id=$1`, wf).Scan(&stored, &version, &after)
	if err != nil || version != 2 || !bytes.Equal(stored, hash[:]) || !updated.Equal(after) || snapshot(t, f, wf).Version != 2 || len(history(t, f, wf)) != 2 {
		t.Fatal("upgrade rewrote public state/hash", err)
	}
	if scalar(t, f, `SELECT count(*) FROM outbox WHERE task_id=$1 AND dispatch_generation=1 AND schema_version=1`, task) != 1 || scalar(t, f, `SELECT dispatch_generation FROM tasks WHERE id=$1`, task) != 1 {
		t.Fatal("old READY row not queued")
	}
	if replay, err := f.s1.Create(f.ctx, "stage2-ready", in); err != nil || replay.ID != receipt.ID || !replay.CreatedAt.Equal(receipt.CreatedAt) {
		t.Fatal("old receipt changed", err)
	}
	in.Tasks[0].Kind = "delay"
	in.Tasks[0].DelayMS = 10
	if _, err = f.s1.Create(f.ctx, "stage2-ready", in); !errors.Is(err, workflow.ErrConflict) {
		t.Fatal("new fields silently ignored", err)
	}
	newReceipt := newGraph(t, f, "delay-definition", in)
	if _, err = f.p1.Exec(f.ctx, `UPDATE tasks SET delay_ms=20 WHERE workflow_id=$1`, newReceipt.ID); err == nil {
		t.Fatal("delay definition mutable")
	}
	if scalar(t, f, `SELECT request_schema_version FROM workflows WHERE id=$1`, newReceipt.ID) != workflow.RequestSchemaVersion {
		t.Fatal("new normalization version missing")
	}
}
func TestExpiryFencingExhaustionAndCancellation(t *testing.T) {
	f := setup(t)
	ia, ib := register(t, f, "worker_a", 1), register(t, f, "worker_b", 1)
	wf, task := runnable(t, f, "lease-fence")
	old := claimTask(t, f, "worker_a", ia, task, 1)
	expire(t, f, old)
	if _, err := f.s1.RenewLease(f.ctx, "worker_a", ia, old.ID, old.Generation); !errors.Is(err, execution.ErrStale) {
		t.Fatal("expired renewal revived ownership", err)
	}
	if _, err := f.s1.FinishAttempt(f.ctx, "worker_a", ia, old.ID, old.Generation, true, []byte(`{}`)); !errors.Is(err, execution.ErrStale) {
		t.Fatal("expired result accepted", err)
	}
	reconcile(t, f, wf)
	awaitRetryReady(t, f, wf, task)
	fresh := claimTask(t, f, "worker_b", ib, task, 2)
	if _, err := f.s1.FinishAttempt(f.ctx, "worker_a", ia, old.ID, 1, true, []byte(`{}`)); !errors.Is(err, execution.ErrStale) {
		t.Fatal("superseded attempt result accepted", err)
	}
	if _, err := f.s1.FinishAttempt(f.ctx, "worker_b", ib, fresh.ID, 2, true, []byte(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, wf)
	if snapshot(t, f, wf).Status != workflow.Succeeded {
		t.Fatal("new generation did not complete")
	}
	wf, task = runnable(t, f, "exhaustion")
	for i := 1; i <= 3; i++ {
		a := claimTask(t, f, "worker_a", ia, task, int64(i))
		expire(t, f, a)
		reconcile(t, f, wf)
		awaitRetryReady(t, f, wf, task)
	}
	if snapshot(t, f, wf).Status != workflow.Failed || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND state='LOST'`, task) != 3 || scalar(t, f, `SELECT count(*) FROM outbox WHERE task_id=$1`, task) != 3 {
		t.Fatal("unbounded attempts or incorrect exhaustion")
	}
	wf, task = runnable(t, f, "active-cancel")
	a := claimTask(t, f, "worker_a", ia, task, 1)
	if _, err := f.s2.Cancel(f.ctx, wf); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s1.FinishAttempt(f.ctx, "worker_a", ia, a.ID, 1, true, []byte(`{}`)); !errors.Is(err, execution.ErrStale) {
		t.Fatal("late cancelled success accepted", err)
	}
	signal, err := f.s1.RenewLease(f.ctx, "worker_a", ia, a.ID, 1)
	if err != nil || !signal.StopRequested || !signal.LeaseExpiresAt.Equal(a.LeaseExpiresAt) {
		t.Fatal("cancellation did not signal stop without renewal", err)
	}
	if snapshot(t, f, wf).Status != workflow.Cancelling {
		t.Fatal("active cancellation falsely claimed remote termination")
	}
	if _, err = f.s1.AcknowledgeStop(f.ctx, "worker_a", ia, a.ID, 1); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, wf)
	if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND state='CANCELLED'`, a.ID) != 1 {
		t.Fatal("cancel did not close active attempt")
	}
}

type rpcFixture struct {
	ca, cb workerv1.CoordinatorClient
	cfg    config.Distributed
	token  string
}

func rpcServer(t *testing.T, f *fixture, drop string) *rpcFixture {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	token, tokenB := uuid.NewString(), uuid.NewString()
	cfg := config.Distributed{RPCAddr: listener.Addr().String(), WorkerToken: token, InsecureLocal: true, RPCTimeout: time.Second, Capacity: 1, Shutdown: time.Second, Limits: execution.DefaultLimits(), Poll: 100 * time.Millisecond}
	opts, err := rpc.ServerOptions(cfg, map[string]string{"worker_a": token, "worker_b": tokenB})
	if err != nil {
		t.Fatal(err)
	}
	var dropped atomic.Bool
	opts = append(opts, grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
		value, err := next(ctx, req)
		if err == nil && drop != "" && strings.HasSuffix(info.FullMethod, "/"+drop) && dropped.CompareAndSwap(false, true) {
			return nil, status.Error(codes.Unavailable, "test_lost_reply")
		}
		return value, err
	}))
	server := grpc.NewServer(opts...)
	workerv1.RegisterCoordinatorServer(server, rpc.New(f.s1))
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := rpc.Dial(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	cfgB := cfg
	cfgB.WorkerToken = tokenB
	connB, err := rpc.Dial(cfgB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { connB.Close() })
	return &rpcFixture{workerv1.NewCoordinatorClient(conn), workerv1.NewCoordinatorClient(connB), cfg, token}
}
func TestAuthenticatedRPCLostClaimAndOutcomeReplies(t *testing.T) {
	for _, drop := range []string{"ClaimTask", "CompleteTask"} {
		t.Run(drop, func(t *testing.T) {
			f := setup(t)
			r := rpcServer(t, f, drop)
			instance := uuid.NewString()
			if _, err := r.ca.RegisterWorker(f.ctx, &workerv1.RegisterRequest{InstanceId: instance, Capabilities: []string{"noop"}, Capacity: 1}); err != nil {
				t.Fatal(err)
			}
			_, task := runnable(t, f, "lost-rpc-reply")
			request := &workerv1.ClaimRequest{InstanceId: instance, TaskId: task.String(), DispatchGeneration: 1, RequestId: uuid.NewString()}
			a, err := r.ca.ClaimTask(f.ctx, request)
			if drop == "ClaimTask" {
				if status.Code(err) != codes.Unavailable {
					t.Fatal("reply not discarded", err)
				}
				a, err = r.ca.ClaimTask(f.ctx, request)
			}
			if err != nil {
				t.Fatal(err)
			}
			if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1`, task) != 1 {
				t.Fatal("lost reply duplicated attempt")
			}
			result := &workerv1.OutcomeRequest{InstanceId: instance, AttemptId: a.Id, Generation: 1, ResultJson: []byte(`{"a":1,"b":true}`)}
			done, err := r.ca.CompleteTask(f.ctx, result)
			if drop == "CompleteTask" {
				if status.Code(err) != codes.Unavailable {
					t.Fatal("result reply not discarded", err)
				}
				done, err = r.ca.CompleteTask(f.ctx, result)
			}
			if err != nil {
				t.Fatal(err)
			}
			result.ResultJson = []byte(`{"b":true,"a":1.0}`)
			again, err := r.ca.CompleteTask(f.ctx, result)
			if err != nil || !proto.Equal(done, again) {
				t.Fatal("logical result retry differs", err)
			}
			if drop == "CompleteTask" {
				waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND lease_expires_at<=clock_timestamp()`, uuid.MustParse(a.Id))
				if again, err = r.ca.CompleteTask(f.ctx, result); err != nil || !proto.Equal(done, again) {
					t.Fatal("accepted receipt retry after lease expiry changed", err)
				}
			}
			result.ResultJson = []byte(`{"a":2}`)
			if _, err = r.ca.CompleteTask(f.ctx, result); status.Code(err) != codes.AlreadyExists {
				t.Fatal("conflicting outcome allowed", err)
			}
			result.ResultJson = []byte(`{"a":1,"b":true}`)
			if _, err = r.ca.FailTask(f.ctx, result); status.Code(err) != codes.AlreadyExists {
				t.Fatal("success rewritten as failure", err)
			}
			if _, err = r.cb.CompleteTask(f.ctx, result); err == nil {
				t.Fatal("another identity overwrote result")
			}
			// An accepted receipt remains retryable after a replacement instance
			// registers. No live lease is required to read an accepted outcome.
			if _, err = r.ca.RegisterWorker(f.ctx, &workerv1.RegisterRequest{InstanceId: uuid.NewString(), Capabilities: []string{"noop"}, Capacity: 1}); err != nil {
				t.Fatal(err)
			}
			if again, err = r.ca.CompleteTask(f.ctx, result); err != nil || !proto.Equal(done, again) {
				t.Fatal("replacement instance lost accepted receipt", err)
			}
			result.ResultJson = bytes.Repeat([]byte("x"), 4097)
			if _, err = r.ca.CompleteTask(f.ctx, result); status.Code(err) != codes.InvalidArgument {
				t.Fatal("unbounded result accepted", err)
			}
			raw, err := grpc.NewClient(r.cfg.RPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				t.Fatal(err)
			}
			defer raw.Close()
			if _, err = workerv1.NewCoordinatorClient(raw).HeartbeatWorker(f.ctx, &workerv1.InstanceRequest{InstanceId: instance}); status.Code(err) != codes.Unauthenticated {
				t.Fatal("unauthenticated RPC reached store", err)
			}
		})
	}
}
func testBroker(t *testing.T, f *fixture) *broker.Broker {
	t.Helper()
	if os.Getenv("TEST_NATS_NAMESPACE") != os.Getenv("TEST_RESOURCE_TOKEN") || os.Getenv("TEST_NATS_URL") == "" {
		t.Fatal("requested owned broker infrastructure unavailable")
	}
	c := config.Distributed{BrokerURL: os.Getenv("TEST_NATS_URL"), NATSToken: os.Getenv("TEST_NATS_TOKEN"), InsecureLocal: true}
	b, err := broker.Open(f.ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(b.Close)
	stream, err := b.JS.Stream(f.ctx, broker.Stream)
	if err != nil {
		t.Fatal(err)
	}
	if err = stream.Purge(f.ctx); err != nil {
		t.Fatal(err)
	}
	return b
}
func message(t *testing.T, b *broker.Broker) jetstream.Msg {
	t.Helper()
	batch, err := b.Consumer.Fetch(1, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	m := <-batch.Messages()
	if m == nil {
		t.Fatal("missing real broker delivery", batch.Error())
	}
	return m
}
func TestWorkerRunConsumesAndDrainsRealBroker(t *testing.T) {
	f := setup(t)
	b := testBroker(t, f)
	r := rpcServer(t, f, "")
	_, task := runnable(t, f, "production-pull")
	if _, err := relay.Step(f.ctx, f.s1, b, uuid.New(), 10, time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx, r.ca, b, r.cfg, slog.New(slog.NewJSONHandler(io.Discard, nil))) }()
	waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND state='SUCCEEDED'`, task)
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("worker shutdown exceeded bound")
	}
	if scalar(t, f, `SELECT count(*) FROM workers WHERE id='worker_a' AND draining`) != 1 {
		t.Fatal("drain not persisted")
	}
}

type pausedPublisher struct{ *broker.Broker }

func (p pausedPublisher) Publish(ctx context.Context, id string, body []byte) error {
	if err := p.Broker.Publish(ctx, id, body); err != nil {
		return err
	}
	os.Stdout.WriteString("PHASE_REACHED\n")
	<-ctx.Done()
	return ctx.Err()
}
func TestRelayProcessHelper(t *testing.T) {
	if os.Getenv("FS_RELAY_PHASE") == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ConnConfig.Database != "forgeswarm_stage01_"+os.Getenv("TEST_RESOURCE_TOKEN") {
		t.Fatal("unowned helper DB")
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = os.Getenv("FS_DB_SCHEMA")
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	b, err := broker.Open(ctx, config.Distributed{BrokerURL: os.Getenv("TEST_NATS_URL"), NATSToken: os.Getenv("TEST_NATS_TOKEN"), InsecureLocal: true})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	_, err = relay.Step(ctx, postgres.New(pool), pausedPublisher{b}, uuid.New(), 1, time.Second)
	if err != nil {
		t.Fatal(err)
	}
}
func killHelper(t *testing.T, f *fixture, selection string, env ...string) {
	t.Helper()
	child := osExec.CommandContext(f.ctx, os.Args[0], "-test.run=^"+selection+"$")
	child.Env = append(os.Environ(), env...)
	stdout, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var diagnostics bytes.Buffer
	child.Stderr = &diagnostics
	if err = child.Start(); err != nil {
		t.Fatal(err)
	}
	reached := make(chan bool, 1)
	go func() {
		var line strings.Builder
		data := make([]byte, 1)
		for line.Len() < 4096 {
			if _, err := stdout.Read(data); err != nil {
				reached <- false
				return
			}
			line.WriteByte(data[0])
			if strings.Contains(line.String(), "PHASE_REACHED") {
				reached <- true
				return
			}
		}
		reached <- false
	}()
	var ok bool
	select {
	case ok = <-reached:
	case <-time.After(10 * time.Second):
	}
	_ = child.Process.Kill()
	_ = child.Wait()
	if !ok {
		t.Fatal("helper did not reach crash boundary", diagnostics.String())
	}
}
func TestRealBrokerPublishCrashRedeliveryDuplicatesAndQuarantine(t *testing.T) {
	f := setup(t)
	b := testBroker(t, f)
	wf, task := runnable(t, f, "relay-crash")
	var item postgres.Dispatch
	if err := f.p1.QueryRow(f.ctx, `SELECT id,payload FROM outbox WHERE task_id=$1`, task).Scan(&item.ID, &item.Payload); err != nil {
		t.Fatal(err)
	}
	killHelper(t, f, "TestRelayProcessHelper", "FS_RELAY_PHASE=after_publish_ack", "FS_DB_SCHEMA="+f.cfg2.ConnConfig.RuntimeParams["search_path"])
	if scalar(t, f, `SELECT count(*) FROM outbox WHERE state='PENDING' AND sent_at IS NULL`) != 1 {
		t.Fatal("killed relay recorded sent before returning publish acknowledgment")
	}
	waitFor(t, f, `SELECT count(*) FROM outbox WHERE relay_lease_expires_at<=clock_timestamp()`)
	if n, err := relay.Step(f.ctx, f.s2, b, uuid.New(), 1, time.Second); err != nil || n != 1 {
		t.Fatal("relay failed to recover acknowledged publish", err)
	}
	if scalar(t, f, `SELECT count(*) FROM outbox WHERE state='SENT'`) != 1 {
		t.Fatal("sent not recorded after durable ack")
	}
	first := message(t, b)
	if err := first.NakWithDelay(50 * time.Millisecond); err != nil {
		t.Fatal(err)
	}
	redelivery := message(t, b)
	meta, err := redelivery.Metadata()
	if err != nil || meta.NumDelivered < 2 {
		t.Fatal("no real redelivery", err)
	}
	i := register(t, f, "worker_a", 1)
	n, err := execution.DecodeNotification(redelivery.Data())
	if err != nil {
		t.Fatal(err)
	}
	a := claimTask(t, f, "worker_a", i, n.TaskID, n.Generation)
	if err = redelivery.DoubleAck(f.ctx); err != nil {
		t.Fatal(err)
	}
	// Distinct transport message IDs bypass broker duplicate suppression; the
	// database still fences duplicate and obsolete-generation notifications.
	if err = b.Publish(f.ctx, uuid.NewString(), item.Payload); err != nil {
		t.Fatal(err)
	}
	duplicate := message(t, b)
	if _, err = f.s2.ClaimTask(f.ctx, "worker_a", i, task, uuid.New(), 1); !errors.Is(err, execution.ErrClaimed) {
		t.Fatal("duplicate dispatch accepted", err)
	}
	if err = duplicate.DoubleAck(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err = f.s1.FinishAttempt(f.ctx, "worker_a", i, a.ID, 1, true, []byte(`{}`)); err != nil {
		t.Fatal(err)
	}
	reconcile(t, f, wf)
	if err = b.Publish(f.ctx, uuid.NewString(), []byte(`{"schema_version":99,"task_id":"bad"}`)); err != nil {
		t.Fatal(err)
	}
	bad := message(t, b)
	if err = b.QuarantineMessage(f.ctx, bad); err != nil {
		t.Fatal(err)
	}
	q, err := b.JS.Stream(f.ctx, broker.Quarantine)
	if err != nil {
		t.Fatal(err)
	}
	info, err := q.Info(f.ctx)
	if err != nil || info.State.Msgs < 1 {
		t.Fatal("unsupported message not quarantined", err)
	}
	if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1`, task) != 1 {
		t.Fatal("duplicates created extra accepted attempts")
	}
}

type pausedClient struct {
	workerv1.CoordinatorClient
	phase string
}

func (p pausedClient) ClaimTask(ctx context.Context, r *workerv1.ClaimRequest, o ...grpc.CallOption) (*workerv1.Attempt, error) {
	if p.phase == "before_claim" {
		os.Stdout.WriteString("PHASE_REACHED\n")
		<-ctx.Done()
		return nil, ctx.Err()
	}
	a, err := p.CoordinatorClient.ClaimTask(ctx, r, o...)
	if err == nil && p.phase == "after_claim" {
		os.Stdout.WriteString("PHASE_REACHED\n")
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return a, err
}
func (p pausedClient) CompleteTask(ctx context.Context, r *workerv1.OutcomeRequest, o ...grpc.CallOption) (*workerv1.Attempt, error) {
	a, err := p.CoordinatorClient.CompleteTask(ctx, r, o...)
	if err == nil && p.phase == "after_complete" {
		os.Stdout.WriteString("PHASE_REACHED\n")
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return a, err
}

// Test-only subprocess wrapper interrupts production worker.Handle at the
// specified boundary. No failpoint/terminal-setter exists in shipped commands.
func TestWorkerProcessHelper(t *testing.T) {
	phase := os.Getenv("FS_WORKER_PHASE")
	if phase == "" {
		return
	}
	c := config.Distributed{RPCAddr: os.Getenv("FS_RPC_ADDR"), WorkerToken: os.Getenv("FS_WORKER_TOKEN"), BrokerURL: os.Getenv("TEST_NATS_URL"), NATSToken: os.Getenv("TEST_NATS_TOKEN"), InsecureLocal: true, RPCTimeout: 30 * time.Second, Limits: execution.DefaultLimits(), Capacity: 1, Poll: 100 * time.Millisecond}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn, err := rpc.Dial(c)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := workerv1.NewCoordinatorClient(conn)
	instance := uuid.New()
	if _, err = client.RegisterWorker(ctx, &workerv1.RegisterRequest{InstanceId: instance.String(), Capabilities: []string{"noop"}, Capacity: 1}); err != nil {
		t.Fatal(err)
	}
	b, err := broker.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	m := message(t, b)
	worker.Handle(ctx, pausedClient{client, phase}, b, c, instance, m, slog.New(slog.NewJSONHandler(io.Discard, nil)))
}
func TestWorkerProcessKillsAtDeliveryAndResultBoundaries(t *testing.T) {
	for _, phase := range []string{"before_claim", "after_claim", "after_complete"} {
		t.Run(phase, func(t *testing.T) {
			f := setup(t)
			b := testBroker(t, f)
			r := rpcServer(t, f, "")
			wf, task := runnable(t, f, "kill-"+strings.ReplaceAll(phase, "_", "-"))
			if _, err := relay.Step(f.ctx, f.s1, b, uuid.New(), 10, 5*time.Second); err != nil {
				t.Fatal(err)
			}
			child := osExec.CommandContext(f.ctx, os.Args[0], "-test.run=^TestWorkerProcessHelper$")
			child.Env = append(os.Environ(), "FS_WORKER_PHASE="+phase, "FS_RPC_ADDR="+r.cfg.RPCAddr, "FS_WORKER_TOKEN="+r.token)
			stdout, err := child.StdoutPipe()
			if err != nil {
				t.Fatal(err)
			}
			if err = child.Start(); err != nil {
				t.Fatal(err)
			}
			reached := make(chan struct{})
			go func() {
				data := make([]byte, 1)
				var line strings.Builder
				for {
					_, err := stdout.Read(data)
					if err != nil {
						return
					}
					line.WriteByte(data[0])
					if strings.Contains(line.String(), "PHASE_REACHED") {
						close(reached)
						return
					}
				}
			}()
			select {
			case <-reached:
			case <-time.After(10 * time.Second):
				child.Process.Kill()
				child.Wait()
				t.Fatal("worker did not reach crash boundary")
			}
			if err = child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			_ = child.Wait()
			if phase == "after_complete" {
				reconcile(t, f, wf)
				if snapshot(t, f, wf).Status != workflow.Succeeded || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1`, task) != 1 {
					t.Fatal("completion disappeared after process kill")
				}
				return
			}
			if phase == "before_claim" {
				if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1`, task) != 0 {
					t.Fatal("killed before claim but ownership exists")
				}
				m := message(t, b)
				if err = m.DoubleAck(f.ctx); err != nil {
					t.Fatal(err)
				}
			} else {
				m := message(t, b)
				other := register(t, f, "worker_b", 1)
				if _, err = f.s2.ClaimTask(f.ctx, "worker_b", other, task, uuid.New(), 1); !errors.Is(err, execution.ErrClaimed) {
					t.Fatal("redelivery bypassed accepted claim", err)
				}
				if err = m.DoubleAck(f.ctx); err != nil {
					t.Fatal(err)
				}
				waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND lease_expires_at<=clock_timestamp()`, task)
				reconcile(t, f, wf)
			}
			// Eligibility is reconstructed from DB even though the original broker
			// message was acked by a different process after the durable claim.
			awaitRetryReady(t, f, wf, task)
			gen := scalar(t, f, `SELECT dispatch_generation FROM tasks WHERE id=$1`, task)
			instance := uuid.New()
			if _, err = f.s1.RegisterWorker(f.ctx, "worker_b", instance, []string{"noop"}, 1); err != nil {
				t.Fatal(err)
			}
			a := claimTask(t, f, "worker_b", instance, task, gen)
			if _, err = f.s1.FinishAttempt(f.ctx, "worker_b", instance, a.ID, gen, true, []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
			reconcile(t, f, wf)
			if snapshot(t, f, wf).Status != workflow.Succeeded {
				t.Fatal("killed worker lost eventual eligibility")
			}
		})
	}
}

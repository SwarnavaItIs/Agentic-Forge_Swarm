//go:build integration && stage03 && stage04 && stage05 && real_sandbox

package integration

import (
	"context"
	"fmt"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/broker"
	"forgeswarm/internal/config"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/relay"
	"forgeswarm/internal/rpc"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/worker"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"os"
	osExec "os/exec"
	"strings"
	"testing"
	"time"
)

func dockerTest(t *testing.T, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c := osExec.CommandContext(ctx, "docker", args...)
	out := &sandbox.CappedWriter{Limit: 16384}
	c.Stdout = out
	c.Stderr = io.Discard
	err := c.Run()
	if out.Truncated {
		return "", fmt.Errorf("test output bound exceeded")
	}
	return strings.TrimSpace(string(out.Data)), err
}
func dockerWait(t *testing.T, description string, check func() bool) {
	t.Helper()
	until := time.Now().Add(20 * time.Second)
	for time.Now().Before(until) {
		if check() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("timed out: " + description)
}
func executionContainer(t *testing.T, attempt uuid.UUID) string {
	t.Helper()
	id := ""
	dockerWait(t, "running repository and descendant", func() bool {
		body, e := dockerTest(t, "ps", "-q", "--no-trunc", "--filter", "label=forgeswarm.attempt="+attempt.String())
		if e != nil || len(body) != 64 {
			return false
		}
		mark, e := dockerTest(t, "exec", body, "/bin/cat", "/workspace/src/started")
		if e != nil || mark != "descendant-started" {
			return false
		}
		id = body
		return true
	})
	return id
}

const longFixture = `package fixture
import("testing";"os";"os/exec";"time")
func TestPrivate(t *testing.T){
 if _,e:=os.Stat("previous");e==nil{t.Fatal("cross-attempt workspace")}
 if e:=os.WriteFile("previous",[]byte("private"),0600);e!=nil{t.Fatal(e)}
 if f,e:=os.OpenFile("/trusted/harness_test.go",os.O_WRONLY,0);e==nil{f.Close();t.Fatal("trusted harness writable")}
 c:=exec.Command("/bin/sh","-c","sleep 30 & wait");if e:=c.Start();e!=nil{t.Fatal(e)}
 os.WriteFile("started",[]byte("descendant-started"),0600)
 time.Sleep(5*time.Second)
 c.Process.Kill();c.Wait()
}`

func processDriver(t *testing.T) (*sandbox.Docker, string) {
	t.Helper()
	registry := t.TempDir()
	helper := os.Getenv("TEST_SANDBOX_HELPER")
	if helper == "" {
		t.Fatal("required real sandbox helper unavailable")
	}
	d, err := sandbox.NewDocker(context.Background(), uuid.New(), registry, helper)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		if err := d.Reconcile(ctx, "worker_a", nil); err != nil {
			t.Error(err)
		}
	})
	return d, registry
}
func createExecutable(t *testing.T, f *fixture, d *sandbox.Docker, key, source string) (uuid.UUID, uuid.UUID) {
	t.Helper()
	id, err := sandbox.SaveSnapshot(d.Registry, sandbox.Snapshot{SchemaVersion: 1, BaseRevision: strings.Repeat("a", 40), Files: map[string][]byte{"go.mod": []byte("module fixture\n\ngo 1.26.3\n"), "fixture_test.go": []byte(source)}})
	if err != nil {
		t.Fatal(err)
	}
	r := newGraph(t, f, key, workflow.CreateInput{Name: key, Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "sandbox_go", Sandbox: &sandbox.Input{SchemaVersion: 1, FixtureID: id, Command: "test"}}}})
	reconcile(t, f, r.ID)
	return r.ID, snapshot(t, f, r.ID).Nodes[0].ID
}
func runningAttempt(t *testing.T, f *fixture, task uuid.UUID) uuid.UUID {
	t.Helper()
	waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND state='RUNNING'`, task)
	var a uuid.UUID
	if e := f.p1.QueryRow(f.ctx, `SELECT id FROM task_attempts WHERE task_id=$1 AND state='RUNNING'`, task).Scan(&a); e != nil {
		t.Fatal(e)
	}
	return a
}

func TestSandboxProductionWorkerAcceptedArtifactsAndLostReply(t *testing.T) {
	f := setup(t)
	b := testBroker(t, f)
	r := rpcServer(t, f, "CompleteSandbox")
	d, _ := processDriver(t)
	_, task := createExecutable(t, f, d, "sandbox-pull", `package fixture
import("testing";"fmt")
func TestFixture(t *testing.T){fmt.Println("real constrained fixture")}`)
	if _, err := relay.Step(f.ctx, f.s1, b, uuid.New(), 10, time.Second); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	done := make(chan error, 1)
	go func() {
		done <- worker.RunWithSandbox(ctx, r.ca, b, r.cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), d)
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(20 * time.Second):
			t.Error("worker did not drain")
		}
	})
	waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND state='SUCCEEDED'`, task)
	if scalar(t, f, `SELECT count(*) FROM sandbox_artifacts WHERE task_id=$1`, task) != 4 || scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1`, task) != 1 {
		t.Fatal("lost reply changed canonical artifacts")
	}
	if scalar(t, f, `SELECT count(*) FROM sandbox_executions x JOIN task_attempts a ON a.id=x.attempt_id WHERE a.task_id=$1 AND x.metadata->>'cleanup_confirmed'='true' AND NOT a.cleanup_required`, task) != 1 {
		t.Fatal("missing actual teardown evidence")
	}
}

func TestSandboxWorkerCancellationAndLeaseLossKillWorkload(t *testing.T) {
	for _, mode := range []string{"cancel", "lease_loss"} {
		t.Run(mode, func(t *testing.T) {
			f := setup(t)
			b := testBroker(t, f)
			r := rpcServer(t, f, "")
			d, _ := processDriver(t)
			wf, task := createExecutable(t, f, d, "physical-"+mode, longFixture)
			if _, err := relay.Step(f.ctx, f.s1, b, uuid.New(), 10, time.Second); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(f.ctx)
			done := make(chan error, 1)
			go func() {
				done <- worker.RunWithSandbox(ctx, r.ca, b, r.cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), d)
			}()
			t.Cleanup(func() {
				cancel()
				select {
				case err := <-done:
					if err != nil {
						t.Error(err)
					}
				case <-time.After(20 * time.Second):
					t.Error("worker drain timeout")
				}
			})
			a := runningAttempt(t, f, task)
			container := executionContainer(t, a)
			if mode == "cancel" {
				if _, err := f.s1.Cancel(f.ctx, wf); err != nil {
					t.Fatal(err)
				}
			} else {
				expire(t, f, execution.Attempt{ID: a})
				reconcile(t, f, wf)
			}
			dockerWait(t, "owned workload physically removed", func() bool {
				ids, e := dockerTest(t, "ps", "-aq", "--no-trunc", "--filter", "id="+container)
				return e == nil && ids == ""
			})
			waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND NOT cleanup_required AND state<>'RUNNING'`, a)
			if scalar(t, f, `SELECT count(*) FROM sandbox_artifacts WHERE attempt_id=$1`, a) != 0 {
				t.Fatal("lost/cancelled attempt published artifacts")
			}
			if mode == "lease_loss" {
				awaitRetryReady(t, f, wf, task)
				if _, err := relay.Step(f.ctx, f.s1, b, uuid.New(), 10, time.Second); err != nil {
					t.Fatal(err)
				}
				dockerWait(t, "fresh B execution accepted", func() bool {
					return scalar(t, f, `SELECT count(*) FROM task_attempts WHERE task_id=$1 AND generation=2 AND state='SUCCEEDED'`, task) == 1
				})
				if scalar(t, f, `SELECT count(*) FROM sandbox_artifacts WHERE task_id=$1 AND generation=2`, task) != 4 {
					t.Fatal("fresh B workspace did not complete")
				}
			}
		})
	}
}

func TestSandboxWorkerProcessHelper(t *testing.T) {
	if os.Getenv("FS_SANDBOX_PROCESS") != "1" {
		return
	}
	c := config.Distributed{RPCAddr: os.Getenv("FS_RPC_ADDR"), WorkerToken: os.Getenv("FS_WORKER_TOKEN"), BrokerURL: os.Getenv("TEST_NATS_URL"), NATSToken: os.Getenv("TEST_NATS_TOKEN"), InsecureLocal: true, RPCTimeout: time.Second, Limits: execution.DefaultLimits(), Capacity: 1, Poll: 100 * time.Millisecond, Shutdown: time.Second}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	go func() { _, _ = io.Copy(io.Discard, os.Stdin); cancel() }()
	installation, err := uuid.Parse(os.Getenv("FS_INSTALLATION"))
	if err != nil {
		t.Fatal(err)
	}
	d, err := sandbox.NewDocker(ctx, installation, os.Getenv("FS_REGISTRY"), os.Getenv("TEST_SANDBOX_HELPER"))
	if err != nil {
		t.Fatal(err)
	}
	conn, err := rpc.Dial(c)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	b, err := broker.Open(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err = worker.RunWithSandbox(ctx, workerv1.NewCoordinatorClient(conn), b, c, slog.New(slog.NewJSONHandler(os.Stdout, nil)), d); err != nil {
		t.Fatal(err)
	}
}

func TestSandboxWorkerCrashRestartReconcilesOnlyOwnedOrphans(t *testing.T) {
	f := setup(t)
	b := testBroker(t, f)
	r := rpcServer(t, f, "")
	d, registry := processDriver(t)
	before, err := dockerTest(t, "ps", "-aq", "--no-trunc")
	if err != nil {
		t.Fatal(err)
	}
	wf, task := createExecutable(t, f, d, "sandbox-worker-crash", strings.Replace(longFixture, "5*time.Second", "30*time.Second", 1))
	start := func() (*osExec.Cmd, io.WriteCloser, *sandbox.CappedWriter) {
		cmd := osExec.CommandContext(f.ctx, os.Args[0], "-test.run=^TestSandboxWorkerProcessHelper$")
		cmd.Env = append(os.Environ(), "FS_SANDBOX_PROCESS=1", "FS_RPC_ADDR="+r.cfg.RPCAddr, "FS_WORKER_TOKEN="+r.token, "FS_INSTALLATION="+d.Installation.String(), "FS_REGISTRY="+registry)
		in, e := cmd.StdinPipe()
		if e != nil {
			t.Fatal(e)
		}
		log := &sandbox.CappedWriter{Limit: 32768}
		cmd.Stdout = log
		cmd.Stderr = log
		if e = cmd.Start(); e != nil {
			t.Fatal(e)
		}
		return cmd, in, log
	}
	if _, err = relay.Step(f.ctx, f.s1, b, uuid.New(), 10, time.Second); err != nil {
		t.Fatal(err)
	}
	first, in, _ := start()
	t.Cleanup(func() {
		_ = in.Close()
		if first.ProcessState == nil {
			_ = first.Process.Kill()
			_ = first.Wait()
		}
	})
	a := runningAttempt(t, f, task)
	container := executionContainer(t, a)
	if err = first.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = first.Wait()
	_ = in.Close()
	if exists, e := dockerTest(t, "ps", "-q", "--no-trunc", "--filter", "id="+container); e != nil || exists != container {
		t.Fatal("crash test did not leave a real orphan", e)
	}
	waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND lease_expires_at<=clock_timestamp()`, a)
	reconcile(t, f, wf)
	second, stop, logs := start()
	t.Cleanup(func() {
		_ = stop.Close()
		if second.ProcessState == nil {
			if e := second.Wait(); e != nil {
				t.Errorf("replacement worker: %v %s", e, logs.Data)
			}
		}
	})
	dockerWait(t, "restart removes old owned container", func() bool {
		ids, e := dockerTest(t, "ps", "-aq", "--no-trunc", "--filter", "id="+container)
		return e == nil && ids == ""
	})
	waitFor(t, f, `SELECT count(*) FROM task_attempts WHERE id=$1 AND NOT cleanup_required`, a)
	after, e := dockerTest(t, "ps", "-aq", "--no-trunc")
	if e != nil {
		t.Fatal(e)
	}
	for _, id := range strings.Fields(before) {
		if !strings.Contains(after, id) {
			t.Fatal("restart removed an unrelated resource")
		}
	}
	if _, err = f.s1.Cancel(f.ctx, wf); err != nil {
		t.Fatal(err)
	}
	if scalar(t, f, `SELECT count(*) FROM sandbox_artifacts WHERE attempt_id=$1`, a) != 0 {
		t.Fatal("crashed attempt published artifacts")
	}
}

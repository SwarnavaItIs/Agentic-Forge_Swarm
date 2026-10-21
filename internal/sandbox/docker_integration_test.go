//go:build sandbox

package sandbox

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func realDriver(t *testing.T) *Docker {
	t.Helper()
	helper := os.Getenv("TEST_SANDBOX_HELPER")
	if helper == "" {
		t.Fatal("real Docker verification requires TEST_SANDBOX_HELPER; run sandbox-test")
	}
	d, err := NewDocker(context.Background(), uuid.New(), t.TempDir(), helper)
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
	return d
}
func runFixture(t *testing.T, d *Docker, s Snapshot, command string, p Policy) (Result, error) {
	t.Helper()
	id, err := SaveSnapshot(d.Registry, s)
	if err != nil {
		t.Fatal(err)
	}
	b := testBinding()
	b.Installation = d.Installation
	r := d.Request(b, Input{1, id, command}, p)
	ctx, cancel := context.WithTimeout(context.Background(), 70*time.Second)
	defer cancel()
	return d.RunCommand(ctx, r, func(context.Context) error { return nil })
}
func TestDockerBuild(t *testing.T) {
	d := realDriver(t)
	r, err := runFixture(t, d, fixtureSnapshot(), "build", DefaultPolicy())
	if err != nil || r.Status != "exited" || !r.CleanupConfirmed {
		t.Fatalf("build: %+v, %v", r, err)
	}
	t.Logf("actual isolation=%s helper=%s", r.IsolationMode, r.HelperSHA256)
}

func TestDockerSuccessCompilerAndNonzero(t *testing.T) {
	d := realDriver(t)
	for _, tc := range []struct{ name, source, want string }{
		{"test_success", "package fixture\nimport \"testing\"\nfunc TestSum(t *testing.T){if Sum(1,2)!=3{t.Fatal(\"sum\")}}\n", "exited"},
		{"compiler_failure", "package fixture\nfunc broken( {\n", "compiler_error"},
		{"nonzero_exit", "package fixture\nimport \"testing\"\nfunc TestBad(t *testing.T){t.Fatal(\"intentional\")}\n", "nonzero_exit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixtureSnapshot()
			s.Files["sum_test.go"] = []byte(tc.source)
			r, err := runFixture(t, d, s, "test", DefaultPolicy())
			if err != nil || r.Status != tc.want || !r.Valid() {
				t.Fatalf("%s: %+v, %v", tc.name, r, err)
			}
			if len(r.Artifacts) != 4 {
				t.Fatal("missing bounded artifacts")
			}
		})
	}
}

// These tests use trusted static probes inside already-constrained containers.
// They cannot be selected through task inputs. Inspection and observed kernel
// enforcement are both mandatory; an unavailable control fails this suite.
func TestDockerKernelResourceAndNetworkControls(t *testing.T) {
	d := realDriver(t)
	p := DefaultPolicy()
	p.MemoryBytes = 64 << 20
	p.WorkspaceBytes = 2 << 20
	p.TemporaryBytes = 1 << 20
	p.PIDs = 16
	p.CPUMillis = 500
	b := testBinding()
	b.Installation = d.Installation
	r := d.Request(b, Input{1, strings.Repeat("a", 64), "build"}, p)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()
	args := d.createArgs(r)
	// Override the entrypoint only in this test, never in the execution adapter.
	args = args[:len(args)-4]
	args = append(args, "--entrypoint", "/bin/sh", r.Image, "-c", `set -eu
test "$(id -u)" = 10001
grep -q 'NoNewPrivs:[[:space:]]*1' /proc/self/status
grep -q 'Seccomp:[[:space:]]*2' /proc/self/status
grep -q 'CapEff:[[:space:]]*0000000000000000' /proc/self/status
test "$(cat /sys/fs/cgroup/memory.max)" = 67108864
test "$(cat /sys/fs/cgroup/pids.max)" = 16
test "$(cat /sys/fs/cgroup/cpu.max)" = '50000 100000'
test ! -e /var/run/docker.sock
test ! -e /root/.ssh
test ! -w /usr/local/go/bin/go
test ! -e /workspace/previous
test "$(ls /sys/class/net)" = lo
! timeout 1 bash -c 'exec 3<>/dev/tcp/169.254.169.254/80'
! timeout 1 bash -c 'exec 3<>/dev/tcp/172.17.0.1/5432'
! dd if=/dev/zero of=/workspace/quota bs=1M count=4 2>/dev/null
test "$(wc -c </workspace/quota)" = 2097152
rm /workspace/quota
echo controls-enforced`)
	body, err := dockerCall(ctx, nil, 1024, args...)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(body))
	defer func() {
		if e := d.Cleanup(context.Background(), b, id); e != nil {
			t.Error(e)
		}
	}()
	i, err := d.inspect(ctx, id)
	if err != nil || !matchesPolicy(i, r) {
		t.Fatalf("inspection: %+v %v", i, err)
	}
	out, err := dockerCall(ctx, nil, 4096, "start", "-a", id)
	if err != nil || !strings.Contains(string(out), "controls-enforced") {
		t.Fatalf("actual controls: %s %v", out, err)
	}
	i, err = d.inspect(ctx, id)
	if err != nil || i.State.ExitCode != 0 {
		t.Fatalf("probe failed %+v %v", i.State, err)
	}
}

func TestDockerCancellationRemovesDescendantsAndScopesCleanup(t *testing.T) {
	d := realDriver(t)
	p := DefaultPolicy()
	b := testBinding()
	b.Installation = d.Installation
	r := d.Request(b, Input{1, strings.Repeat("a", 64), "build"}, p)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	args := d.createArgs(r)
	args = args[:len(args)-4]
	args = append(args, "--entrypoint", "/bin/sh", r.Image, "-c", "sleep 60 & wait")
	body, err := dockerCall(ctx, nil, 1024, args...)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(body))
	defer d.Cleanup(context.Background(), b, id)
	if _, err = dockerCall(ctx, nil, 1024, "start", id); err != nil {
		t.Fatal(err)
	}
	wrong := b
	wrong.Installation = uuid.New()
	if d.Cleanup(ctx, wrong, id) == nil {
		t.Fatal("cross-install cleanup allowed")
	}
	i, err := d.inspect(ctx, id)
	if err != nil || !i.State.Running {
		t.Fatal("unrelated resource removed", err)
	}
	if err = d.Reconcile(ctx, "different_worker", nil); err != nil {
		t.Fatal(err)
	}
	if _, err = d.inspect(ctx, id); err != nil {
		t.Fatal("other worker cleaned")
	}
	if err = d.Reconcile(ctx, b.Worker, nil); err != nil {
		t.Fatal(err)
	}
	if exists, err := d.exists(ctx, id); err != nil || exists {
		t.Fatal("orphan descendants remain", err)
	}
}

func TestDockerOversizedOutputPatchSymlinksAndFreshWorkspace(t *testing.T) {
	d := realDriver(t)
	s := fixtureSnapshot()
	s.Files["sum_test.go"] = []byte(`package fixture
import("testing";"os";"strings";"fmt")
func TestFiles(t *testing.T){
 if _,e:=os.Stat("previous");e==nil{t.Fatal("shared workspace")}
 os.WriteFile("previous",[]byte("attempt private"),0600)
 os.WriteFile("sum.go",[]byte("package fixture\n"+strings.Repeat("// changed\n",2000)),0600)
 os.Symlink("/etc/passwd","escape")
 fmt.Print(strings.Repeat("bounded-output",3000))
}`)
	for i := 0; i < 2; i++ {
		r, err := runFixture(t, d, s, "test", DefaultPolicy())
		if err != nil || r.Status != "exited" {
			t.Fatalf("isolated fixture: %+v %v", r, err)
		}
		seen := map[string]bool{}
		for _, a := range r.Artifacts {
			if len(a.Body) > MaxArtifact || strings.Contains(string(a.Body), "root:x:0") {
				t.Fatal("artifact escape or unbounded output")
			}
			seen[a.Kind] = a.Truncated
		}
		if !seen["stdout"] || !seen["test_result"] || !seen["patch"] {
			wire, _ := json.Marshal(r)
			t.Fatalf("truncation not explicit: %s", wire)
		}
	}
}

func TestDockerRepresentativeCPUAndPIDAndOOM(t *testing.T) {
	d := realDriver(t)
	for _, mode := range []string{"cpu", "pids", "memory"} {
		t.Run(mode, func(t *testing.T) {
			p := DefaultPolicy()
			p.MemoryBytes = 64 << 20
			p.PIDs = 16
			p.CPUMillis = 500
			b := testBinding()
			b.Installation = d.Installation
			r := d.Request(b, Input{1, strings.Repeat("a", 64), "build"}, p)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			args := d.createArgs(r)
			args = args[:len(args)-1]
			args = append(args, "probe", mode)
			body, err := dockerCall(ctx, nil, 1024, args...)
			if err != nil {
				t.Fatal(err)
			}
			id := strings.TrimSpace(string(body))
			defer func() {
				clean, done := context.WithTimeout(context.Background(), 10*time.Second)
				defer done()
				if e := d.Cleanup(clean, b, id); e != nil {
					t.Error(e)
				}
			}()
			before, err := d.inspect(ctx, id)
			if err != nil || !matchesPolicy(before, r) {
				t.Fatal("resource policy inspection", err)
			}
			out, startErr := dockerCall(ctx, nil, 4096, "start", "-a", id)
			after, err := d.inspect(ctx, id)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "memory" {
				if !after.State.OOMKilled || after.State.ExitCode != 137 {
					t.Fatalf("bounded OOM not enforced: %+v", after.State)
				}
			} else if startErr != nil || after.State.ExitCode != 0 || !strings.Contains(string(out), "ENFORCED") {
				t.Fatalf("%s: %s %+v %v", mode, out, after.State, startErr)
			}
		})
	}
}

func TestDockerCommandTimeoutKillsDescendants(t *testing.T) {
	d := realDriver(t)
	s := fixtureSnapshot()
	s.Files["sum_test.go"] = []byte(`package fixture
import("testing";"os/exec";"fmt")
func TestDescendant(t *testing.T){c:=exec.Command("/bin/sh","-c","sleep 60 & wait");if e:=c.Start();e!=nil{t.Fatal(e)};fmt.Println("DESCENDANT_STARTED");c.Wait()}`)
	p := DefaultPolicy()
	p.TimeoutMS = 8000
	r, err := runFixture(t, d, s, "test", p)
	if err != nil || r.Status != "timeout" || !r.CleanupConfirmed {
		t.Fatalf("timeout: %+v %v", r, err)
	}
	found := false
	for _, a := range r.Artifacts {
		if a.Kind == "stdout" && strings.Contains(string(a.Body), "DESCENDANT_STARTED") {
			found = true
		}
	}
	if !found {
		t.Fatal("timeout did not exercise a running descendant")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if exists, err := d.exists(ctx, r.ContainerID); err != nil || exists {
		t.Fatal("timeout container or descendants remain", err)
	}
}

func TestDockerCleanupFailureRemainsRetryable(t *testing.T) {
	d := realDriver(t)
	p := DefaultPolicy()
	b := testBinding()
	b.Installation = d.Installation
	r := d.Request(b, Input{1, strings.Repeat("a", 64), "build"}, p)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	args := d.createArgs(r)
	args = args[:len(args)-4]
	args = append(args, "--entrypoint", "/bin/sleep", r.Image, "30")
	body, err := dockerCall(ctx, nil, 1024, args...)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(body))
	defer d.Cleanup(context.Background(), b, id)
	if _, err = dockerCall(ctx, nil, 1024, "start", id); err != nil {
		t.Fatal(err)
	}
	denied, done := context.WithCancel(context.Background())
	done()
	if err = d.Cleanup(denied, b, id); err == nil {
		t.Fatal("unavailable runtime falsely confirmed cleanup")
	}
	if d.CanStart() {
		t.Fatal("uncertain teardown did not pause new execution")
	}
	i, err := d.inspect(ctx, id)
	if err != nil || !i.State.Running {
		t.Fatal("test did not retain uncertain workload", err)
	}
	if err = d.Cleanup(ctx, b, id); err != nil {
		t.Fatal("cleanup retry failed", err)
	}
	if err = d.Reconcile(ctx, b.Worker, nil); err != nil || !d.CanStart() {
		t.Fatal("cleanup retry did not reopen admission", err)
	}
}

func TestDockerUnregisteredFixturePolicyDenial(t *testing.T) {
	d := realDriver(t)
	b := testBinding()
	b.Installation = d.Installation
	r := d.Request(b, Input{1, strings.Repeat("f", 64), "test"}, DefaultPolicy())
	calls := 0
	result, err := d.RunCommand(context.Background(), r, func(context.Context) error { calls++; return nil })
	if err != nil || result.Status != "policy_denied" || !result.Valid() || calls != 1 {
		t.Fatalf("unregistered input not explicitly denied: %+v %v", result, err)
	}
	_, err = d.RunCommand(context.Background(), r, func(context.Context) error { return ErrPolicy })
	if err == nil {
		t.Fatal("unavailable authorization allowed a tool")
	}
}

func TestDockerCommandReportsOOMSeparately(t *testing.T) {
	d := realDriver(t)
	s := fixtureSnapshot()
	s.Files["sum_test.go"] = []byte(`package fixture
import("testing";"runtime")
func TestMemory(t *testing.T){blocks:=[][]byte{};for i:=0;i<128;i++{b:=make([]byte,8<<20);for j:=0;j<len(b);j+=4096{b[j]=1};blocks=append(blocks,b);runtime.KeepAlive(blocks)}}`)
	r, err := runFixture(t, d, s, "test", DefaultPolicy())
	if err != nil || r.Status != "oom" || r.ExitCode != 137 || !r.CleanupConfirmed {
		t.Fatalf("OOM classification: %+v %v", r, err)
	}
}

func TestDockerHelperWatchdogWithoutGatewayInput(t *testing.T) {
	d := realDriver(t)
	p := DefaultPolicy()
	p.MemoryBytes = 64 << 20
	p.PIDs = 16
	p.CPUMillis = 100
	p.WorkspaceBytes = 2 << 20
	p.TemporaryBytes = 1 << 20
	b := testBinding()
	b.Installation = d.Installation
	r := d.Request(b, Input{1, strings.Repeat("a", 64), "test"}, p)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	body, err := dockerCall(ctx, nil, 1024, d.createArgs(r)...)
	if err != nil {
		t.Fatal(err)
	}
	id := strings.TrimSpace(string(body))
	defer func() {
		clean, done := context.WithTimeout(context.Background(), 10*time.Second)
		defer done()
		if e := d.Cleanup(clean, b, id); e != nil {
			t.Error(e)
		}
	}()
	// Detached start leaves the -i input open, simulating a dead/paused gateway
	// before payload EOF. No repository code or stress workload is started.
	if _, err = dockerCall(ctx, nil, 1024, "start", id); err != nil {
		t.Fatal(err)
	}
	for {
		i, e := d.inspect(ctx, id)
		if e != nil {
			t.Fatal(e)
		}
		if !i.State.Running {
			if i.State.ExitCode != 124 {
				t.Fatalf("unexpected helper exit %+v", i.State)
			}
			break
		}
		timer := time.NewTimer(500 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			t.Fatal("helper lifetime ceiling did not enforce")
		case <-timer.C:
		}
	}
}

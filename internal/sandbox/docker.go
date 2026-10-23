package sandbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

type Docker struct {
	Installation       uuid.UUID
	Registry           string
	helperHash         string
	image              string
	mode               string
	active             sync.Map
	cleanupFailures    atomic.Uint64
	reconciledFailures atomic.Uint64
	command            func(context.Context, io.Reader, int, ...string) ([]byte, error)
}

func (d *Docker) call(ctx context.Context, stdin io.Reader, limit int, args ...string) ([]byte, error) {
	if d.command != nil {
		return d.command(ctx, stdin, limit, args...)
	}
	return dockerCall(ctx, stdin, limit, args...)
}

type runtimeInfo struct {
	OSType                                         string
	CgroupVersion                                  string
	MemoryLimit, SwapLimit, CPUCfsQuota, PidsLimit bool
	SecurityOptions                                []string
}
type Inspection struct {
	ID     string `json:"Id"`
	Config struct {
		User   string
		Image  string
		Labels map[string]string
		Env    []string
	}
	HostConfig struct {
		ReadonlyRootfs                                   bool
		Privileged                                       bool
		NetworkMode                                      string
		PidMode                                          string
		IpcMode                                          string
		Binds                                            []string
		CapDrop                                          []string
		SecurityOpt                                      []string
		Memory, MemorySwap, NanoCpus, PidsLimit, ShmSize int64
		Tmpfs                                            map[string]string
		LogConfig                                        struct{ Type string }
	}
	Mounts []struct {
		Type, Source, Destination string
		RW                        bool
	}
	State struct {
		Running, OOMKilled bool
		ExitCode           int
	}
}

func dockerCall(ctx context.Context, stdin io.Reader, limit int, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "docker", args...)
	cmd.Stdin = stdin
	out, stderr := &CappedWriter{Limit: limit}, &CappedWriter{Limit: 4096}
	cmd.Stdout = out
	cmd.Stderr = stderr
	cmd.WaitDelay = 500 * time.Millisecond
	err := cmd.Run()
	if out.Truncated {
		return nil, fmt.Errorf("%w: docker response exceeds policy", ErrPolicy)
	}
	if err != nil {
		return nil, fmt.Errorf("docker %s failed: %w", args[0], err)
	}
	return out.Data, nil
}
func NewDocker(ctx context.Context, installation uuid.UUID, registry, helper string) (*Docker, error) {
	if installation == uuid.Nil || registry == "" || helper == "" {
		return nil, ErrPolicy
	}
	f, err := os.Open(helper)
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(f, 16<<20+1))
	_ = f.Close()
	if err != nil || len(body) > 16<<20 || len(body) < 4 || !bytes.Equal(body[:4], []byte{127, 'E', 'L', 'F'}) {
		return nil, ErrPolicy
	}
	hash := sha256.Sum256(body)
	d := &Docker{Installation: installation, Registry: registry, helperHash: hex.EncodeToString(hash[:]), mode: "rootful_linux"}
	call, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	info, err := dockerCall(call, nil, 128<<10, "info", "--format", "{{json .}}")
	var actual runtimeInfo
	if err != nil || json.Unmarshal(info, &actual) != nil {
		return nil, fmt.Errorf("Docker runtime inspection unavailable")
	}
	missing := []string{}
	if actual.OSType != "linux" {
		missing = append(missing, "Linux execution engine")
	}
	if actual.CgroupVersion != "2" {
		missing = append(missing, "cgroup v2")
	}
	for name, available := range map[string]bool{"memory": actual.MemoryLimit, "swap": actual.SwapLimit, "CPU quota": actual.CPUCfsQuota, "PID limit": actual.PidsLimit} {
		if !available {
			missing = append(missing, name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("unsupported required sandbox controls: %s", strings.Join(missing, ", "))
	}
	seccomp := false
	for _, option := range actual.SecurityOptions {
		if strings.HasPrefix(option, "name=seccomp") {
			seccomp = true
		}
		if option == "name=rootless" {
			d.mode = "rootless_linux"
		}
	}
	if !seccomp {
		return nil, fmt.Errorf("required default seccomp unavailable")
	}
	// --pull=never below guarantees absence is a startup failure, not an
	// implicit dependency/network preparation phase.
	prepared, err := dockerCall(call, nil, 128<<10, "image", "inspect", "forgeswarm-execution:"+d.helperHash)
	var images []struct {
		ID     string `json:"Id"`
		Config struct{ Labels map[string]string }
	}
	if err != nil || json.Unmarshal(prepared, &images) != nil || len(images) != 1 || !imagePattern.MatchString(images[0].ID) || images[0].Config.Labels["forgeswarm.helper_sha256"] != d.helperHash || images[0].Config.Labels["forgeswarm.base_image"] != Image {
		return nil, fmt.Errorf("trusted pinned execution image missing; run sandbox-prepare")
	}
	d.image = images[0].ID
	return d, nil
}
func (d *Docker) Mode() string       { return d.mode }
func (d *Docker) HelperHash() string { return d.helperHash }
func (d *Docker) CanStart() bool     { return d.cleanupFailures.Load() == d.reconciledFailures.Load() }
func (d *Docker) Suspend()           { d.cleanupFailures.Add(1) }
func (d *Docker) Image() string      { return d.image }
func (d *Docker) Request(b Binding, in Input, p Policy) Request {
	r := NewRequest(b, in, p)
	r.Image = d.image
	return r
}

func labels(b Binding) map[string]string {
	return map[string]string{"forgeswarm.installation": b.Installation.String(), "forgeswarm.workflow": b.Workflow.String(), "forgeswarm.task": b.Task.String(), "forgeswarm.attempt": b.Attempt.String(), "forgeswarm.instance": b.Instance.String(), "forgeswarm.worker": b.Worker, "forgeswarm.generation": strconv.FormatInt(b.Generation, 10), "forgeswarm.request": b.Request.String(), "forgeswarm.resource": "sandbox-v1"}
}
func (d *Docker) inspect(ctx context.Context, id string) (Inspection, error) {
	var items []Inspection
	body, err := d.call(ctx, nil, 128<<10, "container", "inspect", id)
	if err != nil {
		return Inspection{}, err
	}
	if json.Unmarshal(body, &items) != nil || len(items) != 1 || items[0].ID != id {
		return Inspection{}, ErrPolicy
	}
	return items[0], nil
}
func owns(actual Inspection, b Binding) bool {
	for key, value := range labels(b) {
		if actual.Config.Labels[key] != value {
			return false
		}
	}
	return true
}
func (d *Docker) exists(ctx context.Context, id string) (bool, error) {
	body, err := d.call(ctx, nil, 8192, "ps", "-aq", "--no-trunc", "--filter", "id="+id)
	if err != nil {
		return false, err
	}
	for _, line := range strings.Fields(string(body)) {
		if line == id {
			return true, nil
		}
	}
	return false, nil
}

// PrepareWorkspace validates a trusted registered snapshot. Extraction is done
// by the immutable helper inside the bounded filesystem, never on the host.
func (d *Docker) PrepareWorkspace(r Request) (Snapshot, error) {
	if !r.Valid() || r.Binding.Installation != d.Installation || r.Image != d.image {
		return Snapshot{}, ErrPolicy
	}
	return LoadSnapshot(d.Registry, r.Input.FixtureID)
}
func (d *Docker) createArgs(r Request) []string {
	p := r.Policy
	args := []string{"create", "--pull=never", "--platform", "linux/amd64", "--name", "forgeswarm-exec-" + r.Binding.Attempt.String(), "-i", "--network", "none", "--read-only", "--user", "10001:10001", "--cap-drop", "ALL", "--security-opt", "no-new-privileges=true", "--cpus", strconv.FormatFloat(float64(p.CPUMillis)/1000, 'f', 3, 64), "--memory", strconv.FormatInt(p.MemoryBytes, 10), "--memory-swap", strconv.FormatInt(p.MemoryBytes, 10), "--pids-limit", strconv.Itoa(p.PIDs), "--shm-size", "1048576", "--ipc", "private", "--log-driver", "none", "--ulimit", "nofile=256:256", "--tmpfs", fmt.Sprintf("/workspace:rw,exec,nosuid,nodev,size=%d,mode=1777", p.WorkspaceBytes), "--tmpfs", fmt.Sprintf("/tmp:rw,nosuid,nodev,size=%d,mode=1777", p.TemporaryBytes)}
	for k, v := range labels(r.Binding) {
		args = append(args, "--label", k+"="+v)
	}
	// Docker CLI proxy defaults must never carry operator credentials into code.
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"} {
		args = append(args, "--env", key+"=")
	}
	return append(args, "--entrypoint", "/usr/local/bin/forgeswarm-sandbox", r.Image, "execute")
}
func matchesPolicy(i Inspection, r Request) bool {
	h, p := i.HostConfig, r.Policy
	if !owns(i, r.Binding) || i.Config.User != "10001:10001" || i.Config.Image != r.Image || !h.ReadonlyRootfs || h.Privileged || h.NetworkMode != "none" || h.PidMode != "" || h.IpcMode != "private" || len(h.Binds) != 0 || h.Memory != p.MemoryBytes || h.MemorySwap != p.MemoryBytes || h.NanoCpus != int64(p.CPUMillis)*1_000_000 || h.PidsLimit != int64(p.PIDs) || h.ShmSize != 1<<20 || h.LogConfig.Type != "none" || len(h.CapDrop) != 1 || h.CapDrop[0] != "ALL" {
		return false
	}
	if len(h.SecurityOpt) != 1 || h.SecurityOpt[0] != "no-new-privileges=true" {
		return false
	}
	if len(h.Tmpfs) != 2 || h.Tmpfs["/workspace"] != fmt.Sprintf("rw,exec,nosuid,nodev,size=%d,mode=1777", p.WorkspaceBytes) || h.Tmpfs["/tmp"] != fmt.Sprintf("rw,nosuid,nodev,size=%d,mode=1777", p.TemporaryBytes) {
		return false
	}
	for _, m := range i.Mounts {
		if m.Type != "tmpfs" {
			return false
		}
	}
	return true
}

// RunCommand checks the live DB fence both before create and immediately before
// start. The worker's renewal loop cancels ctx on every ambiguous lease failure.
// request IDs and Docker names are stable per attempt; duplicate local handlers
// cannot execute twice. Resource cleanup always outlives the cancelled context.
func (d *Docker) RunCommand(ctx context.Context, r Request, authorize func(context.Context) error) (result Result, err error) {
	return d.runSnapshot(ctx, r, authorize, nil, "")
}

// RunSnapshot accepts a trusted gateway candidate, never model-selected Docker
// options. It uses the identical kernel policy, fences, watchdog and teardown.
// The content-addressed candidate stays in memory; no host checkout is executed.
func (d *Docker) RunSnapshot(ctx context.Context, r Request, s Snapshot, profile string, authorize func(context.Context) error) (Result, error) {
	if !ProtectedProfile(profile) {
		return Result{}, ErrPolicy
	}
	_, id, err := SnapshotBytes(s)
	if err != nil || id != r.Input.FixtureID {
		return Result{}, ErrPolicy
	}
	return d.runSnapshot(ctx, r, authorize, &s, profile)
}
func (d *Docker) runSnapshot(ctx context.Context, r Request, authorize func(context.Context) error, candidate *Snapshot, profile string) (result Result, err error) {
	if !r.Valid() || r.Binding.Installation != d.Installation || r.Image != d.image || authorize == nil || !d.CanStart() {
		return result, ErrPolicy
	}
	if _, loaded := d.active.LoadOrStore(r.Binding.Attempt, true); loaded {
		return result, ErrPolicy
	}
	defer d.active.Delete(r.Binding.Attempt)
	result = Result{SchemaVersion: 1, Status: "infrastructure_error", ExitCode: -1, Image: r.Image, HelperSHA256: d.helperHash, IsolationMode: d.mode, Policy: r.Policy, CleanupConfirmed: true}
	if err = authorize(ctx); err != nil {
		return result, err
	}
	var s Snapshot
	if candidate == nil {
		s, err = d.PrepareWorkspace(r)
	} else {
		s = *candidate
	}
	if err != nil {
		if errors.Is(err, ErrPolicy) || os.IsNotExist(err) {
			result.Status = "policy_denied"
			return result, nil
		}
		return result, err
	}
	result.BaseRevision = s.BaseRevision
	// A failed or ambiguous create may still have created an owned resource.
	// Retain uncertainty until a successful daemon reconciliation proves absence.
	result.CleanupConfirmed = false
	call, cancel := context.WithTimeout(ctx, time.Duration(r.Policy.TimeoutMS)*time.Millisecond+3*time.Second)
	defer cancel()
	body, err := dockerCall(call, nil, 1024, d.createArgs(r)...)
	if err != nil {
		return result, err
	}
	id := strings.TrimSpace(string(body))
	if !digestPattern.MatchString(id) {
		return result, ErrPolicy
	}
	result.ContainerID = id
	defer func() {
		// Reconciliation must not mistake an in-progress/failed teardown for an
		// active workload it can safely skip and then reopen admission.
		d.active.Store(r.Binding.Attempt, false)
		cleanup, done := context.WithTimeout(context.Background(), 12*time.Second)
		defer done()
		e := d.Cleanup(cleanup, r.Binding, id)
		result.CleanupConfirmed = e == nil
		if e != nil {
			err = fmt.Errorf("sandbox_cleanup_uncertain: %w", e)
		} else if err != nil {
			result.Status = "infrastructure_error"
			result.ExitCode = -1
			if errors.Is(err, ErrPolicy) {
				result.Status = "policy_denied"
			}
			err = nil
		}
	}()
	i, e := d.inspect(call, id)
	if e != nil || !matchesPolicy(i, r) {
		return result, ErrPolicy
	}
	if e = authorize(call); e != nil {
		return result, e
	}
	payload, _ := json.Marshal(Payload{Request: r, Snapshot: s, Profile: profile})
	wire, e := dockerCall(call, bytes.NewReader(payload), MaxWire, "start", "-a", "-i", id)
	if call.Err() != nil {
		result.Status = "timeout"
		result.ExitCode = 124
		return result, nil
	}
	observed, inspectErr := d.inspect(call, id)
	if inspectErr != nil {
		return result, inspectErr
	}
	if observed.State.OOMKilled {
		result.Status = "oom"
		result.ExitCode = 137
		return result, nil
	}
	if observed.State.ExitCode == 124 {
		result.Status = "timeout"
		result.ExitCode = 124
		return result, nil
	}
	if errors.Is(e, ErrPolicy) {
		return result, e
	}
	if e != nil || observed.State.Running || observed.State.ExitCode != 0 {
		return result, fmt.Errorf("sandbox helper did not return a bounded result")
	}
	var execution Result
	if Decode(wire, &execution, MaxWire) != nil {
		return result, ErrPolicy
	}
	if execution.SchemaVersion != 1 || execution.Image != r.Image || execution.BaseRevision != s.BaseRevision || execution.Policy != r.Policy {
		return result, ErrPolicy
	}
	execution.ContainerID = id
	execution.HelperSHA256 = d.helperHash
	execution.IsolationMode = d.mode
	execution.CleanupConfirmed = true
	if !execution.Valid() {
		return result, ErrPolicy
	}
	result = execution
	return result, nil
}

// StopAttempt stops all processes through the container boundary, never a host
// PID derived from repository output. Ownership labels are checked before each
// destructive operation. Cleanup failures remain errors and may be retried.
func (d *Docker) StopAttempt(ctx context.Context, b Binding, id string) error {
	if !b.Valid() || b.Installation != d.Installation || !digestPattern.MatchString(id) {
		return ErrPolicy
	}
	exists, err := d.exists(ctx, id)
	if err != nil || !exists {
		return err
	}
	i, err := d.inspect(ctx, id)
	if err != nil {
		return err
	}
	if !owns(i, b) {
		return ErrPolicy
	}
	if i.State.Running {
		stop, done := context.WithTimeout(ctx, 3*time.Second)
		_, _ = dockerCall(stop, nil, 1024, "stop", "--timeout", "1", id)
		done()
	}
	return nil
}
func (d *Docker) Cleanup(ctx context.Context, b Binding, id string) (err error) {
	defer func() {
		if err != nil {
			d.Suspend()
		}
	}()
	if err := d.StopAttempt(ctx, b, id); err != nil {
		return err
	}
	exists, err := d.exists(ctx, id)
	if err != nil || !exists {
		return err
	}
	i, err := d.inspect(ctx, id)
	if err != nil {
		return err
	}
	if !owns(i, b) {
		return ErrPolicy
	}
	if _, err = dockerCall(ctx, nil, 1024, "rm", "--force", "--volumes", id); err != nil {
		return err
	}
	exists, err = d.exists(ctx, id)
	if err != nil {
		return err
	}
	if exists {
		return fmt.Errorf("owned sandbox remains after removal")
	}
	return nil
}

// Reconcile is partitioned by authenticated worker identity, after replacement
// registration. Other workers/installations and active local attempts are never
// touched. The callback clears durable cleanup evidence only after removal.
func (d *Docker) Reconcile(ctx context.Context, worker string, ack func(context.Context, Binding) error) (err error) {
	observed := d.cleanupFailures.Load()
	teardownPending := false
	defer func() {
		if err != nil {
			d.Suspend()
		} else if !teardownPending {
			d.reconciledFailures.Store(observed)
		}
	}()
	body, err := d.call(ctx, nil, 8192, "ps", "-aq", "--no-trunc", "--filter", "label=forgeswarm.installation="+d.Installation.String(), "--filter", "label=forgeswarm.worker="+worker, "--filter", "label=forgeswarm.resource=sandbox-v1")
	if err != nil {
		return err
	}
	ids := strings.Fields(string(body))
	if len(ids) > 100 {
		return fmt.Errorf("sandbox orphan reconciliation bound exceeded")
	}
	for _, id := range ids {
		i, e := d.inspect(ctx, id)
		if e != nil {
			// A normal teardown may remove a listed container before inspect.
			// Only a successful daemon query proving absence makes that benign;
			// an unavailable daemon or still-present resource remains uncertain.
			exists, probeErr := d.exists(ctx, id)
			if probeErr == nil && !exists {
				continue
			}
			return e
		}
		l := i.Config.Labels
		b := Binding{Installation: d.Installation, Worker: worker}
		b.Workflow, _ = uuid.Parse(l["forgeswarm.workflow"])
		b.Task, _ = uuid.Parse(l["forgeswarm.task"])
		b.Attempt, _ = uuid.Parse(l["forgeswarm.attempt"])
		b.Instance, _ = uuid.Parse(l["forgeswarm.instance"])
		b.Request, _ = uuid.Parse(l["forgeswarm.request"])
		b.Generation, _ = strconv.ParseInt(l["forgeswarm.generation"], 10, 64)
		if !b.Valid() || !owns(i, b) {
			return ErrPolicy
		}
		if running, active := d.active.Load(b.Attempt); active {
			if !running.(bool) {
				// The local owner is already performing bounded teardown. Do not
				// mistake normal cleanup for a failure that aborts another live
				// agent tool. Also do not clear earlier cleanup uncertainty until
				// a later scan confirms this teardown has finished.
				teardownPending = true
			}
			continue
		}
		if e = d.Cleanup(ctx, b, id); e != nil {
			return e
		}
		if ack != nil {
			if e = ack(ctx, b); e != nil {
				return e
			}
		}
	}
	return nil
}

// ReconcileAttempt also closes crash-before-create and remove-before-ack gaps:
// a successful daemon query proves no owned resource remains before DB ack.
func (d *Docker) ReconcileAttempt(ctx context.Context, b Binding) error {
	if !b.Valid() || b.Installation != d.Installation {
		return ErrPolicy
	}
	if _, active := d.active.Load(b.Attempt); active {
		return fmt.Errorf("attempt still locally active")
	}
	body, err := dockerCall(ctx, nil, 8192, "ps", "-aq", "--no-trunc", "--filter", "label=forgeswarm.installation="+b.Installation.String(), "--filter", "label=forgeswarm.attempt="+b.Attempt.String(), "--filter", "label=forgeswarm.resource=sandbox-v1")
	if err != nil {
		return err
	}
	ids := strings.Fields(string(body))
	if len(ids) > 1 {
		return ErrPolicy
	}
	for _, id := range ids {
		if err = d.Cleanup(ctx, b, id); err != nil {
			return err
		}
	}
	return nil
}

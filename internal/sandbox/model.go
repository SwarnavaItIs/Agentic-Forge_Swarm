// Package sandbox is the trusted gateway for vetted, small, dependency-free Go
// fixtures. Docker constrains resources; it is not hostile-code isolation.
package sandbox

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path"
	"regexp"
	"slices"
	"strings"

	"github.com/google/uuid"
)

const Image = "golang:1.26.10-bookworm@sha256:632dff7351799a5a9954a48dc06670ade72ee44e8c351cdc0e5a57af2fea21af"
const MaxFixture = 256 << 10
const MaxFile = 64 << 10
const MaxArtifact = 8 << 10
const MaxWire = 48 << 10

var ErrPolicy = errors.New("sandbox_policy_denied")
var digestPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)
var imagePattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
var revisionPattern = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)

type Input struct {
	SchemaVersion int    `json:"schema_version"`
	FixtureID     string `json:"fixture_id"`
	Command       string `json:"command"`
}

func (in Input) Valid() bool {
	return in.SchemaVersion == 1 && digestPattern.MatchString(in.FixtureID) && (in.Command == "build" || in.Command == "test")
}

type Policy struct {
	CPUMillis      int   `json:"cpu_millis"`
	MemoryBytes    int64 `json:"memory_bytes"`
	PIDs           int   `json:"pids"`
	WorkspaceBytes int64 `json:"workspace_bytes"`
	TemporaryBytes int64 `json:"temporary_bytes"`
	OutputBytes    int   `json:"output_bytes"`
	TimeoutMS      int64 `json:"timeout_ms"`
}

func DefaultPolicy() Policy { return Policy{1000, 512 << 20, 64, 256 << 20, 64 << 20, 4096, 60000} }
func (p Policy) Valid() bool {
	return p.CPUMillis >= 100 && p.CPUMillis <= 1000 && p.MemoryBytes >= 64<<20 && p.MemoryBytes <= 768<<20 && p.PIDs >= 16 && p.PIDs <= 64 && p.WorkspaceBytes >= 2<<20 && p.WorkspaceBytes <= 384<<20 && p.TemporaryBytes >= 1<<20 && p.TemporaryBytes <= 64<<20 && p.OutputBytes >= 128 && p.OutputBytes <= 4096 && p.TimeoutMS >= 100 && p.TimeoutMS <= 60000
}

type Binding struct {
	Installation uuid.UUID `json:"installation_id"`
	Workflow     uuid.UUID `json:"workflow_id"`
	Task         uuid.UUID `json:"task_id"`
	Attempt      uuid.UUID `json:"attempt_id"`
	Instance     uuid.UUID `json:"instance_id"`
	Generation   int64     `json:"generation"`
	Worker       string    `json:"worker_id"`
	Request      uuid.UUID `json:"request_id"`
}

func RequestID(attempt uuid.UUID) uuid.UUID {
	return uuid.NewSHA1(attempt, []byte("sandbox-command-v1"))
}
func (b Binding) Valid() bool {
	return b.Installation != uuid.Nil && b.Workflow != uuid.Nil && b.Task != uuid.Nil && b.Attempt != uuid.Nil && b.Instance != uuid.Nil && b.Generation > 0 && regexp.MustCompile(`^[a-z][a-z0-9_-]{0,63}$`).MatchString(b.Worker) && b.Request == RequestID(b.Attempt)
}

// No client-selected Docker flags, mounts, image, paths or environment exist in
// Input. RunCommand still validates the complete server-derived request.
type Request struct {
	Binding     Binding   `json:"binding"`
	Input       Input     `json:"input"`
	Image       string    `json:"image"`
	WorkspaceID uuid.UUID `json:"workspace_id"`
	Argv        []string  `json:"argv"`
	Env         []string  `json:"env"`
	Directory   string    `json:"directory"`
	Policy      Policy    `json:"policy"`
}

func Argv(command string) []string {
	if command == "build" {
		return []string{"/usr/local/go/bin/go", "build", "-p=1", "./..."}
	}
	return []string{"/usr/local/go/bin/go", "test", "-json", "-count=1", "-p=1", "-vet=off", "./..."}
}
func Environment() []string {
	return []string{"PATH=/usr/local/go/bin:/usr/bin:/bin", "HOME=/nonexistent", "GOENV=off", "GOTOOLCHAIN=local", "GOTELEMETRY=off", "GOPROXY=off", "GOSUMDB=off", "CGO_ENABLED=0", "GOWORK=off", "GOMAXPROCS=2", "GOMEMLIMIT=256MiB", "GOCACHE=/workspace/cache", "GOMODCACHE=/workspace/modcache", "GOPATH=/workspace/gopath", "TMPDIR=/workspace/tmp"}
}
func NewRequest(b Binding, in Input, p Policy) Request {
	return Request{b, in, Image, b.Attempt, Argv(in.Command), Environment(), "/workspace/src", p}
}
func (r Request) Valid() bool {
	return r.Binding.Valid() && r.Input.Valid() && (r.Image == Image || imagePattern.MatchString(r.Image)) && r.WorkspaceID == r.Binding.Attempt && r.Directory == "/workspace/src" && slices.Equal(r.Argv, Argv(r.Input.Command)) && slices.Equal(r.Env, Environment()) && r.Policy.Valid()
}

type Snapshot struct {
	SchemaVersion int               `json:"schema_version"`
	BaseRevision  string            `json:"base_revision"`
	Files         map[string][]byte `json:"files"`
}

func SafePath(name string) bool {
	if len(name) < 1 || len(name) > 200 || path.Clean(name) != name || strings.ContainsAny(name, "\\:\x00\r\n\t") || strings.HasPrefix(name, "/") {
		return false
	}
	for _, part := range strings.Split(name, "/") {
		if part == "." || part == ".." || strings.HasPrefix(part, ".") {
			return false
		}
	}
	return true
}
func (s Snapshot) Valid() bool {
	if s.SchemaVersion != 1 || !revisionPattern.MatchString(s.BaseRevision) || len(s.Files) < 1 || len(s.Files) > 128 {
		return false
	}
	total := 0
	for name, body := range s.Files {
		if !SafePath(name) || len(body) > MaxFile {
			return false
		}
		total += len(body)
	}
	// The initial preparation policy deliberately excludes modules, replacement
	// directives, vendored dependencies, toolchains, LFS and submodules.
	mod := regexp.MustCompile(`\Amodule [a-zA-Z0-9._/-]+\r?\n(\r?\n)*go 1\.26\.3\r?\n?\z`)
	return total <= MaxFixture && mod.Match(s.Files["go.mod"])
}
func SnapshotBytes(s Snapshot) ([]byte, string, error) {
	if !s.Valid() {
		return nil, "", ErrPolicy
	}
	body, err := json.Marshal(s)
	if err != nil {
		return nil, "", err
	}
	hash := sha256.Sum256(body)
	return body, hex.EncodeToString(hash[:]), nil
}
func Decode(data []byte, out any, limit int) error {
	if len(data) > limit {
		return ErrPolicy
	}
	check := json.NewDecoder(bytes.NewReader(data))
	if err := uniqueJSON(check, 0); err != nil {
		return err
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return ErrPolicy
	}
	if d.Decode(new(any)) != io.EOF {
		return ErrPolicy
	}
	return nil
}

func uniqueJSON(d *json.Decoder, depth int) error {
	if depth > 24 {
		return ErrPolicy
	}
	t, err := d.Token()
	if err != nil {
		return ErrPolicy
	}
	switch t {
	case json.Delim('{'):
		seen := map[string]bool{}
		for d.More() {
			key, e := d.Token()
			if e != nil {
				return ErrPolicy
			}
			name, ok := key.(string)
			if !ok || seen[name] {
				return ErrPolicy
			}
			seen[name] = true
			if e = uniqueJSON(d, depth+1); e != nil {
				return e
			}
		}
		_, err = d.Token()
	case json.Delim('['):
		for d.More() {
			if err = uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
		_, err = d.Token()
	}
	if err != nil {
		return ErrPolicy
	}
	return nil
}

type Artifact struct {
	Kind      string `json:"kind"`
	MediaType string `json:"media_type"`
	Body      []byte `json:"body"`
	Truncated bool   `json:"truncated"`
}
type Result struct {
	SchemaVersion    int        `json:"schema_version"`
	Status           string     `json:"status"`
	ExitCode         int        `json:"exit_code"`
	Image            string     `json:"image"`
	HelperSHA256     string     `json:"helper_sha256"`
	ContainerID      string     `json:"container_id"`
	IsolationMode    string     `json:"isolation_mode"`
	BaseRevision     string     `json:"base_revision"`
	Policy           Policy     `json:"policy"`
	Artifacts        []Artifact `json:"artifacts"`
	CleanupConfirmed bool       `json:"cleanup_confirmed"`
}

func (r Result) Valid() bool {
	noExecution := r.ContainerID == "" && r.BaseRevision == "" && len(r.Artifacts) == 0 && (r.Status == "policy_denied" || r.Status == "infrastructure_error")
	if r.SchemaVersion != 1 || !slices.Contains([]string{"exited", "nonzero_exit", "compiler_error", "timeout", "oom", "policy_denied", "infrastructure_error"}, r.Status) || r.ExitCode < -1 || r.ExitCode > 255 || !imagePattern.MatchString(r.Image) || !digestPattern.MatchString(r.HelperSHA256) || (!noExecution && (!digestPattern.MatchString(r.ContainerID) || !revisionPattern.MatchString(r.BaseRevision))) || !slices.Contains([]string{"rootless_linux", "rootful_linux"}, r.IsolationMode) || !r.Policy.Valid() || len(r.Artifacts) > 4 || !r.CleanupConfirmed {
		return false
	}
	if r.Status == "exited" && r.ExitCode != 0 {
		return false
	}
	seen := map[string]bool{}
	for _, a := range r.Artifacts {
		if !slices.Contains([]string{"stdout", "stderr", "patch", "test_result"}, a.Kind) || seen[a.Kind] || len(a.Body) > MaxArtifact || a.MediaType != "text/plain; charset=utf-8" {
			return false
		}
		seen[a.Kind] = true
	}
	return true
}

// CappedWriter consumes the entire stream, retaining only a bounded prefix.
// A discarded suffix never blocks the child or grows gateway memory.
type CappedWriter struct {
	Data      []byte
	Limit     int
	Truncated bool
}

func (w *CappedWriter) Write(p []byte) (int, error) {
	n := len(p)
	available := max(0, w.Limit-len(w.Data))
	w.Data = append(w.Data, p[:min(available, n)]...)
	if n > available {
		w.Truncated = true
	}
	return n, nil
}

type ArtifactRecord struct {
	ID            uuid.UUID `json:"id"`
	AttemptID     uuid.UUID `json:"attempt_id"`
	WorkflowID    uuid.UUID `json:"workflow_id"`
	TaskID        uuid.UUID `json:"task_id"`
	Generation    int64     `json:"generation"`
	Kind          string    `json:"kind"`
	MediaType     string    `json:"media_type"`
	BaseRevision  string    `json:"base_revision"`
	SHA256        string    `json:"sha256"`
	ContentLength int       `json:"content_length"`
	Truncated     bool      `json:"truncated"`
	Body          []byte    `json:"body,omitempty"`
}

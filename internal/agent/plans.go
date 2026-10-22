package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"
)

const MathProfile = "math-v1"
const WorkflowTurns = 96
const WorkflowTokens = 800000

var planKey = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,31}$`)
var MathFiles = []string{"add.go", "double.go", "score.go"}

type RoleSpec struct {
	SchemaVersion int      `json:"schema_version"`
	Name          string   `json:"name"`
	Version       int      `json:"version"`
	TaskKey       string   `json:"task_key"`
	Scope         []string `json:"write_scope"`
	Profile       string   `json:"test_profile"`
	MaxCalls      int      `json:"max_calls"`
}

func (r RoleSpec) Valid() bool {
	if r.SchemaVersion != 1 || r.Version < 1 || r.Version > 3 || !planKey.MatchString(r.TaskKey) || r.MaxCalls < 1 || r.MaxCalls > 8 {
		return false
	}
	if !slices.Contains([]string{"planner", "developer", "integration", "reviewer"}, r.Name) {
		return false
	}
	if r.Name != "developer" {
		return len(r.Scope) == 0 && r.Profile == MathProfile
	}
	if len(r.Scope) < 1 || len(r.Scope) > 3 || !slices.Contains([]string{"add-v1", "double-v1", "score-v1", MathProfile}, r.Profile) {
		return false
	}
	seen := map[string]bool{}
	for _, p := range r.Scope {
		if !slices.Contains(MathFiles, p) || seen[p] {
			return false
		}
		seen[p] = true
	}
	return true
}

type PlanTask struct {
	Key          string   `json:"key"`
	Description  string   `json:"description"`
	Acceptance   []string `json:"acceptance_criteria"`
	DependsOn    []string `json:"depends_on"`
	Capabilities []string `json:"required_capabilities"`
	Scope        []string `json:"write_scope"`
	Profile      string   `json:"test_profile"`
	MaxCalls     int      `json:"max_calls"`
}
type Plan struct {
	SchemaVersion      int        `json:"schema_version"`
	RepositoryID       string     `json:"repository_id"`
	BaseRevision       string     `json:"base_revision"`
	Policy             string     `json:"policy"`
	Tasks              []PlanTask `json:"tasks"`
	IntegrationProfile string     `json:"integration_profile"`
	ReviewRequired     bool       `json:"review_required"`
}

func textBound(s string, n int) bool {
	return len(strings.TrimSpace(s)) > 0 && len(s) <= n && utf8.ValidString(s) && !strings.ContainsRune(s, 0)
}
func NormalizePlan(p Plan, job Job) (Plan, error) {
	if p.SchemaVersion != 1 || p.RepositoryID != job.RepositoryID || p.BaseRevision != job.BaseRevision || p.Policy != "collaborative-go-v1" || p.IntegrationProfile != MathProfile || !p.ReviewRequired || len(p.Tasks) < 1 || len(p.Tasks) > 4 {
		return p, sandbox.ErrPolicy
	}
	// Round-trip prevents callers from retaining mutable slices after validation.
	body, _ := json.Marshal(p)
	_ = json.Unmarshal(body, &p)
	slices.SortFunc(p.Tasks, func(a, b PlanTask) int { return strings.Compare(a.Key, b.Key) })
	seen := map[string]bool{}
	edges, calls := 0, 2
	for i := range p.Tasks {
		t := &p.Tasks[i]
		r := RoleSpec{1, "developer", 1, t.Key, t.Scope, t.Profile, t.MaxCalls}
		if !r.Valid() || seen[t.Key] || slices.Contains([]string{"integrate", "review", "plan"}, t.Key) || !textBound(t.Description, 500) || len(t.Acceptance) < 1 || len(t.Acceptance) > 3 || !slices.Equal(t.Capabilities, []string{"agent_go"}) {
			return p, sandbox.ErrPolicy
		}
		for _, a := range t.Acceptance {
			if !textBound(a, 200) {
				return p, sandbox.ErrPolicy
			}
		}
		seen[t.Key] = true
		calls += t.MaxCalls
		edges += len(t.DependsOn)
		slices.Sort(t.DependsOn)
		slices.Sort(t.Scope)
	}
	if edges > 8 || calls > 32 {
		return p, sandbox.ErrPolicy
	}
	done := map[string]bool{}
	for len(done) < len(p.Tasks) {
		changed := false
		for _, t := range p.Tasks {
			if done[t.Key] {
				continue
			}
			ready := true
			prev := ""
			for _, d := range t.DependsOn {
				if !seen[d] || d == t.Key || d == prev {
					return p, sandbox.ErrPolicy
				}
				prev = d
				ready = ready && done[d]
			}
			if ready {
				done[t.Key] = true
				changed = true
			}
		}
		if !changed {
			return p, sandbox.ErrPolicy
		}
	}
	return p, nil
}
func (p Plan) Order() []string {
	result := []string{}
	seen := map[string]bool{}
	for len(result) < len(p.Tasks) {
		for _, t := range p.Tasks {
			if seen[t.Key] {
				continue
			}
			ready := true
			for _, d := range t.DependsOn {
				ready = ready && seen[d]
			}
			if ready {
				result = append(result, t.Key)
				seen[t.Key] = true
			}
		}
	}
	return result
}

type Finding struct {
	Severity  string `json:"severity"`
	File      string `json:"file"`
	Line      int    `json:"line"`
	Rationale string `json:"rationale"`
}
type Review struct {
	SchemaVersion int       `json:"schema_version"`
	Candidate     string    `json:"candidate_sha256"`
	Decision      string    `json:"decision"`
	Findings      []Finding `json:"findings"`
}

func (r Review) Valid(candidate string) bool {
	if r.SchemaVersion != 1 || r.Candidate != candidate || !ValidDigest(candidate) || !slices.Contains([]string{"pass", "reject"}, r.Decision) || len(r.Findings) > 8 || r.Decision == "reject" && len(r.Findings) == 0 {
		return false
	}
	for _, f := range r.Findings {
		if !slices.Contains([]string{"info", "warning", "error"}, f.Severity) || !slices.Contains(MathFiles, f.File) || f.Line < 1 || f.Line > 200 || !textBound(f.Rationale, 500) || r.Decision == "pass" && f.Severity == "error" {
			return false
		}
	}
	return true
}

type ArtifactRef struct {
	Task    string    `json:"task"`
	Attempt uuid.UUID `json:"attempt_id"`
	Digest  string    `json:"artifact_sha256"`
	Input   string    `json:"input_sha256"`
}
type RoleWork struct {
	Origin   sandbox.Snapshot `json:"origin"`
	Snapshot sandbox.Snapshot `json:"snapshot"`
	Inputs   []ArtifactRef    `json:"inputs"`
	Evidence string           `json:"evidence"`
	Error    string           `json:"error"`
}
type RoleOutcome struct {
	Work     RoleWork         `json:"work"`
	Snapshot sandbox.Snapshot `json:"snapshot"`
	Plan     *Plan            `json:"plan,omitempty"`
	Review   *Review          `json:"review,omitempty"`
	Reason   string           `json:"reason"`
}
type RoleNext struct {
	Next         Next     `json:"next"`
	Role         RoleSpec `json:"role"`
	RepositoryID string   `json:"repository_id"`
	BaseRevision string   `json:"base_revision"`
}

func SnapshotDigest(s sandbox.Snapshot) string { _, id, _ := sandbox.SnapshotBytes(s); return id }
func CloneSnapshot(s sandbox.Snapshot) sandbox.Snapshot {
	copy := s
	copy.Files = map[string][]byte{}
	for n, b := range s.Files {
		copy.Files[n] = bytes.Clone(b)
	}
	return copy
}
func ControlledMath(s sandbox.Snapshot) bool {
	if !s.Valid() || len(s.Files) > 8 {
		return false
	}
	total := 0
	for n, b := range s.Files {
		total += len(b)
		if !Visible(n) || !utf8.Valid(b) || bytes.ContainsRune(b, 0) {
			return false
		}
	}
	for _, n := range MathFiles {
		if len(s.Files[n]) == 0 || len(s.Files[n]) > 2048 {
			return false
		}
	}
	return total <= 4096 && strings.HasPrefix(string(s.Files["go.mod"]), "module fixture\n")
}

// MergeSnapshot is a conservative three-way merge of bounded in-memory blobs.
// One enclosing byte hunk per file is supported. Overlap is a visible conflict,
// including identical independent writes; no patch/Git command runs on the host.
func MergeSnapshot(base, current, incoming sandbox.Snapshot) (sandbox.Snapshot, error) {
	if !ControlledMath(base) || !ControlledMath(current) || !ControlledMath(incoming) || base.BaseRevision != current.BaseRevision || base.BaseRevision != incoming.BaseRevision || len(base.Files) != len(current.Files) || len(base.Files) != len(incoming.Files) {
		return current, sandbox.ErrPolicy
	}
	out := CloneSnapshot(current)
	for n, b := range base.Files {
		l, lok := current.Files[n]
		r, rok := incoming.Files[n]
		if !lok || !rok {
			return current, sandbox.ErrPolicy
		}
		if bytes.Equal(b, r) {
			continue
		}
		if !slices.Contains(MathFiles, n) {
			return current, sandbox.ErrPolicy
		}
		if bytes.Equal(b, l) {
			out.Files[n] = bytes.Clone(r)
			continue
		}
		startEnd := func(x []byte) (int, int, []byte) {
			a := 0
			for a < len(b) && a < len(x) && b[a] == x[a] {
				a++
			}
			z, w := len(b), len(x)
			for z > a && w > a && b[z-1] == x[w-1] {
				z--
				w--
			}
			return a, z, x[a:w]
		}
		a, z, left := startEnd(l)
		c, d, right := startEnd(r)
		if a <= d && c <= z {
			return current, fmt.Errorf("overlap_conflict: %s", n)
		}
		if a < c {
			out.Files[n] = append(append(append(append(bytes.Clone(b[:a]), left...), b[z:c]...), right...), b[d:]...)
		} else {
			out.Files[n] = append(append(append(append(bytes.Clone(b[:c]), right...), b[d:a]...), left...), b[z:]...)
		}
	}
	if !ControlledMath(out) {
		return current, sandbox.ErrPolicy
	}
	return out, nil
}
func ScopedCandidate(base, candidate sandbox.Snapshot, scope []string) bool {
	if !ControlledMath(base) || !ControlledMath(candidate) || base.BaseRevision != candidate.BaseRevision || len(base.Files) != len(candidate.Files) {
		return false
	}
	for n, b := range base.Files {
		v, ok := candidate.Files[n]
		if !ok || !bytes.Equal(b, v) && !slices.Contains(scope, n) {
			return false
		}
	}
	return true
}
func MathPatch(base, candidate sandbox.Snapshot) []byte {
	var out strings.Builder
	for _, n := range MathFiles {
		old, next := base.Files[n], candidate.Files[n]
		if bytes.Equal(old, next) {
			continue
		}
		before, after := strings.Split(strings.TrimSuffix(string(old), "\n"), "\n"), strings.Split(strings.TrimSuffix(string(next), "\n"), "\n")
		fmt.Fprintf(&out, "--- a/%s\n+++ b/%s\n@@ -1,%d +1,%d @@\n", n, n, len(before), len(after))
		for _, l := range before {
			out.WriteString("-" + l + "\n")
		}
		for _, l := range after {
			out.WriteString("+" + l + "\n")
		}
	}
	return []byte(out.String())
}

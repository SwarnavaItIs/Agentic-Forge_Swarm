// Package agent implements the trusted, bounded coding policy. Model output is
// data; only this package and the sandbox gateway can perform tool effects.
package agent

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"unicode/utf8"

	"forgeswarm/internal/sandbox"
)

const MaxContext = 12 << 10
const MaxTurns = 12
const MaxTools = 24
const MaxTokens = 200000
const MaxOutput = 1024
const MaxDenials = 2
const Profile = "sum-v1"

var digest = regexp.MustCompile(`^[0-9a-f]{64}$`)
var revision = regexp.MustCompile(`^[0-9a-f]{40}([0-9a-f]{24})?$`)

type Job struct {
	SchemaVersion   int    `json:"schema_version"`
	RepositoryID    string `json:"repository_id"`
	BaseRevision    string `json:"base_revision"`
	TaskDescription string `json:"task_description"`
	Policy          string `json:"policy"`
	Budget          string `json:"budget"`
	TestProfile     string `json:"test_profile"`
}

func (j Job) Valid() bool {
	return j.SchemaVersion == 1 && digest.MatchString(j.RepositoryID) && revision.MatchString(j.BaseRevision) && utf8.ValidString(j.TaskDescription) && len(strings.TrimSpace(j.TaskDescription)) > 0 && len(j.TaskDescription) <= 2000 && !strings.ContainsRune(j.TaskDescription, 0) && j.Policy == "controlled-go-v1" && j.Budget == "small-v1" && j.TestProfile == Profile
}

type Input struct {
	Job
	Role *RoleSpec `json:"role,omitempty"`
}

func (in Input) Valid() bool {
	if in.Role == nil {
		return in.Job.Valid()
	}
	j := in.Job
	j.TestProfile = Profile
	return in.TestProfile == MathProfile && j.Valid() && in.Role.Valid()
}
func (in Input) Sandbox() sandbox.Input {
	return sandbox.Input{SchemaVersion: 1, FixtureID: in.RepositoryID, Command: "test"}
}
func SHA(body []byte) string    { h := sha256.Sum256(body); return hex.EncodeToString(h[:]) }
func JSONSHA(v any) string      { b, _ := json.Marshal(v); return SHA(b) }
func ValidDigest(s string) bool { return digest.MatchString(s) }

// apply_patch v1 is one exact, unique text replacement, conditional on a
// workspace digest. No fuzzy application, shell patch command or arbitrary file.
type Action struct {
	SchemaVersion int             `json:"schema_version"`
	ID            string          `json:"id"`
	Tool          string          `json:"tool"`
	Args          json.RawMessage `json:"args"`
}
type Empty struct{}
type ReadArgs struct {
	Path  string `json:"path"`
	Start int    `json:"start"`
	Lines int    `json:"lines"`
}
type SearchArgs struct {
	Text string `json:"text"`
}
type PatchArgs struct {
	Path      string `json:"path"`
	Before    string `json:"before"`
	After     string `json:"after"`
	Workspace string `json:"workspace_sha256"`
}
type TestArgs struct {
	Profile string `json:"profile"`
}
type Observation struct {
	Status    string `json:"status"`
	Text      string `json:"text"`
	Workspace string `json:"workspace_sha256"`
	Truncated bool   `json:"truncated"`
	Omitted   int    `json:"omitted"`
}
type Next struct {
	SchemaVersion int    `json:"schema_version"`
	Attempt       string `json:"attempt_id"`
	Generation    int64  `json:"generation"`
	RequestID     string `json:"request_id"`
	Turn          int    `json:"turn"`
	Task          string `json:"task"`
	Context       string `json:"context"`
	Workspace     string `json:"workspace_sha256"`
	MaxOutput     int    `json:"max_output_tokens"`
}
type Reply struct {
	Status       string  `json:"status"`
	Action       *Action `json:"action"`
	Provider     string  `json:"provider"`
	Model        string  `json:"model"`
	RequestID    string  `json:"request_id"`
	InputTokens  *int    `json:"input_tokens"`
	OutputTokens *int    `json:"output_tokens"`
	LatencyMS    int64   `json:"latency_ms"`
	RetryAfterMS int     `json:"retry_after_ms"`
}

func ValidReply(r Reply) bool {
	if !slices.Contains([]string{"ok", "disabled", "invalid_response", "timeout", "rate_limited", "provider_error"}, r.Status) || len(r.Provider) < 1 || len(r.Provider) > 64 || len(r.Model) > 100 || len(r.RequestID) > 128 || r.LatencyMS < 0 || r.LatencyMS > 11000 || r.RetryAfterMS < 0 || r.RetryAfterMS > 1000 {
		return false
	}
	for _, v := range []*int{r.InputTokens, r.OutputTokens} {
		if v != nil && (*v < 0 || *v > 200000) {
			return false
		}
	}
	return true
}

// Journal records no prompts, source content or private model reasoning. Test
// evidence is a digest of the gateway result, never a model-reported pass flag.
type Event struct {
	ID             string `json:"id"`
	Phase          string `json:"phase"`
	Kind           string `json:"kind"`
	Name           string `json:"name"`
	InputDigest    string `json:"input_sha256"`
	OutputDigest   string `json:"output_sha256"`
	Workspace      string `json:"workspace_sha256"`
	AfterWorkspace string `json:"after_workspace_sha256"`
	Status         string `json:"status"`
	PromptUpper    int    `json:"prompt_token_upper"`
	Reply          *Reply `json:"reply,omitempty"`
}
type Outcome struct {
	Status        string          `json:"status"`
	Candidate     string          `json:"candidate_sha256"`
	Base          string          `json:"base_sha256"`
	BaseRevision  string          `json:"base_revision"`
	TestID        string          `json:"test_id"`
	TestDigest    string          `json:"test_sha256"`
	Patch         []byte          `json:"patch"`
	Test          *sandbox.Result `json:"test,omitempty"`
	Collaboration *RoleOutcome    `json:"collaboration,omitempty"`
}

func (o Outcome) Success() bool {
	if o.Collaboration != nil {
		return o.Status == "accepted" && ValidDigest(o.Candidate) && ValidDigest(o.Base)
	}
	return o.Status == "accepted" && ValidDigest(o.Candidate) && o.Candidate != o.Base && len(o.Patch) > 0 && len(o.Patch) <= sandbox.MaxArtifact && o.Test != nil && o.Test.Valid() && o.Test.Status == "exited" && o.Test.ExitCode == 0 && o.BaseRevision == o.Test.BaseRevision && o.TestDigest == JSONSHA(o.Test)
}

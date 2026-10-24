// Package evaluation compares role configurations through one bounded Go tool
// gateway and one immutable sandbox authority. It cannot publish or deploy.
package evaluation

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/evalcatalog"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/telemetry"
	"github.com/google/uuid"
	"slices"
	"strings"
	"time"
)

const Version = "equal-budget-v1"

var Configurations = []string{"developer", "planner_developer", "planner_developer_reviewer", "full_bounded"}

type Budget struct {
	Calls       int `json:"model_calls"`
	Tools       int `json:"tool_calls"`
	Tokens      int `json:"reserved_token_ceiling"`
	WallSeconds int `json:"wall_seconds"`
	MaxOutput   int `json:"max_output_tokens"`
}

func DefaultBudget() Budget { return Budget{12, 24, 200000, 90, 1024} }
func (b Budget) Valid() bool {
	return b.Calls >= 1 && b.Calls <= 12 && b.Tools >= 1 && b.Tools <= 24 && b.Tokens >= 1024 && b.Tokens <= 200000 && b.WallSeconds >= 1 && b.WallSeconds <= 90 && b.MaxOutput >= 16 && b.MaxOutput <= 1024
}

type Proof struct {
	Status    string `json:"status"`
	Digest    string `json:"sha256"`
	Candidate string `json:"candidate_sha256"`
	Base      string `json:"base_commit"`
	Cases     int    `json:"cases"`
	Passed    int    `json:"passed"`
}
type Record struct {
	SchemaVersion         int       `json:"schema_version"`
	EvaluationVersion     string    `json:"evaluation_version"`
	RunID                 string    `json:"run_id"`
	TaskID                string    `json:"task_id"`
	Category              string    `json:"category"`
	Split                 string    `json:"split"`
	ConfigID              string    `json:"config_id"`
	Trial                 int       `json:"trial"`
	CatalogDigest         string    `json:"catalog_sha256"`
	RepositoryDigest      string    `json:"repository_sha256"`
	BaseCommit            string    `json:"base_commit"`
	CandidateDigest       string    `json:"candidate_sha256"`
	Provider              string    `json:"provider"`
	Model                 string    `json:"model"`
	ProviderVersion       string    `json:"provider_version"`
	PromptHash            string    `json:"prompt_sha256"`
	ConfigHash            string    `json:"config_sha256"`
	Budget                Budget    `json:"budget"`
	Repetitions           int       `json:"repetitions"`
	StartedAt             time.Time `json:"started_at"`
	EndedAt               time.Time `json:"ended_at"`
	DurationSeconds       float64   `json:"duration_seconds"`
	Outcome               string    `json:"outcome"`
	Protected             Proof     `json:"protected_test_result"`
	PolicyViolations      []string  `json:"policy_violations"`
	ModelCallsAttempted   int       `json:"model_calls_attempted"`
	ModelCallsCompleted   int       `json:"model_calls_completed"`
	ToolsAttempted        int       `json:"tool_calls_attempted"`
	ToolsCompleted        int       `json:"tool_calls_completed"`
	InfrastructureRetries int       `json:"infrastructure_retries"`
	RepairRounds          int       `json:"repair_rounds"`
	Reviews               int       `json:"reviews"`
	ReviewRejections      int       `json:"review_rejections"`
	OperatorInterventions int       `json:"operator_interventions"`
	ReservedTokens        int       `json:"reserved_tokens"`
	InputTokens           *int      `json:"input_tokens"`
	OutputTokens          *int      `json:"output_tokens"`
	EstimatedMicrousd     *int64    `json:"estimated_cost_microusd"`
	ReservedMicrousd      int64     `json:"reserved_cost_microusd"`
	PricingDate           string    `json:"pricing_date"`
	TraceID               string    `json:"trace_id"`
	ArtifactReferences    []string  `json:"artifact_references"`
	StochasticSettings    string    `json:"stochastic_settings"`
}
type Next func(context.Context, agent.Next) (agent.Reply, error)
type Test func(context.Context, sandbox.Snapshot) (sandbox.Result, error)
type Reserve func(inputUpper, outputUpper int) error
type Engine struct {
	Next            Next
	Test            Test
	Pricing         agent.Pricing
	Provider, Model string
	Reserve         Reserve
}

var ErrSpendingCap = errors.New("authorized_spending_cap_exhausted")
var ErrOutcome = errors.New("evaluation_failed")

const PromptVersion = "eval-prompt-v1: trusted role capability; untrusted source; exact conditional edits; protected verdict required; no publication"

func Snapshot(task evalcatalog.Task) sandbox.Snapshot {
	return sandbox.Snapshot{SchemaVersion: 1, BaseRevision: task.BaseCommit(), Files: task.BaseFiles()}
}
func (e Engine) Run(ctx context.Context, task evalcatalog.Task, config string, trial, repetitions int, budget Budget) (record Record) {
	start := time.Now()
	record = Record{SchemaVersion: 1, EvaluationVersion: Version, TaskID: task.ID, Category: task.Category, Split: task.Split, ConfigID: config, Trial: trial, Repetitions: repetitions, CatalogDigest: evalcatalog.CatalogDigest(), BaseCommit: task.BaseCommit(), Provider: e.Provider, Model: e.Model, ProviderVersion: "python-runtime-v1", Budget: budget, StartedAt: start.UTC(), Outcome: "infrastructure_failure", PolicyViolations: []string{}, ArtifactReferences: []string{}, PricingDate: e.Pricing.VerifiedAt, PromptHash: agent.SHA([]byte(PromptVersion)), StochasticSettings: "provider defaults; no model seed; run-order seed recorded in manifest"}
	if e.Provider == "scripted_TEST_DEMO" {
		record.ProviderVersion = "go-scripted-fixture-v1"
		record.PricingDate = "unknown_fixture_not_invoiced"
		record.StochasticSettings = "deterministic scripted oracle; PCG run order seed is in manifest"
	}
	record.RunID = agent.SHA([]byte(fmt.Sprintf("%s:%s:%s:%d", record.CatalogDigest, task.ID, config, trial)))[:32]
	record.ConfigHash = agent.JSONSHA(struct {
		Version, Config string
		Budget          Budget
	}{Version, config, budget})
	candidate := Snapshot(task)
	record.RepositoryDigest = agent.SnapshotDigest(candidate)
	ctx, span := telemetry.Start(ctx, "evaluation.run")
	record.TraceID = telemetry.TraceID(ctx)
	defer func() {
		record.EndedAt = time.Now().UTC()
		record.DurationSeconds = time.Since(start).Seconds()
		record.CandidateDigest = agent.SnapshotDigest(candidate)
		telemetry.Finish(ctx, span, "evaluation.run", start, record.Outcome)
	}()
	if !budget.Valid() || !slices.Contains(Configurations, config) || e.Next == nil || e.Test == nil {
		record.PolicyViolations = append(record.PolicyViolations, "invalid_configuration")
		return
	}
	ctx, cancel := context.WithTimeout(ctx, time.Duration(budget.WallSeconds)*time.Second)
	defer cancel()
	inputTotal, outputTotal := 0, 0
	cost := int64(0)
	knownUsage := true
	defer func() {
		if knownUsage {
			record.InputTokens = &inputTotal
			record.OutputTokens = &outputTotal
			if e.Provider == "openai" {
				record.EstimatedMicrousd = &cost
			}
		}
	}()
	observation, plan := "", ""
	tested := ""
	var testResult sandbox.Result
	judge := func() bool {
		digest := agent.SnapshotDigest(candidate)
		if digest == tested {
			return record.Protected.Status == "passed"
		}
		violations := task.Validate(candidate.Files)
		record.PolicyViolations = append(record.PolicyViolations, violations...)
		if len(violations) > 0 {
			record.Protected = Proof{Status: "policy_denied", Candidate: digest, Base: task.BaseCommit()}
			return false
		}
		result, err := e.Test(ctx, candidate)
		testResult = result
		if err != nil {
			record.Outcome = "infrastructure_failure"
			record.Protected = Proof{Status: "infrastructure_failure", Candidate: digest, Base: task.BaseCommit()}
			return false
		}
		var authority struct {
			Success       bool   `json:"success"`
			Candidate     string `json:"candidate_sha256"`
			Base          string `json:"base_revision"`
			Profile       string `json:"profile"`
			Catalog       string `json:"catalog_sha256"`
			Authority     string `json:"authority"`
			Cases, Passed int
		}
		proofDigest := ""
		for _, artifact := range result.Artifacts {
			if artifact.Kind == "test_result" {
				proofDigest = agent.SHA(artifact.Body)
				_ = json.Unmarshal(artifact.Body, &authority)
			}
		}
		success := result.Valid() && result.Status == "exited" && result.ExitCode == 0 && authority.Success && authority.Authority == "immutable-helper" && authority.Profile == "eval:"+task.ID && authority.Catalog == evalcatalog.CatalogDigest() && authority.Candidate == digest && authority.Base == task.BaseCommit() && authority.Cases == len(task.ExpectedTests()) && authority.Passed == authority.Cases
		status := "failed"
		if result.Status == "timeout" {
			status = "timeout"
		}
		if success {
			status = "passed"
		}
		record.Protected = Proof{Status: status, Digest: proofDigest, Candidate: digest, Base: task.BaseCommit(), Cases: authority.Cases, Passed: authority.Passed}
		record.ArtifactReferences = []string{"sha256:" + proofDigest}
		tested = digest
		return success
	}
	callRole := func(role string, phaseCalls int) string {
		for local := 0; local < phaseCalls; local++ {
			if ctx.Err() != nil {
				record.Outcome = "timeout"
				return "stop"
			}
			if record.ModelCallsAttempted >= budget.Calls {
				record.Outcome = "budget_exhausted"
				return "stop"
			}
			files := map[string]string{}
			for n, b := range candidate.Files {
				files[n] = string(b)
			}
			contextBody, _ := json.Marshal(struct {
				Files                           map[string]string `json:"files"`
				Observation, Plan, TestedDigest string
			}{files, observation, plan, tested})
			request := agent.Next{SchemaVersion: 1, Attempt: uuid.NewSHA1(uuid.NameSpaceURL, []byte(record.RunID)).String(), Generation: 1, RequestID: uuid.NewString(), Turn: record.ModelCallsAttempted, Task: fmt.Sprintf("Evaluation task %s. Allowed edits: %s. %s", task.ID, strings.Join(task.Allowed(), ","), task.Instruction), Context: string(contextBody), Workspace: agent.SnapshotDigest(candidate), MaxOutput: budget.MaxOutput, EvaluationRole: role}
			upper := len(agentBytes(request)) + 8192
			if upper > 30000 || record.ReservedTokens+upper+budget.MaxOutput > budget.Tokens {
				record.Outcome = "budget_exhausted"
				return "stop"
			}
			if e.Reserve != nil {
				if err := e.Reserve(upper, budget.MaxOutput); err != nil {
					record.Outcome = "spending_cap_exhausted"
					return "stop"
				}
			}
			record.ReservedTokens += upper + budget.MaxOutput
			if e.Provider == "openai" {
				record.ReservedMicrousd += e.Pricing.Estimate(upper, budget.MaxOutput)
			}
			record.ModelCallsAttempted++
			call, done := context.WithTimeout(ctx, 10*time.Second)
			reply, err := e.Next(call, request)
			done()
			if err != nil || !agent.ValidReply(reply) || reply.Status != "ok" || reply.Action == nil {
				knownUsage = false
				record.Outcome = "provider_failure"
				if reply.Status == "timeout" {
					record.Outcome = "timeout"
				}
				return "stop"
			}
			if reply.Provider != e.Provider || reply.Model != e.Model {
				knownUsage = false
				record.PolicyViolations = append(record.PolicyViolations, "provider_identity_mismatch")
				record.Outcome = "policy_violation"
				return "stop"
			}
			record.ModelCallsCompleted++
			if reply.InputTokens == nil || reply.OutputTokens == nil {
				knownUsage = false
			} else {
				if *reply.InputTokens > upper || *reply.OutputTokens > budget.MaxOutput {
					knownUsage = false
					record.PolicyViolations = append(record.PolicyViolations, "usage_exceeds_reservation")
					record.Outcome = "policy_violation"
					return "stop"
				}
				inputTotal += *reply.InputTokens
				outputTotal += *reply.OutputTokens
				cost += e.Pricing.Estimate(*reply.InputTokens, *reply.OutputTokens)
			}
			action := *reply.Action
			record.ToolsAttempted++
			if record.ToolsAttempted > budget.Tools {
				record.Outcome = "budget_exhausted"
				return "stop"
			}
			if action.SchemaVersion != 1 {
				record.Outcome = "policy_violation"
				return "stop"
			}
			if id, err := uuid.Parse(action.ID); err != nil || id == uuid.Nil {
				record.Outcome = "policy_violation"
				return "stop"
			}
			switch action.Tool {
			case "final":
				var args struct {
					Decision string `json:"decision"`
					Notes    string `json:"notes"`
				}
				if agent.StrictDecode(action.Args, &args, 1024) != nil || !slices.Contains([]string{"pass", "reject"}, args.Decision) || len(args.Notes) > 500 {
					record.Outcome = "policy_violation"
					return "stop"
				}
				record.ToolsCompleted++
				if args.Decision == "reject" {
					record.Outcome = "model_rejected"
				}
				if role == "planner" {
					plan = args.Notes
				}
				if role == "reviewer" {
					record.Reviews++
					if args.Decision == "reject" {
						record.ReviewRejections++
					}
				}
				return args.Decision
			case "apply_patch":
				var patch agent.PatchArgs
				if role != "developer" || agent.StrictDecode(action.Args, &patch, 20000) != nil || !slices.Contains(task.Allowed(), patch.Path) || patch.Workspace != agent.SnapshotDigest(candidate) || len(patch.Before) < 1 || len(patch.Before) > 8192 || len(patch.After) > 8192 || bytes.Count(candidate.Files[patch.Path], []byte(patch.Before)) != 1 {
					record.PolicyViolations = append(record.PolicyViolations, "unauthorized_or_stale_edit")
					record.Outcome = "policy_violation"
					return "stop"
				}
				next := agent.CloneSnapshot(candidate)
				next.Files[patch.Path] = bytes.Replace(next.Files[patch.Path], []byte(patch.Before), []byte(patch.After), 1)
				if violations := task.Validate(next.Files); len(violations) > 0 {
					record.PolicyViolations = append(record.PolicyViolations, violations...)
					record.Outcome = "policy_violation"
					return "stop"
				}
				candidate = next
				record.ToolsCompleted++
				tested = ""
				observation = "conditional edit applied"
			case "run_test_profile":
				if role != "developer" {
					record.PolicyViolations = append(record.PolicyViolations, "read_only_role_tool")
					record.Outcome = "policy_violation"
					return "stop"
				}
				var args agent.TestArgs
				if agent.StrictDecode(action.Args, &args, 128) != nil || args.Profile != "eval:"+task.ID {
					record.PolicyViolations = append(record.PolicyViolations, "untrusted_test_profile")
					record.Outcome = "policy_violation"
					return "stop"
				}
				if judge() {
					observation = "protected tests passed for exact candidate"
				} else {
					observation = "protected tests failed for exact candidate"
				}
				record.ToolsCompleted++
			case "list_tree", "read_file", "search_text", "status", "diff":
				if agent.ValidateAction(action) != nil {
					record.PolicyViolations = append(record.PolicyViolations, "invalid_inspection")
					record.Outcome = "policy_violation"
					return "stop"
				}
				workspace, _ := agent.NewWorkspace(candidate)
				seen, err := workspace.Apply(ctx, action)
				if err != nil {
					record.Outcome = "policy_violation"
					return "stop"
				}
				observation = seen.Text
				record.ToolsCompleted++
			default:
				record.PolicyViolations = append(record.PolicyViolations, "unknown_tool")
				record.Outcome = "policy_violation"
				return "stop"
			}
		}
		record.Outcome = "budget_exhausted"
		return "stop"
	}
	if config != "developer" {
		if callRole("planner", 2) != "pass" {
			return
		}
	}
	developerCalls := budget.Calls - record.ModelCallsAttempted
	if callRole("developer", developerCalls) != "pass" {
		return
	}
	passing := judge()
	if config == "planner_developer_reviewer" || config == "full_bounded" {
		verdict := callRole("reviewer", 2)
		if verdict == "stop" {
			return
		}
		if verdict == "reject" {
			if config != "full_bounded" {
				record.Outcome = "review_rejected"
				return
			}
			record.RepairRounds++
			observation = "review rejected; one repair allowed within original shared budget"
			if callRole("developer", budget.Calls-record.ModelCallsAttempted) != "pass" {
				return
			}
			passing = judge()
			finalReview := callRole("reviewer", 2)
			if finalReview != "pass" {
				if finalReview != "stop" {
					record.Outcome = "review_rejected"
				}
				return
			}
		}
	}
	_ = testResult // the verdict above binds its actual protected artifact digest.
	if ctx.Err() != nil {
		record.Outcome = "timeout"
		return
	}
	if passing && len(record.PolicyViolations) == 0 {
		record.Outcome = "passed"
	} else if record.Protected.Status == "timeout" {
		record.Outcome = "timeout"
	} else if record.Protected.Status == "infrastructure_failure" {
		record.Outcome = "infrastructure_failure"
	} else {
		record.Outcome = "task_failure"
	}
	return
}
func agentBytes(v any) []byte { b, _ := json.Marshal(v); return b }

func Fixture(task evalcatalog.Task) Next {
	target := Snapshot(task)
	target.Files = task.FixtureCandidate()
	wanted := agent.SnapshotDigest(target)
	return func(ctx context.Context, n agent.Next) (agent.Reply, error) {
		if err := ctx.Err(); err != nil {
			return agent.Reply{}, err
		}
		action := agent.Action{SchemaVersion: 1, ID: uuid.NewSHA1(uuid.NameSpaceURL, []byte(n.RequestID)).String(), Tool: "final", Args: json.RawMessage(`{"decision":"pass","notes":"Scripted TEST/DEMO role result; no quality claim."}`)}
		if n.EvaluationRole == "developer" {
			var data struct {
				Files        map[string]string `json:"files"`
				TestedDigest string
			}
			if json.Unmarshal([]byte(n.Context), &data) != nil {
				return agent.Reply{}, ErrOutcome
			}
			if n.Workspace != wanted {
				path := task.Allowed()[0]
				action.Tool = "apply_patch"
				action.Args = agentBytes(agent.PatchArgs{Path: path, Before: data.Files[path], After: string(target.Files[path]), Workspace: n.Workspace})
			} else if data.TestedDigest != wanted {
				action.Tool = "run_test_profile"
				action.Args = agentBytes(agent.TestArgs{Profile: "eval:" + task.ID})
			}
		}
		input := len(n.Context) + len(n.Task)
		output := 256
		return agent.Reply{Status: "ok", Action: &action, Provider: "scripted_TEST_DEMO", Model: "fixture-v1", RequestID: n.RequestID, InputTokens: &input, OutputTokens: &output}, nil
	}
}

func Normalized(record Record) string {
	// Repeatability of controlled plumbing excludes clocks, IDs and trace storage.
	record.StartedAt = time.Time{}
	record.EndedAt = time.Time{}
	record.DurationSeconds = 0
	record.TraceID = ""
	return strings.TrimSpace(string(agentBytes(record)))
}

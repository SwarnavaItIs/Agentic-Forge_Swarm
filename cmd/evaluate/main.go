package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	agentv1 "forgeswarm/api/agent/v1"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/evalcatalog"
	"forgeswarm/internal/evaluation"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/telemetry/setup"
	"github.com/google/uuid"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
)

type Planned struct {
	RunID, TaskID, ConfigID string
	Trial                   int
}
type Manifest struct {
	SchemaVersion           int                         `json:"schema_version"`
	Mode                    string                      `json:"mode"`
	EvaluationVersion       string                      `json:"evaluation_version"`
	CatalogHash             string                      `json:"catalog_sha256"`
	PromptHash              string                      `json:"prompt_sha256"`
	Budget                  evaluation.Budget           `json:"budget"`
	Repetitions             int                         `json:"repetitions"`
	OrderSeed               uint64                      `json:"run_order_seed"`
	Runs                    []Planned                   `json:"runs"`
	Catalog                 []evalcatalog.Specification `json:"catalog"`
	MaximumEstimateMicrousd *int64                      `json:"maximum_estimate_microusd"`
	Notes                   []string                    `json:"notes"`
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	mode := flag.String("mode", "fixture", "fixture or live; live requires existing authorized caps")
	output := flag.String("out", "artifacts/verification/stage-09/evaluation", "new output directory; results are never silently replaced")
	repeat := flag.Int("repetitions", 1, "1..3 trials per task/config")
	seed := flag.Uint64("seed", 202609, "run-order randomization only; not model determinism")
	split := flag.String("split", "all", "all, development or holdout")
	only := flag.String("task", "", "one catalog task for a diagnostic replay")
	helper := flag.String("helper", "bin/sandbox-helper-linux", "trusted prepared immutable helper")
	authPath := flag.String("authorization", "", "existing operator-authorized spending configuration")
	confirm := flag.Bool("confirm-authorized-spending", false, "confirm the already-authorized total/per-run caps")
	catalogOnly := flag.Bool("catalog", false, "print public frozen catalog without executing")
	flag.Parse()
	if *repeat < 1 || *repeat > 3 || !slices.Contains([]string{"fixture", "live"}, *mode) || !slices.Contains([]string{"all", "development", "holdout"}, *split) {
		return errors.New("bounded evaluation arguments required")
	}
	manifest := Manifest{SchemaVersion: 1, Mode: *mode, EvaluationVersion: evaluation.Version, CatalogHash: evalcatalog.CatalogDigest(), Budget: evaluation.DefaultBudget(), Repetitions: *repeat, OrderSeed: *seed, Notes: []string{"Scripted mode validates plumbing only, never real-agent quality.", "Development/holdout membership is frozen. Do not tune on holdout failures.", "Run order is randomized; a seed does not guarantee live-provider determinism.", "Failures, timeouts, budget exhaustion and missing results remain in the intended denominator.", "All four configurations share total task budgets, model, tools, base and protected authority."}}
	prompt := []byte(evaluation.PromptVersion)
	for _, name := range []string{"agent-runtime/forge_agent/provider.py", "agent-runtime/forge_agent/schema.py"} {
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		prompt = append(prompt, data...)
	}
	manifest.PromptHash = agent.SHA(prompt)
	for _, task := range evalcatalog.All() {
		manifest.Catalog = append(manifest.Catalog, task.Public())
	}
	if *catalogOnly {
		return json.NewEncoder(os.Stdout).Encode(manifest)
	}
	tasks := []evalcatalog.Task{}
	for _, task := range evalcatalog.All() {
		if (*split == "all" || task.Split == *split) && (*only == "" || task.ID == *only) {
			tasks = append(tasks, task)
		}
	}
	if len(tasks) == 0 {
		return errors.New("no selected catalog tasks")
	}
	for _, task := range tasks {
		for _, config := range evaluation.Configurations {
			for trial := 0; trial < *repeat; trial++ {
				id := agent.SHA([]byte(fmt.Sprintf("%s:%s:%s:%d", manifest.CatalogHash, task.ID, config, trial)))[:32]
				manifest.Runs = append(manifest.Runs, Planned{id, task.ID, config, trial})
			}
		}
	}
	random := rand.New(rand.NewPCG(*seed, *seed^0x736f6d6570736575))
	random.Shuffle(len(manifest.Runs), func(a, b int) { manifest.Runs[a], manifest.Runs[b] = manifest.Runs[b], manifest.Runs[a] })
	engine := evaluation.Engine{Provider: "scripted_TEST_DEMO", Model: "fixture-v1"}
	var ledger *evaluation.Ledger
	if *mode == "live" {
		if !*confirm || *authPath == "" {
			return errors.New("live evaluation disabled: existing session-authorized total cap, per-run cap, authorization file, configured runtime credentials and explicit confirmation are required")
		}
		pricing, err := agent.LoadPricing(os.Getenv("AGENT_PRICING_FILE"))
		if err != nil {
			return errors.New("dated configured pricing required")
		}
		data, err := os.ReadFile(*authPath)
		if err != nil || len(data) > 4096 {
			return errors.New("bounded authorization file required")
		}
		var authorization evaluation.Authorization
		if agent.StrictDecode(data, &authorization, 4096) != nil || authorization.CatalogHash != manifest.CatalogHash {
			return errors.New("authorization must pin this catalog and pricing")
		}
		maximum := pricing.Estimate(30000, manifest.Budget.MaxOutput) * int64(manifest.Budget.Calls) * int64(len(manifest.Runs))
		manifest.MaximumEstimateMicrousd = &maximum
		fmt.Printf("Maximum conservative estimate: %d microUSD; execution stops at authorized total/per-run caps.\n", maximum)
		ledger, err = evaluation.OpenLedger(authorization, pricing, ".cache/evaluation/spending")
		if err != nil {
			return err
		}
		defer ledger.Close()
		connection, err := agent.DialRuntime(os.Getenv("AGENT_RUNTIME_ADDR"), os.Getenv("AGENT_RUNTIME_TOKEN"), os.Getenv("AGENT_INSECURE_LOCAL") == "true")
		if err != nil {
			return err
		}
		defer connection.Close()
		engine = evaluation.Engine{Next: agent.NextAction(agentv1.NewRuntimeClient(connection)), Pricing: pricing, Provider: "openai", Model: pricing.Model}
	}
	if err := os.MkdirAll(filepath.Dir(*output), 0700); err != nil {
		return err
	}
	if err := os.Mkdir(*output, 0700); err != nil {
		return errors.New("output directory must be new; retain raw results and use an explicit new run directory")
	}
	if *mode == "fixture" {
		if e := os.WriteFile(filepath.Join(*output, ".owned"), []byte("forgeswarm-evaluation-fixture-v1"), 0600); e != nil {
			return e
		}
	}
	data, _ := json.MarshalIndent(manifest, "", "  ")
	if err := os.WriteFile(filepath.Join(*output, "manifest.json"), append(data, '\n'), 0600); err != nil {
		return err
	}
	results, err := os.OpenFile(filepath.Join(*output, "runs.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer results.Close()
	csvFile, err := os.OpenFile(filepath.Join(*output, "runs.csv"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	defer csvFile.Close()
	writer := csv.NewWriter(csvFile)
	defer writer.Flush()
	_ = writer.Write([]string{"run_id", "task_id", "config_id", "trial", "outcome", "duration_seconds", "input_tokens", "output_tokens", "estimated_cost_microusd", "reserved_tokens", "calls", "tools", "protected_test_result"})
	stopTelemetry := setup.Init("evaluation")
	defer stopTelemetry()
	ctx := context.Background()
	driver, err := sandbox.NewDocker(ctx, uuid.New(), ".cache/evaluation/registry", *helper)
	if err != nil {
		return err
	}
	for _, planned := range manifest.Runs {
		task, _ := evalcatalog.Get(planned.TaskID)
		current := engine
		if *mode == "fixture" {
			current.Next = evaluation.Fixture(task)
		}
		if ledger != nil {
			current.Reserve = ledger.Reservation(current.Pricing)
		}
		current.Test = func(ctx context.Context, candidate sandbox.Snapshot) (sandbox.Result, error) {
			id := uuid.NewSHA1(uuid.NameSpaceURL, []byte(planned.RunID))
			binding := sandbox.Binding{Installation: driver.Installation, Workflow: id, Task: id, Attempt: id, Instance: id, Generation: 1, Worker: "evaluation", Request: sandbox.RequestID(id)}
			_, digest, err := sandbox.SnapshotBytes(candidate)
			if err != nil {
				return sandbox.Result{}, err
			}
			policy := sandbox.DefaultPolicy()
			policy.TimeoutMS = 15000
			result, err := driver.RunSnapshot(ctx, driver.Request(binding, sandbox.Input{SchemaVersion: 1, FixtureID: digest, Command: "test"}, policy), candidate, "eval:"+task.ID, func(ctx context.Context) error { return ctx.Err() })
			if err == nil {
				for _, artifact := range result.Artifacts {
					if artifact.Kind == "test_result" {
						path := filepath.Join(*output, agent.SHA(artifact.Body)+".proof.json")
						if writeErr := os.WriteFile(path, artifact.Body, 0600); writeErr != nil {
							return result, writeErr
						}
					}
				}
			}
			return result, err
		}
		record := current.Run(ctx, task, planned.ConfigID, planned.Trial, *repeat, manifest.Budget)
		record.PromptHash = agent.SHA([]byte(manifest.PromptHash + agent.JSONSHA(task.Public())))
		raw, _ := json.Marshal(record)
		if _, err = results.Write(append(raw, '\n')); err != nil {
			return err
		}
		if err = results.Sync(); err != nil {
			return err
		}
		number := func(v *int) string {
			if v == nil {
				return "unknown"
			}
			return strconv.Itoa(*v)
		}
		money := "unknown"
		if record.EstimatedMicrousd != nil {
			money = strconv.FormatInt(*record.EstimatedMicrousd, 10)
		}
		if err = writer.Write([]string{record.RunID, record.TaskID, record.ConfigID, strconv.Itoa(record.Trial), record.Outcome, strconv.FormatFloat(record.DurationSeconds, 'f', 6, 64), number(record.InputTokens), number(record.OutputTokens), money, strconv.Itoa(record.ReservedTokens), strconv.Itoa(record.ModelCallsAttempted), strconv.Itoa(record.ToolsAttempted), record.Protected.Status}); err != nil {
			return err
		}
		writer.Flush()
		if err = writer.Error(); err != nil {
			return err
		}
		fmt.Printf("%s %s trial=%d outcome=%s calls=%d/%d\n", task.ID, planned.ConfigID, planned.Trial, record.Outcome, record.ModelCallsAttempted, manifest.Budget.Calls)
	}
	if ledger != nil {
		fmt.Println(ledger.Summary())
	}
	fmt.Printf("Saved %d intended runs; mode=%s; no comparative quality conclusion is implied.\n", len(manifest.Runs), *mode)
	return nil
}

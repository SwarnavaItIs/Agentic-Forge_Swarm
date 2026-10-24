package sandbox

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"forgeswarm/internal/evalcatalog"
	"os"
	"os/exec"
	"slices"
	"strings"
	"time"
)

// executeEvaluation never compiles a model-supplied test authority. The frozen
// catalog chooses exact files, test names and base identity in the helper image.
func executeEvaluation(ctx context.Context, r Request, s Snapshot, profile string) (Result, error) {
	task, ok := evalcatalog.Get(strings.TrimPrefix(profile, "eval:"))
	if !ok || s.BaseRevision != task.BaseCommit() {
		return Result{}, ErrPolicy
	}
	call, cancel := context.WithTimeout(ctx, time.Duration(r.Policy.TimeoutMS)*time.Millisecond)
	defer cancel()
	result := Result{SchemaVersion: 1, Status: "policy_denied", ExitCode: 1, Image: r.Image, BaseRevision: s.BaseRevision, Policy: r.Policy}
	violations := task.Validate(s.Files)
	passed := []string{}
	stdout, stderr := &CappedWriter{Limit: 4096}, &CappedWriter{Limit: 4096}
	if len(violations) == 0 {
		if err := os.Mkdir("/workspace/evaluation", 0700); err != nil {
			return result, err
		}
		for name, body := range s.Files {
			if err := os.WriteFile("/workspace/evaluation/"+name, body, 0400); err != nil {
				return result, err
			}
		}
		if err := os.WriteFile("/workspace/evaluation/protected_test.go", []byte(task.ProtectedSource()), 0400); err != nil {
			return result, err
		}
		args := []string{"test", "-json", "-count=1", "-timeout=3s", "-parallel=2", "-p=1", "solution.go", "protected_test.go"}
		if _, exists := s.Files["visible_test.go"]; exists {
			args = append(args, "visible_test.go")
		}
		cmd := exec.CommandContext(call, "/usr/local/go/bin/go", args...)
		cmd.Dir = "/workspace/evaluation"
		cmd.Env = r.Env
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		cmd.WaitDelay = 200 * time.Millisecond
		err := cmd.Run()
		passed = passedEvaluationTests(stdout.Data, task.ExpectedTests())
		result.Status = "nonzero_exit"
		if err == nil && !stdout.Truncated && len(passed) == len(task.ExpectedTests()) {
			result.Status = "exited"
			result.ExitCode = 0
		}
		if call.Err() != nil || bytes.Contains(stdout.Data, []byte("test timed out after")) || bytes.Contains(stderr.Data, []byte("test timed out after")) {
			result.Status = "timeout"
			result.ExitCode = 124
		}
	}
	_, candidate, _ := SnapshotBytes(s)
	proof, _ := json.Marshal(map[string]any{"schema_version": 1, "profile": profile, "catalog_sha256": evalcatalog.CatalogDigest(), "candidate_sha256": candidate, "base_revision": s.BaseRevision, "cases": len(task.ExpectedTests()), "passed": len(passed), "passed_tests": passed, "policy_violations": violations, "success": result.Status == "exited", "authority": "immutable-helper"})
	result.Artifacts = []Artifact{{Kind: "stdout", MediaType: "text/plain; charset=utf-8", Body: stdout.Data, Truncated: stdout.Truncated}, {Kind: "stderr", MediaType: "text/plain; charset=utf-8", Body: stderr.Data, Truncated: stderr.Truncated}, {Kind: "test_result", MediaType: "text/plain; charset=utf-8", Body: proof}, {Kind: "patch", MediaType: "text/plain; charset=utf-8", Body: []byte{}}}
	return result, nil
}

// A fake zero exit, a missing/skipped test or a renamed test cannot pass.
func passedEvaluationTests(body []byte, expected []string) []string {
	passed := map[string]bool{}
	started := map[string]bool{}
	packagePass := false
	scanner := bufio.NewScanner(bytes.NewReader(body))
	scanner.Buffer(make([]byte, 1024), 8192)
	for scanner.Scan() {
		var event struct{ Action, Test string }
		if json.Unmarshal(scanner.Bytes(), &event) != nil {
			return nil
		}
		if event.Test == "" {
			if event.Action == "pass" {
				packagePass = true
			}
			continue
		}
		if !slices.Contains(expected, event.Test) {
			return nil
		}
		switch event.Action {
		case "run":
			started[event.Test] = true
		case "pass":
			passed[event.Test] = started[event.Test]
		case "skip", "fail":
			return nil
		}
	}
	if scanner.Err() != nil || !packagePass {
		return nil
	}
	names := []string{}
	for _, n := range expected {
		if passed[n] {
			names = append(names, n)
		}
	}
	return names
}

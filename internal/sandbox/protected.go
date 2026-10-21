package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strconv"
	"time"
)

// The evaluator and cases are compiled into the read-only helper. Repository
// tests, TestMain, README instructions and model claims cannot set this verdict.
// This is a finite controlled-function profile, not a proof of general correctness.
const sumRunner = `package main
import("fmt";"os";"strconv")
func main(){a,e:=strconv.Atoi(os.Args[1]);if e!=nil{os.Exit(2)};b,e:=strconv.Atoi(os.Args[2]);if e!=nil{os.Exit(2)};fmt.Println(Add(a,b))}
`

func executeProtected(ctx context.Context, r Request, s Snapshot) (Result, error) {
	call, cancel := context.WithTimeout(ctx, time.Duration(r.Policy.TimeoutMS)*time.Millisecond)
	defer cancel()
	result := Result{SchemaVersion: 1, Status: "nonzero_exit", ExitCode: 1, Image: r.Image, BaseRevision: s.BaseRevision, Policy: r.Policy}
	if len(s.Files["sum.go"]) == 0 || len(s.Files["sum.go"]) > 8192 {
		return result, ErrPolicy
	}
	if !pureAdd(s.Files["sum.go"]) {
		result.Status = "policy_denied"
		_, candidate, _ := SnapshotBytes(s)
		proof, _ := json.Marshal(map[string]any{"schema_version": 1, "profile": "sum-v1", "candidate_sha256": candidate, "base_revision": s.BaseRevision, "success": false, "authority": "immutable-helper", "reason": "single_pure_Add_expression_required"})
		result.Artifacts = []Artifact{{Kind: "stdout", MediaType: "text/plain; charset=utf-8", Body: []byte{}}, {Kind: "stderr", MediaType: "text/plain; charset=utf-8", Body: []byte("sum-v1 requires one pure Add(int,int) int arithmetic expression; imports, init, calls and extra declarations are denied")}, {Kind: "test_result", MediaType: "text/plain; charset=utf-8", Body: proof}, {Kind: "patch", MediaType: "text/plain; charset=utf-8", Body: []byte{}}}
		return result, nil
	}
	if err := os.Mkdir("/workspace/evaluation", 0700); err != nil {
		return result, err
	}
	for n, b := range map[string][]byte{"sum.go": s.Files["sum.go"], "runner.go": []byte(sumRunner)} {
		if err := os.WriteFile("/workspace/evaluation/"+n, b, 0400); err != nil {
			return result, err
		}
	}
	out, stderr := &CappedWriter{Limit: 4096}, &CappedWriter{Limit: 4096}
	run := func(args ...string) error {
		cmd := exec.CommandContext(call, args[0], args[1:]...)
		cmd.Dir = "/workspace/evaluation"
		cmd.Env = r.Env
		cmd.Stdout = out
		cmd.Stderr = stderr
		cmd.WaitDelay = 200 * time.Millisecond
		return cmd.Run()
	}
	err := run("/usr/local/go/bin/go", "build", "-p=1", "-o", "/workspace/evaluation/program", "runner.go", "sum.go")
	cases := [][2]int{{2, 3}, {0, 0}, {-4, 7}, {19, -8}, {-3, -11}, {1000, 999}}
	passed := 0
	if err == nil {
		for _, pair := range cases {
			out = &CappedWriter{Limit: 128}
			err = run("/workspace/evaluation/program", strconv.Itoa(pair[0]), strconv.Itoa(pair[1]))
			want := []byte(strconv.Itoa(pair[0]+pair[1]) + "\n")
			if err != nil || out.Truncated || !bytes.Equal(out.Data, want) {
				break
			}
			passed++
		}
	} else {
		result.Status = "compiler_error"
	}
	if passed == len(cases) {
		result.Status = "exited"
		result.ExitCode = 0
	}
	if call.Err() != nil {
		result.Status = "timeout"
		result.ExitCode = 124
	}
	_, candidate, _ := SnapshotBytes(s)
	proof, _ := json.Marshal(map[string]any{"schema_version": 1, "profile": "sum-v1", "candidate_sha256": candidate, "base_revision": s.BaseRevision, "cases": len(cases), "passed": passed, "success": result.Status == "exited", "authority": "immutable-helper"})
	result.Artifacts = []Artifact{{Kind: "stdout", MediaType: "text/plain; charset=utf-8", Body: out.Data, Truncated: out.Truncated}, {Kind: "stderr", MediaType: "text/plain; charset=utf-8", Body: stderr.Data, Truncated: stderr.Truncated}, {Kind: "test_result", MediaType: "text/plain; charset=utf-8", Body: proof}, {Kind: "patch", MediaType: "text/plain; charset=utf-8", Body: []byte{}}}
	return result, nil
}

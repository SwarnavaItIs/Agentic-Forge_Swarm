package sandbox

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"forgeswarm/internal/evalcatalog"
	"os"
	"os/exec"
	"slices"
	"strconv"
	"strings"
	"time"
)

func ProtectedProfile(name string) bool {
	if strings.HasPrefix(name, "eval:") {
		_, ok := evalcatalog.Get(strings.TrimPrefix(name, "eval:"))
		return ok
	}
	return slices.Contains([]string{"sum-v1", "add-v1", "double-v1", "score-v1", "math-v1"}, name)
}

const mathRunner = `package main
import("fmt";"os";"strconv")
func main(){a,e:=strconv.Atoi(os.Args[1]);if e!=nil{os.Exit(2)};b,e:=strconv.Atoi(os.Args[2]);if e!=nil{os.Exit(2)};switch os.Args[3]{case "add-v1":fmt.Println(Add(a,b));case "double-v1":fmt.Println(Double(a));case "score-v1":fmt.Println(Score(a,b));case "math-v1":fmt.Println(Add(a,b),Double(a),Score(a,b))}}
`

func executeMath(ctx context.Context, r Request, s Snapshot, profile string) (Result, error) {
	call, cancel := context.WithTimeout(ctx, time.Duration(r.Policy.TimeoutMS)*time.Millisecond)
	defer cancel()
	result := Result{SchemaVersion: 1, Status: "policy_denied", ExitCode: 1, Image: r.Image, BaseRevision: s.BaseRevision, Policy: r.Policy}
	files := []string{"add.go", "double.go", "score.go"}
	names := []string{"Add", "Double", "Score"}
	arity := []int{2, 1, 2}
	valid := true
	for i, n := range files {
		valid = valid && pureFunction(s.Files[n], names[i], arity[i])
	}
	passed := 0
	cases := [][2]int{{2, 3}, {0, 0}, {-4, 7}, {19, -8}, {-3, -11}, {1000, 999}}
	out, stderr := &CappedWriter{Limit: 4096}, &CappedWriter{Limit: 4096}
	if valid {
		if err := os.Mkdir("/workspace/evaluation", 0700); err != nil {
			return result, err
		}
		for _, n := range files {
			if err := os.WriteFile("/workspace/evaluation/"+n, s.Files[n], 0400); err != nil {
				return result, err
			}
		}
		if err := os.WriteFile("/workspace/evaluation/runner.go", []byte(mathRunner), 0400); err != nil {
			return result, err
		}
		run := func(args ...string) error {
			cmd := exec.CommandContext(call, args[0], args[1:]...)
			cmd.Dir = "/workspace/evaluation"
			cmd.Env = r.Env
			cmd.Stdout = out
			cmd.Stderr = stderr
			cmd.WaitDelay = 200 * time.Millisecond
			return cmd.Run()
		}
		result.Status = "compiler_error"
		if run("/usr/local/go/bin/go", "build", "-p=1", "-o", "/workspace/evaluation/program", "runner.go", "add.go", "double.go", "score.go") == nil {
			result.Status = "nonzero_exit"
			for _, p := range cases {
				out = &CappedWriter{Limit: 256}
				want := fmt.Sprintf("%d %d %d\n", p[0]+p[1], 2*p[0], 3*p[0]+p[1])
				switch profile {
				case "add-v1":
					want = fmt.Sprintln(p[0] + p[1])
				case "double-v1":
					want = fmt.Sprintln(2 * p[0])
				case "score-v1":
					want = fmt.Sprintln(3*p[0] + p[1])
				}
				if run("/workspace/evaluation/program", strconv.Itoa(p[0]), strconv.Itoa(p[1]), profile) != nil || out.Truncated || !bytes.Equal(out.Data, []byte(want)) {
					break
				}
				passed++
			}
			if passed == len(cases) {
				result.Status = "exited"
				result.ExitCode = 0
			}
		}
	}
	if call.Err() != nil {
		result.Status = "timeout"
		result.ExitCode = 124
	}
	_, candidate, _ := SnapshotBytes(s)
	proof, _ := json.Marshal(map[string]any{"schema_version": 1, "profile": profile, "candidate_sha256": candidate, "base_revision": s.BaseRevision, "cases": len(cases), "passed": passed, "success": result.Status == "exited", "authority": "immutable-helper"})
	result.Artifacts = []Artifact{{Kind: "stdout", MediaType: "text/plain; charset=utf-8", Body: out.Data, Truncated: out.Truncated}, {Kind: "stderr", MediaType: "text/plain; charset=utf-8", Body: stderr.Data, Truncated: stderr.Truncated}, {Kind: "test_result", MediaType: "text/plain; charset=utf-8", Body: proof}, {Kind: "patch", MediaType: "text/plain; charset=utf-8", Body: []byte{}}}
	return result, nil
}

package workflow

import (
	"fmt"
	"forgeswarm/internal/agent"
	"strings"
	"testing"
)

func TestAgentVersionAndLegacyGolden(t *testing.T) {
	old := CreateInput{Name: "old v5", Tasks: []TaskDefinition{{Key: "a", Kind: "noop"}}}
	hash, e := HashForVersion(old, 5)
	if e != nil {
		t.Fatal(e)
	}
	const golden = "e0e78f883ad95900318a3b9473760f809ff475cd9b5edc8cc28443337f049a9d"
	if fmt.Sprintf("%x", hash) != golden {
		t.Fatal("v5 drift")
	}
	in := agent.Input{Job: agent.Job{SchemaVersion: 1, RepositoryID: strings.Repeat("a", 64), BaseRevision: strings.Repeat("b", 40), TaskDescription: "fix", Policy: "controlled-go-v1", Budget: "small-v1", TestProfile: agent.Profile}}
	sandboxInput := in.Sandbox()
	job := CreateInput{Name: "agent", Tasks: []TaskDefinition{{Key: "code", Kind: "agent_go", Agent: &in, Sandbox: &sandboxInput}}}
	for v := 1; v <= 5; v++ {
		if _, e := HashForVersion(job, v); e != ErrConflict {
			t.Fatal(v, e)
		}
	}
	if frozen, e := HashForVersion(job, 6); e != nil || fmt.Sprintf("%x", frozen) != "cad2f04af64e2270fcbc9f8fcfcc65cbba2816be7e35cf2f79546a15bea523f4" {
		t.Fatal(e)
	}
	job.Tasks[0].Agent.Role = &agent.RoleSpec{SchemaVersion: 1, Name: "planner", Version: 1, TaskKey: "plan", Scope: []string{}, Profile: agent.MathProfile, MaxCalls: 4}
	job.Tasks[0].Agent.TestProfile = agent.MathProfile
	for version := 1; version <= 6; version++ {
		if _, e := HashForVersion(job, version); e != ErrConflict {
			t.Fatal("role input projected onto legacy version", version, e)
		}
	}
	if _, e := HashForVersion(job, 7); e != nil {
		t.Fatal(e)
	}
}

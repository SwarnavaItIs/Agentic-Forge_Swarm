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
	if _, e := HashForVersion(job, 6); e != nil {
		t.Fatal(e)
	}
}

package workflow

import (
	"encoding/hex"
	"forgeswarm/internal/sandbox"
	"strings"
	"testing"
)

func TestNormalizationV1AndOrder(t *testing.T) {
	a := CreateInput{Name: "  First workflow \n", Tasks: []TaskDefinition{{Key: "diagnostic", Kind: "noop"}, {Key: "second", Kind: "noop"}}}
	b := a
	b.Name = "First workflow"
	h1, err := HashForVersion(a, 1)
	if err != nil {
		t.Fatal(err)
	}
	h2, err := HashForVersion(b, 1)
	if err != nil || h1 != h2 {
		t.Fatal("equivalent normalization changed")
	}
	b.Tasks = []TaskDefinition{a.Tasks[1], a.Tasks[0]}
	h3, err := HashForVersion(b, 1)
	if err != nil || h3 == h1 {
		t.Fatal("task order not included")
	}
	// Golden vector freezes the persisted normalization format, independently of
	// reusing the encoder on both sides of a test.
	one, err := HashForVersion(CreateInput{Name: "First workflow", Tasks: []TaskDefinition{{Key: "diagnostic", Kind: "noop"}}}, 1)
	if err != nil {
		t.Fatal(err)
	}
	const want = "0c2652b71710d4470cb5ffa7288d198b74f7f67c58918b6bb61a2946e67cf861"
	if hex.EncodeToString(one[:]) != want {
		t.Fatalf("normalization v1 hash: %x", one)
	}
	if _, err := HashForVersion(a, 6); err != ErrState {
		t.Fatalf("unknown version: %v", err)
	}
}

func TestValidation(t *testing.T) {
	valid := CreateInput{Name: strings.Repeat("界", 128), Tasks: []TaskDefinition{{Key: "a", Kind: "noop"}}}
	if _, err := Normalize(valid); err != nil {
		t.Fatal(err)
	}
	cases := []CreateInput{
		{Name: " ", Tasks: valid.Tasks}, {Name: strings.Repeat("界", 129), Tasks: valid.Tasks},
		{Name: "a\x00b", Tasks: valid.Tasks}, {Name: "x"}, {Name: "x", Tasks: make([]TaskDefinition, 101)},
		{Name: "x", Tasks: []TaskDefinition{{Key: "A", Kind: "noop"}}}, {Name: "x", Tasks: []TaskDefinition{{Key: "a", Kind: "exec"}}},
		{Name: "x", Tasks: []TaskDefinition{{Key: "a", Kind: "noop"}, {Key: "a", Kind: "noop"}}},
	}
	for i, in := range cases {
		if _, err := Normalize(in); err != ErrInvalid {
			t.Fatalf("case %d: %v", i, err)
		}
	}
	for _, key := range []string{"a", "AZ_09.:-", strings.Repeat("x", 128)} {
		if !ValidCreationKey(key) {
			t.Fatal(key)
		}
	}
	for _, key := range []string{"", "a b", "a,b", strings.Repeat("x", 129)} {
		if ValidCreationKey(key) {
			t.Fatal(key)
		}
	}
}
func TestV3GoldenAndV4PolicyCompatibility(t *testing.T) {
	in := CreateInput{Name: "v3 golden", Tasks: []TaskDefinition{{Key: "a", Kind: "delay", DelayMS: 123}}}
	hash, err := HashForVersion(in, 3)
	if err != nil || hex.EncodeToString(hash[:]) != "67c67ea26778e09384355b8f8af069d42182c46a3bce81beeec2c8b9136257d7" {
		t.Fatal("v3 persisted encoding changed", err)
	}
	for _, change := range []func(*CreateInput){func(v *CreateInput) { v.TimeoutMS = 1000 }, func(v *CreateInput) { v.Tasks[0].TimeoutMS = 1000 }, func(v *CreateInput) { v.Tasks[0].MaxAttempts = 2 }} {
		updated := in
		updated.Tasks = append([]TaskDefinition(nil), in.Tasks...)
		change(&updated)
		if _, err = HashForVersion(updated, 3); err != ErrConflict {
			t.Fatal("new policy field projected onto v3", err)
		}
		if _, err = HashForVersion(updated, 4); err != nil {
			t.Fatal(err)
		}
	}
}

func TestV4GoldenAndV5SandboxCompatibility(t *testing.T) {
	in := CreateInput{Name: "v4 golden", TimeoutMS: 5000, Tasks: []TaskDefinition{{Key: "a", Kind: "delay", DelayMS: 123, TimeoutMS: 1000, MaxAttempts: 2}}}
	hash, err := HashForVersion(in, 4)
	if err != nil || hex.EncodeToString(hash[:]) != "6dfd8d9fd6d47616dfb08bec0dc6563b249ac499f04518dc8bd75b64f6334592" {
		t.Fatal("v4 persisted encoding changed", err)
	}
	in.Tasks[0].Kind = "sandbox_go"
	in.Tasks[0].DelayMS = 0
	in.Tasks[0].Sandbox = &sandbox.Input{SchemaVersion: 1, FixtureID: strings.Repeat("a", 64), Command: "test"}
	if _, err = HashForVersion(in, 4); err != ErrConflict {
		t.Fatal("sandbox projected onto v4", err)
	}
	a, err := HashForVersion(in, 5)
	if err != nil {
		t.Fatal(err)
	}
	in.Tasks[0].Sandbox.Command = "build"
	b, err := HashForVersion(in, 5)
	if err != nil || a == b {
		t.Fatal("command ignored in v5", err)
	}
}

func TestStateRules(t *testing.T) {
	for _, test := range []struct {
		status  string
		version int64
		change  bool
		bad     bool
	}{
		{Pending, 1, true, false}, {Cancelled, 2, false, false}, {"RUNNING", 1, false, true}, {Pending, 2, true, false}, {Cancelled, 1, false, true},
	} {
		change, err := Cancellation(test.status, test.version)
		if change != test.change || (err != nil) != test.bad {
			t.Fatalf("%+v: %v %v", test, change, err)
		}
	}
}

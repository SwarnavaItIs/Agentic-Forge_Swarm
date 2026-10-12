package workflow

import (
	"encoding/hex"
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
	if _, err := HashForVersion(a, 3); err != ErrState {
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

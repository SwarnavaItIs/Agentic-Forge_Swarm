package workflow

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"testing"

	"github.com/google/uuid"
)

func definitions(deps [][]string) []TaskDefinition {
	tasks := make([]TaskDefinition, len(deps))
	for i, depends := range deps {
		tasks[i] = TaskDefinition{Key: fmt.Sprintf("n%d", i), Kind: "noop", DependsOn: depends}
	}
	return tasks
}
func graphFixture(defs []TaskDefinition, states []string) Graph {
	graph := Graph{ID: uuid.NewSHA1(uuid.Nil, []byte("graph")), Status: Pending, Version: 1}
	ids := map[string]uuid.UUID{}
	for i, task := range defs {
		id := uuid.NewSHA1(uuid.Nil, []byte(task.Key))
		ids[task.Key] = id
		state := Pending
		if len(states) > i {
			state = states[i]
		}
		graph.Nodes = append(graph.Nodes, Task{ID: id, Key: task.Key, Kind: task.Kind, Position: i, Status: state})
	}
	for _, task := range defs {
		for _, key := range task.DependsOn {
			graph.Edges = append(graph.Edges, Edge{ids[task.Key], ids[key]})
		}
	}
	return graph
}
func projected(graph Graph, decision Decision) map[string]string {
	byID := map[uuid.UUID]string{}
	states := map[string]string{}
	for _, task := range graph.Nodes {
		byID[task.ID] = task.Key
		states[task.Key] = task.Status
	}
	for _, change := range decision.Transitions {
		states[byID[change.TaskID]] = change.To
	}
	return states
}
func TestGraphValidationFixtures(t *testing.T) {
	cases := []struct {
		name string
		deps [][]string
		ok   bool
	}{
		{"chain", [][]string{nil, {"n0"}, {"n1"}}, true},
		{"diamond", [][]string{nil, {"n0"}, {"n0"}, {"n1", "n2"}}, true},
		{"independent_roots", [][]string{nil, nil, nil}, true},
		{"disconnected", [][]string{nil, {"n0"}, nil, {"n2"}}, true},
		{"fan_in", [][]string{nil, nil, nil, {"n0", "n1", "n2"}}, true},
		{"fan_out", [][]string{nil, {"n0"}, {"n0"}, {"n0"}}, true},
		{"empty", nil, false},
		{"self", [][]string{{"n0"}}, false},
		{"cycle", [][]string{{"n2"}, {"n0"}, {"n1"}}, false},
		{"unknown", [][]string{{"missing"}}, false},
		{"duplicate", [][]string{nil, {"n0", "n0"}}, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			defs := definitions(test.deps)
			order, err := ValidateGraph(defs)
			if (err == nil) != test.ok {
				t.Fatalf("validation: %v", err)
			}
			if test.ok {
				positions := map[string]int{}
				for i, index := range order {
					positions[defs[index].Key] = i
				}
				for _, task := range defs {
					for _, dep := range task.DependsOn {
						if positions[dep] >= positions[task.Key] {
							t.Fatal("not a topological order")
						}
					}
				}
			}
		})
	}
	if _, err := ValidateGraph(definitions(make([][]string, 101))); err != ErrInvalid {
		t.Fatal("oversized nodes")
	}
	deps := make([][]string, 46)
	for i := range deps {
		for j := 0; j < i; j++ {
			deps[i] = append(deps[i], fmt.Sprintf("n%d", j))
		}
	}
	if _, err := ValidateGraph(definitions(deps)); err != ErrInvalid {
		t.Fatal("oversized edges")
	}
}
func TestNormalizationVersionsAndDependencySets(t *testing.T) {
	original := CreateInput{Name: "x", Tasks: definitions([][]string{nil, nil, {"n1", "n0"}})}
	equivalent := CreateInput{Name: " x ", Tasks: definitions([][]string{nil, nil, {"n0", "n1"}})}
	one, err := HashForVersion(original, 2)
	if err != nil {
		t.Fatal(err)
	}
	two, err := HashForVersion(equivalent, 2)
	if err != nil || one != two {
		t.Fatal("dependency order is semantic", err)
	}
	if !reflect.DeepEqual(original.Tasks[2].DependsOn, []string{"n1", "n0"}) {
		t.Fatal("normalization mutated caller")
	}
	if _, err := HashForVersion(original, 1); !errors.Is(err, ErrConflict) {
		t.Fatal("legacy silently ignored edges", err)
	}
	legacy := CreateInput{Name: "x", Tasks: definitions([][]string{nil, nil})}
	h1, err := HashForVersion(legacy, 1)
	if err != nil {
		t.Fatal(err)
	}
	legacy.Tasks[0].DependsOn = []string{}
	h2, err := HashForVersion(legacy, 1)
	if err != nil || h1 != h2 {
		t.Fatal("explicit empty not legacy-compatible")
	}
	legacy.Tasks[0], legacy.Tasks[1] = legacy.Tasks[1], legacy.Tasks[0]
	h3, _ := HashForVersion(legacy, 1)
	if h3 == h1 {
		t.Fatal("display order disappeared")
	}
}

func TestGraphValidationExactLimits(t *testing.T) {
	defs := definitions(make([][]string, 100))
	// A complete DAG on 45 nodes has 990 edges. Ten predecessors for the
	// next node reach the accepted edge limit without introducing a cycle.
	for i := 1; i < 45; i++ {
		for j := 0; j < i; j++ {
			defs[i].DependsOn = append(defs[i].DependsOn, defs[j].Key)
		}
	}
	for j := 0; j < 10; j++ {
		defs[45].DependsOn = append(defs[45].DependsOn, defs[j].Key)
	}
	if order, err := ValidateGraph(defs); err != nil || len(order) != 100 {
		t.Fatal("100 tasks and 1,000 edges rejected", err)
	}
	defs[45].DependsOn = append(defs[45].DependsOn, defs[10].Key)
	if _, err := ValidateGraph(defs); !errors.Is(err, ErrInvalid) {
		t.Fatal("1,001 edges accepted", err)
	}
}

func TestReadinessFailureAndAggregateRules(t *testing.T) {
	defs := definitions([][]string{nil, {"n0"}, {"n0"}, {"n1", "n2"}, nil})
	graph := graphFixture(defs, nil)
	decision, err := Reconcile(graph)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{"n0": Ready, "n1": Pending, "n2": Pending, "n3": Pending, "n4": Ready}
	if !reflect.DeepEqual(projected(graph, decision), want) || decision.Status != Pending {
		t.Fatal("roots are eligible, not successful")
	}
	graph = graphFixture(defs, []string{Succeeded, Succeeded, Ready, Pending, Failed})
	decision, err = Reconcile(graph)
	if err != nil || len(decision.Transitions) != 0 || decision.Status != Pending {
		t.Fatal("fan-in scheduled early", decision, err)
	}
	graph = graphFixture(defs, []string{Failed, Pending, Ready, Pending, Ready})
	decision, err = Reconcile(graph)
	if err != nil {
		t.Fatal(err)
	}
	states := projected(graph, decision)
	if states["n1"] != Blocked || states["n2"] != Blocked || states["n3"] != Blocked || states["n4"] != Ready || decision.Status != Pending {
		t.Fatal("failure stopped independent work", states)
	}
	graph = graphFixture(defs, []string{Failed, Blocked, Blocked, Blocked, Succeeded})
	decision, err = Reconcile(graph)
	if err != nil || decision.Status != Failed {
		t.Fatal("merely blocked graph successful", decision)
	}
	graph = graphFixture(defs, []string{Succeeded, Succeeded, Succeeded, Succeeded, Succeeded})
	decision, err = Reconcile(graph)
	if err != nil || decision.Status != Succeeded {
		t.Fatal("all succeeded not aggregated")
	}
	graph.Nodes[4].Status = Running
	decision, err = Reconcile(graph)
	if err != nil || decision.Status != Pending {
		t.Fatal("running work counted complete")
	}
	graph.Status = Cancelled
	decision, err = Reconcile(graph)
	if err != nil || len(decision.Transitions) != 0 || decision.Status != Cancelled {
		t.Fatal("cancellation lost")
	}
	if TaskTransition(Succeeded, Ready) || !TaskTransition(Ready, Running) || !TaskTransition(Running, Succeeded) {
		t.Fatal("future claiming transition")
	}
}

// A deliberately simple fixed-point reference, independent of Kahn's ordering.
func reference(graph Graph, defs []TaskDefinition) map[string]string {
	states := map[string]string{}
	for _, task := range graph.Nodes {
		states[task.Key] = task.Status
	}
	for changed := true; changed; {
		changed = false
		for _, task := range defs {
			from := states[task.Key]
			if from != Pending && from != Ready {
				continue
			}
			succeeded, bad := true, false
			for _, dep := range task.DependsOn {
				s := states[dep]
				succeeded = succeeded && s == Succeeded
				bad = bad || s == Failed || s == Blocked || s == Cancelled
			}
			next := from
			if bad {
				next = Blocked
			} else if from == Pending && succeeded {
				next = Ready
			}
			if next != from {
				states[task.Key] = next
				changed = true
			}
		}
	}
	return states
}
func TestRandomizedReadinessMatchesIndependentReference(t *testing.T) {
	rng := rand.New(rand.NewSource(20261010))
	for trial := 0; trial < 300; trial++ {
		n := 1 + rng.Intn(30)
		defs := definitions(make([][]string, n))
		for i := range defs {
			for j := 0; j < i; j++ {
				if rng.Intn(5) == 0 {
					defs[i].DependsOn = append(defs[i].DependsOn, defs[j].Key)
				}
			}
		}
		rng.Shuffle(n, func(i, j int) { defs[i], defs[j] = defs[j], defs[i] })
		states := make([]string, n)
		choices := []string{Pending, Ready, Succeeded, Failed, Blocked}
		for i := range states {
			states[i] = choices[rng.Intn(len(choices))]
		}
		graph := graphFixture(defs, states)
		decision, err := Reconcile(graph)
		if err != nil {
			t.Fatal(trial, err)
		}
		if !reflect.DeepEqual(projected(graph, decision), reference(graph, defs)) {
			t.Fatal("reference mismatch", trial)
		}
	}
}

func TestNormalizationV2Golden(t *testing.T) {
	hash, err := HashForVersion(CreateInput{Name: "First workflow", Tasks: []TaskDefinition{{Key: "diagnostic", Kind: "noop"}}}, 2)
	if err != nil {
		t.Fatal(err)
	}
	const want = "bb22b457c3a94e50adbfe80ff9b2ddfe39f51971495a8e6672b8f31a9332e083"
	if hex.EncodeToString(hash[:]) != want {
		t.Fatalf("normalization v2 changed: %x", hash)
	}
}

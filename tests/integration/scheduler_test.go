//go:build integration

package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"forgeswarm/internal/postgres"
	"forgeswarm/internal/workflow"
	"forgeswarm/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

func diamond(t *testing.T) workflow.CreateInput {
	t.Helper()
	raw, err := os.ReadFile("../fixtures/diamond.json")
	if err != nil {
		t.Fatal(err)
	}
	var input workflow.CreateInput
	if err = json.Unmarshal(raw, &input); err != nil {
		t.Fatal(err)
	}
	return input
}
func newGraph(t *testing.T, f *fixture, key string, in workflow.CreateInput) workflow.Receipt {
	t.Helper()
	receipt, err := f.s1.Create(f.ctx, key, in)
	if err != nil {
		t.Fatal(err)
	}
	return receipt
}
func snapshot(t *testing.T, f *fixture, id uuid.UUID) workflow.Graph {
	t.Helper()
	graph, err := f.s1.Graph(f.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return graph
}
func states(graph workflow.Graph) map[string]string {
	result := map[string]string{}
	for _, task := range graph.Nodes {
		result[task.Key] = task.Status
	}
	return result
}
func reconcile(t *testing.T, f *fixture, id uuid.UUID) int {
	t.Helper()
	n, err := f.s1.Reconcile(f.ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// Explicit test-only terminal seeding, serialized exactly like trusted writers.
// This helper is compiled only under the integration tag; no production endpoint.
func seed(t *testing.T, f *fixture, id uuid.UUID, key, status string) {
	t.Helper()
	tx, err := f.p1.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(f.ctx, `SELECT id FROM workflows WHERE id=$1 FOR UPDATE`, id); err != nil {
		t.Fatal(err)
	}
	tag, err := tx.Exec(f.ctx, `UPDATE tasks SET status=$3,updated_at=clock_timestamp() WHERE workflow_id=$1 AND task_key=$2`, id, key, status)
	if err != nil || tag.RowsAffected() != 1 {
		t.Fatal("test-only terminal seed", err)
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
}
func history(t *testing.T, f *fixture, id uuid.UUID) []workflow.Event {
	t.Helper()
	events, err := f.s1.Events(f.ctx, id, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for i, event := range events.Events {
		if event.Sequence != int64(i+1) {
			t.Fatal("history gap/order")
		}
	}
	return events.Events
}
func TestDurableDiamondTwoSchedulersAndRepeatedReconciliation(t *testing.T) {
	f := setup(t)
	r := newGraph(t, f, "diamond", diamond(t))
	before := snapshot(t, f, r.ID)
	if len(before.Edges) != 4 || len(before.Nodes) != 4 {
		t.Fatal("durable graph missing")
	}
	counts := make([]int, 2)
	allOK(t, parallel(2, func(i int) error {
		s := f.s1
		if i == 1 {
			s = f.s2
		}
		n, err := s.Reconcile(f.ctx, r.ID)
		counts[i] = n
		return err
	}))
	if counts[0]+counts[1] != 1 {
		t.Fatal("duplicate logical readiness", counts)
	}
	want := map[string]string{"a": workflow.Ready, "b": workflow.Pending, "c": workflow.Pending, "d": workflow.Pending}
	after := snapshot(t, f, r.ID)
	if !reflect.DeepEqual(states(after), want) || after.Status != workflow.Pending {
		t.Fatal("scheduled work counted successful", after)
	}
	if reconcile(t, f, r.ID) != 0 || !reflect.DeepEqual(after, snapshot(t, f, r.ID)) || len(history(t, f, r.ID)) != 2 {
		t.Fatal("repeat emitted/mutated")
	}
	seed(t, f, r.ID, "a", workflow.Succeeded)
	if reconcile(t, f, r.ID) != 2 {
		t.Fatal("fan-out")
	}
	seed(t, f, r.ID, "b", workflow.Succeeded)
	if reconcile(t, f, r.ID) != 0 || states(snapshot(t, f, r.ID))["d"] != workflow.Pending {
		t.Fatal("fan-in early")
	}
	seed(t, f, r.ID, "c", workflow.Succeeded)
	if reconcile(t, f, r.ID) != 1 || states(snapshot(t, f, r.ID))["d"] != workflow.Ready {
		t.Fatal("fan-in never ready")
	}
	seed(t, f, r.ID, "d", workflow.Succeeded)
	if reconcile(t, f, r.ID) != 1 || snapshot(t, f, r.ID).Status != workflow.Succeeded {
		t.Fatal("success aggregate")
	}
	before = snapshot(t, f, r.ID)
	if reconcile(t, f, r.ID) != 0 {
		t.Fatal("terminal rescheduled")
	}
	result, err := f.s1.Cancel(f.ctx, r.ID)
	if err != nil || result.Status != workflow.Succeeded || !reflect.DeepEqual(before, snapshot(t, f, r.ID)) {
		t.Fatal("terminal cancellation mutated", err)
	}
	if len(history(t, f, r.ID)) != 6 {
		t.Fatal("missing/surplus events")
	}
}
func TestGraphInsertionValidationAndAtomicRollback(t *testing.T) {
	f := setup(t)
	bad := diamond(t)
	bad.Tasks[0].DependsOn = []string{"d"}
	if _, err := f.s1.Create(f.ctx, "invalid-cycle", bad); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatal(err)
	}
	exec(t, f, `CREATE FUNCTION reject_edge() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'test edge failure'; END $$`)
	exec(t, f, `CREATE TRIGGER reject_edge BEFORE INSERT ON task_dependencies FOR EACH ROW EXECUTE FUNCTION reject_edge()`)
	if _, err := f.s1.Create(f.ctx, "graph-rollback", diamond(t)); err == nil {
		t.Fatal("edge failure committed")
	}
	for _, table := range []string{"workflows", "tasks", "task_dependencies", "workflow_events"} {
		if scalar(t, f, "SELECT count(*) FROM "+table) != 0 {
			t.Fatal("partial graph", table)
		}
	}
	exec(t, f, `DROP TRIGGER reject_edge ON task_dependencies`)
	r := newGraph(t, f, "graph-rollback", diamond(t))
	reordered := diamond(t)
	reordered.Tasks[3].DependsOn = []string{"c", "b"}
	replay, err := f.s2.Create(f.ctx, "graph-rollback", reordered)
	if err != nil || replay.ID != r.ID {
		t.Fatal("dependency order conflict", err)
	}
	reordered.Tasks[3].DependsOn = []string{"a"}
	if _, err = f.s2.Create(f.ctx, "graph-rollback", reordered); !errors.Is(err, workflow.ErrConflict) {
		t.Fatal("new field ignored", err)
	}
	if scalar(t, f, `SELECT count(*) FROM task_dependencies`) != 4 {
		t.Fatal("replay changed graph")
	}
}
func TestGraphCompositeConstraintsAndImmutability(t *testing.T) {
	f := setup(t)
	// Unfinalized definitions isolate real unique/FK constraints from the later
	// finalization guard. These are disposable test setup, not an API mutation.
	w1, w2, a, b, c := uuid.New(), uuid.New(), uuid.New(), uuid.New(), uuid.New()
	hash, _ := workflow.HashForVersion(input("constraint fixture"), 1)
	for i, id := range []uuid.UUID{w1, w2} {
		exec(t, f, `INSERT INTO workflows(id,name,status,version,creation_key,request_hash,request_schema_version) VALUES($1,'constraint fixture','PENDING',1,$2,$3,1)`, id, fmt.Sprintf("unfinalized-%d", i), hash[:])
	}
	exec(t, f, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,'a',0,'noop','PENDING'),($3,$2,'b',1,'noop','PENDING'),($4,$5,'c',0,'noop','PENDING')`, a, w1, b, c, w2)
	tests := []struct {
		sql, code string
		args      []any
	}{
		{`INSERT INTO task_dependencies VALUES($1,$2,$3)`, "23503", []any{w1, a, c}},
		{`INSERT INTO task_dependencies VALUES($1,$2,$3)`, "23503", []any{w2, a, c}},
		{`INSERT INTO task_dependencies VALUES($1,$2,$3)`, "23503", []any{w1, a, uuid.New()}},
		{`INSERT INTO task_dependencies VALUES($1,$2,$2)`, "23514", []any{w1, a}},
		{`INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,'a',2,'noop','PENDING')`, "23505", []any{uuid.New(), w1}},
		{`INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,'z',0,'noop','PENDING')`, "23505", []any{uuid.New(), w1}},
	}
	for _, test := range tests {
		_, err := f.p1.Exec(f.ctx, test.sql, test.args...)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != test.code {
			t.Fatalf("real constraint %s: %v", test.code, err)
		}
	}
	exec(t, f, `INSERT INTO task_dependencies VALUES($1,$2,$3)`, w1, b, a)
	_, err := f.p1.Exec(f.ctx, `INSERT INTO task_dependencies VALUES($1,$2,$3)`, w1, b, a)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23505" {
		t.Fatal("duplicate edge PK", err)
	}
	exec(t, f, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created','{}')`, w1)
	for _, query := range []string{
		`UPDATE tasks SET task_key='changed' WHERE workflow_id=$1`,
		`DELETE FROM tasks WHERE workflow_id=$1`,
		`UPDATE task_dependencies SET depends_on_task_id=task_id WHERE workflow_id=$1`,
		`DELETE FROM task_dependencies WHERE workflow_id=$1`,
		`INSERT INTO task_dependencies SELECT workflow_id,depends_on_task_id,task_id FROM task_dependencies WHERE workflow_id=$1`,
		`UPDATE workflows SET name='changed' WHERE id=$1`,
	} {
		_, err := f.p1.Exec(f.ctx, query, w1)
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Fatal("finalized definition changed", query, err)
		}
	}
	if scalar(t, f, `SELECT count(*) FROM task_dependencies WHERE workflow_id=$1`, w1) != 1 {
		t.Fatal("immutable graph changed")
	}
}
func TestHistorySequenceSeparateFromVersionAndCancellation(t *testing.T) {
	f := setup(t)
	in := workflow.CreateInput{Name: "roots", Tasks: []workflow.TaskDefinition{{Key: "a", Kind: "noop"}, {Key: "b", Kind: "noop"}, {Key: "c", Kind: "noop"}}}
	r := newGraph(t, f, "roots", in)
	if reconcile(t, f, r.ID) != 3 {
		t.Fatal("roots not ready")
	}
	if snapshot(t, f, r.ID).Version != 2 || scalar(t, f, `SELECT last_event_sequence FROM workflows WHERE id=$1`, r.ID) != 4 {
		t.Fatal("event/version counters conflated")
	}
	events := history(t, f, r.ID)
	for i, event := range events[1:] {
		var payload struct {
			TaskID uuid.UUID `json:"task_id"`
		}
		if err := json.Unmarshal(event.Payload, &payload); err != nil || payload.TaskID != snapshot(t, f, r.ID).Nodes[i].ID {
			t.Fatal("unstable event order")
		}
	}
	cancellation, err := f.s2.Cancel(f.ctx, r.ID)
	if err != nil || cancellation.Version != 3 || len(history(t, f, r.ID)) != 5 {
		t.Fatal("cancel history sequence", err)
	}
	if reconcile(t, f, r.ID) != 0 || len(history(t, f, r.ID)) != 5 {
		t.Fatal("cancelled rescheduled")
	}
}
func TestSchedulerHistoryFailureRollsBackEveryState(t *testing.T) {
	f := setup(t)
	r := newGraph(t, f, "event-failure", diamond(t))
	before := snapshot(t, f, r.ID)
	exec(t, f, `CREATE FUNCTION reject_ready() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type='task.ready' THEN RAISE EXCEPTION 'test history failure'; END IF; RETURN NEW; END $$`)
	exec(t, f, `CREATE TRIGGER reject_ready BEFORE INSERT ON workflow_events FOR EACH ROW EXECUTE FUNCTION reject_ready()`)
	if _, err := f.s1.Reconcile(f.ctx, r.ID); err == nil {
		t.Fatal("history failure accepted")
	}
	if !reflect.DeepEqual(before, snapshot(t, f, r.ID)) || len(history(t, f, r.ID)) != 1 || scalar(t, f, `SELECT last_event_sequence FROM workflows WHERE id=$1`, r.ID) != 1 {
		t.Fatal("partial readiness/history")
	}
	exec(t, f, `DROP TRIGGER reject_ready ON workflow_events`)
	if reconcile(t, f, r.ID) != 1 {
		t.Fatal("rollback lost eligibility")
	}
}

func TestSchedulerLaterHistoryFailureRollsBackAllTransitions(t *testing.T) {
	f := setup(t)
	in := workflow.CreateInput{Name: "later history rollback", Tasks: []workflow.TaskDefinition{
		{Key: "a", Kind: "noop"}, {Key: "b", Kind: "noop"}, {Key: "c", Kind: "noop"},
	}}
	r := newGraph(t, f, "later-event-failure", in)
	before := snapshot(t, f, r.ID)
	// Creation is event 1. Event 2 is inserted successfully before event 3
	// fails, so this exercises rollback of already-written history and states.
	exec(t, f, `CREATE FUNCTION reject_later_ready() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type='task.ready' AND NEW.sequence=3 THEN RAISE EXCEPTION 'test later history failure'; END IF; RETURN NEW; END $$`)
	exec(t, f, `CREATE TRIGGER reject_later_ready BEFORE INSERT ON workflow_events FOR EACH ROW EXECUTE FUNCTION reject_later_ready()`)
	n, err := f.s1.Reconcile(f.ctx, r.ID)
	var pgErr *pgconn.PgError
	if n != 0 || !errors.As(err, &pgErr) || pgErr.Code != "P0001" || pgErr.Message != "test later history failure" {
		t.Fatal("later-event injection was not reached or partial success was reported", n, err)
	}
	if !reflect.DeepEqual(before, snapshot(t, f, r.ID)) || len(history(t, f, r.ID)) != 1 || scalar(t, f, `SELECT last_event_sequence FROM workflows WHERE id=$1`, r.ID) != 1 {
		t.Fatal("earlier transitions/history survived transaction failure")
	}
	exec(t, f, `DROP TRIGGER reject_later_ready ON workflow_events`)
	if reconcile(t, f, r.ID) != 3 || snapshot(t, f, r.ID).Version != 2 || len(history(t, f, r.ID)) != 4 {
		t.Fatal("retry lost eligibility or left an event-sequence gap")
	}
}

func TestMaximumGraphDurabilityAndReadinessHistoryPagination(t *testing.T) {
	f := setup(t)
	in := workflow.CreateInput{Name: "maximum graph", Tasks: make([]workflow.TaskDefinition, 100)}
	for i := range in.Tasks {
		in.Tasks[i] = workflow.TaskDefinition{Key: fmt.Sprintf("n%d", i), Kind: "noop"}
	}
	for i := 1; i < 45; i++ {
		for j := 0; j < i; j++ {
			in.Tasks[i].DependsOn = append(in.Tasks[i].DependsOn, in.Tasks[j].Key)
		}
	}
	for j := 0; j < 10; j++ {
		in.Tasks[45].DependsOn = append(in.Tasks[45].DependsOn, in.Tasks[j].Key)
	}
	r := newGraph(t, f, "maximum-graph", in)
	graph := snapshot(t, f, r.ID)
	if len(graph.Nodes) != 100 || len(graph.Edges) != 1000 || scalar(t, f, `SELECT count(*) FROM task_dependencies WHERE workflow_id=$1`, r.ID) != 1000 {
		t.Fatal("graph at accepted limits was truncated or not durable")
	}
	keys := make(map[uuid.UUID]string, len(graph.Nodes))
	want := make(map[string]map[string]bool, len(in.Tasks))
	got := make(map[string]map[string]bool, len(in.Tasks))
	for i, node := range graph.Nodes {
		if node.Position != i || node.Key != in.Tasks[i].Key {
			t.Fatal("display order changed at the task limit")
		}
		keys[node.ID] = node.Key
		want[node.Key], got[node.Key] = make(map[string]bool), make(map[string]bool)
		for _, predecessor := range in.Tasks[i].DependsOn {
			want[node.Key][predecessor] = true
		}
	}
	for _, edge := range graph.Edges {
		key, exists := keys[edge.TaskID]
		predecessor, predecessorExists := keys[edge.DependsOnTaskID]
		if !exists || !predecessorExists {
			t.Fatal("graph edge lost a node")
		}
		got[key][predecessor] = true
	}
	if !reflect.DeepEqual(want, got) || reconcile(t, f, r.ID) != 55 {
		t.Fatal("stored dependencies or root eligibility changed at the graph limit")
	}

	// Independent roots emit 100 readiness events in one logical mutation,
	// exceeding one HTTP event page once workflow.created is included.
	roots := workflow.CreateInput{Name: "maximum independent roots", Tasks: make([]workflow.TaskDefinition, 100)}
	for i := range roots.Tasks {
		roots.Tasks[i] = workflow.TaskDefinition{Key: fmt.Sprintf("r%d", i), Kind: "noop"}
	}
	rootReceipt := newGraph(t, f, "maximum-roots", roots)
	if reconcile(t, f, rootReceipt.ID) != 100 {
		t.Fatal("not every independent root became eligible")
	}
	rootGraph := snapshot(t, f, rootReceipt.ID)
	if rootGraph.Status != workflow.Pending || rootGraph.Version != 2 {
		t.Fatal("readiness was counted as success or 100 separate mutations")
	}
	for _, node := range rootGraph.Nodes {
		if node.Status != workflow.Ready {
			t.Fatal("an independent root lost eligibility")
		}
	}
	page, err := f.s1.Events(f.ctx, rootReceipt.ID, 0, 100)
	if err != nil || len(page.Events) != 100 || page.NextAfter != 100 {
		t.Fatal("first full history page", err)
	}
	for i, event := range page.Events {
		if event.Sequence != int64(i+1) {
			t.Fatal("history ordering changed across the page boundary")
		}
	}
	tail, err := f.s2.Events(f.ctx, rootReceipt.ID, page.NextAfter, 100)
	if err != nil || len(tail.Events) != 1 || tail.NextAfter != 101 || tail.Events[0].Sequence != 101 || tail.Events[0].EventType != "task.ready" {
		t.Fatal("last readiness event was lost or repeated", err)
	}
	empty, err := f.s1.Events(f.ctx, rootReceipt.ID, tail.NextAfter, 100)
	if err != nil || len(empty.Events) != 0 || empty.NextAfter != 101 || reconcile(t, f, rootReceipt.ID) != 0 || scalar(t, f, `SELECT count(*) FROM workflow_events WHERE workflow_id=$1`, rootReceipt.ID) != 101 {
		t.Fatal("exhausted cursor or repeated reconciliation changed history", err)
	}

	overEdges := in
	overEdges.Tasks = append([]workflow.TaskDefinition(nil), in.Tasks...)
	overEdges.Tasks[45].DependsOn = append(append([]string(nil), in.Tasks[45].DependsOn...), "n10")
	if _, err := f.s1.Create(f.ctx, "too-many-edges", overEdges); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatal("1,001 edges accepted", err)
	}
	tooManyTasks := roots
	tooManyTasks.Tasks = append(append([]workflow.TaskDefinition(nil), roots.Tasks...), workflow.TaskDefinition{Key: "extra", Kind: "noop"})
	if _, err := f.s2.Create(f.ctx, "too-many-tasks", tooManyTasks); !errors.Is(err, workflow.ErrInvalid) || scalar(t, f, `SELECT count(*) FROM workflows`) != 2 {
		t.Fatal("oversized graph wrote workflow data", err)
	}
}

func TestFailedAncestorBlocksDescendantsIndependentBranchContinues(t *testing.T) {
	f := setup(t)
	in := diamond(t)
	in.Tasks = append(in.Tasks, workflow.TaskDefinition{Key: "independent", Kind: "noop"})
	r := newGraph(t, f, "failed-branch", in)
	reconcile(t, f, r.ID)
	seed(t, f, r.ID, "a", workflow.Failed)
	if reconcile(t, f, r.ID) != 3 {
		t.Fatal("descendant blocking")
	}
	graph := snapshot(t, f, r.ID)
	if graph.Status != workflow.Pending || states(graph)["independent"] != workflow.Ready || states(graph)["d"] != workflow.Blocked {
		t.Fatal("independent branch stopped", graph)
	}
	seed(t, f, r.ID, "independent", workflow.Succeeded)
	if reconcile(t, f, r.ID) != 1 || snapshot(t, f, r.ID).Status != workflow.Failed {
		t.Fatal("terminal failure aggregate")
	}
	if reconcile(t, f, r.ID) != 0 {
		t.Fatal("failure repeated")
	}
}
func TestBatchBoundedFairRotationAndSkipLocked(t *testing.T) {
	f := setup(t)
	ids := make([]uuid.UUID, 4)
	for i := range ids {
		ids[i] = newGraph(t, f, fmt.Sprintf("batch-%d", i), input("batch")).ID
	}
	tx, err := f.p1.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(f.ctx, `SELECT id FROM workflows WHERE id=$1 FOR UPDATE`, ids[0]); err != nil {
		t.Fatal(err)
	}
	result, err := f.s2.ReconcileBatch(f.ctx, 1)
	if err != nil || result.Workflows != 1 || result.Transitions != 2 {
		t.Fatal("skip locked blocked/overran batch", result, err)
	}
	if states(snapshot(t, f, ids[0]))["first"] != workflow.Pending {
		t.Fatal("locked workflow mutated")
	}
	if err = tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	for range 4 {
		result, err = f.s2.ReconcileBatch(f.ctx, 1)
		if err != nil || result.Workflows > 1 {
			t.Fatal("unbounded batch", err)
		}
	}
	for _, id := range ids {
		if states(snapshot(t, f, id))["first"] != workflow.Ready {
			t.Fatal("unchanged early graph starved later roots")
		}
	}
	if _, err = f.s1.ReconcileBatch(f.ctx, 101); !errors.Is(err, workflow.ErrInvalid) {
		t.Fatal("unbounded batch accepted")
	}
}
func TestCancellationAndSchedulingSerializedRaces(t *testing.T) {
	for _, first := range []string{"cancel", "schedule"} {
		t.Run(first, func(t *testing.T) {
			f := setup(t)
			r := newGraph(t, f, "race", diamond(t))
			// Hold the first operation at history insertion while its real parent
			// lock is held. Start the other operation and observe its DB lock wait.
			kind := "workflow.cancelled"
			if first == "schedule" {
				kind = "task.ready"
			}
			exec(t, f, `CREATE FUNCTION gate_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.event_type='`+kind+`' THEN PERFORM pg_advisory_xact_lock(9202); END IF; RETURN NEW; END $$`)
			exec(t, f, `CREATE TRIGGER gate_event BEFORE INSERT ON workflow_events FOR EACH ROW EXECUTE FUNCTION gate_event()`)
			gate, err := f.p1.Acquire(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer gate.Release()
			if _, err = gate.Exec(f.ctx, `SELECT pg_advisory_lock(9202)`); err != nil {
				t.Fatal(err)
			}
			defer gate.Exec(context.Background(), `SELECT pg_advisory_unlock(9202)`)
			a, b := make(chan error, 1), make(chan error, 1)
			if first == "cancel" {
				go func() { _, err := f.s1.Cancel(f.ctx, r.ID); a <- err }()
			} else {
				go func() { _, err := f.s1.Reconcile(f.ctx, r.ID); a <- err }()
			}
			waitFor(t, f, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=9202 AND NOT granted`)
			if first == "cancel" {
				go func() { _, err := f.s2.Reconcile(f.ctx, r.ID); b <- err }()
			} else {
				go func() { _, err := f.s2.Cancel(f.ctx, r.ID); b <- err }()
			}
			waitFor(t, f, `SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND cardinality(pg_blocking_pids(pid))>0`, f.cfg2.ConnConfig.RuntimeParams["application_name"])
			if _, err = gate.Exec(f.ctx, `SELECT pg_advisory_unlock(9202)`); err != nil {
				t.Fatal(err)
			}
			for _, done := range []chan error{a, b} {
				select {
				case err := <-done:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("serialized operation stuck")
				}
			}
			graph := snapshot(t, f, r.ID)
			if graph.Status != workflow.Cancelled {
				t.Fatal("cancellation lost")
			}
			for _, node := range graph.Nodes {
				if node.Status != workflow.Cancelled {
					t.Fatal("unfinished node not cancelled")
				}
			}
			events := history(t, f, r.ID)
			if first == "cancel" && len(events) != 2 {
				t.Fatal("READY after committed cancellation")
			}
			if first == "schedule" && (len(events) != 3 || events[1].EventType != "task.ready" || events[2].EventType != "workflow.cancelled") {
				t.Fatal("wrong serialized event order")
			}
		})
	}
	f := setup(t)
	for i := 0; i < 12; i++ {
		r := newGraph(t, f, fmt.Sprintf("ungated-%d", i), diamond(t))
		allOK(t, parallel(2, func(j int) error {
			if j == 0 {
				_, err := f.s1.Cancel(f.ctx, r.ID)
				return err
			}
			_, err := f.s2.Reconcile(f.ctx, r.ID)
			return err
		}))
		if snapshot(t, f, r.ID).Status != workflow.Cancelled {
			t.Fatal("race not cancelled")
		}
		events := history(t, f, r.ID)
		seenCancel := false
		for _, event := range events {
			if event.EventType == "workflow.cancelled" {
				seenCancel = true
			}
			if seenCancel && event.EventType == "task.ready" {
				t.Fatal("ready follows cancellation")
			}
		}
	}
}
func TestStoreRestartReconstructsFromPersistedRows(t *testing.T) {
	f := setup(t)
	r := newGraph(t, f, "persisted-restart", diamond(t))
	reconcile(t, f, r.ID)
	seed(t, f, r.ID, "a", workflow.Succeeded)
	fresh, err := pgxpool.NewWithConfig(f.ctx, f.cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	recovered := postgres.New(fresh)
	result, err := recovered.ReconcileBatch(f.ctx, 1)
	if err != nil || result.Transitions != 2 {
		t.Fatal("restart lost eligibility", result, err)
	}
	if result, err = recovered.ReconcileBatch(f.ctx, 1); err != nil || result.Transitions != 0 {
		t.Fatal("restart repeated eligibility", result, err)
	}
}
func TestMigrationUpgradePreservesV1CreateCancelReplay(t *testing.T) {
	f := setupAt(t, 1)
	in := input("legacy upgrade")
	hash, err := workflow.HashForVersion(in, 1)
	if err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	var original workflow.Receipt
	if err = f.p1.QueryRow(f.ctx, `INSERT INTO workflows(id,name,status,version,creation_key,request_hash,request_schema_version) VALUES($1,$2,'CANCELLED',2,'legacy-upgrade',$3,1) RETURNING id,created_at`, id, in.Name, hash[:]).Scan(&original.ID, &original.CreatedAt); err != nil {
		t.Fatal(err)
	}
	for i, task := range in.Tasks {
		exec(t, f, `INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,$3,$4,'noop','CANCELLED')`, uuid.New(), id, task.Key, i)
	}
	exec(t, f, `INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created','{"task_count":2}'),($1,2,'workflow.cancelled','{"cancelled_task_count":2}')`, id)
	if err = f.s1.Ready(f.ctx); !errors.Is(err, workflow.ErrUnavailable) {
		t.Fatal("old schema accepted by new binary")
	}
	cfg := f.cfg2.ConnConfig.Copy()
	db := stdlib.OpenDB(*cfg)
	defer db.Close()
	if err = migrations.Up(f.ctx, db); err != nil {
		t.Fatal(err)
	}
	if err = f.s1.Ready(f.ctx); err != nil {
		t.Fatal(err)
	}
	in.Tasks[0].DependsOn = []string{}
	replay, err := f.s1.Create(f.ctx, "legacy-upgrade", in)
	if err != nil || replay.ID != original.ID || !replay.CreatedAt.Equal(original.CreatedAt) {
		t.Fatal("v1 receipt/hash changed", err)
	}
	if _, err = f.s1.Cancel(f.ctx, id); err != nil || len(history(t, f, id)) != 2 {
		t.Fatal("legacy cancel no-op changed")
	}
	var stored []byte
	var version int
	if err = f.p1.QueryRow(f.ctx, `SELECT request_hash,request_schema_version FROM workflows WHERE id=$1`, id).Scan(&stored, &version); err != nil || !bytes.Equal(stored, hash[:]) || version != 1 {
		t.Fatal("persisted normalization rewritten")
	}
	if scalar(t, f, `SELECT last_event_sequence FROM workflows WHERE id=$1`, id) != 2 {
		t.Fatal("legacy sequence not backfilled")
	}
	in.Tasks[1].DependsOn = []string{"first"}
	if _, err = f.s1.Create(f.ctx, "legacy-upgrade", in); !errors.Is(err, workflow.ErrConflict) {
		t.Fatal("new dependency ignored for legacy key", err)
	}
	if reconcile(t, f, id) != 0 {
		t.Fatal("legacy cancellation lost")
	}
}

func TestGraphSnapshotConsistentAcrossCancellation(t *testing.T) {
	f := setup(t)
	r := newGraph(t, f, "graph-snapshot", diamond(t))
	exec(t, f, `ALTER TABLE tasks RENAME TO task_data`)
	// Gate the reader connection explicitly. A writer join may evaluate this
	// volatile view projection under a different valid PostgreSQL query plan.
	exec(t, f, `CREATE FUNCTION gated_graph_key(k text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$ BEGIN IF current_setting('application_name') LIKE '%_two' THEN PERFORM pg_advisory_xact_lock(9302); END IF; RETURN k; END $$`)
	exec(t, f, `CREATE VIEW tasks AS SELECT id,workflow_id,gated_graph_key(task_key) AS task_key,position,kind,status,created_at,updated_at,next_attempt_at,last_error_class FROM task_data`)
	gate, err := f.p1.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Release()
	if _, err = gate.Exec(f.ctx, `SELECT pg_advisory_lock(9302)`); err != nil {
		t.Fatal(err)
	}
	defer gate.Exec(context.Background(), `SELECT pg_advisory_unlock(9302)`)
	type answer struct {
		graph workflow.Graph
		err   error
	}
	done := make(chan answer, 1)
	go func() { graph, err := f.s2.Graph(f.ctx, r.ID); done <- answer{graph, err} }()
	waitFor(t, f, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=9302 AND NOT granted`)
	if _, err = f.s1.Cancel(f.ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err = gate.Exec(f.ctx, `SELECT pg_advisory_unlock(9302)`); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil || result.graph.Status != workflow.Pending || result.graph.Version != 1 || len(result.graph.Edges) != 4 {
			t.Fatal("graph parent/edge snapshot changed", result.err)
		}
		for _, task := range result.graph.Nodes {
			if task.Status != workflow.Pending {
				t.Fatal("mixed graph node/parent snapshots")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("graph read did not finish")
	}
	graph := snapshot(t, f, r.ID)
	if graph.Status != workflow.Cancelled {
		t.Fatal("cancellation not persisted")
	}
	for _, task := range graph.Nodes {
		if task.Status != workflow.Cancelled {
			t.Fatal("final graph not cancelled")
		}
	}
}

//go:build integration

package integration

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"forgeswarm/internal/postgres"
	"forgeswarm/internal/workflow"
	"forgeswarm/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
)

type fixture struct {
	ctx    context.Context
	p1, p2 *pgxpool.Pool
	s1, s2 *postgres.Store
	cfg2   *pgxpool.Config
}

func setup(t *testing.T) *fixture { return setupAt(t, migrations.Version) }

func setupAt(t *testing.T, target int64) *fixture {
	t.Helper()
	token := os.Getenv("TEST_RESOURCE_TOKEN")
	if !regexp.MustCompile(`^[0-9a-f]{32}$`).MatchString(token) {
		t.Fatal("integration requires an owned disposable database; run python scripts/dev.py test-integration")
	}
	cfg, err := pgxpool.ParseConfig(os.Getenv("TEST_DATABASE_URL"))
	if err != nil {
		t.Fatal("invalid test database configuration")
	}
	if cfg.ConnConfig.Database != "forgeswarm_stage01_"+token || (cfg.ConnConfig.Host != "127.0.0.1" && cfg.ConnConfig.Host != "db") {
		t.Fatal("refusing integration writes to a non-disposable database")
	}
	cfg.MaxConns = 4
	cfg.MinConns = 0
	cfg.ConnConfig.ConnectTimeout = time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	root, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := root.Ping(ctx); err != nil {
		root.Close()
		t.Fatal("requested PostgreSQL infrastructure unavailable")
	}
	schema := "stage01_" + uuid.NewString()
	schema = strings.ReplaceAll(schema, "-", "")
	ident := pgx.Identifier{schema}.Sanitize()
	if _, err := root.Exec(ctx, "CREATE SCHEMA "+ident); err != nil {
		root.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		if _, err := root.Exec(cleanup, "DROP SCHEMA "+ident+" CASCADE"); err != nil {
			t.Error("owned test schema cleanup failed")
		}
		root.Close()
	})
	cfg.ConnConfig.RuntimeParams["search_path"] = schema
	cfg.ConnConfig.RuntimeParams["application_name"] = schema + "_one"
	p1, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p1.Close)
	cfg2 := cfg.Copy()
	cfg2.ConnConfig.RuntimeParams["application_name"] = schema + "_two"
	p2, err := pgxpool.NewWithConfig(ctx, cfg2)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p2.Close)
	f := &fixture{ctx: ctx, p1: p1, p2: p2, s1: postgres.New(p1), s2: postgres.New(p2), cfg2: cfg2}
	if err := f.s1.Ready(ctx); err == nil {
		t.Fatal("unmigrated schema reported ready")
	}
	db := stdlib.OpenDB(*cfg.ConnConfig)
	db.SetMaxOpenConns(2)
	defer db.Close()
	provider, err := migrations.Provider(db)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = provider.UpTo(ctx, target); err != nil {
		t.Fatal(err)
	}
	if err := f.s1.Ready(ctx); (err == nil) != (target == migrations.Version) {
		t.Fatal("schema-version readiness", err)
	}
	return f
}

func input(name string) workflow.CreateInput {
	return workflow.CreateInput{Name: name, Tasks: []workflow.TaskDefinition{{Key: "first", Kind: "noop"}, {Key: "second", Kind: "noop"}}}
}
func create(t *testing.T, f *fixture, key string) workflow.Receipt {
	t.Helper()
	r, err := f.s1.Create(f.ctx, key, input("First workflow"))
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func scalar(t *testing.T, f *fixture, query string, args ...any) int64 {
	t.Helper()
	var n int64
	if err := f.p1.QueryRow(f.ctx, query, args...).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}
func exec(t *testing.T, f *fixture, query string, args ...any) {
	t.Helper()
	if _, err := f.p1.Exec(f.ctx, query, args...); err != nil {
		t.Fatal(err)
	}
}

func parallel(n int, fn func(int) error) []error {
	start := make(chan struct{})
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) { defer wg.Done(); <-start; errs[i] = fn(i) }(i)
	}
	close(start)
	wg.Wait()
	return errs
}
func allOK(t *testing.T, errs []error) {
	t.Helper()
	for _, err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestTwentyCreatesTwoPoolsAndConflicts(t *testing.T) {
	f := setup(t)
	receipts := make([]workflow.Receipt, 20)
	allOK(t, parallel(20, func(i int) error {
		s := f.s1
		if i%2 == 1 {
			s = f.s2
		}
		r, err := s.Create(f.ctx, "twenty-matches", input("First workflow"))
		receipts[i] = r
		return err
	}))
	for _, r := range receipts {
		if r.ID != receipts[0].ID || !r.CreatedAt.Equal(receipts[0].CreatedAt) {
			t.Fatal("different receipts")
		}
	}
	if scalar(t, f, "SELECT count(*) FROM workflows") != 1 || scalar(t, f, "SELECT count(*) FROM tasks") != 2 || scalar(t, f, "SELECT count(*) FROM workflow_events") != 1 {
		t.Fatal("creation not singular and atomic")
	}
	if _, err := f.s2.Create(f.ctx, "twenty-matches", input("different")); !errors.Is(err, workflow.ErrConflict) {
		t.Fatal("sequential conflict", err)
	}
	allOK(t, parallel(20, func(i int) error {
		s := f.s1
		if i%2 == 1 {
			s = f.s2
		}
		_, err := s.Create(f.ctx, "twenty-matches", input(fmt.Sprintf("different-%d", i)))
		if !errors.Is(err, workflow.ErrConflict) {
			return fmt.Errorf("concurrent conflict: %v", err)
		}
		return nil
	}))
	// Race two differing bodies on an unused key; exactly one winner is required.
	var winners int
	var mu sync.Mutex
	allOK(t, parallel(20, func(i int) error {
		s := f.s1
		if i%2 == 1 {
			s = f.s2
		}
		_, err := s.Create(f.ctx, "fresh-conflict", input(fmt.Sprintf("unique-%d", i)))
		if err == nil {
			mu.Lock()
			winners++
			mu.Unlock()
			return nil
		}
		if !errors.Is(err, workflow.ErrConflict) {
			return err
		}
		return nil
	}))
	if winners != 1 || scalar(t, f, "SELECT count(*) FROM workflows") != 2 {
		t.Fatal("unused-key conflict had multiple winners")
	}
	if _, err := f.s2.Create(f.ctx, "different-key", input("First workflow")); err != nil {
		t.Fatal("different keys can carry identical input", err)
	}
}

func TestEventFailureRollsBackCreationAndCancellation(t *testing.T) {
	f := setup(t)
	exec(t, f, `CREATE FUNCTION reject_event() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'injected event failure'; END $$`)
	exec(t, f, `CREATE TRIGGER reject_event BEFORE INSERT ON workflow_events FOR EACH ROW EXECUTE FUNCTION reject_event()`)
	if _, err := f.s1.Create(f.ctx, "rollback", input("First workflow")); err == nil {
		t.Fatal("failed event returned success")
	}
	for _, table := range []string{"workflows", "tasks", "workflow_events"} {
		if scalar(t, f, "SELECT count(*) FROM "+table) != 0 {
			t.Fatal("partial creation", table)
		}
	}
	exec(t, f, `DROP TRIGGER reject_event ON workflow_events`)
	r := create(t, f, "rollback")
	before, err := f.s1.Details(f.ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	exec(t, f, `CREATE TRIGGER reject_event BEFORE INSERT ON workflow_events FOR EACH ROW EXECUTE FUNCTION reject_event()`)
	if _, err := f.s1.Cancel(f.ctx, r.ID); err == nil {
		t.Fatal("failed cancellation event returned success")
	}
	after, err := f.s2.Details(f.ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) || scalar(t, f, "SELECT count(*) FROM workflow_events") != 1 {
		t.Fatal("partial cancellation")
	}
}

func TestDiscardedReceiptPoolRestartAndReplayAfterCancellation(t *testing.T) {
	f := setup(t)
	// Discard the successful response entirely, as a disconnected client would.
	if _, err := f.s1.Create(f.ctx, "lost-response", input("First workflow")); err != nil {
		t.Fatal(err)
	}
	var original workflow.Receipt
	if err := f.p2.QueryRow(f.ctx, `SELECT id,created_at FROM workflows WHERE creation_key='lost-response'`).Scan(&original.ID, &original.CreatedAt); err != nil {
		t.Fatal(err)
	}
	f.p1.Close()
	fresh, err := pgxpool.NewWithConfig(f.ctx, f.cfg2)
	if err != nil {
		t.Fatal(err)
	}
	defer fresh.Close()
	s := postgres.New(fresh)
	replayed, err := s.Create(f.ctx, "lost-response", input(" First workflow "))
	if err != nil || replayed.ID != original.ID || !replayed.CreatedAt.Equal(original.CreatedAt) {
		t.Fatal("restart replay", err)
	}
	if _, err := s.Cancel(f.ctx, original.ID); err != nil {
		t.Fatal(err)
	}
	again, err := s.Create(f.ctx, "lost-response", input("First workflow"))
	if err != nil || again.ID != original.ID || !again.CreatedAt.Equal(original.CreatedAt) {
		t.Fatal("replay after cancellation", err)
	}
	var version int
	if err := fresh.QueryRow(f.ctx, "SELECT request_schema_version FROM workflows WHERE id=$1", original.ID).Scan(&version); err != nil || version != workflow.RequestSchemaVersion {
		t.Fatal("normalization version not persisted", err)
	}
}

func TestConcurrentCancellationNoopAndPagination(t *testing.T) {
	f := setup(t)
	r := create(t, f, "cancel-many")
	allOK(t, parallel(20, func(i int) error {
		s := f.s1
		if i%2 == 1 {
			s = f.s2
		}
		result, err := s.Cancel(f.ctx, r.ID)
		if err != nil {
			return err
		}
		if result.Status != workflow.Cancelled || result.Version != 2 {
			return fmt.Errorf("bad cancellation %+v", result)
		}
		return nil
	}))
	before, err := f.s1.Details(f.ctx, r.ID)
	if err != nil {
		t.Fatal(err)
	}
	if before.Status != workflow.Cancelled || before.Version != 2 {
		t.Fatal(before)
	}
	for i, task := range before.Tasks {
		if task.Status != workflow.Cancelled || task.Position != i || !task.UpdatedAt.Equal(before.UpdatedAt) {
			t.Fatal("task cancellation/order mismatch")
		}
	}
	events, err := f.s1.Events(f.ctx, r.ID, 0, 100)
	if err != nil || len(events.Events) != 2 || events.NextAfter != 2 {
		t.Fatal("events", events, err)
	}
	if string(events.Events[0].Payload) != `{"task_count": 2}` || string(events.Events[1].Payload) != `{"cancelled_task_count": 2}` {
		t.Fatal("event counts", events)
	}
	if _, err := f.s2.Cancel(f.ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	after, err := f.s2.Details(f.ctx, r.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("no-op changed metadata", err)
	}
	later, err := f.s2.Events(f.ctx, r.ID, 0, 100)
	if err != nil || !reflect.DeepEqual(events, later) {
		t.Fatal("no-op changed history", err)
	}
	first, err := f.s1.Events(f.ctx, r.ID, 0, 1)
	if err != nil || len(first.Events) != 1 || first.NextAfter != 1 {
		t.Fatal("first page", err)
	}
	second, err := f.s1.Events(f.ctx, r.ID, first.NextAfter, 1)
	if err != nil || len(second.Events) != 1 || second.NextAfter != 2 {
		t.Fatal("second page", err)
	}
	empty, err := f.s1.Events(f.ctx, r.ID, 42, 50)
	if err != nil || len(empty.Events) != 0 || empty.NextAfter != 42 {
		t.Fatal("empty page", err)
	}
	if _, err := f.s1.Events(f.ctx, uuid.New(), 0, 1); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatal("unknown events", err)
	}
	if _, err := f.s1.Details(f.ctx, uuid.New()); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatal("unknown details", err)
	}
	if _, err := f.s1.Cancel(f.ctx, uuid.New()); !errors.Is(err, workflow.ErrNotFound) {
		t.Fatal("unknown cancel", err)
	}
}

// Stage 02 supersedes post-finalization definition edits with immutable-definition
// rejection (23514). Unique/FK insertion constraints are independently exercised
// before finalization in TestGraphCompositeConstraintsAndImmutability.
func TestSQLConstraints(t *testing.T) {
	f := setup(t)
	r := create(t, f, "constraints")
	for _, test := range []struct {
		sql, code string
		args      []any
	}{
		{`UPDATE workflows SET name=NULL WHERE id=$1`, "23514", []any{r.ID}},
		{`UPDATE workflows SET name='' WHERE id=$1`, "23514", []any{r.ID}},
		{`UPDATE workflows SET version=0 WHERE id=$1`, "23514", []any{r.ID}},
		// Stage 03 admits RUNNING only through trusted claims; unsupported
		// status values remain rejected by the persisted state constraint.
		{`UPDATE workflows SET status='INVALID' WHERE id=$1`, "23514", []any{r.ID}},
		{`UPDATE workflows SET request_hash='\x00' WHERE id=$1`, "23514", []any{r.ID}},
		{`UPDATE workflows SET request_schema_version=5 WHERE id=$1`, "23514", []any{r.ID}},
		{`UPDATE workflows SET creation_key='invalid space' WHERE id=$1`, "23514", []any{r.ID}},
		{`INSERT INTO workflows(id,name,status,version,creation_key,request_hash,request_schema_version) SELECT $1,name,status,version,creation_key,request_hash,request_schema_version FROM workflows WHERE id=$2`, "23505", []any{uuid.New(), r.ID}},
		{`DELETE FROM workflows WHERE id=$1`, "23503", []any{r.ID}},
		{`INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,'first',2,'noop','PENDING')`, "23514", []any{uuid.New(), r.ID}},
		{`INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,'third',0,'noop','PENDING')`, "23514", []any{uuid.New(), r.ID}},
		{`INSERT INTO tasks(id,workflow_id,task_key,position,kind,status) VALUES($1,$2,'third',2,'noop','PENDING')`, "23503", []any{uuid.New(), uuid.New()}},
		{`UPDATE tasks SET position=-1 WHERE workflow_id=$1`, "23514", []any{r.ID}},
		{`UPDATE tasks SET kind='shell' WHERE workflow_id=$1`, "23514", []any{r.ID}},
		{`UPDATE tasks SET status='DONE' WHERE workflow_id=$1`, "23514", []any{r.ID}},
		{`UPDATE tasks SET task_key='Invalid' WHERE workflow_id=$1`, "23514", []any{r.ID}},
		{`UPDATE workflow_events SET sequence=0 WHERE workflow_id=$1`, "23514", []any{r.ID}},
		{`UPDATE workflow_events SET payload='[]' WHERE workflow_id=$1`, "23514", []any{r.ID}},
		{`UPDATE workflow_events SET payload=jsonb_build_object('x',$2::text) WHERE workflow_id=$1`, "23514", []any{r.ID, strings.Repeat("x", 4096)}},
		{`UPDATE workflow_events SET event_type='unknown' WHERE workflow_id=$1`, "23514", []any{r.ID}},
		{`INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created','{}')`, "23505", []any{r.ID}},
		{`INSERT INTO workflow_events(workflow_id,sequence,event_type,payload) VALUES($1,1,'workflow.created','{}')`, "23503", []any{uuid.New()}},
	} {
		_, err := f.p1.Exec(f.ctx, test.sql, test.args...)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != test.code {
			t.Fatalf("constraint %s: %v", test.sql, err)
		}
	}
}

func waitFor(t *testing.T, f *fixture, query string, args ...any) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if scalar(t, f, query, args...) > 0 {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("database interleaving barrier not reached")
}

func TestBlockedQueryCancellationReleasesPool(t *testing.T) {
	f := setup(t)
	r := create(t, f, "blocked")
	tx, err := f.p1.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(f.ctx, `SELECT id FROM workflows WHERE id=$1 FOR UPDATE`, r.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(f.ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := f.s2.Cancel(ctx, r.ID); done <- err }()
	waitFor(t, f, `SELECT count(*) FROM pg_stat_activity WHERE application_name=$1 AND cardinality(pg_blocking_pids(pid))>0`, f.cfg2.ConnConfig.RuntimeParams["application_name"])
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, workflow.ErrUnavailable) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("blocked query ignored context")
	}
	deadline := time.Now().Add(3 * time.Second)
	for f.p2.Stat().AcquiredConns() != 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if f.p2.Stat().AcquiredConns() != 0 {
		t.Fatal("connection leaked after cancellation")
	}
	if err := tx.Rollback(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := f.s2.Cancel(f.ctx, r.ID); err != nil {
		t.Fatal("pool not reusable", err)
	}
}

func TestDetailsConsistentAcrossCancellation(t *testing.T) {
	f := setup(t)
	r := create(t, f, "snapshot")
	// Turn the task projection into a deterministic PostgreSQL barrier. The API
	// code is unchanged: its first SELECT finishes before its task SELECT blocks.
	exec(t, f, `ALTER TABLE tasks RENAME TO task_data`)
	exec(t, f, `CREATE FUNCTION gated_key(k text) RETURNS text LANGUAGE plpgsql VOLATILE AS $$ BEGIN PERFORM pg_advisory_xact_lock(9101); RETURN k; END $$`)
	exec(t, f, `CREATE VIEW tasks AS SELECT id,workflow_id,gated_key(task_key) AS task_key,position,kind,status,created_at,updated_at,next_attempt_at,last_error_class FROM task_data`)
	gate, err := f.p1.Acquire(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer gate.Release()
	if _, err := gate.Exec(f.ctx, `SELECT pg_advisory_lock(9101)`); err != nil {
		t.Fatal(err)
	}
	defer gate.Exec(context.Background(), `SELECT pg_advisory_unlock(9101)`)
	type answer struct {
		details workflow.Details
		err     error
	}
	done := make(chan answer, 1)
	go func() { d, err := f.s2.Details(f.ctx, r.ID); done <- answer{d, err} }()
	waitFor(t, f, `SELECT count(*) FROM pg_locks WHERE locktype='advisory' AND objid=9101 AND NOT granted`)
	if _, err := f.s1.Cancel(f.ctx, r.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.Exec(f.ctx, `SELECT pg_advisory_unlock(9101)`); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		if result.details.Status != workflow.Pending || result.details.Version != 1 {
			t.Fatal("parent snapshot changed")
		}
		for _, task := range result.details.Tasks {
			if task.Status != workflow.Pending {
				t.Fatal("mixed parent/task snapshots")
			}
		}
	case <-time.After(5 * time.Second):
		t.Fatal("details did not finish")
	}
	final, err := f.s1.Details(f.ctx, r.ID)
	if err != nil || final.Status != workflow.Cancelled {
		t.Fatal("cancellation not committed", err)
	}
}

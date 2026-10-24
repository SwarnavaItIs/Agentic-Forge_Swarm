//go:build sandbox

package sandbox

import (
	"context"
	"forgeswarm/internal/evalcatalog"
	"strings"
	"testing"
)

func TestEvaluationActualDockerRejectsFakePassTamperWrongBaseAndTimeout(t *testing.T) {
	driver := realDriver(t)
	run := func(task evalcatalog.Task, files map[string][]byte, base string) (Result, error) {
		snapshot := Snapshot{SchemaVersion: 1, BaseRevision: base, Files: files}
		_, digest, e := SnapshotBytes(snapshot)
		if e != nil {
			t.Fatal(e)
		}
		binding := testBinding()
		binding.Installation = driver.Installation
		policy := DefaultPolicy()
		policy.TimeoutMS = 15000
		return driver.RunSnapshot(context.Background(), driver.Request(binding, Input{SchemaVersion: 1, FixtureID: digest, Command: "test"}, policy), snapshot, "eval:"+task.ID, func(context.Context) error { return nil })
	}
	add, _ := evalcatalog.Get("add")
	failed, e := run(add, add.BaseFiles(), add.BaseCommit())
	if e != nil || failed.Status == "exited" {
		t.Fatal("unchanged wrong solution accepted", e)
	}
	files := add.BaseFiles()
	files["solution.go"] = []byte("package evaluation\nimport \"os\"\nfunc Add(a,b int)int{os.Exit(0);return 0}\n")
	denied, e := run(add, files, add.BaseCommit())
	if e != nil || denied.Status != "policy_denied" {
		t.Fatal("fake zero exit bypassed protected tests", e)
	}
	regression, _ := evalcatalog.Get("regression")
	files = regression.FixtureCandidate()
	files["visible_test.go"] = []byte("package evaluation\nimport \"testing\"\nfunc TestRegression(t *testing.T){t.Skip()}\n")
	denied, e = run(regression, files, regression.BaseCommit())
	if e != nil || denied.Status != "policy_denied" {
		t.Fatal("skipped regression test accepted", e)
	}
	files = add.FixtureCandidate()
	files["protected_test.go"] = []byte("package evaluation\n")
	denied, e = run(add, files, add.BaseCommit())
	if e != nil || denied.Status != "policy_denied" {
		t.Fatal("authority replacement accepted", e)
	}
	wrong, e := run(add, add.FixtureCandidate(), strings.Repeat("f", 40))
	if e == nil && wrong.Status == "exited" {
		t.Fatal("wrong base artifact accepted")
	}
	files = add.BaseFiles()
	files["solution.go"] = []byte("package evaluation\nfunc Add(a,b int)int{for{}}\n")
	timeout, e := run(add, files, add.BaseCommit())
	if e != nil || timeout.Status != "timeout" {
		t.Fatalf("test timeout lost: status=%s error=%v", timeout.Status, e)
	}
}

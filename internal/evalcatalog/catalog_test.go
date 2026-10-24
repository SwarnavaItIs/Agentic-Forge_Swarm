package evalcatalog

import (
	"bytes"
	"go/parser"
	"go/token"
	"testing"
)

func TestFrozenCoverageAndCandidatePolicy(t *testing.T) {
	if len(All()) != 8 {
		t.Fatal("eight frozen tasks required")
	}
	categories, splits := map[string]bool{}, map[string]int{}
	for _, task := range All() {
		if _, err := parser.ParseFile(token.NewFileSet(), "protected_test.go", task.ProtectedSource(), parser.AllErrors); err != nil {
			t.Fatalf("%s protected source does not compile: %v", task.ID, err)
		}
		categories[task.Category] = true
		splits[task.Split]++
		if len(task.BaseCommit()) != 40 || len(task.Public().ProtectedDigest) != 64 {
			t.Fatal("missing immutable pin")
		}
		if violations := task.Validate(task.FixtureCandidate()); len(violations) != 0 {
			t.Fatalf("%s fixture policy: %v", task.ID, violations)
		}
		candidate := task.FixtureCandidate()
		candidate["go.mod"] = []byte("module attacker\n")
		if len(task.Validate(candidate)) == 0 {
			t.Fatal("dependency/harness edit accepted")
		}
		candidate = task.FixtureCandidate()
		candidate[".github/workflows/ci.yml"] = []byte("ignored")
		if len(task.Validate(candidate)) == 0 {
			t.Fatal("CI edit accepted")
		}
	}
	if len(categories) < 6 || splits["development"] != 4 || splits["holdout"] != 4 {
		t.Fatal("coverage/split changed without evaluation version")
	}
}
func TestSkipOverrideAndTestWeakeningRejected(t *testing.T) {
	task, _ := Get("regression")
	for _, body := range []string{
		"package evaluation\nimport \"testing\"\nfunc TestRegression(t *testing.T){t.Skip()}\n",
		"package evaluation\nimport \"testing\"\nfunc TestRegression(t *testing.T){}\n",
		"package evaluation\nimport \"testing\"\nfunc TestMain(m *testing.M){}\n",
	} {
		candidate := task.FixtureCandidate()
		candidate["visible_test.go"] = []byte(body)
		if len(task.Validate(candidate)) == 0 {
			t.Fatal("weakened test accepted")
		}
	}
	task, _ = Get("add")
	candidate := task.FixtureCandidate()
	candidate["solution.go"] = bytes.Join([][]byte{[]byte("package evaluation"), []byte("import \"os\""), []byte("func Add(a,b int)int{os.Exit(0);return 0}")}, []byte("\n"))
	if len(task.Validate(candidate)) == 0 {
		t.Fatal("fake zero exit accepted")
	}
}

func TestBodylessCounterMethodsCannotPanicValidation(t *testing.T) {
	task, _ := Get("counter")
	candidate := task.FixtureCandidate()
	candidate["solution.go"] = []byte("package evaluation\nimport \"sync\"\ntype Counter struct{mu sync.Mutex;n int}\nfunc(c *Counter)Add()\nfunc(c *Counter)Value()int\n")
	if violations := task.Validate(candidate); len(violations) == 0 {
		t.Fatal("bodyless methods accepted")
	}
}

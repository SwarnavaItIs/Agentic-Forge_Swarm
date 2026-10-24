// Package evalcatalog is the frozen, dependency-free evaluation authority.
// Only Public() and BaseFiles() enter model context. Protected tests and fixture
// solutions stay in the trusted helper/provider, outside writable workspaces.
package evalcatalog

import (
	"bytes"
	"crypto/sha1" // Git object identity only; integrity bindings use SHA-256.
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"slices"
	"strconv"
	"strings"
)

const Version = "go-small-v1"
const Module = "module evaluation\n\ngo 1.26.3\n"

type Task struct {
	ID, Category, Split, Instruction               string
	source, solution, visible, fixedVisible, tests string
	imports                                        []string
}
type Specification struct {
	Version         string   `json:"version"`
	ID              string   `json:"id"`
	Category        string   `json:"category"`
	Split           string   `json:"split"`
	Instruction     string   `json:"instruction"`
	BaseCommit      string   `json:"base_commit"`
	AllowedEdits    []string `json:"allowed_edits"`
	RequiredTests   []string `json:"required_tests"`
	ProtectedDigest string   `json:"protected_test_sha256"`
}

func Get(id string) (Task, bool) {
	for _, t := range catalog {
		if t.ID == id {
			return t, true
		}
	}
	return Task{}, false
}
func All() []Task { return append([]Task(nil), catalog...) }
func (t Task) BaseFiles() map[string][]byte {
	files := map[string][]byte{"go.mod": []byte(Module), "solution.go": []byte(t.source)}
	if t.visible != "" {
		files["visible_test.go"] = []byte(t.visible)
	}
	return files
}
func (t Task) Allowed() []string {
	if t.visible != "" {
		return []string{"visible_test.go"}
	}
	return []string{"solution.go"}
}
func (t Task) ExpectedTests() []string {
	names := []string{"TestProtected", "TestEdge"}
	if t.visible != "" {
		names = append(names, "TestRegression")
	}
	return names
}
func Digest(value []byte) string { v := sha256.Sum256(value); return hex.EncodeToString(v[:]) }
func (t Task) Public() Specification {
	return Specification{Version, t.ID, t.Category, t.Split, t.Instruction, t.BaseCommit(), t.Allowed(), t.ExpectedTests(), Digest([]byte(t.tests))}
}
func gitObject(kind string, body []byte) []byte {
	h := sha1.New()
	fmt.Fprintf(h, "%s %d%c", kind, len(body), byte(0))
	h.Write(body)
	return h.Sum(nil)
}
func (t Task) BaseCommit() string {
	names := []string{}
	files := t.BaseFiles()
	for n := range files {
		names = append(names, n)
	}
	slices.Sort(names)
	var tree bytes.Buffer
	for _, n := range names {
		fmt.Fprintf(&tree, "100644 %s%c", n, byte(0))
		tree.Write(gitObject("blob", files[n]))
	}
	body := fmt.Sprintf("tree %x\nauthor ForgeSwarm Evaluation <eval@localhost> 1700000000 +0000\ncommitter ForgeSwarm Evaluation <eval@localhost> 1700000000 +0000\n\n%s/%s\n", gitObject("tree", tree.Bytes()), Version, t.ID)
	return hex.EncodeToString(gitObject("commit", []byte(body)))
}

// ProtectedSource is used only by the immutable, read-only sandbox helper.
func (t Task) ProtectedSource() string { return t.tests }

// FixtureCandidate is an explicitly scripted oracle. Never used by live mode.
func (t Task) FixtureCandidate() map[string][]byte {
	v := t.BaseFiles()
	if t.visible != "" {
		v["visible_test.go"] = []byte(t.fixedVisible)
	} else {
		v["solution.go"] = []byte(t.solution)
	}
	return v
}
func CatalogDigest() string {
	v := []Specification{}
	for _, t := range catalog {
		v = append(v, t.Public())
	}
	b, _ := json.Marshal(v)
	return Digest(b)
}

// Policy violations are fixed codes; source text never becomes telemetry.
func (t Task) Validate(files map[string][]byte) []string {
	failures := []string{}
	base := t.BaseFiles()
	if len(files) != len(base) {
		failures = append(failures, "unexpected_paths")
	}
	for n, b := range files {
		if _, ok := base[n]; !ok {
			failures = append(failures, "unexpected_paths")
			continue
		}
		if len(b) > 8192 {
			failures = append(failures, "source_ceiling")
		}
		if !slices.Contains(t.Allowed(), n) && !bytes.Equal(b, base[n]) {
			failures = append(failures, "immutable_file_changed")
		}
		if !strings.HasSuffix(n, ".go") {
			continue
		}
		tree, err := parser.ParseFile(token.NewFileSet(), n, b, parser.ParseComments)
		if err != nil {
			continue
		} // compiler failure is measured as a task failure
		for _, imp := range tree.Imports {
			name, _ := strconv.Unquote(imp.Path.Value)
			if n == "solution.go" && !slices.Contains(t.imports, name) || n == "visible_test.go" && name != "testing" || imp.Name != nil {
				failures = append(failures, "unexpected_import")
			}
		}
		for _, group := range tree.Comments {
			for _, c := range group.List {
				if strings.Contains(c.Text, "//go:") || strings.Contains(c.Text, "#cgo") {
					failures = append(failures, "compiler_directive")
				}
			}
		}
		ast.Inspect(tree, func(node ast.Node) bool {
			if fn, ok := node.(*ast.FuncDecl); ok && (fn.Name.Name == "init" || fn.Name.Name == "TestMain" || n == "solution.go" && strings.HasPrefix(fn.Name.Name, "Test")) {
				failures = append(failures, "harness_override")
			}
			if sel, ok := node.(*ast.SelectorExpr); ok && slices.Contains([]string{"Skip", "Skipf", "SkipNow", "FileServer", "Dir", "ServeFile", "ServeContent", "NewFileTransport", "DefaultTransport", "DefaultClient", "DefaultServeMux"}, sel.Sel.Name) {
				failures = append(failures, "harness_or_filesystem_bypass")
			}
			return true
		})
	}
	for n := range base {
		if _, ok := files[n]; !ok {
			failures = append(failures, "missing_file")
		}
	}
	if t.visible != "" {
		// The regression repair may change only the stale expected integer. The
		// assertion and test declaration cannot be removed, skipped or weakened.
		normalize := func(body []byte) string {
			source := string(body)
			start := strings.Index(source, "got != ")
			if start < 0 {
				return source
			}
			start += len("got != ")
			end := start
			for end < len(source) && (source[end] == '-' || source[end] >= '0' && source[end] <= '9') {
				end++
			}
			return source[:start] + "EXPECTED" + source[end:]
		}
		if normalize(files["visible_test.go"]) != normalize([]byte(t.visible)) {
			failures = append(failures, "test_tampering")
		}
	}
	if t.ID == "counter" {
		tree, err := parser.ParseFile(token.NewFileSet(), "solution.go", files["solution.go"], 0)
		if err == nil {
			for _, decl := range tree.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Recv == nil {
					continue
				}
				if fn.Body == nil || len(fn.Body.List) < 2 {
					failures = append(failures, "mutex_protocol")
					continue
				}
				first, ok := fn.Body.List[0].(*ast.ExprStmt)
				if !ok {
					failures = append(failures, "mutex_protocol")
					continue
				}
				lock, ok := first.X.(*ast.CallExpr)
				if !ok {
					failures = append(failures, "mutex_protocol")
					continue
				}
				sel, ok := lock.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Lock" {
					failures = append(failures, "mutex_protocol")
				}
				unlock, ok := fn.Body.List[1].(*ast.DeferStmt)
				if !ok {
					failures = append(failures, "mutex_protocol")
					continue
				}
				release, ok := unlock.Call.Fun.(*ast.SelectorExpr)
				if !ok || release.Sel.Name != "Unlock" {
					failures = append(failures, "mutex_protocol")
				}
			}
		}
	}
	slices.Sort(failures)
	return slices.Compact(failures)
}

var catalog = []Task{
	{ID: "add", Category: "bug_fix", Split: "development", Instruction: "Fix Add to return a+b for positive, zero and negative integers. Edit solution.go only.",
		source: "package evaluation\nfunc Add(a,b int)int{return a-b}\n", solution: "package evaluation\nfunc Add(a,b int)int{return a+b}\n",
		tests: `package evaluation
import "testing"
func TestProtected(t *testing.T){if Add(17,9)!=26{t.Fatal("arithmetic")}}
func TestEdge(t *testing.T){if Add(-5,2)!=-3 || Add(0,0)!=0{t.Fatal("edge")}}
`},
	{ID: "health", Category: "endpoint_implementation", Split: "development", Instruction: "Implement Health: respond HTTP 204 with an empty body. Use only net/http. Edit solution.go only.",
		source:   "package evaluation\nimport \"net/http\"\nfunc Health(w http.ResponseWriter,r *http.Request){http.Error(w,\"unimplemented\",501)}\n",
		solution: "package evaluation\nimport \"net/http\"\nfunc Health(w http.ResponseWriter,r *http.Request){w.WriteHeader(http.StatusNoContent)}\n", imports: []string{"net/http"},
		tests: `package evaluation
import("testing";"net/http/httptest")
func TestProtected(t *testing.T){r:=httptest.NewRecorder();Health(r,httptest.NewRequest("GET","/health",nil));if r.Code!=204{t.Fatal("status")}}
func TestEdge(t *testing.T){r:=httptest.NewRecorder();Health(r,httptest.NewRequest("GET","/health",nil));if r.Body.Len()!=0{t.Fatal("body")}}
`},
	{ID: "port", Category: "validation", Split: "development", Instruction: "ParsePort accepts decimal ports 1..65535; invalid text or out-of-range values must return an error. Edit solution.go; permitted imports strconv and fmt.",
		source:   "package evaluation\nimport \"strconv\"\nfunc ParsePort(s string)(int,error){return strconv.Atoi(s)}\n",
		solution: "package evaluation\nimport(\"strconv\";\"fmt\")\nfunc ParsePort(s string)(int,error){n,e:=strconv.Atoi(s);if e!=nil{return 0,e};if n<1 || n>65535{return 0,fmt.Errorf(\"port out of range\")};return n,nil}\n", imports: []string{"strconv", "fmt"},
		tests: `package evaluation
import "testing"
func TestProtected(t *testing.T){for _,s:=range []string{"1","8080","65535"}{if _,e:=ParsePort(s);e!=nil{t.Fatal("valid")}}}
func TestEdge(t *testing.T){for _,s:=range []string{"0","-1","65536","abc",""}{if _,e:=ParsePort(s);e==nil{t.Fatal("invalid")}}}
`},
	{ID: "wrap", Category: "error_handling", Split: "development", Instruction: "Wrap must preserve errors.Is identity while adding context 'load: '. For nil return nil. Edit solution.go; permitted import fmt.",
		source:   "package evaluation\nimport \"fmt\"\nfunc Wrap(e error)error{if e==nil{return nil};return fmt.Errorf(\"load: %v\",e)}\n",
		solution: "package evaluation\nimport \"fmt\"\nfunc Wrap(e error)error{if e==nil{return nil};return fmt.Errorf(\"load: %w\",e)}\n", imports: []string{"fmt"},
		tests: `package evaluation
import("testing";"errors";"strings")
func TestProtected(t *testing.T){original:=errors.New("fixture");if !errors.Is(Wrap(original),original){t.Fatal("identity")}}
func TestEdge(t *testing.T){if Wrap(nil)!=nil || !strings.HasPrefix(Wrap(errors.New("x")).Error(),"load: "){t.Fatal("context")}}
`},
	{ID: "counter", Category: "concurrency", Split: "holdout", Instruction: "Make Counter safe for concurrent Add/Value. Each method must lock c.mu as its first statement and defer c.mu.Unlock as its second statement. Use sync.Mutex only. Edit solution.go.",
		source:   "package evaluation\nimport \"sync\"\ntype Counter struct{mu sync.Mutex;n int}\nfunc(c *Counter)Add(){c.n++}\nfunc(c *Counter)Value()int{return c.n}\n",
		solution: "package evaluation\nimport \"sync\"\ntype Counter struct{mu sync.Mutex;n int}\nfunc(c *Counter)Add(){c.mu.Lock();defer c.mu.Unlock();c.n++}\nfunc(c *Counter)Value()int{c.mu.Lock();defer c.mu.Unlock();return c.n}\n", imports: []string{"sync"},
		tests: `package evaluation
import("testing";"sync")
func TestProtected(t *testing.T){c:=&Counter{};var w sync.WaitGroup;for i:=0;i<16;i++{w.Add(1);go func(){defer w.Done();for j:=0;j<1000;j++{c.Add()}}()};w.Wait();if c.Value()!=16000{t.Fatal("lost updates")}}
func TestEdge(t *testing.T){c:=&Counter{};if c.Value()!=0{t.Fatal("initial")};c.Add();if c.Value()!=1{t.Fatal("single")}}
`},
	{ID: "regression", Category: "regression_test_repair", Split: "holdout", Instruction: "Sum is correct. Repair only the stale integer expected by TestRegression in visible_test.go. Preserve the test and assertion exactly; edit only its expected integer.",
		source:       "package evaluation\nfunc Sum(a,b int)int{return a+b}\n",
		visible:      "package evaluation\nimport \"testing\"\nfunc TestRegression(t *testing.T){got:=Sum(2,5);if got != 8 {t.Fatalf(\"got %d\",got)}}\n",
		fixedVisible: "package evaluation\nimport \"testing\"\nfunc TestRegression(t *testing.T){got:=Sum(2,5);if got != 7 {t.Fatalf(\"got %d\",got)}}\n",
		tests: `package evaluation
import "testing"
func TestProtected(t *testing.T){if Sum(5,8)!=13{t.Fatal("sum")}}
func TestEdge(t *testing.T){if Sum(-8,3)!=-5{t.Fatal("negative")}}
`},
	{ID: "runes", Category: "bug_fix", Split: "holdout", Instruction: "Count returns the number of Unicode code points (UTF-8 runes), not bytes. Empty input returns 0. Edit solution.go; unicode/utf8 is permitted.",
		source:   "package evaluation\nfunc Count(s string)int{return len(s)}\n",
		solution: "package evaluation\nimport \"unicode/utf8\"\nfunc Count(s string)int{return utf8.RuneCountInString(s)}\n", imports: []string{"unicode/utf8"},
		tests: `package evaluation
import "testing"
func TestProtected(t *testing.T){if Count("a\u2603\u754c")!=3{t.Fatal("unicode")}}
func TestEdge(t *testing.T){if Count("")!=0 || Count("ASCII")!=5{t.Fatal("ascii")}}
`},
	{ID: "method", Category: "endpoint_validation", Split: "holdout", Instruction: "OnlyGET responds 204 with an empty body to GET and responds 405 to all other methods. Use net/http only; edit solution.go.",
		source:   "package evaluation\nimport \"net/http\"\nfunc OnlyGET(w http.ResponseWriter,r *http.Request){w.WriteHeader(http.StatusNoContent)}\n",
		solution: "package evaluation\nimport \"net/http\"\nfunc OnlyGET(w http.ResponseWriter,r *http.Request){if r.Method!=http.MethodGet{w.WriteHeader(http.StatusMethodNotAllowed);return};w.WriteHeader(http.StatusNoContent)}\n", imports: []string{"net/http"},
		tests: `package evaluation
import("testing";"net/http/httptest")
func TestProtected(t *testing.T){r:=httptest.NewRecorder();OnlyGET(r,httptest.NewRequest("GET","/",nil));if r.Code!=204 || r.Body.Len()!=0{t.Fatal("get")}}
func TestEdge(t *testing.T){for _,m:=range []string{"POST","DELETE","HEAD"}{r:=httptest.NewRecorder();OnlyGET(r,httptest.NewRequest(m,"/",nil));if r.Code!=405{t.Fatal("method")}}}
`},
}

package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"reflect"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

// StrictDecode also rejects case aliases, missing/null required fields and
// duplicate keys, which encoding/json alone would otherwise accept.
func StrictDecode(body []byte, out any, limit int) error {
	if sandbox.Decode(body, out, limit) != nil {
		return sandbox.ErrPolicy
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return sandbox.ErrPolicy
	}
	typ := reflect.TypeOf(out).Elem()
	allowed := map[string]bool{}
	for i := 0; i < typ.NumField(); i++ {
		f := typ.Field(i)
		tag := strings.Split(f.Tag.Get("json"), ",")[0]
		allowed[tag] = true
		if !strings.Contains(f.Tag.Get("json"), "omitempty") {
			v, ok := fields[tag]
			if !ok || bytes.Equal(v, []byte("null")) {
				return sandbox.ErrPolicy
			}
		}
	}
	for k := range fields {
		if !allowed[k] {
			return sandbox.ErrPolicy
		}
	}
	return nil
}
func ValidateAction(a Action) error {
	id, e := uuid.Parse(a.ID)
	if e != nil || id == uuid.Nil || a.SchemaVersion != 1 {
		return sandbox.ErrPolicy
	}
	switch a.Tool {
	case "list_tree", "status", "diff", "final":
		var v Empty
		return StrictDecode(a.Args, &v, 2)
	case "read_file":
		var v ReadArgs
		if StrictDecode(a.Args, &v, 512) != nil || !Visible(v.Path) || v.Start < 1 || v.Lines < 1 || v.Lines > 80 {
			return sandbox.ErrPolicy
		}
	case "search_text":
		var v SearchArgs
		if StrictDecode(a.Args, &v, 512) != nil || len(v.Text) < 1 || len(v.Text) > 128 || !utf8.ValidString(v.Text) {
			return sandbox.ErrPolicy
		}
	case "apply_patch":
		var v PatchArgs
		if StrictDecode(a.Args, &v, 8192) != nil || v.Path != "sum.go" || !ValidDigest(v.Workspace) || len(v.Before) < 1 || len(v.Before) > 2048 || len(v.After) > 2048 || !utf8.ValidString(v.After) || strings.ContainsRune(v.After, 0) {
			return sandbox.ErrPolicy
		}
	case "run_test_profile":
		var v TestArgs
		if StrictDecode(a.Args, &v, 128) != nil || v.Profile != Profile {
			return sandbox.ErrPolicy
		}
	default:
		return sandbox.ErrPolicy
	}
	return nil
}
func Visible(name string) bool {
	if !sandbox.SafePath(name) {
		return false
	}
	lower := strings.ToLower(name)
	for _, part := range strings.Split(lower, "/") {
		if slices.Contains([]string{"vendor", "generated", "build", "dist", "node_modules", "target", "coverage", "credentials", "secrets", "id_rsa", "id_ed25519"}, part) || strings.Contains(part, "password") || strings.Contains(part, "credential") || strings.Contains(part, "secret") || strings.Contains(part, "token") || strings.Contains(part, "api_key") || strings.Contains(part, "apikey") || strings.HasPrefix(part, "env") || strings.HasSuffix(part, ".pem") || strings.HasSuffix(part, ".key") || strings.HasSuffix(part, ".pfx") || strings.HasSuffix(part, ".p12") {
			return false
		}
	}
	return true
}

type Workspace struct{ Base, Candidate sandbox.Snapshot }

func NewWorkspace(s sandbox.Snapshot) (*Workspace, error) {
	if !s.Valid() {
		return nil, sandbox.ErrPolicy
	}
	candidate := s
	candidate.Files = map[string][]byte{}
	for n, b := range s.Files {
		candidate.Files[n] = bytes.Clone(b)
	}
	return &Workspace{Base: s, Candidate: candidate}, nil
}
func (w *Workspace) Digest() string { _, id, _ := sandbox.SnapshotBytes(w.Candidate); return id }
func (w *Workspace) Patch() []byte {
	old, newBody := w.Base.Files["sum.go"], w.Candidate.Files["sum.go"]
	if bytes.Equal(old, newBody) {
		return nil
	}
	lines := func(b []byte) []string {
		if len(b) == 0 {
			return nil
		}
		p := strings.SplitAfter(string(b), "\n")
		if p[len(p)-1] == "" {
			p = p[:len(p)-1]
		}
		return p
	}
	before, after := lines(old), lines(newBody)
	var out strings.Builder
	fmt.Fprintf(&out, "--- a/sum.go\n+++ b/sum.go\n@@ -1,%d +1,%d @@\n", len(before), len(after))
	for _, group := range []struct {
		prefix string
		lines  []string
	}{{"-", before}, {"+", after}} {
		for _, line := range group.lines {
			out.WriteString(group.prefix + line)
			if !strings.HasSuffix(line, "\n") {
				out.WriteString("\n\\ No newline at end of file\n")
			}
		}
	}
	return []byte(out.String())
}

// Apply performs only bounded operations on a private file map. Even malformed
// paths never reach a host filesystem API. Repository commands use RunSnapshot.
func (w *Workspace) Apply(ctx context.Context, a Action) (Observation, error) {
	result := Observation{Status: "ok", Workspace: w.Digest()}
	if ValidateAction(a) != nil {
		result.Status = "denied"
		return result, nil
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	names := make([]string, 0, len(w.Candidate.Files))
	for n := range w.Candidate.Files {
		names = append(names, n)
	}
	slices.Sort(names)
	visible := func(n string) bool {
		b := w.Candidate.Files[n]
		return Visible(n) && utf8.Valid(b) && !bytes.ContainsRune(b, 0)
	}
	switch a.Tool {
	case "list_tree":
		for _, n := range names {
			if !visible(n) {
				result.Omitted++
				continue
			}
			if len(strings.Split(result.Text, "\n")) > 64 {
				result.Omitted++
				result.Truncated = true
				continue
			}
			result.Text += n + "\n"
		}
	case "read_file":
		var arg ReadArgs
		_ = json.Unmarshal(a.Args, &arg)
		if !visible(arg.Path) {
			result.Status = "denied"
			break
		}
		body, exists := w.Candidate.Files[arg.Path]
		if !exists {
			result.Status = "not_found"
			break
		}
		lines := strings.Split(string(body), "\n")
		start := min(arg.Start-1, len(lines))
		end := min(start+arg.Lines, len(lines))
		result.Text = strings.Join(lines[start:end], "\n")
		result.Truncated = end < len(lines)
	case "search_text":
		var arg SearchArgs
		_ = json.Unmarshal(a.Args, &arg)
		deadline := time.Now().Add(100 * time.Millisecond)
		count := 0
		for _, n := range names {
			if !visible(n) {
				result.Omitted++
				continue
			}
			for i, line := range strings.Split(string(w.Candidate.Files[n]), "\n") {
				if ctx.Err() != nil {
					return result, ctx.Err()
				}
				if time.Now().After(deadline) || count >= 32 {
					result.Truncated = true
					break
				}
				if strings.Contains(line, arg.Text) {
					result.Text += fmt.Sprintf("%s:%d:%s\n", n, i+1, line)
					count++
				}
			}
			if result.Truncated {
				break
			}
		}
	case "apply_patch":
		var arg PatchArgs
		_ = json.Unmarshal(a.Args, &arg)
		old := w.Candidate.Files[arg.Path]
		if arg.Workspace != w.Digest() || bytes.Count(old, []byte(arg.Before)) != 1 {
			result.Status = "conflict"
			break
		}
		newBody := bytes.Replace(old, []byte(arg.Before), []byte(arg.After), 1)
		if len(newBody) > 8192 {
			result.Status = "denied"
			break
		}
		w.Candidate.Files[arg.Path] = newBody
		if !w.Candidate.Valid() || len(w.Patch()) > sandbox.MaxArtifact {
			w.Candidate.Files[arg.Path] = old
			result.Status = "denied"
			break
		}
		result.Text = "exact replacement accepted"
		result.Workspace = w.Digest()
	case "diff":
		result.Text = string(w.Patch())
	case "status":
		result.Text = fmt.Sprintf("base_revision=%s changed=%t required_test_profile=%s", w.Base.BaseRevision, len(w.Patch()) > 0, Profile)
	default:
		result.Status = "denied"
	}
	// All observations have a byte and line bound, including long single lines.
	if len(result.Text) > 4096 {
		result.Text = result.Text[:4096]
		for !utf8.ValidString(result.Text) {
			result.Text = result.Text[:len(result.Text)-1]
		}
		result.Truncated = true
	}
	return result, nil
}

package github_test

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"forgeswarm/internal/agent"
	gh "forgeswarm/internal/github"
	"forgeswarm/internal/githubtest"
	"forgeswarm/internal/publication"
	"github.com/google/uuid"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func fixture(t *testing.T) (*githubtest.Server, *gh.Client) {
	t.Helper()
	fake := githubtest.New()
	server := httptest.NewServer(fake)
	t.Cleanup(server.Close)
	c, e := gh.New(fake.Config(server.URL))
	if e != nil {
		t.Fatal(e)
	}
	return fake, c
}
func operation(t *testing.T, c *gh.Client) publication.Operation {
	t.Helper()
	p, e := c.Prepare(context.Background(), 1)
	if e != nil {
		t.Fatal(e)
	}
	candidate := agent.CloneSnapshot(p.Snapshot)
	candidate.Files["add.go"] = []byte("package main\nfunc Add(a,b int)int{return a + b}\n")
	candidate.Files["double.go"] = []byte("package main\nfunc Double(a int)int{return a * 2}\n")
	candidate.Files["score.go"] = []byte("package main\nfunc Score(a,b int)int{return 3 * a + b}\n")
	id := uuid.New()
	return publication.Operation{ID: id, Approval: uuid.New(), Binding: publication.Binding{Repository: p.Repository, BaseCommit: p.Snapshot.BaseRevision, BaseTree: p.BaseTree, Candidate: agent.SnapshotDigest(candidate), CandidateTree: publication.TreeSHA(candidate), Patch: agent.SHA(agent.MathPatch(p.Snapshot, candidate)), Test: strings.Repeat("a", 64), Review: strings.Repeat("b", 64), Action: publication.Action, Policy: publication.PolicyVersion, TestDemo: true}, Snapshot: candidate, CreatedAt: time.Unix(1700000123, 0), Branch: "forgeswarm/op-" + id.String()}
}
func signature(raw []byte) string {
	h := hmac.New(sha256.New, []byte(githubtest.Secret))
	h.Write(raw)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
func TestGitHubSignedWebhookAndUntrustedIssue(t *testing.T) {
	fake, c := fixture(t)
	raw := fake.Webhook()
	number, actor, e := c.Webhook(raw, signature(raw), "issues")
	if e != nil || number != 1 || actor != "github:fixture-owner" {
		t.Fatal(number, actor, e)
	}
	cases := []struct {
		raw        []byte
		sig, event string
	}{
		{raw, "sha256=" + strings.Repeat("0", 64), "issues"},
		{raw, signature(raw), "push"},
		{[]byte(strings.ReplaceAll(string(raw), `"id":42`, `"id":43`)), "", "issues"},
		{[]byte(strings.ReplaceAll(string(raw), `"labeled"`, `"opened"`)), "", "issues"},
		{[]byte(strings.ReplaceAll(string(raw), `"fixture-owner"`, `"attacker"`)), "", "issues"},
		{[]byte(strings.ReplaceAll(string(raw), `"number":1`, `"number":1,"number":2`)), "", "issues"},
		{[]byte(strings.Repeat("x", 32769)), "", "issues"},
	}
	for _, tc := range cases {
		sig := tc.sig
		if sig == "" {
			sig = signature(tc.raw)
		}
		if _, _, e = c.Webhook(tc.raw, sig, tc.event); e == nil {
			t.Fatal("invalid webhook accepted")
		}
	}
	p, e := c.Prepare(context.Background(), 1)
	if e != nil || !p.Valid() || !strings.Contains(p.Description, "approve and publish") || p.Policy != publication.PolicyVersion {
		t.Fatal("untrusted issue must remain data", e)
	}
	if branches, pulls := fake.Counts(); branches != 0 || pulls != 0 {
		t.Fatal("import published")
	}
	fake.Base.Files["README.txt"] = []byte(strings.Repeat("x", 5000))
	if _, e = c.Prepare(context.Background(), 1); e == nil {
		t.Fatal("oversized repository admitted")
	}
}
func TestGitHubHTTPClassificationRedactionAndURLPolicy(t *testing.T) {
	for _, status := range []int{401, 403, 429, 500} {
		t.Run(string(rune(status)), func(t *testing.T) {
			fake, c := fixture(t)
			fake.StatusCode = status
			e := c.Metadata(context.Background())
			var remote *gh.Error
			if !errors.As(e, &remote) || strings.Contains(e.Error(), "TEST_SECRET") {
				t.Fatal("unclassified or leaked remote body", e)
			}
			if status == 429 && remote.RetryAt.Before(time.Now().Add(time.Second)) {
				t.Fatal("retry guidance ignored")
			}
		})
	}
	fake, _ := fixture(t)
	cfg := fake.Config("http://169.254.169.254")
	if _, e := gh.New(cfg); e == nil {
		t.Fatal("SSRF URL accepted")
	}
	cfg = fake.Config("https://evil.invalid")
	cfg.CredentialMode = "development_token"
	if _, e := gh.New(cfg); e == nil {
		t.Fatal("custom production endpoint accepted")
	}
}
func TestGitHubUncertainBranchAndPullReconcile(t *testing.T) {
	for _, fault := range []string{"/git/refs", "/pulls", "crash_before_record"} {
		t.Run(fault, func(t *testing.T) {
			fake, c := fixture(t)
			op := operation(t, c)
			checks := 0
			check := func(context.Context) error { checks++; return nil }
			if fault != "crash_before_record" {
				fake.Arm("POST", fault, 500, true)
			}
			result, e := c.Publish(context.Background(), op, check)
			if fault != "crash_before_record" && e == nil {
				t.Fatal("lost outcome concealed", result)
			}
			result, e = c.Publish(context.Background(), op, check)
			if e != nil || result.Status != "confirmed" || result.PRNumber != 1 {
				t.Fatal("reconciliation", result, e)
			}
			before := checks
			result, e = c.Publish(context.Background(), op, func(context.Context) error { t.Fatal("confirmed replay attempted write"); return nil })
			if e != nil || result.Status != "confirmed" || checks != before {
				t.Fatal(result, e)
			}
			branches, pulls := fake.Counts()
			if branches != 1 || pulls != 1 {
				t.Fatal("duplicate remote effects", branches, pulls)
			}
		})
	}
}
func TestGitHubChangedCandidateActionPolicyAndForeignPull(t *testing.T) {
	for _, scenario := range []string{"candidate", "action", "policy", "base", "foreign_pull"} {
		t.Run(scenario, func(t *testing.T) {
			fake, c := fixture(t)
			op := operation(t, c)
			switch scenario {
			case "candidate":
				op.Snapshot.Files["score.go"] = []byte("package main\nfunc Score(a,b int)int{return 99}\n")
			case "action":
				op.Binding.Action = "merge"
			case "policy":
				op.Binding.Policy = "new-policy"
			case "base":
				op.Binding.BaseCommit = strings.Repeat("f", 40)
			case "foreign_pull":
				fake.ForeignPull(op.Branch)
			}
			result, e := c.Publish(context.Background(), op, func(context.Context) error { t.Fatal("stale binding reached external write"); return nil })
			if e == nil && result.Status != "needs_review" {
				t.Fatal(result)
			}
			if branches, pulls := fake.Counts(); branches != 0 || pulls != 0 {
				t.Fatal("stale candidate or foreign pull overwritten")
			}
		})
	}
}
func TestGitHubPreconditionsDriftAndCancellation(t *testing.T) {
	for _, scenario := range []string{"foreign_branch", "base_drift", "window_drift", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			fake, c := fixture(t)
			op := operation(t, c)
			switch scenario {
			case "foreign_branch":
				fake.ForeignBranch(op.Branch)
			case "base_drift":
				fake.Drift()
			case "window_drift":
				fake.DriftAfterPull = true
			}
			check := func(context.Context) error {
				if scenario == "cancel" {
					return errors.New("cancelled")
				}
				return nil
			}
			result, e := c.Publish(context.Background(), op, check)
			if scenario == "foreign_branch" {
				if !gh.IsClass(e, "branch_conflict") {
					t.Fatal(result, e)
				}
			} else if e != nil {
				t.Fatal(e)
			} else if scenario == "cancel" && result.Status != "cancelled" || scenario != "cancel" && result.Status != "needs_review" {
				t.Fatal(result)
			}
			branches, pulls := fake.Counts()
			if scenario != "window_drift" && (branches != 0 || pulls != 0) {
				t.Fatal("invalid operation published")
			}
			if scenario == "window_drift" && (branches != 1 || pulls != 1 || result.URL == "") {
				t.Fatal("in-flight drift concealed observed effect", result)
			}
		})
	}
}

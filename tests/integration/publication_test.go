//go:build integration && stage03 && stage04 && stage05 && stage06 && stage07 && real_sandbox

package integration

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"forgeswarm/internal/agent"
	gh "forgeswarm/internal/github"
	"forgeswarm/internal/githubtest"
	"forgeswarm/internal/httpapi"
	"forgeswarm/internal/operator"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/publication"
	"forgeswarm/internal/relay"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

type githubFlow struct {
	f                *fixture
	fake             *githubtest.Server
	client           *gh.Client
	api              *httptest.Server
	approval         postgres.Approval
	operator, reader string
}

func webhookSignature(body []byte) string {
	h := hmac.New(sha256.New, []byte(githubtest.Secret))
	h.Write(body)
	return "sha256=" + hex.EncodeToString(h.Sum(nil))
}
func (g *githubFlow) request(t *testing.T, method, path, token string, body []byte, headers map[string]string) (int, []byte) {
	t.Helper()
	r, e := http.NewRequest(method, g.api.URL+path, bytes.NewReader(body))
	if e != nil {
		t.Fatal(e)
	}
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	r.Header.Set("Content-Type", "application/json")
	for k, v := range headers {
		r.Header.Set(k, v)
	}
	response, e := http.DefaultClient.Do(r)
	if e != nil {
		t.Fatal(e)
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, 65537))
	if e != nil {
		t.Fatal(e)
	}
	return response.StatusCode, raw
}
func newGitHubFlow(t *testing.T, expiry bool) *githubFlow {
	t.Helper()
	f := setup(t)
	agentPricing(t, f)
	if expiry {
		if e := f.s1.ConfigureApprovalTTL(time.Second); e != nil {
			t.Fatal(e)
		}
	}
	fake := githubtest.New()
	remote := httptest.NewServer(fake)
	t.Cleanup(remote.Close)
	client, e := gh.New(fake.Config(remote.URL))
	if e != nil {
		t.Fatal(e)
	}
	g := &githubFlow{f: f, fake: fake, client: client, operator: strings.Repeat("o", 40), reader: strings.Repeat("r", 40)}
	auth, e := operator.New(operator.Config{Operator: operator.Credential{Actor: "verified-owner", Token: g.operator}, Reader: operator.Credential{Actor: "observer", Token: g.reader}}, nil)
	if e != nil {
		t.Fatal(e)
	}
	b := testBroker(t, f)
	r := rpcServer(t, f, "CompleteAgent")
	r.cfg.Capacity = 2
	d, _ := processDriver(t)
	api := httpapi.New(f.s1, slog.New(slog.NewJSONHandler(io.Discard, nil)), 5*time.Second, time.Second).WithOperator(auth).WithGitHub(client).WithCollaboration(d.Registry, 2)
	g.api = httptest.NewServer(httpapi.PrivateUI(api.Handler(), ""))
	t.Cleanup(g.api.Close)
	startAgentWorker(t, f, b, r, d, agentRuntime(t))
	payload := fake.Webhook()
	delivery := uuid.NewString()
	headers := map[string]string{"X-Hub-Signature-256": webhookSignature(payload), "X-GitHub-Delivery": delivery, "X-GitHub-Event": "issues"}
	code, raw := g.request(t, "POST", "/v1/github/webhook", "", payload, headers)
	var receipt workflow.Receipt
	if code != 202 || json.Unmarshal(raw, &receipt) != nil {
		t.Fatalf("signed import status=%d body=%s", code, raw)
	}
	code, raw = g.request(t, "POST", "/v1/github/webhook", "", payload, headers)
	var replay workflow.Receipt
	_ = json.Unmarshal(raw, &replay)
	if code != 202 || replay.ID != receipt.ID || scalar(t, f, `SELECT count(*) FROM github_imports WHERE root_id=$1`, receipt.ID) != 1 {
		t.Fatal("delivery replay duplicated workflow")
	}
	mismatched := bytes.Replace(payload, []byte("Fix math"), []byte("New math"), 1)
	headers["X-Hub-Signature-256"] = webhookSignature(mismatched)
	if code, _ = g.request(t, "POST", "/v1/github/webhook", "", mismatched, headers); code != 409 {
		t.Fatal("mismatched delivery accepted", code)
	}
	until := time.Now().Add(30 * time.Second)
	for time.Now().Before(until) {
		if _, e = f.s1.ReconcileBatch(f.ctx, 16); e != nil {
			t.Fatal(e)
		}
		if e = f.s1.ReconcileCollaboration(f.ctx, 16); e != nil {
			t.Fatal(e)
		}
		if _, e = relay.Step(f.ctx, f.s1, b, uuid.New(), 20, time.Second); e != nil {
			t.Fatal(e)
		}
		p, e := f.s2.Project(f.ctx, receipt.ID)
		if e != nil {
			t.Fatal(e)
		}
		if p.Status == "WAITING_FOR_APPROVAL" {
			break
		}
		if p.Status == "FAILED" || p.Status == "NEEDS_HUMAN" {
			for _, v := range p.Versions {
				rows, err := f.p2.Query(f.ctx, `SELECT t.task_key,a.state,a.last_error_class,a.stop_reason,a.result_metadata FROM tasks t JOIN task_attempts a ON a.task_id=t.id WHERE t.workflow_id=$1 ORDER BY t.position,a.attempt_number`, v.Workflow)
				if err == nil {
					for rows.Next() {
						var key, state, last, stop string
						var meta json.RawMessage
						if rows.Scan(&key, &state, &last, &stop, &meta) == nil {
							t.Logf("pipeline attempt task=%s state=%s error=%s stop=%s metadata=%s", key, state, last, stop, meta)
						}
					}
					rows.Close()
				}
			}
			t.Fatal("pipeline failed", p.Reason)
		}
		time.Sleep(50 * time.Millisecond)
	}
	p, e := f.s2.Project(f.ctx, receipt.ID)
	if e != nil || p.Status != "WAITING_FOR_APPROVAL" {
		t.Fatal("not durably waiting", p.Status, e)
	}
	values, e := f.s2.Approvals(f.ctx, receipt.ID, uuid.Nil)
	if e != nil || len(values) != 1 {
		t.Fatal("missing approval", e)
	}
	g.approval = values[0]
	if scalar(t, f, `SELECT count(*) FROM task_attempts WHERE state='RUNNING'`) != 0 {
		t.Fatal("approval wait occupied worker slot")
	}
	if branches, pulls := fake.Counts(); branches != 0 || pulls != 0 {
		t.Fatal("issue import auto-published")
	}
	return g
}
func (g *githubFlow) decision() publication.Decision {
	a := g.approval
	return publication.Decision{ExpectedVersion: a.Version, Binding: a.Digest, Candidate: a.Binding.Candidate, Decision: "approve"}
}
func (g *githubFlow) approve(t *testing.T) postgres.Approval {
	t.Helper()
	body, _ := json.Marshal(g.decision())
	code, raw := g.request(t, "POST", "/v1/approvals/"+g.approval.ID.String(), g.operator, body, nil)
	var a postgres.Approval
	if code != 200 || json.Unmarshal(raw, &a) != nil || a.Status != "APPROVED" || a.Actor != "verified-owner" || a.Operation == nil {
		t.Fatal("approval", code, string(raw))
	}
	return a
}
func TestGitHubDurableWaitRolesAndConcurrentPublication(t *testing.T) {
	g := newGitHubFlow(t, false)
	f := g.f
	a := g.approval
	// Fresh controller/store (no preparation/planner memory) finds the same request.
	restarted := postgres.New(f.p2)
	if e := restarted.ReconcileCollaboration(f.ctx, 16); e != nil {
		t.Fatal(e)
	}
	again, e := restarted.Approval(f.ctx, a.ID)
	if e != nil || again.Digest != a.Digest || again.ID != a.ID {
		t.Fatal("restart lost approval", e)
	}
	d := g.decision()
	body, _ := json.Marshal(d)
	// Removing auth configuration must not reopen persisted integration data.
	unconfigured := httptest.NewServer(httpapi.New(f.s2, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, time.Second).Handler())
	response, requestErr := http.Get(unconfigured.URL + "/v1/workflows/" + a.Root.String())
	if requestErr != nil {
		t.Fatal(requestErr)
	}
	response.Body.Close()
	unconfigured.Close()
	if response.StatusCode != 503 {
		t.Fatal("persisted GitHub database reopened without auth")
	}
	if code, _ := g.request(t, "POST", "/v1/approvals/"+a.ID.String(), strings.Repeat("w", 40), body, nil); code != 401 {
		t.Fatal("worker approved")
	}
	if code, _ := g.request(t, "POST", "/v1/approvals/"+a.ID.String(), g.reader, body, nil); code != 403 {
		t.Fatal("reader approved")
	}
	artifact := "/v1/workflows/" + a.Binding.Workflow.String() + "/artifacts/" + uuid.NewString()
	if code, _ := g.request(t, "GET", artifact, "", nil, nil); code != 401 {
		t.Fatal("unprotected artifact")
	}
	d.ExpectedVersion++
	body, _ = json.Marshal(d)
	if code, _ := g.request(t, "POST", "/v1/approvals/"+a.ID.String(), g.operator, body, nil); code != 409 {
		t.Fatal("stale version")
	}
	d = g.decision()
	d.Candidate = strings.Repeat("f", 64)
	body, _ = json.Marshal(d)
	if code, _ := g.request(t, "POST", "/v1/approvals/"+a.ID.String(), g.operator, body, nil); code != 409 {
		t.Fatal("stale candidate")
	}
	if _, e = f.p1.Exec(f.ctx, `UPDATE approvals SET binding_sha256=$2 WHERE id=$1`, a.ID, strings.Repeat("f", 64)); e == nil {
		t.Fatal("approval binding mutable")
	}
	approved := g.approve(t)
	if _, e = f.p1.Exec(f.ctx, `UPDATE approvals SET binding=jsonb_set(binding,'{policy_version}','"future-policy"') WHERE id=$1`, a.ID); e == nil {
		t.Fatal("approval policy mutated")
	}
	duplicate := g.approve(t)
	if duplicate.Operation.ID != approved.Operation.ID {
		t.Fatal("duplicate publish intent")
	}
	d = g.decision()
	d.Decision = "reject"
	if _, e = f.s2.DecideApproval(f.ctx, a.ID, operator.Principal{Actor: "verified-owner", Role: "operator"}, d); !errors.Is(e, workflow.ErrConflict) {
		t.Fatal("decision changed", e)
	}
	var wg sync.WaitGroup
	failures := make(chan error, 2)
	for _, store := range []*postgres.Store{f.s1, f.s2} {
		wg.Add(1)
		go func(s *postgres.Store) {
			defer wg.Done()
			_, e := s.PublishOne(f.ctx, g.client, approved.Operation.ID, false)
			failures <- e
		}(store)
	}
	wg.Wait()
	close(failures)
	for e := range failures {
		if e != nil {
			t.Fatal(e)
		}
	}
	result, e := restarted.Approval(f.ctx, a.ID)
	if e != nil || result.Operation.Status != "confirmed" || result.Operation.PRNumber != 1 || result.Operation.Head == "" {
		t.Fatal("publication not persisted", result.Operation, e)
	}
	if branches, pulls := g.fake.Counts(); branches != 1 || pulls != 1 {
		t.Fatal("duplicate external effects", branches, pulls)
	}
	if scalar(t, f, `SELECT count(*) FROM external_operations WHERE approval_id=$1`, a.ID) != 1 {
		t.Fatal("multiple logical operations")
	}
	p, e := f.s2.Project(f.ctx, a.Root)
	if e != nil || p.Status != "PUBLISHED" {
		t.Fatal("aggregate outcome", p.Status, e)
	}
}
func TestGitHubExpiredApprovedPolicyAndInFlightCancellation(t *testing.T) {
	for _, scenario := range []string{"approved_expiry", "policy_change", "cancel_after_branch", "foreign_pull"} {
		t.Run(scenario, func(t *testing.T) {
			g := newGitHubFlow(t, scenario == "approved_expiry")
			f := g.f
			a := g.approve(t)
			client := g.client
			switch scenario {
			case "approved_expiry":
				time.Sleep(1100 * time.Millisecond)
			case "policy_change":
				config := g.fake.Config("http://127.0.0.1:1")
				config.Repository.Target = "release"
				var e error
				client, e = gh.New(config)
				if e != nil {
					t.Fatal(e)
				}
			case "cancel_after_branch":
				g.fake.AfterBranch = func() {
					if _, e := f.s2.Cancel(f.ctx, a.Root); e != nil {
						t.Error(e)
					}
				}
			case "foreign_pull":
				g.fake.ForeignPull(a.Operation.Branch)
			}
			if _, e := f.s1.PublishOne(f.ctx, client, a.Operation.ID, false); e != nil {
				t.Fatal(e)
			}
			observed, e := f.s2.Approval(f.ctx, a.ID)
			if e != nil {
				t.Fatal(e)
			}
			branches, pulls := g.fake.Counts()
			if pulls != 0 || scenario != "cancel_after_branch" && branches != 0 {
				t.Fatal("stale or cancelled intent published", branches, pulls)
			}
			if scenario == "cancel_after_branch" {
				if branches != 1 || observed.Operation.Head == "" || observed.Operation.Reason != "cancelled_external_effect_may_exist" {
					t.Fatal("in-flight cancellation concealed branch", observed.Operation)
				}
				if e = f.s1.ReconcileCollaboration(f.ctx, 16); e != nil {
					t.Fatal(e)
				}
				p, e := f.s2.Project(f.ctx, a.Root)
				if e != nil || p.Status != "CANCELLED" {
					t.Fatal("cancel failed to settle")
				}
			} else if observed.Operation.Status != "needs_review" && observed.Operation.Status != "cancelled" {
				t.Fatal("invalid approval published", observed.Operation)
			}
		})
	}
}
func TestGitHubRejectExpiryAndCancelApproveRace(t *testing.T) {
	for _, scenario := range []string{"reject", "expiry", "cancel_race"} {
		t.Run(scenario, func(t *testing.T) {
			g := newGitHubFlow(t, scenario == "expiry")
			f := g.f
			d := g.decision()
			actor := operator.Principal{Actor: "verified-owner", Role: "operator"}
			switch scenario {
			case "reject":
				d.Decision = "reject"
				if _, e := f.s1.DecideApproval(f.ctx, g.approval.ID, actor, d); e != nil {
					t.Fatal(e)
				}
				d.Decision = "approve"
				if _, e := f.s2.DecideApproval(f.ctx, g.approval.ID, actor, d); !errors.Is(e, workflow.ErrConflict) {
					t.Fatal("rejected approval reused", e)
				}
			case "expiry":
				time.Sleep(1100 * time.Millisecond)
				if e := f.s2.ReconcileCollaboration(f.ctx, 16); e != nil {
					t.Fatal(e)
				}
				a, e := f.s2.Approval(f.ctx, g.approval.ID)
				if e != nil || a.Status != "EXPIRED" {
					t.Fatal("expiry", a.Status, e)
				}
				if _, e = f.s1.DecideApproval(f.ctx, a.ID, actor, d); !errors.Is(e, workflow.ErrConflict) {
					t.Fatal("expired request approved", e)
				}
			case "cancel_race":
				var wg sync.WaitGroup
				wg.Add(2)
				go func() {
					defer wg.Done()
					_, e := f.s1.Cancel(f.ctx, g.approval.Root)
					if e != nil {
						t.Error(e)
					}
				}()
				go func() {
					defer wg.Done()
					_, e := f.s2.DecideApproval(f.ctx, g.approval.ID, actor, d)
					if e != nil && !errors.Is(e, workflow.ErrConflict) {
						t.Error(e)
					}
				}()
				wg.Wait()
				if _, e := f.s2.PublishOne(f.ctx, g.client, uuid.Nil, false); e != nil {
					t.Fatal(e)
				}
				if e := f.s1.ReconcileCollaboration(f.ctx, 16); e != nil {
					t.Fatal(e)
				}
				p, e := f.s2.Project(f.ctx, g.approval.Root)
				if e != nil || p.Status != "CANCELLED" {
					t.Fatal("cancel did not settle", p.Status, e)
				}
			}
			if branches, pulls := g.fake.Counts(); branches != 0 || pulls != 0 {
				t.Fatal("invalid approval published")
			}
		})
	}
}
func TestGitHubUncertainPublicationRestartsAndBoundsRetries(t *testing.T) {
	for _, scenario := range []string{"branch_timeout", "pull_timeout", "record_crash", "foreign_branch", "target_drift", "window_drift", "rate_limit"} {
		t.Run(scenario, func(t *testing.T) {
			g := newGitHubFlow(t, false)
			f := g.f
			a := g.approve(t)
			op := *a.Operation
			switch scenario {
			case "branch_timeout":
				g.fake.Arm("POST", "/git/refs", 500, true)
			case "pull_timeout":
				g.fake.Arm("POST", "/pulls", 500, true)
			case "foreign_branch":
				g.fake.ForeignBranch(op.Branch)
			case "target_drift":
				g.fake.Drift()
			case "window_drift":
				g.fake.DriftAfterPull = true
			case "rate_limit":
				g.fake.StatusCode = 429
			case "record_crash":
				// Persist in-flight intent, let remote success happen, then omit the DB
				// acknowledgement to model process death before recording that success.
				if _, e := f.p1.Exec(f.ctx, `UPDATE external_operations SET status='in_flight',attempts=1 WHERE id=$1`, op.ID); e != nil {
					t.Fatal(e)
				}
				p, e := g.client.Prepare(f.ctx, 1)
				if e != nil {
					t.Fatal(e)
				}
				candidates, e := f.s1.Candidates(f.ctx, a.Binding.Workflow)
				if e != nil {
					t.Fatal(e)
				}
				for _, c := range candidates {
					if c.Role == "integration" {
						var out agent.RoleOutcome
						if json.Unmarshal(c.Body, &out) != nil {
							t.Fatal("decode")
						}
						op.Snapshot = out.Snapshot
					}
				}
				if p.Snapshot.BaseRevision != op.Binding.BaseCommit {
					t.Fatal("wrong pinned base")
				}
				if result, e := g.client.Publish(f.ctx, op, func(context.Context) error { return nil }); e != nil || result.Status != "confirmed" {
					t.Fatal(result, e)
				}
			}
			if _, e := f.s1.PublishOne(f.ctx, g.client, op.ID, false); e != nil {
				t.Fatal(e)
			}
			observed, e := f.s2.Approval(f.ctx, a.ID)
			if e != nil {
				t.Fatal(e)
			}
			if scenario == "rate_limit" {
				if observed.Operation.Status != "uncertain" || time.Until(observed.Operation.NextAttempt) < time.Second {
					t.Fatal("retry guidance not durable", observed.Operation)
				}
				if worked, e := f.s2.PublishOne(f.ctx, g.client, op.ID, false); e != nil || worked {
					t.Fatal("retry before reset")
				}
				// Advance only mutable retry scheduling in this controlled test, retaining
				// the six-attempt cap and every immutable approval/remote identity.
				for i := 1; i < 6; i++ {
					if _, e = f.p1.Exec(f.ctx, `UPDATE external_operations SET next_attempt_at=clock_timestamp() WHERE id=$1`, op.ID); e != nil {
						t.Fatal(e)
					}
					if _, e = f.s2.PublishOne(f.ctx, g.client, op.ID, false); e != nil {
						t.Fatal(e)
					}
				}
				observed, e = f.s1.Approval(f.ctx, a.ID)
				if e != nil || observed.Operation.Status != "manual_reconciliation" || observed.Operation.Attempts != 6 {
					t.Fatal("unbounded retry", observed.Operation, e)
				}
			} else if scenario == "branch_timeout" || scenario == "pull_timeout" {
				if observed.Operation.Status != "uncertain" {
					t.Fatal("uncertain result concealed", observed.Operation)
				}
				if _, e = f.p1.Exec(f.ctx, `UPDATE external_operations SET next_attempt_at=clock_timestamp() WHERE id=$1`, op.ID); e != nil {
					t.Fatal(e)
				}
				restarted := postgres.New(f.p2)
				if _, e = restarted.PublishOne(f.ctx, g.client, op.ID, false); e != nil {
					t.Fatal(e)
				}
				observed, e = restarted.Approval(f.ctx, a.ID)
				if e != nil || observed.Operation.Status != "confirmed" {
					t.Fatal("restart reconciliation", observed.Operation, e)
				}
			} else if scenario == "foreign_branch" || scenario == "target_drift" || scenario == "window_drift" {
				if observed.Operation.Status != "needs_review" || observed.Status != "INVALIDATED" {
					t.Fatal("stale approval retained", observed, e)
				}
			} else if observed.Operation.Status != "confirmed" {
				t.Fatal(observed.Operation)
			}
			branches, pulls := g.fake.Counts()
			if branches > 1 || pulls > 1 {
				t.Fatal("duplicate effects")
			}
			if scenario == "window_drift" && (pulls != 1 || observed.Operation.URL == "") {
				t.Fatal("post-publication drift lost observed PR")
			}
		})
	}
}

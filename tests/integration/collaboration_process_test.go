//go:build integration && stage03 && stage04 && stage05 && stage06 && stage07 && real_sandbox

package integration

import (
	"context"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/relay"
	"forgeswarm/internal/sandbox"
	"github.com/google/uuid"
	"strings"
	"testing"
	"time"
)

func TestCollaborationRealMultiRoleParallelIntegrationAndRepair(t *testing.T) {
	for _, scenario := range []struct {
		name, marker, want string
		versions           int
	}{{"accepted", "", "SUCCEEDED", 1}, {"one_correction", " TEST_DEMO_REJECT_ONCE", "SUCCEEDED", 2}, {"exhausted", " TEST_DEMO_REJECT_ALWAYS", "NEEDS_HUMAN", 3}} {
		t.Run(scenario.name, func(t *testing.T) {
			f := setup(t)
			agentPricing(t, f)
			b := testBroker(t, f)
			r := rpcServer(t, f, "CompleteAgent")
			r.cfg.Capacity = 2
			d, _ := processDriver(t)
			runtime := agentRuntime(t)
			base := collabBase()
			job := collabJob(base)
			job.TaskDescription += scenario.marker
			if id, e := sandbox.SaveSnapshot(d.Registry, base); e != nil || id != job.RepositoryID {
				t.Fatal("base registry", e)
			}
			receipt, e := f.s1.CreateProject(f.ctx, "real-collab-"+uuid.NewString(), job, base, 2)
			if e != nil {
				t.Fatal(e)
			}
			startAgentWorker(t, f, b, r, d, runtime)
			until := time.Now().Add(52 * time.Second)
			var state string
			controller := f.s1
			restarted := false
			for time.Now().Before(until) {
				if _, e = f.s1.ReconcileBatch(f.ctx, 16); e != nil {
					t.Fatal(e)
				}
				if e = controller.ReconcileCollaboration(f.ctx, 16); e != nil {
					t.Fatal(e)
				}
				if _, e = relay.Step(f.ctx, f.s1, b, uuid.New(), 20, time.Second); e != nil {
					t.Fatal(e)
				}
				p, err := f.s2.Project(f.ctx, receipt.ID)
				if err != nil {
					t.Fatal(err)
				}
				state = p.Status
				if p.Round > 0 && !restarted {
					controller = postgres.New(f.p2)
					pricing, err := agent.LoadPricing("../../config/agent-pricing.json")
					if err != nil {
						t.Fatal(err)
					}
					if err = controller.ConfigureAgent(pricing); err != nil {
						t.Fatal(err)
					}
					restarted = true
				}
				if state == "SUCCEEDED" || state == "NEEDS_HUMAN" || state == "FAILED" {
					break
				}
				time.Sleep(50 * time.Millisecond)
			}
			if state != scenario.want {
				t.Fatalf("project state=%s want=%s", state, scenario.want)
			}
			project, e := f.s2.Project(f.ctx, receipt.ID)
			if e != nil || len(project.Plans) != scenario.versions || project.Round != scenario.versions-1 {
				t.Fatal("lost immutable correction versions", project, e)
			}
			if scalar(t, f, `SELECT count(*) FROM collab_executions WHERE root_id=$1`, receipt.ID) != int64(scenario.versions) {
				t.Fatal("missing child versions")
			}
			// Actual independent developer lifetimes overlap. Their data maps and
			// Docker workspaces are separate; no shared checkout is involved.
			if scalar(t, f, `SELECT count(*) FROM task_attempts a JOIN tasks ta ON ta.id=a.task_id JOIN task_attempts b ON b.workflow_id=a.workflow_id JOIN tasks tb ON tb.id=b.task_id JOIN collab_workflows family ON family.workflow_id=a.workflow_id WHERE family.root_id=$1 AND family.version=1 AND ta.task_key='add' AND tb.task_key='double' AND a.started_at<b.finished_at AND b.started_at<a.finished_at`, receipt.ID) != 1 {
				t.Fatal("independent developers did not overlap")
			}
			if scalar(t, f, `SELECT count(*) FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id JOIN collab_workflows family ON family.workflow_id=a.workflow_id WHERE family.root_id=$1 AND r.role='integration' AND r.accepted AND r.test_sha256<>''`, receipt.ID) != int64(scenario.versions) {
				t.Fatal("new version skipped fresh integration tests")
			}
			if scalar(t, f, `SELECT count(*) FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id JOIN collab_workflows family ON family.workflow_id=a.workflow_id WHERE family.root_id=$1 AND r.role='reviewer' AND r.accepted AND NOT EXISTS(SELECT 1 FROM collab_results i JOIN task_attempts ia ON ia.id=i.attempt_id WHERE ia.workflow_id=a.workflow_id AND i.role='integration' AND i.accepted AND i.candidate_sha256=r.candidate_sha256 AND i.test_sha256<>'')`, receipt.ID) != 0 {
				t.Fatal("review accepted a candidate without exact integration proof")
			}
			// For each dependent Score, reconstruct exact successful predecessor
			// snapshots and compare the persisted input digest after a new Store read.
			for _, v := range project.Versions {
				if v.Phase != "execution" {
					continue
				}
				candidates, e := f.s2.Candidates(f.ctx, v.Workflow)
				if e != nil {
					t.Fatal(e)
				}
				found := false
				for _, candidate := range candidates {
					if candidate.Role == "integration" {
						found = true
						patch, e := f.s2.Artifact(f.ctx, v.Workflow, uuid.NewSHA1(candidate.Attempt, []byte("artifact-v1:patch")))
						if e != nil || !strings.Contains(string(patch.Body), "+++ b/add.go") || patch.BaseRevision != base.BaseRevision {
							t.Fatal("combined patch provenance", e)
						}
					}
				}
				if !found {
					t.Fatal("missing final integration candidate")
				}
			}
			if scalar(t, f, `SELECT count(*) FROM collab_attempt_inputs i JOIN task_attempts a ON a.id=i.attempt_id JOIN tasks t ON t.id=a.task_id JOIN collab_workflows family ON family.workflow_id=a.workflow_id WHERE family.root_id=$1 AND t.task_key='score' AND jsonb_array_length(i.work->'inputs')=2 AND convert_from(decode(i.work->'snapshot'->'files'->>'add.go','base64'),'UTF8') LIKE '%a + b%' AND convert_from(decode(i.work->'snapshot'->'files'->>'double.go','base64'),'UTF8') LIKE '%a * 2%'`, receipt.ID) != int64(scenario.versions) {
				t.Fatal("dependent task did not receive recorded predecessor snapshot")
			}
			usage, e := f.s2.Usage(f.ctx, receipt.ID)
			if e != nil || usage.Calls < 10*scenario.versions || usage.MaximumMicrousd == nil || usage.EstimatedMicrousd > *usage.MaximumMicrousd {
				t.Fatal("shared persisted budget", usage, e)
			}
			if scenario.want == "NEEDS_HUMAN" && project.Reason != "correction_rounds_exhausted" {
				t.Fatal("unclear exhaustion", project.Reason)
			}
			if scenario.versions > 1 && !restarted {
				t.Fatal("correction did not recover through a fresh controller")
			}
		})
	}
}

func TestCollaborationRealConflictsAndStaleReviewNeedHuman(t *testing.T) {
	for _, scenario := range []struct{ marker, reason string }{{"TEST_DEMO_OVERLAP", "overlap_conflict_requires_human"}, {"TEST_DEMO_SEMANTIC_CONFLICT", "semantic_conflict_or_test_failure"}, {"TEST_DEMO_STALE_REVIEW", "integration_conflict_test_failure_or_execution_failure"}} {
		t.Run(scenario.marker, func(t *testing.T) {
			f := setup(t)
			agentPricing(t, f)
			b := testBroker(t, f)
			r := rpcServer(t, f, "")
			r.cfg.Capacity = 2
			d, _ := processDriver(t)
			runtime := agentRuntime(t)
			base := collabBase()
			job := collabJob(base)
			job.TaskDescription += " " + scenario.marker
			if _, e := sandbox.SaveSnapshot(d.Registry, base); e != nil {
				t.Fatal(e)
			}
			receipt, e := f.s1.CreateProject(f.ctx, "conflict-"+uuid.NewString(), job, base, 2)
			if e != nil {
				t.Fatal(e)
			}
			startAgentWorker(t, f, b, r, d, runtime)
			until := time.Now().Add(35 * time.Second)
			state := ""
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
				state = p.Status
				if state == "NEEDS_HUMAN" {
					if p.Reason != scenario.reason {
						t.Fatal("conflict classification", p.Reason)
					}
					break
				}
				if state == "SUCCEEDED" {
					t.Fatal("conflict/review bypass accepted")
				}
				time.Sleep(50 * time.Millisecond)
			}
			if state != "NEEDS_HUMAN" {
				t.Fatal("conflict not settled", state)
			}
			if scalar(t, f, `SELECT count(*) FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id JOIN collab_workflows family ON family.workflow_id=a.workflow_id WHERE family.root_id=$1 AND r.role='reviewer' AND r.accepted`, receipt.ID) != 0 {
				t.Fatal("stale review or failed tests authorized accepted review")
			}
			if scalar(t, f, `SELECT count(*) FROM collab_executions WHERE root_id=$1`, receipt.ID) != 1 {
				t.Fatal("conflict created an unbounded repair loop")
			}
		})
	}
}

// These cases run the same protected Docker profile on combined content.
func TestCollaborationRealSemanticConflictCannotUseSeparateTestClaims(t *testing.T) {
	d, _ := processDriver(t)
	base := collabBase()
	left, right := agent.CloneSnapshot(base), agent.CloneSnapshot(base)
	left.Files["add.go"] = []byte(strings.Replace(string(left.Files["add.go"]), "a - b", "a + b", 1))
	right.Files["double.go"] = []byte(strings.Replace(string(right.Files["double.go"]), "a * 3", "a * 2", 1))
	combined, e := agent.MergeSnapshot(base, left, right)
	if e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for _, item := range []struct {
		source  string
		profile string
	}{{"left", "add-v1"}, {"right", "double-v1"}, {"combined", agent.MathProfile}} {
		snapshot := combined
		if item.source == "left" {
			snapshot = left
		}
		if item.source == "right" {
			snapshot = right
		}
		attempt := uuid.New()
		binding := sandbox.Binding{Installation: d.Installation, Workflow: uuid.New(), Task: uuid.New(), Attempt: attempt, Instance: uuid.New(), Generation: 1, Worker: "worker_a", Request: sandbox.RequestID(attempt)}
		request := d.Request(binding, agent.Input{Job: collabJob(snapshot)}.Sandbox(), sandbox.DefaultPolicy())
		request.Input.FixtureID = agent.SnapshotDigest(snapshot)
		result, e := d.RunSnapshot(ctx, request, snapshot, item.profile, func(context.Context) error { return nil })
		if e != nil {
			t.Fatal(e)
		}
		passed := result.Status == "exited" && result.ExitCode == 0
		if passed != (item.source != "combined") {
			t.Fatal("separate branch success bypassed combined semantics", item.source, result.Status)
		}
	}
}

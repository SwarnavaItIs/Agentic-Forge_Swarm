package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/execution"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"slices"
	"strings"
	"time"
	"unicode/utf8"
)

func roleTask(job agent.Job, r agent.RoleSpec, description string, deps []string) workflow.TaskDefinition {
	if len(description) > 2000 {
		description = description[:2000]
		for !utf8.ValidString(description) {
			description = description[:len(description)-1]
		}
	}
	job.TaskDescription = description
	in := agent.Input{Job: job, Role: &r}
	si := in.Sandbox()
	return workflow.TaskDefinition{Key: r.TaskKey, Kind: "agent_go", Agent: &in, Sandbox: &si, DependsOn: deps, MaxAttempts: 2, TimeoutMS: 300000}
}
func (s *Store) CreateProject(ctx context.Context, key string, job agent.Job, base sandbox.Snapshot, maxRounds int) (workflow.Receipt, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return workflow.Receipt{}, classify(err)
	}
	defer rollback(tx)
	receipt, err := s.createProjectTx(ctx, tx, key, job, base, maxRounds)
	if err != nil {
		return receipt, err
	}
	return receipt, classify(tx.Commit(ctx))
}
func (s *Store) createProjectTx(ctx context.Context, tx pgx.Tx, key string, job agent.Job, base sandbox.Snapshot, maxRounds int) (workflow.Receipt, error) {
	in := agent.Input{Job: job, Role: &agent.RoleSpec{SchemaVersion: 1, Name: "planner", Version: 1, TaskKey: "plan", Scope: []string{}, Profile: agent.MathProfile, MaxCalls: 4}}
	if !in.Valid() || !agent.ControlledMath(base) || agent.SnapshotDigest(base) != job.RepositoryID || base.BaseRevision != job.BaseRevision || maxRounds < 0 || maxRounds > 2 || s.agentPricing == nil {
		return workflow.Receipt{}, workflow.ErrInvalid
	}
	title := []rune(strings.TrimSpace(job.TaskDescription))
	if len(title) > 80 {
		title = title[:80]
	}
	receipt, err := s.createTx(ctx, tx, key, workflow.CreateInput{Name: "Plan: " + string(title), TimeoutMS: 600000, Tasks: []workflow.TaskDefinition{roleTask(job, *in.Role, job.TaskDescription, nil)}})
	if err != nil {
		return receipt, err
	}
	_, err = tx.Exec(ctx, `INSERT INTO collab_projects(root_id,job,status,max_rounds,maximum_microusd,deadline_at) VALUES($1,$2,'PLANNING',$3,$4,clock_timestamp()+interval '10 minutes') ON CONFLICT DO NOTHING`, receipt.ID, job, maxRounds, s.agentPricing.Maximum)
	if err != nil {
		return receipt, classify(err)
	}
	// Replays must also preserve project-level policy omitted from the legacy DAG.
	var matches bool
	err = tx.QueryRow(ctx, `SELECT job=$2::jsonb AND max_rounds=$3 FROM collab_projects WHERE root_id=$1`, receipt.ID, job, maxRounds).Scan(&matches)
	if err != nil {
		return receipt, classify(err)
	}
	if !matches {
		return receipt, workflow.ErrConflict
	}
	_, err = tx.Exec(ctx, `INSERT INTO collab_workflows(workflow_id,root_id,version,phase,base_snapshot,base_sha256) VALUES($1,$1,1,'planning',$2,$3) ON CONFLICT DO NOTHING`, receipt.ID, base, agent.SnapshotDigest(base))
	if err != nil {
		return receipt, classify(err)
	}
	return receipt, nil
}

func (s *Store) FinalizePlan(ctx context.Context, root uuid.UUID, version int, digest string) (workflow.Receipt, error) {
	if version < 1 || version > 3 || !agent.ValidDigest(digest) {
		return workflow.Receipt{}, workflow.ErrInvalid
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return workflow.Receipt{}, classify(err)
	}
	defer rollback(tx)
	// Follow creation's admission-lock order. The project lock serializes all
	// finalizers and keeps one immutable child receipt per plan version.
	if err = admission(ctx, tx); err != nil {
		return workflow.Receipt{}, err
	}
	parent, err := lockParent(ctx, tx, root)
	if err != nil {
		return workflow.Receipt{}, err
	}
	var job agent.Job
	var status string
	var deadline time.Time
	err = tx.QueryRow(ctx, `SELECT job,status,deadline_at FROM collab_projects WHERE root_id=$1 FOR UPDATE`, root).Scan(&job, &status, &deadline)
	if err != nil {
		return workflow.Receipt{}, classify(err)
	}
	var plan agent.Plan
	var stored string
	err = tx.QueryRow(ctx, `SELECT body,digest FROM collab_plans WHERE root_id=$1 AND version=$2`, root, version).Scan(&plan, &stored)
	if err != nil {
		return workflow.Receipt{}, classify(err)
	}
	normalized, e := agent.NormalizePlan(plan, job)
	if e != nil || stored != digest || agent.JSONSHA(normalized) != digest {
		return workflow.Receipt{}, workflow.ErrConflict
	}
	var receipt workflow.Receipt
	err = tx.QueryRow(ctx, `SELECT w.id,w.created_at FROM collab_executions e JOIN workflows w ON w.id=e.child_id WHERE e.root_id=$1 AND e.version=$2`, root, version).Scan(&receipt.ID, &receipt.CreatedAt)
	if err == nil {
		return receipt, classify(tx.Commit(ctx))
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return receipt, classify(err)
	}
	now, err := dbNow(ctx, tx)
	if err != nil {
		return receipt, err
	}
	if status != "PLANNING" || !now.Before(deadline) {
		return receipt, workflow.ErrConflict
	}
	var turns, tokens int
	var cost int64
	err = tx.QueryRow(ctx, `SELECT COALESCE(sum(s.turns),0),COALESCE(sum(s.tokens),0),COALESCE(sum(s.estimated_microusd),0) FROM agent_sessions s JOIN task_attempts a ON a.id=s.attempt_id JOIN collab_workflows f ON f.workflow_id=a.workflow_id WHERE f.root_id=$1`, root).Scan(&turns, &tokens, &cost)
	if err != nil {
		return receipt, classify(err)
	}
	calls := 2
	tasks := []workflow.TaskDefinition{}
	all := []string{}
	for _, t := range normalized.Tasks {
		calls += t.MaxCalls
		all = append(all, t.Key)
		r := agent.RoleSpec{SchemaVersion: 1, Name: "developer", Version: version, TaskKey: t.Key, Scope: t.Scope, Profile: t.Profile, MaxCalls: t.MaxCalls}
		description := t.Description + " Acceptance: " + strings.Join(t.Acceptance, "; ")
		tasks = append(tasks, roleTask(job, r, description, t.DependsOn))
	}
	var maximum int64
	err = tx.QueryRow(ctx, `SELECT maximum_microusd FROM collab_projects WHERE root_id=$1`, root).Scan(&maximum)
	if err != nil {
		return receipt, classify(err)
	}
	// Call/token maxima must fit the family ceiling. Money is one shared
	// allowance, not fresh grants per task: admit only if the next worst-sized
	// reservation fits, and serialize/recheck every actual reservation in SQL.
	if s.agentPricing == nil || turns+calls > agent.WorkflowTurns || tokens+calls*(30000+agent.MaxOutput) > agent.WorkflowTokens || cost+s.agentPricing.Estimate(30000, agent.MaxOutput) > maximum {
		return receipt, execution.ErrCapacity
	}
	tasks = append(tasks, roleTask(job, agent.RoleSpec{SchemaVersion: 1, Name: "integration", Version: version, TaskKey: "integrate", Scope: []string{}, Profile: agent.MathProfile, MaxCalls: 1}, "Integrate all accepted source artifacts and run mandatory math-v1 tests.", all))
	tasks = append(tasks, roleTask(job, agent.RoleSpec{SchemaVersion: 1, Name: "reviewer", Version: version, TaskKey: "review", Scope: []string{}, Profile: agent.MathProfile, MaxCalls: 2}, "Review the exact integrated candidate and protected test evidence. Reject substantive findings. User request: "+job.TaskDescription, []string{"integrate"}))
	receipt, err = s.createTx(ctx, tx, fmt.Sprintf("collab:%s:v%d:execute", root, version), workflow.CreateInput{Name: fmt.Sprintf("Execution v%d", version), TimeoutMS: max(100, deadline.Sub(now).Milliseconds()), Tasks: tasks})
	if err != nil {
		return receipt, err
	}
	var base sandbox.Snapshot
	var sha string
	err = tx.QueryRow(ctx, `SELECT base_snapshot,base_sha256 FROM collab_workflows WHERE root_id=$1 AND version=$2 AND phase='planning'`, root, version).Scan(&base, &sha)
	if err != nil {
		return receipt, classify(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO collab_workflows(workflow_id,root_id,version,phase,parent_version,base_snapshot,base_sha256) VALUES($1,$2,$3,'execution',$4,$5,$6)`, receipt.ID, root, version, version-1, base, sha)
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO collab_executions(root_id,version,plan_digest,child_id) VALUES($1,$2,$3,$4)`, root, version, digest, receipt.ID)
	}
	if err == nil {
		_, err = tx.Exec(ctx, `UPDATE collab_projects SET status='EXECUTING',reason='' WHERE root_id=$1`, root)
	}
	if err != nil {
		return receipt, classify(err)
	}
	if err = mutation(ctx, tx, parent, parent.status, []eventWrite{{"project.state", map[string]any{"status": "EXECUTING", "version": version, "child_id": receipt.ID, "plan_digest": digest}}}); err != nil {
		return receipt, err
	}
	return receipt, classify(tx.Commit(ctx))
}

func (s *Store) PrepareRole(ctx context.Context, worker string, instance, id uuid.UUID, generation int64) (agent.RoleWork, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return agent.RoleWork{}, classify(err)
	}
	defer rollback(tx)
	_, a, live, err := withAttempt(ctx, tx, worker, instance, id, generation)
	if err != nil {
		return agent.RoleWork{}, err
	}
	if !live {
		return agent.RoleWork{}, execution.ErrStale
	}
	if a.Agent == nil || a.Agent.Role == nil || s.agentPricing == nil {
		return agent.RoleWork{}, workflow.ErrInvalid
	}
	var existing agent.RoleWork
	err = tx.QueryRow(ctx, `SELECT work FROM collab_attempt_inputs WHERE attempt_id=$1`, id).Scan(&existing)
	if err == nil {
		return existing, classify(tx.Commit(ctx))
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return existing, classify(err)
	}
	var root uuid.UUID
	var base sandbox.Snapshot
	var sha string
	var status string
	err = tx.QueryRow(ctx, `SELECT f.root_id,f.base_snapshot,f.base_sha256,p.status FROM collab_workflows f JOIN collab_projects p ON p.root_id=f.root_id WHERE f.workflow_id=$1`, a.WorkflowID).Scan(&root, &base, &sha, &status)
	if err != nil {
		return existing, classify(err)
	}
	if sha != agent.SnapshotDigest(base) || !agent.ControlledMath(base) || status == "CANCELLING" || status == "CANCELLED" {
		return existing, execution.ErrStale
	}
	work := agent.RoleWork{Snapshot: base, Inputs: []agent.ArtifactRef{}, Evidence: "", Error: ""}
	if err = tx.QueryRow(ctx, `SELECT base_snapshot FROM collab_workflows WHERE root_id=$1 AND version=1 AND phase='planning'`, root).Scan(&work.Origin); err != nil {
		return work, classify(err)
	}
	role := a.Agent.Role
	if role.Name != "planner" {
		var plan agent.Plan
		if err = tx.QueryRow(ctx, `SELECT body FROM collab_plans WHERE root_id=$1 AND version=$2`, root, role.Version).Scan(&plan); err != nil {
			return work, classify(err)
		}
		wanted := map[string]bool{}
		var add func(string)
		add = func(key string) {
			if wanted[key] {
				return
			}
			wanted[key] = true
			for _, t := range plan.Tasks {
				if t.Key == key {
					for _, d := range t.DependsOn {
						add(d)
					}
				}
			}
		}
		if role.Name == "developer" {
			for _, t := range plan.Tasks {
				if t.Key == role.TaskKey {
					for _, d := range t.DependsOn {
						add(d)
					}
				}
			}
		} else {
			for _, t := range plan.Tasks {
				add(t.Key)
			}
		}
		for _, key := range plan.Order() {
			if !wanted[key] {
				continue
			}
			var outcome agent.RoleOutcome
			var artifact, candidate string
			var attempt uuid.UUID
			err = tx.QueryRow(ctx, `SELECT r.body,r.artifact_sha256,r.candidate_sha256,r.attempt_id FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id JOIN tasks t ON t.id=a.task_id WHERE t.workflow_id=$1 AND t.task_key=$2 AND t.status='SUCCEEDED' AND t.current_attempt_id=a.id AND a.state='SUCCEEDED' AND r.accepted`, a.WorkflowID, key).Scan(&outcome, &artifact, &candidate, &attempt)
			if err != nil {
				return work, classify(err)
			}
			if agent.JSONSHA(outcome) != artifact || agent.SnapshotDigest(outcome.Snapshot) != candidate || outcome.Snapshot.BaseRevision != base.BaseRevision {
				return work, execution.ErrOutcome
			}
			work.Inputs = append(work.Inputs, agent.ArtifactRef{Task: key, Attempt: attempt, Digest: artifact, Input: agent.SnapshotDigest(outcome.Work.Snapshot)})
			merged, e := agent.MergeSnapshot(outcome.Work.Snapshot, work.Snapshot, outcome.Snapshot)
			if e != nil {
				work.Error = "overlap_conflict_requires_human"
				break
			}
			work.Snapshot = merged
		}
		if role.Name == "reviewer" && work.Error == "" {
			var integrated agent.RoleOutcome
			var tested string
			var accepted bool
			var candidate string
			err = tx.QueryRow(ctx, `SELECT r.body,r.test_sha256,r.accepted,r.candidate_sha256 FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id JOIN tasks t ON t.id=a.task_id WHERE t.workflow_id=$1 AND t.task_key='integrate' AND t.current_attempt_id=a.id AND a.state='SUCCEEDED'`, a.WorkflowID).Scan(&integrated, &tested, &accepted, &candidate)
			if err != nil {
				return work, classify(err)
			}
			if !accepted || tested == "" || candidate != agent.SnapshotDigest(work.Snapshot) || candidate != agent.SnapshotDigest(integrated.Snapshot) {
				return work, execution.ErrOutcome
			}
			work.Evidence = fmt.Sprintf("Authoritative immutable-helper math-v1 passed. candidate_sha256=%s test_sha256=%s\nEscaped patch data:\n%s", candidate, tested, agent.MathPatch(work.Origin, work.Snapshot))
		}
	}
	if role.Name == "planner" && role.Version > 1 {
		work.Evidence = "Correction version: inspect prior rejection; required tests/review remain mandatory."
		var prior json.RawMessage
		err = tx.QueryRow(ctx, `SELECT r.body->'review' FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id JOIN collab_workflows f ON f.workflow_id=a.workflow_id WHERE f.root_id=$1 AND f.version=$2 AND r.role='reviewer' AND r.accepted ORDER BY r.recorded_at DESC LIMIT 1`, root, role.Version-1).Scan(&prior)
		if err == nil {
			work.Evidence += " Findings (untrusted): " + string(prior)
		} else if !errors.Is(err, pgx.ErrNoRows) {
			return work, classify(err)
		}
	}
	_, err = tx.Exec(ctx, `INSERT INTO collab_attempt_inputs(attempt_id,generation,work,input_sha256) VALUES($1,$2,$3,$4)`, id, generation, work, agent.SnapshotDigest(work.Snapshot))
	if err == nil {
		_, err = tx.Exec(ctx, `INSERT INTO agent_sessions(attempt_id,generation,base_sha256,workspace_sha256,deadline_at,pricing) VALUES($1,$2,$3,$4,LEAST($5,clock_timestamp()+interval '90 seconds'),$6)`, id, generation, a.Agent.RepositoryID, agent.SnapshotDigest(work.Snapshot), a.DeadlineAt, s.agentPricing.Bytes())
	}
	if err == nil && role.Name == "integration" {
		// All integrators for the same version lock one durable operation. A
		// replacement must reproduce its immutable manifest and candidate exactly.
		if _, err = tx.Exec(ctx, `SELECT root_id FROM collab_projects WHERE root_id=$1 FOR UPDATE`, root); err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO collab_integrations(root_id,version,work,candidate_sha256) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING`, root, role.Version, work, agent.SnapshotDigest(work.Snapshot))
		}
		var matches bool
		if err == nil {
			err = tx.QueryRow(ctx, `SELECT work=$3::jsonb AND candidate_sha256=$4 FROM collab_integrations WHERE root_id=$1 AND version=$2`, root, role.Version, work, agent.SnapshotDigest(work.Snapshot)).Scan(&matches)
			if err == nil && !matches {
				err = execution.ErrOutcome
			}
		}
	}
	if err != nil {
		return work, classify(err)
	}
	return work, classify(tx.Commit(ctx))
}

func (s *Store) validateRole(ctx context.Context, tx pgx.Tx, a execution.Attempt, o agent.Outcome) error {
	if a.Agent == nil || a.Agent.Role == nil || o.Collaboration == nil {
		return workflow.ErrInvalid
	}
	role := a.Agent.Role
	detail := *o.Collaboration
	var work agent.RoleWork
	var initial string
	if err := tx.QueryRow(ctx, `SELECT work,input_sha256 FROM collab_attempt_inputs WHERE attempt_id=$1`, a.ID).Scan(&work, &initial); err != nil {
		return classify(err)
	}
	patchBase := work.Snapshot
	if role.Name == "integration" {
		patchBase = work.Origin
	}
	if agent.JSONSHA(work) != agent.JSONSHA(detail.Work) || initial != agent.SnapshotDigest(work.Snapshot) || o.Candidate != agent.SnapshotDigest(detail.Snapshot) || !agent.ScopedCandidate(work.Snapshot, detail.Snapshot, role.Scope) || !slices.Equal(o.Patch, agent.MathPatch(patchBase, detail.Snapshot)) {
		return execution.ErrOutcome
	}
	var current string
	var pendingModel, pendingTool *uuid.UUID
	var live bool
	if err := tx.QueryRow(ctx, `SELECT workspace_sha256,pending_model,pending_tool,deadline_at>clock_timestamp() FROM agent_sessions WHERE attempt_id=$1`, a.ID).Scan(&current, &pendingModel, &pendingTool, &live); err != nil {
		return classify(err)
	}
	if current != o.Candidate || pendingTool != nil || o.Success() && (pendingModel != nil || !live) {
		return execution.ErrOutcome
	}
	if o.Success() {
		switch role.Name {
		case "planner":
			if detail.Plan == nil || detail.Review != nil {
				return workflow.ErrInvalid
			}
			normalized, e := agent.NormalizePlan(*detail.Plan, a.Agent.Job)
			if e != nil || agent.JSONSHA(normalized) != agent.JSONSHA(detail.Plan) {
				return workflow.ErrInvalid
			}
			var root uuid.UUID
			if err := tx.QueryRow(ctx, `SELECT root_id FROM collab_workflows WHERE workflow_id=$1`, a.WorkflowID).Scan(&root); err != nil {
				return classify(err)
			}
			if _, e = tx.Exec(ctx, `INSERT INTO collab_plans(root_id,version,schema_version,digest,body,planner_attempt,policy) VALUES($1,$2,1,$3,$4,$5,'collaborative-go-v1')`, root, role.Version, agent.JSONSHA(normalized), normalized, a.ID); e != nil {
				return classify(e)
			}
		case "developer", "integration":
			if o.Test == nil || o.Test.Status != "exited" || o.Test.ExitCode != 0 || o.TestDigest != agent.JSONSHA(o.Test) {
				return execution.ErrOutcome
			}
			var proved bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM agent_steps WHERE attempt_id=$1 AND request_id=$2 AND phase='end' AND name='run_test_profile' AND status='passed' AND workspace_sha256=$3 AND after_workspace_sha256=$3 AND output_sha256=$4)`, a.ID, o.TestID, o.Candidate, o.TestDigest).Scan(&proved); err != nil {
				return classify(err)
			}
			if !proved {
				return execution.ErrOutcome
			}
			proof := false
			for _, art := range o.Test.Artifacts {
				if art.Kind == "test_result" {
					var v struct {
						Profile   string `json:"profile"`
						Candidate string `json:"candidate_sha256"`
						Success   bool   `json:"success"`
						Authority string `json:"authority"`
					}
					_ = json.Unmarshal(art.Body, &v)
					proof = v.Profile == role.Profile && v.Candidate == o.Candidate && v.Success && v.Authority == "immutable-helper"
				}
			}
			if !proof {
				return execution.ErrOutcome
			}
		case "reviewer":
			if detail.Review == nil || !detail.Review.Valid(o.Candidate) || detail.Plan != nil {
				return execution.ErrOutcome
			}
			var proved bool
			if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM collab_results r JOIN task_attempts a ON a.id=r.attempt_id JOIN tasks t ON t.id=a.task_id WHERE t.workflow_id=$1 AND t.task_key='integrate' AND t.current_attempt_id=a.id AND a.state='SUCCEEDED' AND r.accepted AND r.candidate_sha256=$2 AND r.test_sha256<>'')`, a.WorkflowID, o.Candidate).Scan(&proved); err != nil {
				return classify(err)
			}
			if !proved {
				return execution.ErrOutcome
			}
		}
	}
	_, err := tx.Exec(ctx, `INSERT INTO collab_results(attempt_id,generation,role,accepted,body,artifact_sha256,candidate_sha256,test_sha256) VALUES($1,$2,$3,$4,$5,$6,$7,$8)`, a.ID, a.Generation, role.Name, o.Success(), detail, agent.JSONSHA(detail), o.Candidate, o.TestDigest)
	return classify(err)
}

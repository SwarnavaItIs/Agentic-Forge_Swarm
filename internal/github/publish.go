package github

import (
	"context"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/publication"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type Result struct {
	Status   string
	Reason   string
	Head     string
	PRNumber int
	URL      string
}
type pull struct {
	Number  int
	HTMLURL string `json:"html_url"`
	Body    string
	Draft   bool
	State   string
	Head    struct {
		Ref  string
		SHA  string
		Repo struct{ ID int64 }
	}
	Base struct {
		Ref  string
		SHA  string
		Repo struct{ ID int64 }
	}
}

func (c *Client) observedURL(p pull) string {
	r := c.Repository()
	if p.Number > 0 && p.HTMLURL == "https://github.com/"+r.Owner+"/"+r.Name+"/pull/"+strconv.Itoa(p.Number) {
		return p.HTMLURL
	}
	return ""
}
func (c *Client) validPull(p pull, o publication.Operation) bool {
	r := o.Binding.Repository
	return p.Number > 0 && p.HTMLURL == "https://github.com/"+r.Owner+"/"+r.Name+"/pull/"+strconv.Itoa(p.Number) && p.Draft && p.State == "open" && p.Head.Ref == o.Branch && p.Head.SHA == o.CommitSHA() && p.Head.Repo.ID == r.ID && p.Base.Ref == r.Target && p.Base.SHA == o.Binding.BaseCommit && p.Base.Repo.ID == r.ID && strings.Contains("\n"+p.Body+"\n", "\n"+o.Marker()+"\n")
}
func (c *Client) pulls(ctx context.Context, o publication.Operation) ([]pull, error) {
	var values []pull
	q := url.Values{"state": {"all"}, "head": {o.Binding.Repository.Owner + ":" + o.Branch}, "base": {o.Binding.Repository.Target}, "per_page": {"10"}}
	e := c.call(ctx, "GET", c.prefix()+"/pulls?"+q.Encode(), nil, &values)
	if len(values) > 1 && e == nil {
		e = &Error{Class: "pull_conflict"}
	}
	return values, e
}

// Publish first discovers remote resources. check is a fresh durable approval/
// cancellation fence before EVERY write. The read-only mode still reconciles
// uncertain effects after expiry/cancellation; it never creates new resources.
func (c *Client) Publish(ctx context.Context, o publication.Operation, check func(context.Context) error) (Result, error) {
	result := Result{Status: "failed", Reason: "invalid_intent"}
	b := o.Binding
	if o.ID == [16]byte{} || b.Repository != c.Repository() || b.Policy != c.Policy() || b.Action != publication.Action || b.TestDemo != c.TestDemo() || agent.SnapshotDigest(o.Snapshot) != b.Candidate || publication.TreeSHA(o.Snapshot) != b.CandidateTree || o.Branch != "forgeswarm/op-"+o.ID.String() {
		return result, &Error{Class: "intent_conflict"}
	}
	if e := c.Metadata(ctx); e != nil {
		return result, e
	}
	target, e := c.Ref(ctx, b.Repository.Target)
	if e != nil {
		return result, e
	}
	head, e := c.Ref(ctx, o.Branch)
	exists := e == nil
	if e != nil && !IsClass(e, "not_found") {
		return result, e
	}
	if exists && head != o.CommitSHA() {
		return result, &Error{Class: "branch_conflict"}
	}
	if exists {
		commit, e := c.commit(ctx, head)
		if e != nil {
			return result, e
		}
		if commit.Tree.SHA != b.CandidateTree || commit.Message != o.Message() || len(commit.Parents) != 1 || commit.Parents[0].SHA != b.BaseCommit {
			return result, &Error{Class: "branch_conflict"}
		}
		result.Head = head
	}
	prs, e := c.pulls(ctx, o)
	if e != nil {
		return result, e
	}
	if len(prs) == 1 {
		p := prs[0]
		result.Head = head
		result.PRNumber = p.Number
		result.URL = c.observedURL(p)
		if !exists || !c.validPull(p, o) {
			result.Status = "needs_review"
			result.Reason = "existing_pull_mismatch"
			return result, nil
		}
		result.Status = "confirmed"
		result.Reason = "reconciled_existing_draft"
		if target != b.BaseCommit {
			result.Status = "needs_review"
			result.Reason = "target_drift_after_publication"
		}
		// Re-read target after inspection; the two HTTP calls cannot be atomic.
		now, err := c.Ref(ctx, b.Repository.Target)
		if err != nil {
			return result, err
		}
		if now != b.BaseCommit {
			result.Status = "needs_review"
			result.Reason = "target_drift_after_publication"
		}
		return result, nil
	}
	if target != b.BaseCommit {
		result.Status = "needs_review"
		result.Reason = "target_drift_requires_new_approval"
		return result, nil
	}
	if e = check(ctx); e != nil {
		result.Status = "cancelled"
		result.Reason = "approval_not_current_no_new_effect"
		return result, nil
	}
	if !exists {
		tree := []map[string]string{}
		// All bounded root blobs, including unchanged files, determine exact Git tree.
		for path, body := range o.Snapshot.Files {
			tree = append(tree, map[string]string{"path": path, "mode": "100644", "type": "blob", "content": string(body)})
		}
		var created struct{ SHA string }
		if e = c.write(ctx, c.prefix()+"/git/trees", map[string]any{"base_tree": b.BaseTree, "tree": tree}, &created, check); e != nil {
			return result, e
		}
		if created.SHA != b.CandidateTree {
			return result, &Error{Class: "tree_conflict"}
		}
		if e = check(ctx); e != nil {
			result.Status = "cancelled"
			result.Reason = "approval_not_current_no_branch"
			return result, nil
		}
		person := map[string]string{"name": "ForgeSwarm", "email": "forgeswarm@localhost", "date": o.CreatedAt.UTC().Truncate(time.Second).Format(time.RFC3339)}
		if e = c.write(ctx, c.prefix()+"/git/commits", map[string]any{"tree": b.CandidateTree, "parents": []string{b.BaseCommit}, "message": o.Message(), "author": person, "committer": person}, &created, check); e != nil {
			return result, e
		}
		if created.SHA != o.CommitSHA() {
			return result, &Error{Class: "commit_conflict"}
		}
		target, e = c.Ref(ctx, b.Repository.Target)
		if e != nil {
			return result, e
		}
		if target != b.BaseCommit {
			result.Status = "needs_review"
			result.Reason = "target_drift_before_branch"
			return result, nil
		}
		if e = check(ctx); e != nil {
			result.Status = "cancelled"
			result.Reason = "approval_not_current_no_branch"
			return result, nil
		}
		var ref any
		if e = c.write(ctx, c.prefix()+"/git/refs", map[string]string{"ref": "refs/heads/" + o.Branch, "sha": o.CommitSHA()}, &ref, check); e != nil {
			return result, e
		}
		// No update-ref/PATCH/force capability exists in this adapter.
		head, e = c.Ref(ctx, o.Branch)
		if e != nil {
			return result, e
		}
		if head != o.CommitSHA() {
			return result, &Error{Class: "branch_conflict"}
		}
		result.Head = head
	}
	target, e = c.Ref(ctx, b.Repository.Target)
	if e != nil {
		return result, e
	}
	if target != b.BaseCommit {
		result.Status = "needs_review"
		result.Reason = "target_drift_before_pull"
		return result, nil
	}
	if e = check(ctx); e != nil {
		result.Status = "cancelled"
		result.Reason = "approval_not_current_branch_may_exist"
		return result, nil
	}
	body := o.Marker() + "\n\nReviewed candidate: " + b.Candidate + "\nPatch SHA-256: " + b.Patch + "\nProtected math-v1 test evidence: " + b.Test + "\nRead-only review evidence: " + b.Review + "\nPinned base: " + b.BaseCommit + "\n\nControlled pure-function fixture; scripted TEST/DEMO where configured. No live-model quality, production readiness or comparative performance claim. Human review required; no automatic merge, deployment or migration.\n"
	var p pull
	if e = c.write(ctx, c.prefix()+"/pulls", map[string]any{"title": "ForgeSwarm reviewed change " + o.ID.String(), "head": o.Branch, "base": b.Repository.Target, "draft": true, "body": body}, &p, check); e != nil {
		return result, e
	}
	result.PRNumber = p.Number
	result.URL = c.observedURL(p)
	if !c.validPull(p, o) {
		result.Status = "needs_review"
		result.Reason = "created_pull_precondition_mismatch"
		return result, nil
	}
	// Inspect observed result, including drift during the external write window.
	observed, e := c.pulls(ctx, o)
	if e != nil {
		return result, e
	}
	target, e = c.Ref(ctx, b.Repository.Target)
	if e != nil {
		return result, e
	}
	result.Status = "confirmed"
	result.Reason = "draft_created"
	if target != b.BaseCommit || len(observed) != 1 || !c.validPull(observed[0], o) {
		result.Status = "needs_review"
		result.Reason = "target_or_pull_drift_after_publication"
	}
	return result, nil
}

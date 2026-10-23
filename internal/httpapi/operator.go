package httpapi

import (
	"context"
	"errors"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/github"
	"forgeswarm/internal/operator"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/publication"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"net/http"
	"sync"
	"time"
)

func (a *API) WithOperator(auth *operator.Auth) *API { a.operatorAuth = auth; return a }
func (a *API) WithGitHub(client *github.Client) *API { a.github = client; return a }
func (a *API) authenticate(w http.ResponseWriter, r *http.Request) (*http.Request, bool) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.Path == "/v1/session" || r.URL.Path == "/v1/github/webhook" || len(r.URL.Path) < 4 || r.URL.Path[:4] != "/v1/" {
		return r, true
	}
	if a.operatorAuth == nil {
		if store, ok := a.service.(interface {
			RequiresOperator(context.Context) (bool, error)
		}); ok {
			required, err := store.RequiresOperator(r.Context())
			if err != nil || required {
				a.fail(w, r, 503, "operator_auth_configuration_required", "Operator authentication must be configured for this database.")
				return r, false
			}
		}
		return r, true
	}
	p, _, ok := a.operatorAuth.Authenticate(r)
	if !ok {
		a.fail(w, r, 401, "authentication_required", "Operator or read-only authentication required.")
		return r, false
	}
	if r.Method != "GET" && r.Method != "HEAD" && !p.IsOperator() {
		a.fail(w, r, 403, "operator_required", "Operator permission required.")
		return r, false
	}
	return r.WithContext(operator.WithPrincipal(r.Context(), p)), true
}
func (a *API) session(w http.ResponseWriter, r *http.Request) {
	if a.operatorAuth == nil {
		if r.Method == "GET" {
			writeJSON(w, 200, map[string]any{"enabled": false, "role": "local_operator"})
			return
		}
		a.fail(w, r, 503, "auth_disabled", "Authentication is not configured.")
		return
	}
	if r.Method == "POST" {
		body, err := readBody(w, r)
		if err != nil {
			a.bodyError(w, r, err)
			return
		}
		var input struct {
			Token string `json:"token"`
		}
		if agent.StrictDecode(body, &input, 1024) != nil {
			a.serviceError(w, r, workflow.ErrInvalid)
			return
		}
		id, csrf, p, err := a.operatorAuth.Login(r, input.Token)
		if err != nil {
			a.fail(w, r, 401, "authentication_required", "Authentication or same-origin check failed.")
			return
		}
		http.SetCookie(w, operator.Cookie(id, r.TLS != nil))
		writeJSON(w, 200, map[string]any{"enabled": true, "actor": p.Actor, "role": p.Role, "csrf": csrf})
		return
	}
	p, csrf, ok := a.operatorAuth.Authenticate(r)
	if !ok {
		a.fail(w, r, 401, "authentication_required", "Sign in required.")
		return
	}
	if r.Method == "DELETE" {
		a.operatorAuth.Logout(r)
		cookie := operator.Cookie("", r.TLS != nil)
		cookie.MaxAge = -1
		http.SetCookie(w, cookie)
		writeJSON(w, 200, map[string]bool{"signed_out": true})
		return
	}
	if r.Method != "GET" {
		a.fail(w, r, 405, "method_not_allowed", "GET, POST or DELETE required.")
		return
	}
	writeJSON(w, 200, map[string]any{"enabled": true, "actor": p.Actor, "role": p.Role, "csrf": csrf})
}

type approvalsStore interface {
	Approvals(context.Context, uuid.UUID, uuid.UUID) ([]postgres.Approval, error)
	Approval(context.Context, uuid.UUID) (postgres.Approval, error)
	DecideApproval(context.Context, uuid.UUID, operator.Principal, publication.Decision) (postgres.Approval, error)
	Imported(context.Context, string, string) (workflow.Receipt, error)
	ImportGitHub(context.Context, string, string, string, publication.Prepared) (workflow.Receipt, error)
}

func (a *API) approvals(w http.ResponseWriter, r *http.Request) {
	store, ok := a.service.(approvalsStore)
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	root, after := uuid.Nil, uuid.Nil
	var err error
	for k, v := range r.URL.Query() {
		if len(v) != 1 || (k != "root" && k != "after") {
			a.serviceError(w, r, workflow.ErrInvalid)
			return
		}
		id, e := uuid.Parse(v[0])
		if e != nil {
			err = e
		}
		if k == "root" {
			root = id
		} else {
			after = id
		}
	}
	if err != nil {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	items, err := store.Approvals(r.Context(), root, after)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}
func (a *API) approval(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	store, ok := a.service.(approvalsStore)
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	if r.URL.RawQuery != "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	var result postgres.Approval
	var err error
	if r.Method == "GET" {
		result, err = store.Approval(r.Context(), id)
	} else if r.Method == "POST" {
		p := operator.Identity(r.Context())
		if !p.IsOperator() {
			a.fail(w, r, 403, "operator_required", "Authenticated operator required.")
			return
		}
		body, e := readBody(w, r)
		if e != nil {
			a.bodyError(w, r, e)
			return
		}
		var decision publication.Decision
		if agent.StrictDecode(body, &decision, 1024) != nil {
			a.serviceError(w, r, workflow.ErrInvalid)
			return
		}
		result, err = store.DecideApproval(r.Context(), id, p, decision)
	} else {
		a.fail(w, r, 405, "method_not_allowed", "GET or POST required.")
		return
	}
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}

// One bounded rate window and two preparation slots, with no per-issue cache.
type importLimit struct {
	mu     sync.Mutex
	at     time.Time
	count  int
	active int
}

func (l *importLimit) enter() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if time.Since(l.at) > time.Minute {
		l.at = time.Now()
		l.count = 0
	}
	if l.count >= 30 || l.active >= 2 {
		return false
	}
	l.count++
	l.active++
	return true
}
func (l *importLimit) leave() { l.mu.Lock(); l.active--; l.mu.Unlock() }
func (a *API) importIssue(w http.ResponseWriter, r *http.Request) {
	if a.github == nil || a.operatorAuth == nil {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	if !operator.Identity(r.Context()).IsOperator() {
		a.fail(w, r, 403, "operator_required", "Authenticated operator required.")
		return
	}
	if !a.creationHeaders(w, r) {
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		a.bodyError(w, r, err)
		return
	}
	var input struct {
		Number int `json:"issue_number"`
	}
	if agent.StrictDecode(body, &input, 128) != nil {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	a.prepareIssue(w, r, "api:"+r.Header.Get("Idempotency-Key"), agent.SHA(body), operator.Identity(r.Context()).Actor, input.Number)
}
func (a *API) webhook(w http.ResponseWriter, r *http.Request) {
	if a.github == nil || a.operatorAuth == nil {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		a.bodyError(w, r, err)
		return
	}
	for _, header := range []string{"X-Hub-Signature-256", "X-GitHub-Delivery", "X-GitHub-Event"} {
		if len(r.Header.Values(header)) != 1 {
			a.fail(w, r, 400, "invalid_webhook", "One delivery, event and signature required.")
			return
		}
	}
	delivery, err := uuid.Parse(r.Header.Get("X-GitHub-Delivery"))
	if err != nil || delivery == uuid.Nil || len(r.Header.Get("X-GitHub-Delivery")) != 36 {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	number, actor, err := a.github.Webhook(body, r.Header.Get("X-Hub-Signature-256"), r.Header.Get("X-GitHub-Event"))
	if err != nil {
		a.fail(w, r, 403, "invalid_webhook", "Signature or trigger policy rejected.")
		return
	}
	a.prepareIssue(w, r, "webhook:"+delivery.String(), agent.SHA(body), actor, number)
}
func (a *API) prepareIssue(w http.ResponseWriter, r *http.Request, delivery, digest, actor string, number int) {
	store, ok := a.service.(approvalsStore)
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	if !a.imports.enter() {
		w.Header().Set("Retry-After", "60")
		a.fail(w, r, 429, "import_saturated", "Issue import limit reached.")
		return
	}
	defer a.imports.leave()
	existing, err := store.Imported(r.Context(), delivery, digest)
	if err == nil {
		writeJSON(w, 202, existing)
		return
	}
	if !errors.Is(err, workflow.ErrNotFound) {
		a.serviceError(w, r, err)
		return
	}
	prepared, err := a.github.Prepare(r.Context(), number)
	if err != nil {
		var remote *github.Error
		if errors.As(err, &remote) && remote.Class == "rate_limit" {
			w.Header().Set("Retry-After", "60")
			a.fail(w, r, 429, "github_rate_limit", "GitHub rate limit; retry this delivery safely.")
			return
		}
		a.fail(w, r, 422, "github_preparation_rejected", "GitHub input, policy or dependency could not be verified.")
		return
	}
	if a.collaborationRegistry == "" {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	if _, err = sandbox.SaveSnapshot(a.collaborationRegistry, prepared.Snapshot); err != nil {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	receipt, err := store.ImportGitHub(r.Context(), delivery, digest, actor, prepared)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/projects/"+receipt.ID.String())
	writeJSON(w, 202, receipt)
}

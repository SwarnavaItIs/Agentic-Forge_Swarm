package httpapi

import (
	"context"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"net/http"
	"strconv"
)

type dashboardStore interface {
	ListWorkflows(context.Context, uuid.UUID, int) (postgres.WorkflowPage, error)
	Usage(context.Context, uuid.UUID) (postgres.UsageSummary, error)
	Candidates(context.Context, uuid.UUID) ([]postgres.CandidateEvidence, error)
}
type projectStore interface {
	CreateProject(context.Context, string, agent.Job, sandbox.Snapshot, int) (workflow.Receipt, error)
	FinalizePlan(context.Context, uuid.UUID, int, string) (workflow.Receipt, error)
	Project(context.Context, uuid.UUID) (postgres.ProjectDetails, error)
}

func (a *API) WithCollaboration(registry string, rounds int) *API {
	a.collaborationRegistry = registry
	a.correctionRounds = rounds
	return a
}
func (a *API) index(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		a.method(http.MethodPost, a.create)(w, r)
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET, POST")
		a.fail(w, r, 405, "method_not_allowed", "Method not allowed.")
		return
	}
	store, ok := a.service.(dashboardStore)
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	query := r.URL.Query()
	if len(query) > 2 {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	for k, v := range query {
		if (k != "after" && k != "limit") || len(v) != 1 {
			a.serviceError(w, r, workflow.ErrInvalid)
			return
		}
	}
	after := uuid.Nil
	var err error
	if query.Get("after") != "" {
		after, err = uuid.Parse(query.Get("after"))
	}
	limit := 20
	if query.Get("limit") != "" {
		limit, err = strconv.Atoi(query.Get("limit"))
	}
	if err != nil || limit < 1 || limit > 25 {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	result, err := store.ListWorkflows(r.Context(), after, limit)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}
func (a *API) usage(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	if r.URL.RawQuery != "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	store, ok := a.service.(dashboardStore)
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	value, err := store.Usage(r.Context(), id)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, value)
}
func (a *API) candidates(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	if r.URL.RawQuery != "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	store, ok := a.service.(dashboardStore)
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	if _, err := a.service.Details(r.Context(), id); err != nil {
		a.serviceError(w, r, err)
		return
	}
	value, err := store.Candidates(r.Context(), id)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, value)
}
func (a *API) projectCreate(w http.ResponseWriter, r *http.Request) {
	if !a.creationHeaders(w, r) {
		return
	}
	store, ok := a.service.(projectStore)
	if !ok || a.collaborationRegistry == "" {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		a.bodyError(w, r, err)
		return
	}
	var job agent.Job
	if agent.StrictDecode(body, &job, 4096) != nil || job.TestProfile != agent.MathProfile {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	registered, err := agent.Lookup(a.agentCatalog, job.RepositoryID)
	if err != nil || registered.BaseRevision != job.BaseRevision || registered.TestProfile != agent.MathProfile {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	base, err := sandbox.LoadSnapshot(a.collaborationRegistry, job.RepositoryID)
	if err != nil {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	receipt, err := store.CreateProject(r.Context(), r.Header.Get("Idempotency-Key"), job, base, a.correctionRounds)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/projects/"+receipt.ID.String())
	writeJSON(w, 202, receipt)
}
func (a *API) project(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	if r.URL.RawQuery != "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	store, ok := a.service.(projectStore)
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	value, err := store.Project(r.Context(), id)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, value)
}
func (a *API) finalize(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	version, err := strconv.Atoi(r.PathValue("version"))
	if err != nil || r.URL.RawQuery != "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		a.bodyError(w, r, err)
		return
	}
	var in struct {
		Digest string `json:"digest"`
	}
	if agent.StrictDecode(body, &in, 128) != nil {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	store, ok := a.service.(projectStore)
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	value, err := store.FinalizePlan(r.Context(), id, version, in.Digest)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 202, value)
}

package httpapi

import (
	"forgeswarm/internal/agent"
	"forgeswarm/internal/workflow"
	"mime"
	"net/http"
)

func (a *API) creationHeaders(w http.ResponseWriter, r *http.Request) bool {
	types := r.Header.Values("Content-Type")
	if len(types) != 1 {
		a.fail(w, r, 415, "unsupported_media_type", "Content-Type must be application/json.")
		return false
	}
	media, _, err := mime.ParseMediaType(types[0])
	if err != nil || media != "application/json" {
		a.fail(w, r, 415, "unsupported_media_type", "Content-Type must be application/json.")
		return false
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || !workflow.ValidCreationKey(keys[0]) {
		a.fail(w, r, 400, "invalid_idempotency_key", "A single valid Idempotency-Key is required.")
		return false
	}
	return true
}

// WithAgentCatalog enables jobs using a trusted local registration directory.
// HTTP never accepts host paths or creates catalog entries.
func (a *API) WithAgentCatalog(catalog string) *API { a.agentCatalog = catalog; return a }
func (a *API) job(w http.ResponseWriter, r *http.Request) {
	if !a.creationHeaders(w, r) {
		return
	}
	body, err := readBody(w, r)
	if err != nil {
		a.bodyError(w, r, err)
		return
	}
	var job agent.Job
	if agent.StrictDecode(body, &job, 4096) != nil || !job.Valid() {
		a.fail(w, r, 400, "invalid_request", "Invalid controlled job.")
		return
	}
	registered, err := agent.Lookup(a.agentCatalog, job.RepositoryID)
	if err != nil || registered.BaseRevision != job.BaseRevision || registered.TestProfile != job.TestProfile {
		a.fail(w, r, 400, "invalid_request", "Repository revision/profile is not registered.")
		return
	}
	input := agent.Input{Job: job}
	sandboxInput := input.Sandbox()
	receipt, err := a.service.Create(r.Context(), r.Header.Get("Idempotency-Key"), workflow.CreateInput{Name: "controlled coding job", Tasks: []workflow.TaskDefinition{{Key: "code", Kind: "agent_go", Sandbox: &sandboxInput, Agent: &input, TimeoutMS: 120000, MaxAttempts: 3}}, TimeoutMS: 180000})
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/workflows/"+receipt.ID.String())
	writeJSON(w, http.StatusAccepted, receipt)
}

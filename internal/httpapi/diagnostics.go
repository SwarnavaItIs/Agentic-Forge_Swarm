package httpapi

import (
	"context"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/telemetry/dbtrace"
	"net/http"
	"runtime"
)

// Operational observations contain no SQL, source, credentials or identifiers.
// Authentication is mandatory even when legacy local workflows are unprotected.
func (a *API) diagnostics(w http.ResponseWriter, r *http.Request) {
	if a.operatorAuth == nil {
		a.fail(w, r, 503, "operator_auth_configuration_required", "Configure operator authentication.")
		return
	}
	if r.URL.RawQuery != "" {
		a.fail(w, r, 400, "invalid_query", "No query parameters supported.")
		return
	}
	store, ok := a.service.(interface {
		Diagnostics(context.Context) (postgres.Diagnostics, error)
	})
	if !ok {
		a.fail(w, r, 503, "unavailable", "Observations unavailable.")
		return
	}
	value, err := store.Diagnostics(r.Context())
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	writeJSON(w, 200, map[string]any{"schema_version": 1, "database": value, "queries": dbtrace.DatabaseSnapshot(), "goroutines": runtime.NumGoroutine(), "heap_bytes": memory.HeapAlloc, "heap_inuse_bytes": memory.HeapInuse})
}

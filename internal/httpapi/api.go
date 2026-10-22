package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
)

const MaxBodyBytes = 64 << 10

type API struct {
	service                      workflow.Service
	logger                       *slog.Logger
	requestTimeout, readyTimeout time.Duration
	stopping                     atomic.Bool
	agentCatalog                 string
	collaborationRegistry        string
	correctionRounds             int
	streams                      atomic.Int32
}

func New(service workflow.Service, logger *slog.Logger, requestTimeout, readyTimeout time.Duration) *API {
	return &API{service: service, logger: logger, requestTimeout: requestTimeout, readyTimeout: readyTimeout}
}

func (a *API) Stop() { a.stopping.Store(true) }

type requestInfo struct{ id, workflowID, errorClass string }
type infoKey struct{}

func info(r *http.Request) *requestInfo { return r.Context().Value(infoKey{}).(*requestInfo) }

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *responseWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *responseWriter) Write(b []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(b)
}

func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health/live", a.method(http.MethodGet, a.live))
	mux.HandleFunc("/health/ready", a.method(http.MethodGet, a.ready))
	mux.HandleFunc("/v1/workflows", a.index)
	mux.HandleFunc("/v1/projects", a.method(http.MethodPost, a.projectCreate))
	mux.HandleFunc("/v1/projects/{id}", a.method(http.MethodGet, a.project))
	mux.HandleFunc("/v1/projects/{id}/plans/{version}/finalize", a.method(http.MethodPost, a.finalize))
	mux.HandleFunc("/v1/workflows/{id}/summary", a.method(http.MethodGet, a.usage))
	mux.HandleFunc("/v1/workflows/{id}/candidates", a.method(http.MethodGet, a.candidates))
	mux.HandleFunc("/v1/workflows/{id}/stream", a.method(http.MethodGet, a.stream))
	mux.HandleFunc("/v1/jobs", a.method(http.MethodPost, a.job))
	mux.HandleFunc("/v1/workflows/{id}", a.method(http.MethodGet, a.details))
	mux.HandleFunc("/v1/workflows/{id}/cancel", a.method(http.MethodPost, a.cancel))
	mux.HandleFunc("/v1/workflows/{id}/graph", a.method(http.MethodGet, a.graph))
	mux.HandleFunc("/v1/workflows/{id}/events", a.method(http.MethodGet, a.events))
	mux.HandleFunc("/v1/workflows/{id}/tasks/{task_id}", a.method(http.MethodGet, a.task))
	mux.HandleFunc("/v1/workflows/{id}/tasks/{task_id}/attempts", a.method(http.MethodGet, a.attempts))
	mux.HandleFunc("/v1/workflows/{id}/artifacts/{artifact_id}", a.method(http.MethodGet, a.artifact))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { a.fail(w, r, 404, "not_found", "Resource not found.") })
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		meta := &requestInfo{id: uuid.NewString()}
		r = r.WithContext(context.WithValue(r.Context(), infoKey{}, meta))
		timeout := a.requestTimeout
		if strings.HasSuffix(r.URL.Path, "/stream") {
			timeout = 35 * time.Second
		}
		ctx, cancel := context.WithTimeout(r.Context(), timeout)
		defer cancel()
		r = r.WithContext(ctx)
		w.Header().Set("X-Request-ID", meta.id)
		writer := &responseWriter{ResponseWriter: w}
		defer func() {
			if recover() != nil {
				a.fail(writer, r, 500, "internal_error", "An internal error occurred.")
			}
			route := r.Pattern
			if route == "" || route == "/" {
				route = "unmatched"
			}
			attrs := []any{"request_id", meta.id, "route", route, "method", r.Method, "duration_ms", time.Since(start).Milliseconds(), "status", writer.status}
			if meta.workflowID != "" {
				attrs = append(attrs, "workflow_id", meta.workflowID)
			}
			if meta.errorClass != "" {
				attrs = append(attrs, "error_class", meta.errorClass)
			}
			a.logger.InfoContext(r.Context(), "http_request", attrs...)
		}()
		mux.ServeHTTP(writer, r)
	})
}

func (a *API) method(method string, handler http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != method {
			w.Header().Set("Allow", method)
			a.fail(w, r, 405, "method_not_allowed", "Method not allowed.")
			return
		}
		if a.stopping.Load() && r.URL.Path != "/health/live" {
			a.fail(w, r, 503, "unavailable", "Service is shutting down.")
			return
		}
		handler(w, r)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func (a *API) fail(w http.ResponseWriter, r *http.Request, status int, code, message string) {
	info(r).errorClass = code
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message, "request_id": info(r).id}})
}

func (a *API) serviceError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, workflow.ErrSaturated):
		w.Header().Set("Retry-After", "1")
		a.fail(w, r, 429, "admission_saturated", "New-work capacity is temporarily full; retry safely.")
	case errors.Is(err, workflow.ErrInvalid):
		a.fail(w, r, 400, "invalid_request", "Invalid request.")
	case errors.Is(err, workflow.ErrNotFound):
		a.fail(w, r, 404, "workflow_not_found", "Workflow not found.")
	case errors.Is(err, workflow.ErrConflict):
		a.fail(w, r, 409, "idempotency_conflict", "Idempotency key was used with different input.")
	case errors.Is(err, workflow.ErrUnavailable), errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		a.fail(w, r, 503, "unavailable", "Service temporarily unavailable; retry safely.")
	default:
		a.fail(w, r, 500, "internal_error", "An internal error occurred.")
	}
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	r.Body = http.MaxBytesReader(w, r.Body, MaxBodyBytes)
	return io.ReadAll(r.Body)
}

func (a *API) bodyError(w http.ResponseWriter, r *http.Request, err error) {
	var tooLarge *http.MaxBytesError
	if errors.As(err, &tooLarge) {
		a.fail(w, r, 413, "body_too_large", "Body exceeds 64 KiB.")
		return
	}
	if r.Context().Err() != nil {
		a.serviceError(w, r, r.Context().Err())
		return
	}
	var timeout interface{ Timeout() bool }
	if errors.As(err, &timeout) && timeout.Timeout() {
		a.serviceError(w, r, context.DeadlineExceeded)
		return
	}
	a.fail(w, r, 400, "invalid_request", "Invalid request body.")
}

// Inspect tokens before typed decoding: encoding/json otherwise accepts duplicate
// keys and case-insensitive field aliases. Reject both at the wire boundary.
func objectKeys(dec *json.Decoder, allowed map[string]bool) error {
	token, err := dec.Token()
	if err != nil || token != json.Delim('{') {
		return workflow.ErrInvalid
	}
	seen := map[string]bool{}
	for dec.More() {
		token, err = dec.Token()
		if err != nil {
			return workflow.ErrInvalid
		}
		key, ok := token.(string)
		if !ok || seen[key] || !allowed[key] {
			return workflow.ErrInvalid
		}
		seen[key] = true
		if key == "tasks" {
			token, err = dec.Token()
			if err != nil || token != json.Delim('[') {
				return workflow.ErrInvalid
			}
			for dec.More() {
				if err := objectKeys(dec, map[string]bool{"key": true, "kind": true, "depends_on": true, "delay_ms": true, "timeout_ms": true, "max_attempts": true, "sandbox": true}); err != nil {
					return err
				}
			}
			token, err = dec.Token()
			if err != nil || token != json.Delim(']') {
				return workflow.ErrInvalid
			}
		} else if key == "sandbox" {
			if err := objectKeys(dec, map[string]bool{"schema_version": true, "fixture_id": true, "command": true}); err != nil {
				return err
			}
		} else if key == "depends_on" {
			token, err = dec.Token()
			if err != nil || token != json.Delim('[') {
				return workflow.ErrInvalid
			}
			for dec.More() {
				token, err = dec.Token()
				if err != nil {
					return workflow.ErrInvalid
				}
				if _, ok := token.(string); !ok {
					return workflow.ErrInvalid
				}
			}
			token, err = dec.Token()
			if err != nil || token != json.Delim(']') {
				return workflow.ErrInvalid
			}
		} else if key == "delay_ms" || key == "timeout_ms" || key == "max_attempts" || key == "schema_version" {
			token, err = dec.Token()
			if err != nil {
				return workflow.ErrInvalid
			}
			if _, ok := token.(float64); !ok {
				return workflow.ErrInvalid
			}
		} else {
			token, err = dec.Token()
			if err != nil {
				return workflow.ErrInvalid
			}
			if _, ok := token.(string); !ok {
				return workflow.ErrInvalid
			}
		}
	}
	token, err = dec.Token()
	if err != nil || token != json.Delim('}') {
		return workflow.ErrInvalid
	}
	return nil
}

// Small local artifact bodies are returned as bounded JSON/base64; never render
// repository-controlled HTML, infer executable types or accept a host path.
func (a *API) artifact(w http.ResponseWriter, r *http.Request) {
	wf, ok := a.id(w, r)
	if !ok {
		return
	}
	text := r.PathValue("artifact_id")
	id, err := uuid.Parse(text)
	if err != nil || len(text) != 36 || id == uuid.Nil || r.URL.RawQuery != "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	store, ok := a.service.(interface {
		Artifact(context.Context, uuid.UUID, uuid.UUID) (sandbox.ArtifactRecord, error)
	})
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	value, err := store.Artifact(r.Context(), wf, id)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	w.Header().Set("X-Content-Type-Options", "nosniff")
	writeJSON(w, 200, value)
}

func decodeCreate(data []byte) (workflow.CreateInput, error) {
	if !utf8.Valid(data) {
		return workflow.CreateInput{}, workflow.ErrInvalid
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	if err := objectKeys(dec, map[string]bool{"name": true, "tasks": true, "timeout_ms": true}); err != nil {
		return workflow.CreateInput{}, err
	}
	if _, err := dec.Token(); err != io.EOF {
		return workflow.CreateInput{}, workflow.ErrInvalid
	}
	var input workflow.CreateInput
	dec = json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&input); err != nil {
		return workflow.CreateInput{}, workflow.ErrInvalid
	}
	return workflow.Normalize(input)
}

func (a *API) create(w http.ResponseWriter, r *http.Request) {
	types := r.Header.Values("Content-Type")
	if len(types) != 1 {
		a.fail(w, r, 415, "unsupported_media_type", "Content-Type must be application/json.")
		return
	}
	media, _, err := mime.ParseMediaType(types[0])
	if err != nil || media != "application/json" {
		a.fail(w, r, 415, "unsupported_media_type", "Content-Type must be application/json.")
		return
	}
	keys := r.Header.Values("Idempotency-Key")
	if len(keys) != 1 || !workflow.ValidCreationKey(keys[0]) {
		a.fail(w, r, 400, "invalid_idempotency_key", "A single valid Idempotency-Key is required.")
		return
	}
	data, err := readBody(w, r)
	if err != nil {
		a.bodyError(w, r, err)
		return
	}
	input, err := decodeCreate(data)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	receipt, err := a.service.Create(r.Context(), keys[0], input)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	info(r).workflowID = receipt.ID.String()
	w.Header().Set("Location", "/v1/workflows/"+receipt.ID.String())
	writeJSON(w, 201, receipt)
}

func (a *API) id(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	text := r.PathValue("id")
	id, err := uuid.Parse(text)
	if err != nil || len(text) != 36 || !strings.EqualFold(text, id.String()) {
		a.fail(w, r, 400, "invalid_id", "Workflow ID must be a UUID.")
		return uuid.Nil, false
	}
	info(r).workflowID = id.String()
	return id, true
}

func (a *API) details(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	result, err := a.service.Details(r.Context(), id)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}

func (a *API) cancel(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	data, err := readBody(w, r)
	if err != nil {
		a.bodyError(w, r, err)
		return
	}
	if len(data) != 0 {
		a.fail(w, r, 400, "invalid_request", "Cancellation requires an empty body.")
		return
	}
	result, err := a.service.Cancel(r.Context(), id)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}

func pagination(raw string) (int64, int, error) {
	values, err := url.ParseQuery(raw)
	if err != nil {
		return 0, 0, workflow.ErrInvalid
	}
	after, limit := int64(0), 50
	for key, value := range values {
		if len(value) != 1 || value[0] == "" {
			return 0, 0, workflow.ErrInvalid
		}
		n, err := strconv.ParseInt(value[0], 10, 64)
		if err != nil {
			return 0, 0, workflow.ErrInvalid
		}
		switch key {
		case "after":
			if n < 0 {
				return 0, 0, workflow.ErrInvalid
			}
			after = n
		case "limit":
			if n < 1 || n > 100 {
				return 0, 0, workflow.ErrInvalid
			}
			limit = int(n)
		default:
			return 0, 0, workflow.ErrInvalid
		}
	}
	return after, limit, nil
}

func (a *API) events(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	after, limit, err := pagination(r.URL.RawQuery)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	result, err := a.service.Events(r.Context(), id, after, limit)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}

func (a *API) live(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "live"})
}
func (a *API) ready(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), a.readyTimeout)
	defer cancel()
	if err := a.service.Ready(ctx); err != nil {
		a.fail(w, r, 503, "unavailable", "Service is not ready.")
		return
	}
	writeJSON(w, 200, map[string]string{"status": "ready"})
}

func (a *API) graph(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	if r.URL.RawQuery != "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	result, err := a.service.Graph(r.Context(), id)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, result)
}
func (a *API) taskIDs(w http.ResponseWriter, r *http.Request) (uuid.UUID, uuid.UUID, bool) {
	wf, ok := a.id(w, r)
	if !ok {
		return uuid.Nil, uuid.Nil, false
	}
	text := r.PathValue("task_id")
	task, err := uuid.Parse(text)
	if err != nil || len(text) != 36 || task == uuid.Nil {
		a.fail(w, r, 400, "invalid_task_id", "Task ID must be a UUID.")
		return wf, uuid.Nil, false
	}
	return wf, task, true
}
func (a *API) task(w http.ResponseWriter, r *http.Request) {
	wf, task, ok := a.taskIDs(w, r)
	if !ok {
		return
	}
	if r.URL.RawQuery != "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	value, err := a.service.InspectTask(r.Context(), wf, task)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, value)
}
func (a *API) attempts(w http.ResponseWriter, r *http.Request) {
	wf, task, ok := a.taskIDs(w, r)
	if !ok {
		return
	}
	after, limit, err := pagination(r.URL.RawQuery)
	if r.URL.Query().Get("limit") == "" {
		limit = 10
	}
	if err != nil || limit > 10 {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	value, err := a.service.Attempts(r.Context(), wf, task, after, limit)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	writeJSON(w, 200, value)
}

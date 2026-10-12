package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
)

// This double verifies HTTP contracts only. Transaction claims are tested against
// real PostgreSQL in tests/integration, never inferred from this double.
type stubService struct {
	err    error
	called int
	ready  bool
	wait   bool
	after  int64
	limit  int
}

func (s *stubService) result(ctx context.Context) error {
	s.called++
	if s.wait {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.err
}
func (s *stubService) Create(ctx context.Context, _ string, _ workflow.CreateInput) (workflow.Receipt, error) {
	return workflow.Receipt{ID: uuid.MustParse("61c9f55b-624b-401b-a250-0db16741d412"), CreatedAt: time.Unix(0, 0).UTC()}, s.result(ctx)
}
func (s *stubService) Details(ctx context.Context, id uuid.UUID) (workflow.Details, error) {
	return workflow.Details{ID: id, Tasks: []workflow.Task{}}, s.result(ctx)
}
func (s *stubService) Cancel(ctx context.Context, id uuid.UUID) (workflow.CancellationResult, error) {
	return workflow.CancellationResult{ID: id, Status: workflow.Cancelled, Version: 2}, s.result(ctx)
}
func (s *stubService) Events(ctx context.Context, _ uuid.UUID, after int64, limit int) (workflow.EventPage, error) {
	s.after = after
	s.limit = limit
	return workflow.EventPage{Events: []workflow.Event{}, NextAfter: after}, s.result(ctx)
}
func (s *stubService) Ready(ctx context.Context) error {
	if !s.ready {
		return workflow.ErrUnavailable
	}
	return s.result(ctx)
}

const validBody = `{"name":" First workflow ","tasks":[{"key":"diagnostic","kind":"noop"}]}`
const validID = "61c9f55b-624b-401b-a250-0db16741d412"

func request(t *testing.T, handler http.Handler, method, path, body string, headers map[string][]string) *httptest.ResponseRecorder {
	t.Helper()
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	for k, v := range headers {
		r.Header[k] = v
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	return w
}
func newTestAPI(s *stubService) *API {
	return New(s, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, 100*time.Millisecond)
}

func TestCreationBoundary(t *testing.T) {
	for _, test := range []struct {
		name, body, content string
		keys                []string
		status              int
	}{
		{"valid", validBody, "application/json", []string{"test-1"}, 201},
		{"media_parameters", validBody, "application/json; charset=utf-8", []string{"test-1"}, 201},
		{"media", validBody, "text/plain", []string{"test-1"}, 415},
		{"missing_key", validBody, "application/json", nil, 400},
		{"multiple_keys", validBody, "application/json", []string{"one", "two"}, 400},
		{"combined_keys", validBody, "application/json", []string{"one,two"}, 400},
		{"unknown", `{"name":"x","tasks":[{"key":"a","kind":"noop"}],"execute":true}`, "application/json", []string{"one"}, 400},
		{"depends_null", `{"name":"x","tasks":[{"key":"a","kind":"noop","depends_on":null}]}`, "application/json", []string{"one"}, 400},
		{"depends_number", `{"name":"x","tasks":[{"key":"a","kind":"noop","depends_on":[1]}]}`, "application/json", []string{"one"}, 400},
		{"depends_cycle", `{"name":"x","tasks":[{"key":"a","kind":"noop","depends_on":["a"]}]}`, "application/json", []string{"one"}, 400},
		{"depends_valid", `{"name":"x","tasks":[{"key":"a","kind":"noop"},{"key":"b","kind":"noop","depends_on":["a"]}]}`, "application/json", []string{"one"}, 201},
		{"delay_valid", `{"name":"x","tasks":[{"key":"a","kind":"delay","delay_ms":30000}]}`, "application/json", []string{"one"}, 201},
		{"delay_zero", `{"name":"x","tasks":[{"key":"a","kind":"delay","delay_ms":0}]}`, "application/json", []string{"one"}, 400},
		{"delay_over_limit", `{"name":"x","tasks":[{"key":"a","kind":"delay","delay_ms":30001}]}`, "application/json", []string{"one"}, 400},
		{"delay_fractional", `{"name":"x","tasks":[{"key":"a","kind":"delay","delay_ms":1.5}]}`, "application/json", []string{"one"}, 400},
		{"delay_null", `{"name":"x","tasks":[{"key":"a","kind":"delay","delay_ms":null}]}`, "application/json", []string{"one"}, 400},
		{"delay_duplicate", `{"name":"x","tasks":[{"key":"a","kind":"delay","delay_ms":1,"delay_ms":2}]}`, "application/json", []string{"one"}, 400},
		{"noop_delay", `{"name":"x","tasks":[{"key":"a","kind":"noop","delay_ms":1}]}`, "application/json", []string{"one"}, 400},
		{"task_unknown", `{"name":"x","tasks":[{"key":"a","kind":"noop","command":"x"}]}`, "application/json", []string{"one"}, 400},
		{"trailing", validBody + ` {}`, "application/json", []string{"one"}, 400},
		{"trailing_garbage", validBody + ` x`, "application/json", []string{"one"}, 400},
		{"null", "null", "application/json", []string{"one"}, 400},
		{"array", "[]", "application/json", []string{"one"}, 400},
		{"empty", "", "application/json", []string{"one"}, 400},
		{"duplicate_fields", `{"name":"x","name":"y","tasks":[{"key":"a","kind":"noop"}]}`, "application/json", []string{"one"}, 400},
		{"null_task", `{"name":"x","tasks":[null]}`, "application/json", []string{"one"}, 400},
		{"wrong_case", `{"Name":"x","tasks":[{"key":"a","kind":"noop"}]}`, "application/json", []string{"one"}, 400},
		{"oversized", validBody + strings.Repeat(" ", MaxBodyBytes), "application/json", []string{"one"}, 413},
	} {
		t.Run(test.name, func(t *testing.T) {
			s := &stubService{}
			h := newTestAPI(s).Handler()
			w := request(t, h, "POST", "/v1/workflows", test.body, map[string][]string{"Content-Type": {test.content}, "Idempotency-Key": test.keys})
			if w.Code != test.status {
				t.Fatalf("%d: %s", w.Code, w.Body.String())
			}
			if test.status == 201 {
				var receipt map[string]any
				if err := json.Unmarshal(w.Body.Bytes(), &receipt); err != nil {
					t.Fatal(err)
				}
				if len(receipt) != 2 || w.Header().Get("Location") != "/v1/workflows/"+validID {
					t.Fatal("mutable receipt")
				}
			} else {
				if s.called != 0 {
					t.Fatal("invalid request reached service")
				}
				checkError(t, w)
			}
		})
	}
}

func checkError(t *testing.T, w *httptest.ResponseRecorder) {
	t.Helper()
	var envelope struct {
		Error struct {
			Code, Message string
			RequestID     string `json:"request_id"`
		} `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Error.Code == "" || envelope.Error.Message == "" || envelope.Error.RequestID != w.Header().Get("X-Request-ID") {
		t.Fatal("bad error shape")
	}
}

func TestServiceErrorsAndSafeLogs(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
	}{{workflow.ErrNotFound, 404}, {workflow.ErrConflict, 409}, {workflow.ErrInvalid, 400}, {workflow.ErrUnavailable, 503}, {context.DeadlineExceeded, 503}, {errors.New("password=TOPSECRET"), 500}} {
		s := &stubService{err: test.err}
		var logs bytes.Buffer
		a := New(s, slog.New(slog.NewJSONHandler(&logs, nil)), time.Second, time.Second)
		w := request(t, a.Handler(), "POST", "/v1/workflows", validBody, map[string][]string{"Content-Type": {"application/json"}, "Idempotency-Key": {"PRIVATE-KEY"}})
		if w.Code != test.status {
			t.Fatal(w.Code)
		}
		checkError(t, w)
		for _, secret := range []string{"TOPSECRET", "PRIVATE-KEY", "diagnostic"} {
			if strings.Contains(w.Body.String()+logs.String(), secret) {
				t.Fatal("leaked secret", secret)
			}
		}
		if !strings.Contains(logs.String(), `"route":"/v1/workflows"`) {
			t.Fatal("missing route template", logs.String())
		}
	}
}

func TestOtherRoutesPaginationAndShutdown(t *testing.T) {
	s := &stubService{ready: true}
	a := newTestAPI(s)
	h := a.Handler()
	for _, test := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "/health/live", "", 200}, {"GET", "/health/ready", "", 200},
		{"GET", "/v1/workflows/" + validID, "", 200}, {"GET", "/v1/workflows/no", "", 400},
		{"GET", "/v1/workflows/" + validID + "/graph", "", 200},
		{"GET", "/v1/workflows/" + validID + "/graph?limit=1", "", 400},
		{"POST", "/v1/workflows/" + validID + "/cancel", "", 200}, {"POST", "/v1/workflows/" + validID + "/cancel", " ", 400},
		{"GET", "/v1/workflows/" + validID + "/events?after=2&limit=1", "", 200},
		{"GET", "/v1/workflows/" + validID + "/events?after=-1", "", 400},
		{"GET", "/v1/workflows/" + validID + "/events?limit=101", "", 400},
		{"GET", "/v1/workflows/" + validID + "/events?after=0&after=1", "", 400},
		{"GET", "/v1/workflows/" + validID + "/events?limit=%zz", "", 400},
		{"GET", "/v1/workflows/" + validID + "/events?unknown=1", "", 400},
		{"GET", "/unknown", "", 404}, {"DELETE", "/v1/workflows/" + validID, "", 405},
	} {
		w := request(t, h, test.method, test.path, test.body, nil)
		if w.Code != test.status {
			t.Fatalf("%s: %d %s", test.path, w.Code, w.Body.String())
		}
		if w.Code >= 400 {
			checkError(t, w)
		}
	}
	if s.after != 2 || s.limit != 1 {
		t.Fatal("pagination")
	}
	s.ready = false
	if request(t, h, "GET", "/health/ready", "", nil).Code != 503 {
		t.Fatal("outage")
	}
	if request(t, h, "GET", "/health/live", "", nil).Code != 200 {
		t.Fatal("liveness depends on DB")
	}
	s.ready = true
	if request(t, h, "GET", "/health/ready", "", nil).Code != 200 {
		t.Fatal("no recovery")
	}
	a.Stop()
	if request(t, h, "GET", "/health/ready", "", nil).Code != 503 || request(t, h, "GET", "/health/live", "", nil).Code != 200 {
		t.Fatal("shutdown health")
	}
}

func TestDeadlines(t *testing.T) {
	s := &stubService{wait: true, ready: true}
	a := New(s, slog.New(slog.NewJSONHandler(io.Discard, nil)), 40*time.Millisecond, 10*time.Millisecond)
	for _, path := range []string{"/health/ready", "/v1/workflows/" + validID} {
		start := time.Now()
		w := request(t, a.Handler(), "GET", path, "", nil)
		if w.Code != 503 || time.Since(start) > time.Second {
			t.Fatal("deadline not propagated")
		}
	}
}

func TestWireNormalization(t *testing.T) {
	a, err := decodeCreate([]byte(validBody))
	if err != nil {
		t.Fatal(err)
	}
	b, err := decodeCreate([]byte(`{"tasks":[{"kind":"noop","key":"diagnostic"}], "name":"First workflow"}`))
	if err != nil {
		t.Fatal(err)
	}
	x, _ := workflow.HashForVersion(a, 1)
	y, _ := workflow.HashForVersion(b, 1)
	if x != y {
		t.Fatal("wire ordering changed hash")
	}
}

func (s *stubService) Graph(ctx context.Context, id uuid.UUID) (workflow.Graph, error) {
	return workflow.Graph{ID: id, Nodes: []workflow.Task{}, Edges: []workflow.Edge{}}, s.result(ctx)
}

package httpapi

import (
	"bufio"
	"context"
	"errors"
	"forgeswarm/internal/postgres"
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

type streamService struct {
	stubService
	first, last int64
}

func (s *streamService) EventBounds(context.Context, uuid.UUID) (postgres.EventBounds, error) {
	return postgres.EventBounds{First: s.first, Last: s.last}, nil
}
func (s *streamService) Events(_ context.Context, _ uuid.UUID, after int64, limit int) (workflow.EventPage, error) {
	page := workflow.EventPage{Events: []workflow.Event{}, NextAfter: after}
	for i := max(s.first, after+1); i <= s.last && len(page.Events) < limit; i++ {
		page.Events = append(page.Events, workflow.Event{Sequence: i, EventType: "task.claimed", Payload: []byte(`{"untrusted":"<script>alert(1)</script>"}`), OccurredAt: time.Unix(i, 0).UTC()})
		page.NextAfter = i
	}
	return page, nil
}
func TestStreamDurableCursorReconnectAndOrder(t *testing.T) {
	service := &streamService{first: 1, last: 3}
	a := New(service, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, time.Second)
	server := httptest.NewServer(a.Handler())
	defer server.Close()
	for _, after := range []string{"0", "2"} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		req, _ := http.NewRequestWithContext(ctx, "GET", server.URL+"/v1/workflows/"+validID+"/stream", nil)
		req.Header.Set("Last-Event-ID", after)
		response, e := http.DefaultClient.Do(req)
		if e != nil {
			cancel()
			t.Fatal(e)
		}
		if response.Header.Get("Content-Type") != "text/event-stream" {
			t.Fatal("stream MIME type")
		}
		scanner := bufio.NewScanner(response.Body)
		ids := []string{}
		expected := 3
		if after == "2" {
			expected = 1
		}
		for scanner.Scan() {
			line := scanner.Text()
			if strings.HasPrefix(line, "id: ") {
				ids = append(ids, line)
			}
			if strings.HasPrefix(line, "data: ") {
				if strings.Contains(line, "<script>") {
					t.Fatal("unescaped event JSON")
				}
				if len(ids) == expected {
					break
				}
			}
		}
		response.Body.Close()
		cancel()
		want := []string{"id: 1", "id: 2", "id: 3"}
		if after == "2" {
			want = want[2:]
		}
		if strings.Join(ids, ",") != strings.Join(want, ",") {
			t.Fatal("cursor/order", ids)
		}
	}
}
func TestStreamRejectsRetentionGapsAndAmbiguousCursors(t *testing.T) {
	service := &streamService{first: 8, last: 12}
	a := New(service, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, time.Second)
	for _, test := range []struct {
		query, header string
		status        int
	}{{"", "3", 409}, {"", "13", 409}, {"", "-1", 400}, {"?unknown=2", "", 400}, {"?after=0&after=2", "", 400}} {
		req := httptest.NewRequest("GET", "/v1/workflows/"+validID+"/stream"+test.query, nil)
		if test.header != "" {
			req.Header.Set("Last-Event-ID", test.header)
		}
		w := httptest.NewRecorder()
		a.Handler().ServeHTTP(w, req)
		if w.Code != test.status {
			t.Fatal(test, w.Code, w.Body.String())
		}
	}
}

type slowStreamWriter struct {
	header   http.Header
	mu       sync.Mutex
	deadline time.Time
}

func (w *slowStreamWriter) Header() http.Header { return w.header }
func (w *slowStreamWriter) WriteHeader(int)     {}
func (w *slowStreamWriter) SetWriteDeadline(at time.Time) error {
	w.mu.Lock()
	w.deadline = at
	w.mu.Unlock()
	return nil
}
func (w *slowStreamWriter) Write([]byte) (int, error) {
	w.mu.Lock()
	deadline := w.deadline
	w.mu.Unlock()
	time.Sleep(time.Until(deadline))
	return 0, errors.New("write deadline exceeded")
}
func (w *slowStreamWriter) Flush() {}
func TestStreamSlowClientDisconnectAndPrivateControls(t *testing.T) {
	service := &streamService{first: 1, last: 1}
	a := New(service, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, time.Second)
	writer := &slowStreamWriter{header: http.Header{}}
	done := make(chan struct{})
	go func() {
		a.Handler().ServeHTTP(writer, httptest.NewRequest("GET", "/v1/workflows/"+validID+"/stream", nil))
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow client retained a handler")
	}
	if a.streams.Load() != 0 {
		t.Fatal("stream capacity leaked")
	}
	handler := PrivateUI(a.Handler(), "")
	req := httptest.NewRequest("POST", "http://127.0.0.1:8080/v1/workflows/"+validID+"/cancel", nil)
	req.Header.Set("Origin", "http://attacker.invalid")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("cross-origin control accepted")
	}
	req = httptest.NewRequest("GET", "http://rebind.invalid:8080/health/live", nil)
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("rebound host accepted")
	}
}

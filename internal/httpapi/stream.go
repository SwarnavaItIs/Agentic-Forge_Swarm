package httpapi

import (
	"context"
	"encoding/json"
	"fmt"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/workflow"
	"github.com/google/uuid"
	"net/http"
	"strconv"
	"time"
)

func (a *API) stream(w http.ResponseWriter, r *http.Request) {
	id, ok := a.id(w, r)
	if !ok {
		return
	}
	store, ok := a.service.(interface {
		EventBounds(context.Context, uuid.UUID) (postgres.EventBounds, error)
	})
	if !ok {
		a.serviceError(w, r, workflow.ErrUnavailable)
		return
	}
	after := int64(0)
	query := r.URL.Query()
	if len(query) > 1 || len(query["after"]) > 1 || len(query) > 0 && query.Get("after") == "" {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	text := query.Get("after")
	headers := r.Header.Values("Last-Event-ID")
	if len(headers) > 1 {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	if len(headers) == 1 {
		text = headers[0]
	}
	var err error
	if text != "" {
		after, err = strconv.ParseInt(text, 10, 64)
	}
	if err != nil || after < 0 {
		a.serviceError(w, r, workflow.ErrInvalid)
		return
	}
	bounds, err := store.EventBounds(r.Context(), id)
	if err != nil {
		a.serviceError(w, r, err)
		return
	}
	if after > bounds.Last {
		a.fail(w, r, 409, "invalid_event_cursor", "Cursor is ahead of persisted history.")
		return
	}
	if after < bounds.First-1 {
		a.fail(w, r, 409, "event_retention_gap", "History was retained from a later sequence; refresh detail and resume from its cursor.")
		return
	}
	if a.streams.Add(1) > 64 {
		a.streams.Add(-1)
		a.fail(w, r, 429, "stream_capacity", "Too many streams.")
		return
	}
	defer a.streams.Add(-1)
	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	control := http.NewResponseController(w)
	write := func(frame string) error {
		if e := control.SetWriteDeadline(time.Now().Add(time.Second)); e != nil {
			return e
		}
		if _, e := fmt.Fprint(w, frame); e != nil {
			return e
		}
		return control.Flush()
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache, no-transform")
	w.Header().Set("X-Accel-Buffering", "no")
	if write("retry: 1000\n: persisted cursor stream; at-least-once delivery\n\n") != nil {
		return
	}
	poll := time.NewTicker(250 * time.Millisecond)
	defer poll.Stop()
	heartbeat := time.NewTicker(5 * time.Second)
	defer heartbeat.Stop()
	for {
		if a.stopping.Load() {
			return
		}
		read, done := context.WithTimeout(ctx, time.Second)
		page, e := a.service.Events(read, id, after, 32)
		done()
		if e != nil {
			return
		}
		for _, event := range page.Events {
			if event.Sequence != after+1 {
				_ = write("event: retention_gap\ndata: {\"refresh\":true}\n\n")
				return
			}
			wire, e := json.Marshal(event)
			if e != nil || len(wire) > 32<<10 {
				return
			}
			if write(fmt.Sprintf("id: %d\nevent: workflow\ndata: %s\n\n", event.Sequence, wire)) != nil {
				return
			}
			after = event.Sequence
		}
		select {
		case <-ctx.Done():
			return
		case <-heartbeat.C:
			if write(": heartbeat\n\n") != nil {
				return
			}
		case <-poll.C:
		}
	}
}

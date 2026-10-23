package github

import (
	"context"
	"errors"
	"forgeswarm/internal/publication"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestApprovalFenceRunsAfterExternalWritePacing(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { calls.Add(1); w.Write([]byte("{}")) }))
	defer server.Close()
	c, e := New(Config{Repository: publication.Repository{ID: 1, Owner: "fixture", Name: "fixture", Target: "main"}, Token: strings.Repeat("t", 40), CredentialMode: "TEST_DEMO", WebhookSecret: strings.Repeat("s", 40), TriggerLabel: "forgeswarm", TriggerActor: "owner", APIURL: server.URL})
	if e != nil {
		t.Fatal(e)
	}
	// Exercise production pacing against a controlled local transport only.
	c.cfg.CredentialMode = "development_token"
	c.lastWrite = time.Now()
	expires := time.Now().Add(500 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var out any
	e = c.write(ctx, "/unused", map[string]string{}, &out, func(context.Context) error {
		if time.Now().After(expires) {
			return errors.New("expired")
		}
		return nil
	})
	if !IsClass(e, "approval_not_current") || calls.Load() != 0 {
		t.Fatal("approval checked before delay rather than before request", e, calls.Load())
	}
}

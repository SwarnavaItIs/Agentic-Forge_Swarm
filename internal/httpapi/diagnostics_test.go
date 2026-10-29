package httpapi

import (
	"context"
	"forgeswarm/internal/operator"
	"forgeswarm/internal/postgres"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type diagnosticService struct{ stubService }

func (*diagnosticService) Diagnostics(context.Context) (postgres.Diagnostics, error) {
	return postgres.Diagnostics{Pool: map[string]any{"total": 1, "max": 4}}, nil
}

func TestDiagnosticsRequireIdentityAndBoundQuerySurface(t *testing.T) {
	auth, err := operator.New(operator.Config{Operator: operator.Credential{Actor: "owner", Token: strings.Repeat("o", 40)}, Reader: operator.Credential{Actor: "reader", Token: strings.Repeat("r", 40)}}, []string{strings.Repeat("w", 40)})
	if err != nil {
		t.Fatal(err)
	}
	api := New(&diagnosticService{}, slog.New(slog.NewTextHandler(io.Discard, nil)), time.Second, time.Second)
	call := func(route, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("GET", route, nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		api.Handler().ServeHTTP(w, r)
		return w
	}
	if call("/v1/diagnostics", "").Code != 503 {
		t.Fatal("anonymous legacy observations enabled")
	}
	api.WithOperator(auth)
	for _, token := range []string{"", strings.Repeat("w", 40), "model-token"} {
		if call("/v1/diagnostics", token).Code != 401 {
			t.Fatal("worker/model read observations")
		}
	}
	if call("/v1/diagnostics?sql=private", strings.Repeat("r", 40)).Code != 400 {
		t.Fatal("unbounded query surface")
	}
	w := call("/v1/diagnostics", strings.Repeat("r", 40))
	if w.Code != 200 || w.Body.Len() > 16384 || !strings.Contains(w.Body.String(), `"heap_bytes"`) {
		t.Fatal("bounded reader observations", w.Code)
	}
}

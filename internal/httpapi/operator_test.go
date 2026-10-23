package httpapi

import (
	"bytes"
	"encoding/json"
	"forgeswarm/internal/operator"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOperatorArtifactControlsRolesAndCSRF(t *testing.T) {
	cfg := operator.Config{Operator: operator.Credential{Actor: "owner", Token: strings.Repeat("o", 40)}, Reader: operator.Credential{Actor: "observer", Token: strings.Repeat("r", 40)}}
	auth, e := operator.New(cfg, []string{strings.Repeat("w", 40)})
	if e != nil {
		t.Fatal(e)
	}
	service := &stubService{ready: true}
	api := New(service, slog.New(slog.NewJSONHandler(io.Discard, nil)), time.Second, time.Second).WithOperator(auth)
	server := httptest.NewServer(PrivateUI(api.Handler(), ""))
	defer server.Close()
	call := func(method, path, token string, body []byte, cookie *http.Cookie, csrf string, origin bool) *http.Response {
		t.Helper()
		req, _ := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		if cookie != nil {
			req.AddCookie(cookie)
		}
		if csrf != "" {
			req.Header.Set("X-CSRF-Token", csrf)
		}
		if origin {
			req.Header.Set("Origin", server.URL)
		}
		r, e := http.DefaultClient.Do(req)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { r.Body.Close() })
		return r
	}
	path := "/v1/workflows/" + validID
	for _, token := range []string{"", strings.Repeat("w", 40), "model-token"} {
		r := call("GET", path+"/artifacts/"+validID, token, nil, nil, "", false)
		if r.StatusCode != 401 {
			t.Fatal("worker/unverified artifact access", r.StatusCode)
		}
		r = call("POST", "/v1/approvals/"+validID, token, []byte(`{"actor":"owner"}`), nil, "", false)
		if r.StatusCode != 401 {
			t.Fatal("agent approval allowed", r.StatusCode)
		}
	}
	if call("GET", path, cfg.Reader.Token, nil, nil, "", false).StatusCode != 200 {
		t.Fatal("reader denied")
	}
	if call("POST", path+"/cancel", cfg.Reader.Token, nil, nil, "", false).StatusCode != 403 {
		t.Fatal("reader cancelled")
	}
	if call("POST", path+"/cancel", cfg.Operator.Token, nil, nil, "", false).StatusCode != 200 {
		t.Fatal("operator denied")
	}
	login, _ := json.Marshal(map[string]string{"token": cfg.Operator.Token})
	if call("POST", "/v1/session", "", login, nil, "", false).StatusCode != 401 {
		t.Fatal("cross-site login")
	}
	r := call("POST", "/v1/session", "", login, nil, "", true)
	var session struct {
		CSRF  string
		Actor string
	}
	json.NewDecoder(r.Body).Decode(&session)
	if r.StatusCode != 200 || session.Actor != "owner" || session.CSRF == "" || len(r.Cookies()) != 1 {
		t.Fatal("login")
	}
	cookie := r.Cookies()[0]
	if !cookie.HttpOnly || cookie.SameSite != http.SameSiteStrictMode || cookie.Path != "/v1/" {
		t.Fatal("unsafe session cookie")
	}
	if call("POST", path+"/cancel", "", nil, cookie, "", true).StatusCode != 401 {
		t.Fatal("missing CSRF accepted")
	}
	if call("POST", path+"/cancel", "", nil, cookie, session.CSRF, false).StatusCode != 401 {
		t.Fatal("missing origin accepted")
	}
	if call("POST", path+"/cancel", "", nil, cookie, session.CSRF, true).StatusCode != 200 {
		t.Fatal("valid CSRF rejected")
	}
	if call("DELETE", "/v1/session", "", nil, cookie, session.CSRF, true).StatusCode != 200 {
		t.Fatal("logout")
	}
	if call("GET", path, "", nil, cookie, "", false).StatusCode != 401 {
		t.Fatal("revoked session accepted")
	}
}

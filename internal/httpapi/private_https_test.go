package httpapi

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHTTPSOriginCannotBeGrantedByForwardedHeaders(t *testing.T) {
	for _, tc := range []struct {
		host, origin string
		secure       bool
		status       int
	}{{"control.internal:8443", "https://control.internal:8443", true, 200}, {"attacker.invalid:8443", "https://attacker.invalid:8443", true, 403}, {"control.internal:8443", "http://control.internal:8443", true, 403}, {"control.internal:8443", "https://control.internal:8443", false, 403}} {
		h := PrivateUIOrigin(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }), "", "https://control.internal:8443")
		r := httptest.NewRequest("GET", "http://"+tc.host+"/v1/workflows", nil)
		if tc.secure {
			r.TLS = &tls.ConnectionState{}
		}
		r.Header.Set("Origin", tc.origin)
		r.Header.Set("X-Forwarded-Proto", "https")
		r.Header.Set("X-Forwarded-Host", "control.internal:8443")
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatal(tc, w.Code)
		}
	}
}

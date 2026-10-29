package httpapi

import (
	"net"
	"net/http"
	"net/url"
	"strings"
)

func LoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	ip := net.ParseIP(host)
	return err == nil && (host == "localhost" || ip != nil && ip.IsLoopback())
}

// The unauthenticated dashboard is a local operator surface. Reject rebinding
// hosts and cross-origin control requests; deployment authentication is deferred.
func PrivateUI(api http.Handler, dir string) http.Handler {
	return PrivateUIOrigin(api, dir, "")
}

func PrivateUIOrigin(api http.Handler, dir, expected string) http.Handler {
	files := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if !strings.Contains(host, ":") {
			host = net.JoinHostPort(host, "80")
		}
		if expected != "" {
			value, err := url.Parse(expected)
			if err != nil || value.Scheme != "https" || value.Host != r.Host || value.User != nil || value.Path != "" || value.RawQuery != "" || value.Fragment != "" || r.TLS == nil {
				http.Error(w, "Configured HTTPS origin required.", http.StatusForbidden)
				return
			}
		} else if !LoopbackAddress(host) {
			http.Error(w, "Local operator access only.", http.StatusForbidden)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			value, err := url.Parse(origin)
			scheme := "http"
			if r.TLS != nil {
				scheme = "https"
			}
			if err != nil || len(r.Header.Values("Origin")) != 1 || value.Scheme != scheme || value.Host != r.Host || value.User != nil || value.Path != "" || value.RawQuery != "" || value.Fragment != "" {
				http.Error(w, "Same-origin access required.", http.StatusForbidden)
				return
			}
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")
		if strings.HasPrefix(r.URL.Path, "/v1/") || strings.HasPrefix(r.URL.Path, "/health/") {
			api.ServeHTTP(w, r)
			return
		}
		if dir == "" || r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.NotFound(w, r)
			return
		}
		files.ServeHTTP(w, r)
	})
}

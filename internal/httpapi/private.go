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
	files := http.FileServer(http.Dir(dir))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host := r.Host
		if !strings.Contains(host, ":") {
			host = net.JoinHostPort(host, "80")
		}
		if !LoopbackAddress(host) {
			http.Error(w, "Local operator access only.", http.StatusForbidden)
			return
		}
		if origin := r.Header.Get("Origin"); origin != "" {
			value, err := url.Parse(origin)
			if err != nil || value.Scheme != "http" || value.Host != r.Host {
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

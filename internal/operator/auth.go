// Package operator provides single-owner bearer identities and bounded opaque
// browser sessions using standard Go crypto/http primitives, without passwords.
package operator

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"forgeswarm/internal/agent"
	"io"
	"net/http"
	"net/url"
	"os"
	"sync"
	"time"
)

type Principal struct {
	Actor string `json:"actor"`
	Role  string `json:"role"`
}

func (p Principal) IsOperator() bool {
	return p.Role == "operator" && len(p.Actor) > 0 && len(p.Actor) <= 100
}

type key struct{}

func WithPrincipal(ctx context.Context, p Principal) context.Context {
	return context.WithValue(ctx, key{}, p)
}
func Identity(ctx context.Context) Principal { p, _ := ctx.Value(key{}).(Principal); return p }

type Credential struct {
	Actor string `json:"actor"`
	Token string `json:"token"`
}
type Config struct {
	Operator Credential `json:"operator"`
	Reader   Credential `json:"reader"`
}
type session struct {
	Principal
	CSRF    string
	Expires time.Time
}
type Auth struct {
	config   Config
	mu       sync.Mutex
	sessions map[string]session
	window   time.Time
	attempts int
}

func Load(path string, forbidden []string) (*Auth, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, errors.New("operator credentials required")
	}
	defer f.Close()
	body, e := io.ReadAll(io.LimitReader(f, 4097))
	if e != nil {
		return nil, errors.New("operator credentials unreadable")
	}
	var cfg Config
	if agent.StrictDecode(body, &cfg, 4096) != nil {
		return nil, errors.New("invalid operator credentials")
	}
	return New(cfg, forbidden)
}
func New(cfg Config, forbidden []string) (*Auth, error) {
	if len(cfg.Operator.Actor) < 1 || len(cfg.Operator.Actor) > 100 || len(cfg.Reader.Actor) < 1 || len(cfg.Reader.Actor) > 100 || cfg.Operator.Actor == cfg.Reader.Actor || len(cfg.Operator.Token) < 32 || len(cfg.Operator.Token) > 256 || len(cfg.Reader.Token) < 32 || len(cfg.Reader.Token) > 256 || cfg.Operator.Token == cfg.Reader.Token {
		return nil, errors.New("distinct operator and read-only credentials required")
	}
	for _, token := range forbidden {
		if token != "" && (cfg.Operator.Token == token || cfg.Reader.Token == token) {
			return nil, errors.New("operator, worker and service credentials must differ")
		}
	}
	return &Auth{config: cfg, sessions: map[string]session{}}, nil
}
func Random() string {
	var b [32]byte
	if _, e := rand.Read(b[:]); e != nil {
		panic("cryptographic random unavailable")
	}
	return hex.EncodeToString(b[:])
}
func (a *Auth) Token(value string) Principal {
	got := sha256.Sum256([]byte(value))
	p := Principal{}
	for _, entry := range []struct {
		Credential
		role string
	}{{a.config.Operator, "operator"}, {a.config.Reader, "read_only"}} {
		want := sha256.Sum256([]byte(entry.Token))
		if subtle.ConstantTimeCompare(got[:], want[:]) == 1 {
			p = Principal{Actor: entry.Actor, Role: entry.role}
		}
	}
	return p
}
func SameOrigin(r *http.Request) bool {
	values := r.Header.Values("Origin")
	if len(values) != 1 {
		return false
	}
	u, e := url.Parse(values[0])
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	return e == nil && u.Scheme == scheme && u.Host == r.Host && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == ""
}
func (a *Auth) Login(r *http.Request, token string) (string, string, Principal, error) {
	if !SameOrigin(r) {
		return "", "", Principal{}, errors.New("same_origin_required")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	if now.Sub(a.window) > time.Minute {
		a.window = now
		a.attempts = 0
	}
	a.attempts++
	if a.attempts > 20 {
		return "", "", Principal{}, errors.New("login_rate_limit")
	}
	p := a.Token(token)
	if p.Role == "" {
		return "", "", p, errors.New("authentication_required")
	}
	for id, s := range a.sessions {
		if !now.Before(s.Expires) {
			delete(a.sessions, id)
		}
	}
	if len(a.sessions) >= 64 {
		return "", "", p, errors.New("session_capacity")
	}
	id, csrf := Random(), Random()
	a.sessions[id] = session{Principal: p, CSRF: csrf, Expires: now.Add(time.Hour)}
	return id, csrf, p, nil
}
func (a *Auth) Authenticate(r *http.Request) (Principal, string, bool) {
	if headers := r.Header.Values("Authorization"); len(headers) > 0 {
		if len(headers) != 1 || len(headers[0]) > 300 || len(headers[0]) < 8 || headers[0][:7] != "Bearer " {
			return Principal{}, "", false
		}
		p := a.Token(headers[0][7:])
		return p, "", p.Role != ""
	}
	cookies := r.CookiesNamed("forgeswarm_session")
	if len(cookies) != 1 {
		return Principal{}, "", false
	}
	a.mu.Lock()
	s, ok := a.sessions[cookies[0].Value]
	if ok && !time.Now().Before(s.Expires) {
		delete(a.sessions, cookies[0].Value)
		ok = false
	}
	a.mu.Unlock()
	if !ok {
		return Principal{}, "", false
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		headers := r.Header.Values("X-CSRF-Token")
		if !SameOrigin(r) || len(headers) != 1 || subtle.ConstantTimeCompare([]byte(headers[0]), []byte(s.CSRF)) != 1 {
			return Principal{}, "", false
		}
	}
	return s.Principal, s.CSRF, true
}
func (a *Auth) Logout(r *http.Request) {
	if c, e := r.Cookie("forgeswarm_session"); e == nil {
		a.mu.Lock()
		delete(a.sessions, c.Value)
		a.mu.Unlock()
	}
}
func Cookie(value string, secure bool) *http.Cookie {
	return &http.Cookie{Name: "forgeswarm_session", Value: value, Path: "/v1/", HttpOnly: true, Secure: secure, SameSite: http.SameSiteStrictMode, MaxAge: 3600}
}

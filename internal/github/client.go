// Package github is a trusted, bounded GitHub REST adapter. It follows no URLs
// supplied by issues, repositories or API responses, and runs no host Git.
package github

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"forgeswarm/internal/agent"
	"forgeswarm/internal/operator"
	"forgeswarm/internal/publication"
	"forgeswarm/internal/sandbox"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Config struct {
	Repository     publication.Repository `json:"repository"`
	Token          string                 `json:"token"`
	CredentialMode string                 `json:"credential_mode"`
	WebhookSecret  string                 `json:"webhook_secret"`
	TriggerLabel   string                 `json:"trigger_label"`
	TriggerActor   string                 `json:"trigger_actor"`
	APIURL         string                 `json:"test_api_url,omitempty"`
}
type Client struct {
	cfg       Config
	http      *http.Client
	base      string
	mu        sync.Mutex
	lastWrite time.Time
}

func Load(path string) (*Client, error) {
	f, e := os.Open(path)
	if e != nil {
		return nil, errors.New("GitHub configuration required")
	}
	defer f.Close()
	b, e := io.ReadAll(io.LimitReader(f, 8193))
	if e != nil {
		return nil, errors.New("GitHub configuration unreadable")
	}
	var cfg Config
	if agent.StrictDecode(b, &cfg, 8192) != nil {
		return nil, errors.New("invalid GitHub configuration")
	}
	return New(cfg)
}
func New(cfg Config) (*Client, error) {
	if !cfg.Repository.Valid() || len(cfg.Token) < 20 || len(cfg.Token) > 256 || strings.ContainsAny(cfg.Token, "\r\n") || len(cfg.WebhookSecret) < 32 || len(cfg.WebhookSecret) > 256 || len(cfg.TriggerLabel) < 1 || len(cfg.TriggerLabel) > 80 || len(cfg.TriggerActor) < 1 || len(cfg.TriggerActor) > 100 {
		return nil, errors.New("invalid GitHub policy or credentials")
	}
	base := "https://api.github.com"
	switch cfg.CredentialMode {
	case "installation_token", "development_token":
		if cfg.APIURL != "" {
			return nil, errors.New("custom GitHub URL forbidden")
		}
	case "TEST_DEMO":
		u, e := url.Parse(cfg.APIURL)
		if e != nil || u.Scheme != "http" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || net.ParseIP(u.Hostname()) == nil || !net.ParseIP(u.Hostname()).IsLoopback() {
			return nil, errors.New("TEST_DEMO requires a numeric loopback HTTP server")
		}
		base = cfg.APIURL
	default:
		return nil, errors.New("explicit GitHub credential mode required")
	}
	transport := &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS13}, MaxConnsPerHost: 2, MaxIdleConnsPerHost: 2, ResponseHeaderTimeout: 4 * time.Second}
	return &Client{cfg: cfg, base: base, http: &http.Client{Timeout: 5 * time.Second, Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}, nil
}
func (c *Client) Repository() publication.Repository { return c.cfg.Repository }
func (c *Client) TestDemo() bool                     { return c.cfg.CredentialMode == "TEST_DEMO" }
func (c *Client) Policy() string                     { return publication.PolicyVersion }

type Error struct {
	Class     string
	RetryAt   time.Time
	Uncertain bool
}

func (c *Client) OperatorCredentialConflict(auth *operator.Auth) bool {
	return auth.Token(c.cfg.Token).Role != ""
}
func (e *Error) Error() string           { return "github_" + e.Class }
func IsClass(e error, class string) bool { var v *Error; return errors.As(e, &v) && v.Class == class }
func (c *Client) prefix() string {
	return "/repos/" + c.cfg.Repository.Owner + "/" + c.cfg.Repository.Name
}
func (c *Client) write(ctx context.Context, path string, in, out any, check func(context.Context) error) error {
	if !c.TestDemo() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if wait := time.Until(c.lastWrite.Add(time.Second)); wait > 0 {
			timer := time.NewTimer(wait)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return &Error{Class: "transport", Uncertain: true}
			case <-timer.C:
			}
		}
		c.lastWrite = time.Now()
	}
	// Pacing precedes the SQL fence: expiry/cancellation is rechecked at the
	// actual start of the external request rather than before a retry delay.
	if check(ctx) != nil {
		return &Error{Class: "approval_not_current"}
	}
	return c.call(ctx, "POST", path, in, out)
}
func (c *Client) call(ctx context.Context, method, path string, in, out any) error {
	var body []byte
	if in != nil {
		body, _ = json.Marshal(in)
	}
	if len(body) > 16384 {
		return &Error{Class: "policy"}
	}
	req, e := http.NewRequestWithContext(ctx, method, c.base+path, bytes.NewReader(body))
	if e != nil {
		return &Error{Class: "policy"}
	}
	req.Header.Set("Authorization", "Bearer "+c.cfg.Token)
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("X-GitHub-Api-Version", "2026-03-10")
	req.Header.Set("User-Agent", "ForgeSwarm")
	req.Header.Set("Content-Type", "application/json")
	response, e := c.http.Do(req)
	if e != nil {
		return &Error{Class: "transport", Uncertain: method != "GET"}
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, 65537))
	if e != nil {
		return &Error{Class: "transport", Uncertain: method != "GET"}
	}
	if response.StatusCode < 200 || response.StatusCode > 299 {
		class := "protocol"
		switch response.StatusCode {
		case 401:
			class = "unauthorized"
		case 403:
			class = "forbidden"
		case 404:
			class = "not_found"
		case 409, 422:
			class = "conflict"
		case 429:
			class = "rate_limit"
		default:
			if response.StatusCode >= 500 {
				class = "server"
			}
		}
		retry := time.Time{}
		if response.StatusCode == 429 || response.StatusCode == 403 && (response.Header.Get("Retry-After") != "" || response.Header.Get("X-RateLimit-Remaining") == "0") {
			class = "rate_limit"
		}
		if class == "rate_limit" {
			retry = time.Now().Add(time.Minute)
			if seconds, err := strconv.Atoi(response.Header.Get("Retry-After")); err == nil && seconds > 0 {
				retry = time.Now().Add(time.Duration(min(seconds, 86400)) * time.Second)
			} else if at, err := http.ParseTime(response.Header.Get("Retry-After")); err == nil && at.After(retry) {
				retry = at
			}
			if seconds, err := strconv.ParseInt(response.Header.Get("X-RateLimit-Reset"), 10, 64); err == nil && time.Unix(seconds, 0).After(retry) {
				retry = time.Unix(seconds, 0)
			}
		}
		return &Error{Class: class, RetryAt: retry, Uncertain: method != "GET" && response.StatusCode >= 500}
	}
	if len(raw) > 65536 || !utf8.Valid(raw) || response.Header.Get("Link") != "" {
		return &Error{Class: "response_bounds", Uncertain: method != "GET"}
	}
	// Reject duplicate JSON keys at every depth while tolerating GitHub's extra
	// documented response fields. Never decode errors or credentials into logs.
	var check any
	if sandbox.Decode(raw, &check, 65536) != nil || json.Unmarshal(raw, out) != nil {
		return &Error{Class: "protocol", Uncertain: method != "GET"}
	}
	return nil
}
func (c *Client) Metadata(ctx context.Context) error {
	var r struct {
		ID       int64
		FullName string `json:"full_name"`
		Archived bool
	}
	if e := c.call(ctx, "GET", c.prefix(), nil, &r); e != nil {
		return e
	}
	if r.ID != c.cfg.Repository.ID || !strings.EqualFold(r.FullName, c.cfg.Repository.Owner+"/"+c.cfg.Repository.Name) || r.Archived {
		return &Error{Class: "repository_conflict"}
	}
	return nil
}
func (c *Client) Ref(ctx context.Context, branch string) (string, error) {
	var v struct {
		Ref    string
		Object struct {
			SHA  string
			Type string
		}
	}
	if e := c.call(ctx, "GET", c.prefix()+"/git/ref/heads/"+url.PathEscape(branch), nil, &v); e != nil {
		return "", e
	}
	if v.Ref != "refs/heads/"+branch || v.Object.Type != "commit" || !publication.SHA.MatchString(v.Object.SHA) {
		return "", &Error{Class: "ref_conflict"}
	}
	return v.Object.SHA, nil
}

type Commit struct {
	SHA     string
	Message string
	Tree    struct{ SHA string }
	Parents []struct{ SHA string }
}

func (c *Client) commit(ctx context.Context, sha string) (Commit, error) {
	var v Commit
	if !publication.SHA.MatchString(sha) {
		return v, &Error{Class: "policy"}
	}
	e := c.call(ctx, "GET", c.prefix()+"/git/commits/"+sha, nil, &v)
	if e == nil && (v.SHA != sha || !publication.SHA.MatchString(v.Tree.SHA)) {
		e = &Error{Class: "commit_conflict"}
	}
	return v, e
}

type Issue struct {
	Number      int
	Title       string
	Body        string
	State       string
	PullRequest json.RawMessage `json:"pull_request"`
	Labels      []struct{ Name string }
}

func (c *Client) issue(ctx context.Context, number int) (Issue, error) {
	var i Issue
	if number < 1 || number > 1000000000 {
		return i, &Error{Class: "policy"}
	}
	if e := c.call(ctx, "GET", c.prefix()+"/issues/"+strconv.Itoa(number), nil, &i); e != nil {
		return i, e
	}
	label := false
	for _, l := range i.Labels {
		label = label || l.Name == c.cfg.TriggerLabel
	}
	if i.Number != number || i.State != "open" || len(i.PullRequest) > 0 || len(i.Title) < 1 || len(i.Title) > 200 || len(i.Body) > 1600 || !label || strings.ContainsRune(i.Title+i.Body, 0) {
		return i, &Error{Class: "issue_policy"}
	}
	return i, nil
}
func (c *Client) Prepare(ctx context.Context, number int) (publication.Prepared, error) {
	p := publication.Prepared{Repository: c.Repository(), Issue: number, Policy: c.Policy(), TestDemo: c.TestDemo()}
	if e := c.Metadata(ctx); e != nil {
		return p, e
	}
	issue, e := c.issue(ctx, number)
	if e != nil {
		return p, e
	}
	p.Description = "Untrusted GitHub issue data:\n" + issue.Title + "\n" + issue.Body
	base, e := c.Ref(ctx, p.Repository.Target)
	if e != nil {
		return p, e
	}
	commit, e := c.commit(ctx, base)
	if e != nil {
		return p, e
	}
	p.BaseTree = commit.Tree.SHA
	var tree struct {
		SHA       string
		Truncated bool
		Tree      []struct {
			Path string
			Mode string
			Type string
			SHA  string
			Size int
		}
	}
	if e = c.call(ctx, "GET", c.prefix()+"/git/trees/"+p.BaseTree, nil, &tree); e != nil {
		return p, e
	}
	if tree.SHA != p.BaseTree || tree.Truncated || len(tree.Tree) > 8 || len(tree.Tree) < 4 {
		return p, &Error{Class: "preparation_policy"}
	}
	p.Snapshot = sandbox.Snapshot{SchemaVersion: 1, BaseRevision: base, Files: map[string][]byte{}}
	total := 0
	for _, entry := range tree.Tree {
		if entry.Mode != "100644" || entry.Type != "blob" || !sandbox.SafePath(entry.Path) || strings.Contains(entry.Path, "/") || !publication.SHA.MatchString(entry.SHA) || entry.Size > 4096 || entry.Size < 0 {
			return p, &Error{Class: "preparation_policy"}
		}
		if _, exists := p.Snapshot.Files[entry.Path]; exists {
			return p, &Error{Class: "preparation_policy"}
		}
		var blob struct {
			SHA      string
			Encoding string
			Content  string
			Size     int
		}
		if e = c.call(ctx, "GET", c.prefix()+"/git/blobs/"+entry.SHA, nil, &blob); e != nil {
			return p, e
		}
		data, err := base64.StdEncoding.DecodeString(strings.ReplaceAll(blob.Content, "\n", ""))
		total += len(data)
		if err != nil || blob.Encoding != "base64" || len(data) != entry.Size || len(data) != blob.Size || blob.SHA != entry.SHA || publication.ObjectSHA("blob", data) != entry.SHA || total > 4096 || bytes.HasPrefix(data, []byte("version https://git-lfs.github.com/spec/")) {
			return p, &Error{Class: "preparation_policy"}
		}
		p.Snapshot.Files[entry.Path] = data
	}
	if !p.Valid() {
		return p, &Error{Class: "preparation_policy"}
	}
	return p, nil
}

// Verify raw bytes before JSON processing. Header cardinality is enforced by HTTP.
func (c *Client) Webhook(raw []byte, signature, event string) (int, string, error) {
	if len(raw) > 32768 || event != "issues" || !strings.HasPrefix(signature, "sha256=") {
		return 0, "", &Error{Class: "webhook_policy"}
	}
	supplied, e := hex.DecodeString(strings.TrimPrefix(signature, "sha256="))
	mac := hmac.New(sha256.New, []byte(c.cfg.WebhookSecret))
	mac.Write(raw)
	if e != nil || !hmac.Equal(supplied, mac.Sum(nil)) {
		return 0, "", &Error{Class: "signature"}
	}
	var check any
	if sandbox.Decode(raw, &check, 32768) != nil {
		return 0, "", &Error{Class: "webhook_policy"}
	}
	var v struct {
		Action     string
		Repository struct {
			ID       int64
			FullName string `json:"full_name"`
		}
		Sender struct{ Login string }
		Issue  Issue
		Label  struct{ Name string }
	}
	if json.Unmarshal(raw, &v) != nil || v.Action != "labeled" || v.Repository.ID != c.Repository().ID || !strings.EqualFold(v.Repository.FullName, c.Repository().Owner+"/"+c.Repository().Name) || !strings.EqualFold(v.Sender.Login, c.cfg.TriggerActor) || v.Label.Name != c.cfg.TriggerLabel || v.Issue.Number < 1 || v.Issue.State != "open" || len(v.Issue.PullRequest) > 0 || len(v.Issue.Title) > 200 || len(v.Issue.Body) > 1600 {
		return 0, "", &Error{Class: "webhook_policy"}
	}
	return v.Issue.Number, "github:" + strings.ToLower(v.Sender.Login), nil
}

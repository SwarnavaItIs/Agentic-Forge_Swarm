package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"forgeswarm/internal/sandbox"
	"io"
	"os"
	"strings"
	"unicode/utf8"
)

type Registration struct {
	RepositoryID string `json:"repository_id"`
	BaseRevision string `json:"base_revision"`
	TestProfile  string `json:"test_profile"`
}

func Register(ctx context.Context, registry, catalog, repository, rev string) (Registration, error) {
	return RegisterProfile(ctx, registry, catalog, repository, rev, Profile)
}
func RegisterProfile(ctx context.Context, registry, catalog, repository, rev, profile string) (Registration, error) {
	if profile != Profile && profile != MathProfile {
		return Registration{}, sandbox.ErrPolicy
	}
	id, err := sandbox.RegisterGit(ctx, registry, repository, rev)
	if err != nil {
		return Registration{}, err
	}
	s, err := sandbox.LoadSnapshot(registry, id)
	if err != nil {
		return Registration{}, err
	}
	for name, body := range s.Files {
		if !Visible(name) || !utf8.Valid(body) || bytes.ContainsRune(body, 0) {
			return Registration{}, sandbox.ErrPolicy
		}
	}
	// This first controlled profile evaluates a single dependency-free function.
	// New profiles require an operator-authored immutable evaluator, not a model
	// command or tests from the writable source tree.
	if !strings.HasPrefix(string(s.Files["go.mod"]), "module fixture\n") || profile == Profile && len(s.Files["sum.go"]) == 0 || profile == MathProfile && !ControlledMath(s) {
		return Registration{}, sandbox.ErrPolicy
	}
	r := Registration{id, rev, profile}
	body, _ := json.Marshal(r)
	if err = os.MkdirAll(catalog, 0700); err != nil {
		return r, err
	}
	root, err := os.OpenRoot(catalog)
	if err != nil {
		return r, err
	}
	defer root.Close()
	f, err := root.OpenFile(id+".json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		old, e := Lookup(catalog, id)
		if e != nil || old != r {
			return r, sandbox.ErrPolicy
		}
		return r, nil
	}
	if err != nil {
		return r, err
	}
	_, err = f.Write(body)
	if err == nil {
		err = f.Sync()
	}
	ce := f.Close()
	if err != nil {
		return r, err
	}
	return r, ce
}
func Lookup(catalog, id string) (Registration, error) {
	var r Registration
	if catalog == "" || !ValidDigest(id) {
		return r, sandbox.ErrPolicy
	}
	root, err := os.OpenRoot(catalog)
	if err != nil {
		return r, err
	}
	defer root.Close()
	f, err := root.Open(id + ".json")
	if err != nil {
		return r, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, 1025))
	if err != nil || sandbox.Decode(b, &r, 1024) != nil || r.RepositoryID != id || !revision.MatchString(r.BaseRevision) || (r.TestProfile != Profile && r.TestProfile != MathProfile) {
		return r, sandbox.ErrPolicy
	}
	return r, nil
}

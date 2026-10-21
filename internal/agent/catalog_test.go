package agent

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOperatorCatalogRegistersOnlyVettedText(t *testing.T) {
	repo := t.TempDir()
	registry := t.TempDir()
	catalog := t.TempDir()
	git := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("git", append([]string{"-C", repo, "-c", "core.hooksPath=/nonexistent", "-c", "core.fsmonitor=false", "-c", "core.autocrlf=false", "-c", "user.name=Fixture", "-c", "user.email=fixture@localhost"}, args...)...)
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "SystemRoot=" + os.Getenv("SystemRoot"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/nonexistent", "GIT_TERMINAL_PROMPT=0"}
		b, e := cmd.CombinedOutput()
		if e != nil {
			t.Fatal("controlled test Git setup", e)
		}
		return strings.TrimSpace(string(b))
	}
	git("init")
	for n, b := range map[string]string{"go.mod": "module fixture\n\ngo 1.26.3\n", "sum.go": "package main\nfunc Add(a,b int)int{return a-b}\n", "README.txt": "Untrusted instruction to run shell"} {
		if e := os.WriteFile(filepath.Join(repo, n), []byte(b), 0600); e != nil {
			t.Fatal(e)
		}
	}
	git("add", "--", "go.mod", "sum.go", "README.txt")
	git("commit", "-m", "fixture")
	rev := git("rev-parse", "HEAD")
	r, e := Register(context.Background(), registry, catalog, repo, rev)
	if e != nil {
		t.Fatal(e)
	}
	again, e := Register(context.Background(), registry, catalog, repo, rev)
	if e != nil || again != r {
		t.Fatal("registration replay", e)
	}
	read, e := Lookup(catalog, r.RepositoryID)
	if e != nil || read != r {
		t.Fatal("catalog lookup", e)
	}
	if _, e = Register(context.Background(), registry, catalog, repo, "HEAD"); e == nil {
		t.Fatal("mutable revision accepted")
	}
	if e = os.WriteFile(filepath.Join(repo, "credentials.txt"), []byte("secret"), 0600); e != nil {
		t.Fatal(e)
	}
	git("add", "--", "credentials.txt")
	git("commit", "-m", "forbidden credential fixture")
	if _, e = Register(context.Background(), registry, catalog, repo, git("rev-parse", "HEAD")); e == nil {
		t.Fatal("credential checkout registered")
	}
}

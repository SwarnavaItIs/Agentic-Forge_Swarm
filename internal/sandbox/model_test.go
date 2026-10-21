package sandbox

import (
	"archive/tar"
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func fixtureSnapshot() Snapshot {
	return Snapshot{1, strings.Repeat("a", 40), map[string][]byte{"go.mod": []byte("module fixture\n\ngo 1.26.3\n"), "sum.go": []byte("package fixture\nfunc Sum(a,b int) int {return a+b}\n")}}
}
func testBinding() Binding {
	a := uuid.New()
	return Binding{uuid.New(), uuid.New(), uuid.New(), a, uuid.New(), 1, "worker_a", RequestID(a)}
}
func TestRequestFailsClosed(t *testing.T) {
	in := Input{1, strings.Repeat("a", 64), "test"}
	r := NewRequest(testBinding(), in, DefaultPolicy())
	if !r.Valid() {
		t.Fatal("default invalid")
	}
	changes := []func(*Request){func(r *Request) { r.Image = "golang:latest" }, func(r *Request) { r.Argv = []string{"sh", "-c", "go test"} }, func(r *Request) { r.Directory = "/" }, func(r *Request) { r.Env = append(r.Env, "AWS_SECRET_ACCESS_KEY=x") }, func(r *Request) { r.WorkspaceID = uuid.New() }, func(r *Request) { r.Binding.Generation = 0 }, func(r *Request) { r.Binding.Request = uuid.New() }, func(r *Request) { r.Policy.MemoryBytes = 0 }, func(r *Request) { r.Policy.WorkspaceBytes = 0 }, func(r *Request) { r.Input.Command = "test;echo bad" }}
	for i, change := range changes {
		copy := NewRequest(r.Binding, in, DefaultPolicy())
		change(&copy)
		if copy.Valid() {
			t.Fatalf("mutation %d authorized", i)
		}
	}
	var value Input
	if Decode([]byte(`{"schema_version":1,"schema_version":1,"fixture_id":"a","command":"test"}`), &value, 1024) == nil {
		t.Fatal("duplicate keys accepted")
	}
}
func TestArchiveSnapshotAndRegistrationLimits(t *testing.T) {
	for _, name := range []string{"/etc/passwd", "../outside", "a/../../outside", "a\\outside", "C:/file", "a//b", "a/.git/config", ".gitmodules", "a\nfile", "a/./b"} {
		if SafePath(name) {
			t.Fatal(name)
		}
	}
	for _, typ := range []byte{tar.TypeSymlink, tar.TypeLink, tar.TypeChar, tar.TypeBlock, tar.TypeFifo} {
		var buffer bytes.Buffer
		w := tar.NewWriter(&buffer)
		if err := w.WriteHeader(&tar.Header{Name: "escape", Typeflag: typ, Linkname: "/etc/passwd"}); err != nil {
			t.Fatal(err)
		}
		_ = w.Close()
		if _, err := ArchiveInput(&buffer, strings.Repeat("a", 40)); err == nil {
			t.Fatal("archive special file accepted")
		}
	}
	s := fixtureSnapshot()
	id, err := SaveSnapshot(t.TempDir(), s)
	if err != nil || len(id) != 64 {
		t.Fatal(err)
	}
	s.Files["go.mod"] = []byte("module fixture\n\ngo 1.26.3\nrequire evil.example/tool v1.0.0\n")
	if s.Valid() {
		t.Fatal("dependency preparation bypass")
	}
	s = fixtureSnapshot()
	s.Files["big"] = make([]byte, MaxFile+1)
	if s.Valid() {
		t.Fatal("oversize fixture accepted")
	}
	s = fixtureSnapshot()
	s.Files[".git/config"] = nil
	if s.Valid() {
		t.Fatal("git config accepted")
	}
	registry := t.TempDir()
	s = fixtureSnapshot()
	id, err = SaveSnapshot(registry, s)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(registry, id+".json"), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = LoadSnapshot(registry, id); err == nil {
		t.Fatal("mutable registered snapshot accepted")
	}
}
func TestBoundedStreamsAndConfinedArtifacts(t *testing.T) {
	w := &CappedWriter{Limit: 128}
	n, err := w.Write(make([]byte, 1<<20))
	if err != nil || n != 1<<20 || len(w.Data) != 128 || !w.Truncated {
		t.Fatal("stream not bounded")
	}
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err = os.WriteFile(filepath.Join(dir, "safe"), []byte("ok"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err = ConfinedRead(root, "../outside", 8); !errors.Is(err, ErrPolicy) {
		t.Fatal(err)
	}
	if err = os.Symlink("safe", filepath.Join(dir, "link")); err == nil {
		if _, err = ConfinedRead(root, "link", 8); !errors.Is(err, ErrPolicy) {
			t.Fatal("symlink read", err)
		}
	}
	if _, err = ConfinedRead(root, "safe", 1); !errors.Is(err, ErrPolicy) {
		t.Fatal("oversized file read")
	}
	if _, err = RegisterGit(context.Background(), t.TempDir(), ".", "HEAD"); !errors.Is(err, ErrPolicy) {
		t.Fatal("symbolic revision accepted")
	}
}

func TestPatchGoldenTrailingNewlinesAddedAndDeletedFiles(t *testing.T) {
	dir := t.TempDir()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err = os.WriteFile(filepath.Join(dir, "changed"), []byte("new\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(dir, "added"), []byte("added\n"), 0600); err != nil {
		t.Fatal(err)
	}
	body, truncated := CollectPatch(root, map[string][]byte{"changed": []byte("old\n"), "deleted": []byte("deleted")})
	want := "--- /dev/null\n+++ b/added\n@@ -0,0 +1,1 @@\n+added\n--- a/changed\n+++ b/changed\n@@ -1,1 +1,1 @@\n-old\n+new\n--- a/deleted\n+++ /dev/null\n@@ -1,1 +0,0 @@\n-deleted\n\\ No newline at end of file\n"
	if truncated || string(body) != want {
		t.Fatalf("invalid unified patch:\n%s", body)
	}
}

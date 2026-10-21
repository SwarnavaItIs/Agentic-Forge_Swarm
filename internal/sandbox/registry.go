package sandbox

import (
	"archive/tar"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

// RegisterGit reads immutable Git objects, never a checkout, archive filter,
// hook, smudge command, submodule or LFS helper. The operator vets this fixture.
func RegisterGit(ctx context.Context, registry, repository, revision string) (string, error) {
	if !revisionPattern.MatchString(revision) {
		return "", ErrPolicy
	}
	git := func(args ...string) ([]byte, error) {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		cmd := exec.CommandContext(call, "git", append([]string{"--no-optional-locks", "-C", repository, "-c", "core.hooksPath=/nonexistent", "-c", "core.fsmonitor=false", "-c", "core.attributesFile=/nonexistent"}, args...)...)
		// Do not inherit GIT_DIR, external object stores, config injection, proxy
		// credentials, replace objects or user/global Git configuration.
		cmd.Env = []string{"PATH=" + os.Getenv("PATH"), "SystemRoot=" + os.Getenv("SystemRoot"), "GIT_CONFIG_NOSYSTEM=1", "GIT_CONFIG_GLOBAL=/nonexistent", "GIT_NO_REPLACE_OBJECTS=1", "GIT_NO_LAZY_FETCH=1", "GIT_TERMINAL_PROMPT=0"}
		out := &CappedWriter{Limit: MaxFixture}
		cmd.Stdout = out
		cmd.Stderr = io.Discard
		if err := cmd.Run(); err != nil || out.Truncated {
			return nil, ErrPolicy
		}
		return out.Data, nil
	}
	// cat-file -t verifies a commit rather than accepting a tree/blob label.
	kind, err := git("cat-file", "-t", revision)
	if err != nil || strings.TrimSpace(string(kind)) != "commit" {
		return "", ErrPolicy
	}
	tree, err := git("ls-tree", "-r", "-z", "--full-tree", revision)
	if err != nil {
		return "", err
	}
	s := Snapshot{1, revision, map[string][]byte{}}
	for _, entry := range bytes.Split(tree, []byte{0}) {
		if len(entry) == 0 {
			continue
		}
		parts := bytes.SplitN(entry, []byte{'\t'}, 2)
		if len(parts) != 2 {
			return "", ErrPolicy
		}
		fields := strings.Fields(string(parts[0]))
		name := string(parts[1])
		if len(fields) != 3 || fields[0] != "100644" || fields[1] != "blob" || !revisionPattern.MatchString(fields[2]) || !SafePath(name) || len(s.Files) >= 128 {
			return "", ErrPolicy
		}
		body, e := git("cat-file", "blob", fields[2])
		if e != nil || len(body) > MaxFile {
			return "", ErrPolicy
		}
		s.Files[name] = body
	}
	return SaveSnapshot(registry, s)
}

func SaveSnapshot(registry string, s Snapshot) (string, error) {
	body, id, err := SnapshotBytes(s)
	if err != nil {
		return "", err
	}
	if err = os.MkdirAll(registry, 0700); err != nil {
		return "", err
	}
	root, err := os.OpenRoot(registry)
	if err != nil {
		return "", err
	}
	defer root.Close()
	f, err := root.OpenFile(id+".json", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		existing, e := LoadSnapshot(registry, id)
		if e != nil {
			return "", e
		}
		_, same, e := SnapshotBytes(existing)
		if e != nil || same != id {
			return "", ErrPolicy
		}
		return id, nil
	}
	if err != nil {
		return "", err
	}
	_, err = f.Write(body)
	if err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err != nil {
		return "", err
	}
	return id, closeErr
}
func LoadSnapshot(registry, id string) (Snapshot, error) {
	var s Snapshot
	if !digestPattern.MatchString(id) {
		return s, ErrPolicy
	}
	root, err := os.OpenRoot(registry)
	if err != nil {
		return s, err
	}
	defer root.Close()
	f, err := root.Open(id + ".json")
	if err != nil {
		return s, err
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, 2*MaxFixture+1))
	if err != nil || Decode(body, &s, 2*MaxFixture) != nil {
		return s, ErrPolicy
	}
	_, actual, err := SnapshotBytes(s)
	if err != nil || actual != id {
		return s, ErrPolicy
	}
	return s, nil
}

// ArchiveInput accepts only bounded regular files. No tar links, sparse files,
// devices, absolute paths or duplicate entries ever reach a filesystem.
func ArchiveInput(reader io.Reader, revision string) (Snapshot, error) {
	s := Snapshot{1, revision, map[string][]byte{}}
	tr := tar.NewReader(io.LimitReader(reader, 2*MaxFixture+1))
	total := int64(0)
	for {
		h, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return s, ErrPolicy
		}
		if h.Typeflag != tar.TypeReg || !SafePath(h.Name) || h.Size < 0 || h.Size > MaxFile || len(s.Files) >= 128 {
			return s, ErrPolicy
		}
		if _, ok := s.Files[h.Name]; ok {
			return s, ErrPolicy
		}
		total += h.Size
		if total > MaxFixture {
			return s, ErrPolicy
		}
		body, err := io.ReadAll(io.LimitReader(tr, MaxFile+1))
		if err != nil || int64(len(body)) != h.Size {
			return s, ErrPolicy
		}
		s.Files[h.Name] = body
	}
	if !s.Valid() {
		return s, fmt.Errorf("%w: unsupported fixture", ErrPolicy)
	}
	return s, nil
}

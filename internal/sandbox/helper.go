package sandbox

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"
)

type Payload struct {
	Request  Request  `json:"request"`
	Snapshot Snapshot `json:"snapshot"`
	Profile  string   `json:"profile,omitempty"`
}

// ExecuteHelper runs ONLY in the constrained container. The immutable helper
// and policy are authoritative; repository tests and their reported output are
// untrusted diagnostics. The gateway always removes the entire container,
// including descendants left behind after the go command exits.
func ExecuteHelper(ctx context.Context, p Payload) (Result, error) {
	r, s := p.Request, p.Snapshot
	_, id, err := SnapshotBytes(s)
	if err != nil || !r.Valid() || id != r.Input.FixtureID {
		return Result{}, ErrPolicy
	}
	if err = verifyKernel(r.Policy); err != nil {
		return Result{}, err
	}
	if err = os.MkdirAll("/workspace/src", 0700); err != nil {
		return Result{}, err
	}
	if err = os.MkdirAll("/workspace/tmp", 0700); err != nil {
		return Result{}, err
	}
	if err = copyTrustedCache(); err != nil {
		return Result{}, err
	}
	root, err := os.OpenRoot("/workspace/src")
	if err != nil {
		return Result{}, err
	}
	defer root.Close()
	for name, body := range s.Files {
		if err = root.MkdirAll(path.Dir(name), 0700); err != nil {
			return Result{}, err
		}
		f, e := root.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if e != nil {
			return Result{}, e
		}
		_, e = f.Write(body)
		ce := f.Close()
		if e != nil {
			return Result{}, e
		}
		if ce != nil {
			return Result{}, ce
		}
	}
	if p.Profile != "" {
		if p.Profile != "sum-v1" || r.Input.Command != "test" {
			return Result{}, ErrPolicy
		}
		return executeProtected(ctx, r, s)
	}
	call, cancel := context.WithTimeout(ctx, time.Duration(r.Policy.TimeoutMS)*time.Millisecond)
	defer cancel()
	out, stderr := &CappedWriter{Limit: r.Policy.OutputBytes}, &CappedWriter{Limit: r.Policy.OutputBytes}
	cmd := exec.CommandContext(call, r.Argv[0], r.Argv[1:]...)
	cmd.Env = r.Env
	cmd.Dir = r.Directory
	cmd.Stdout = out
	cmd.Stderr = stderr
	cmd.WaitDelay = 200 * time.Millisecond
	beforeOOM := oomKills()
	err = cmd.Run()
	result := Result{SchemaVersion: 1, Status: "exited", ExitCode: 0, Image: r.Image, BaseRevision: s.BaseRevision, Policy: r.Policy}
	if err != nil {
		result.Status = "nonzero_exit"
		result.ExitCode = 1
		if exit, ok := err.(*exec.ExitError); ok {
			result.ExitCode = exit.ExitCode()
			if result.ExitCode < 0 {
				result.ExitCode = 137
			}
		} else {
			result.Status = "infrastructure_error"
		}
		if strings.Contains(string(stderr.Data), "syntax error") || strings.Contains(string(stderr.Data), "undefined:") || strings.Contains(string(stderr.Data), "build failed") || strings.Contains(string(out.Data), `"Action":"build-fail"`) {
			result.Status = "compiler_error"
		}
	}
	if call.Err() != nil {
		result.Status = "timeout"
		result.ExitCode = 124
	}
	if after := oomKills(); beforeOOM >= 0 && after > beforeOOM {
		result.Status = "oom"
		result.ExitCode = 137
	}
	patch, patchTruncated := CollectPatch(root, s.Files)
	result.Artifacts = []Artifact{{"stdout", "text/plain; charset=utf-8", out.Data, out.Truncated}, {"stderr", "text/plain; charset=utf-8", stderr.Data, stderr.Truncated}, {"patch", "text/plain; charset=utf-8", patch, patchTruncated}}
	if r.Input.Command == "test" {
		result.Artifacts = append(result.Artifacts, Artifact{"test_result", "text/plain; charset=utf-8", out.Data, out.Truncated})
	}
	return result, nil
}

func oomKills() int64 {
	body, err := os.ReadFile("/sys/fs/cgroup/memory.events")
	if err != nil {
		return -1
	}
	fields := strings.Fields(string(body))
	for i := 0; i+1 < len(fields); i += 2 {
		if fields[i] == "oom_kill" {
			value, e := strconv.ParseInt(fields[i+1], 10, 64)
			if e == nil {
				return value
			}
		}
	}
	return -1
}

// Copy only immutable, trusted preparation-cache entries. The destination is
// charged to the same hard tmpfs and memory ceilings as repository writes.
func copyTrustedCache() error {
	if err := os.MkdirAll("/workspace/cache", 0700); err != nil {
		return err
	}
	total := int64(0)
	return fs.WalkDir(os.DirFS("/opt/forgeswarm-cache"), ".", func(name string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		target := "/workspace/cache/" + name
		if d.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		if !d.Type().IsRegular() {
			return ErrPolicy
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		total += info.Size()
		if total > 192<<20 {
			return ErrPolicy
		}
		source, err := os.Open("/opt/forgeswarm-cache/" + name)
		if err != nil {
			return err
		}
		defer source.Close()
		dest, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		if err != nil {
			return err
		}
		_, err = io.Copy(dest, source)
		ce := dest.Close()
		if err != nil {
			return err
		}
		return ce
	})
}

// ConfinedRead neither trusts a string prefix nor follows a final symlink.
// Root confines parent symlink races; comparing the open handle to Lstat rejects
// a final-component substitution. Growing files are read with a hard bound.
func ConfinedRead(root *os.Root, name string, limit int) ([]byte, error) {
	if !SafePath(name) {
		return nil, ErrPolicy
	}
	before, err := root.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() {
		return nil, ErrPolicy
	}
	f, err := openConfined(root, name)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	after, err := f.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		return nil, ErrPolicy
	}
	body, err := io.ReadAll(io.LimitReader(f, int64(limit)+1))
	if err != nil || len(body) > limit {
		return nil, ErrPolicy
	}
	return body, nil
}
func CollectPatch(root *os.Root, original map[string][]byte) ([]byte, bool) {
	names := map[string]bool{}
	for name := range original {
		names[name] = true
	}
	denied := false
	count := 0
	_ = fs.WalkDir(root.FS(), ".", func(name string, d fs.DirEntry, err error) error {
		count++
		if count > 256 {
			denied = true
			return fs.SkipAll
		}
		if err != nil {
			denied = true
			return fs.SkipDir
		}
		if name == "." {
			return nil
		}
		if !SafePath(name) {
			denied = true
			if d.IsDir() {
				return fs.SkipDir
			}
			return nil
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			denied = true
			return nil
		}
		names[name] = true
		return nil
	})
	ordered := make([]string, 0, len(names))
	for n := range names {
		ordered = append(ordered, n)
	}
	slices.Sort(ordered)
	out := &CappedWriter{Limit: MaxArtifact}
	for _, name := range ordered {
		old, existed := original[name]
		body, err := ConfinedRead(root, name, MaxFile)
		removed := os.IsNotExist(err)
		if os.IsNotExist(err) {
			body = nil
		} else if err != nil {
			denied = true
			continue
		}
		if string(body) == string(old) {
			continue
		}
		lines := func(body []byte) []string {
			if len(body) == 0 {
				return nil
			}
			parts := strings.SplitAfter(string(body), "\n")
			if parts[len(parts)-1] == "" {
				parts = parts[:len(parts)-1]
			}
			return parts
		}
		oldLines, newLines := lines(old), lines(body)
		oldStart, newStart := 1, 1
		if len(oldLines) == 0 {
			oldStart = 0
		}
		if len(newLines) == 0 {
			newStart = 0
		}
		oldName, newName := "a/"+name, "b/"+name
		if !existed {
			oldName = "/dev/null"
		}
		if removed {
			newName = "/dev/null"
		}
		fmt.Fprintf(out, "--- %s\n+++ %s\n@@ -%d,%d +%d,%d @@\n", oldName, newName, oldStart, len(oldLines), newStart, len(newLines))
		for _, line := range oldLines {
			fmt.Fprintf(out, "-%s", line)
			if !strings.HasSuffix(line, "\n") {
				fmt.Fprint(out, "\n\\ No newline at end of file\n")
			}
		}
		for _, line := range newLines {
			fmt.Fprintf(out, "+%s", line)
			if !strings.HasSuffix(line, "\n") {
				fmt.Fprint(out, "\n\\ No newline at end of file\n")
			}
		}
	}
	return out.Data, out.Truncated || denied
}

//go:build linux

package sandbox

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestConfinedOpenCannotBlockOnFIFOReplacement(t *testing.T) {
	dir := t.TempDir()
	if err := syscall.Mkfifo(filepath.Join(dir, "replacement"), 0600); err != nil {
		t.Fatal(err)
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	done := make(chan error, 1)
	go func() {
		f, e := openConfined(root, "replacement")
		if e == nil {
			e = f.Close()
		}
		done <- e
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("untrusted FIFO blocked confined open")
	}
}

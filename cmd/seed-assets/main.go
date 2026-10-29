// Only trusted public fixture/catalog JSON is copied. The API may register
// additional allowlisted issue snapshots; workers mount the resulting volume RO.
package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
)

func main() {
	if err := seed(); err != nil {
		fmt.Fprintln(os.Stderr, "trusted public fixture seeding failed:", err)
		os.Exit(1)
	}
}
func seed() error {
	for _, folder := range []string{"fixtures", "catalog"} {
		from, to := filepath.Join("/seed", folder), filepath.Join("/assets", folder)
		if err := os.MkdirAll(to, 0750); err != nil {
			return err
		}
		if err := os.Chown(to, 10002, 10002); err != nil {
			return err
		}
		entries, err := os.ReadDir(from)
		if err != nil {
			return err
		}
		if len(entries) > 256 {
			return errors.New("fixture count ceiling")
		}
		for _, entry := range entries {
			if !entry.Type().IsRegular() || !regexp.MustCompile(`^[0-9a-f]{64}\.json$`).MatchString(entry.Name()) {
				return errors.New("unexpected seed path")
			}
			file, err := os.Open(filepath.Join(from, entry.Name()))
			if err != nil {
				return err
			}
			body, err := io.ReadAll(io.LimitReader(file, (1<<20)+1))
			file.Close()
			if err != nil || len(body) > 1<<20 {
				return errors.New("seed size ceiling")
			}
			path := filepath.Join(to, entry.Name())
			old, err := os.ReadFile(path)
			if err == nil {
				if !bytes.Equal(old, body) {
					return errors.New("immutable fixture differs")
				}
				continue
			}
			if !os.IsNotExist(err) {
				return err
			}
			if err = os.WriteFile(path, body, 0600); err != nil {
				return err
			}
			if err = os.Chown(path, 10002, 10002); err != nil {
				return err
			}
		}
	}
	return nil
}

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"forgeswarm/internal/operator"
	"os"
	"path/filepath"
)

func main() {
	path := flag.String("file", ".cache/operator-auth.json", "Private credential file; created exclusively, never overwritten")
	flag.Parse()
	if err := os.MkdirAll(filepath.Dir(*path), 0700); err != nil {
		fmt.Fprintln(os.Stderr, "credential directory unavailable")
		os.Exit(1)
	}
	file, err := os.OpenFile(*path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		fmt.Fprintln(os.Stderr, "credential file already exists or cannot be created")
		os.Exit(1)
	}
	cfg := operator.Config{Operator: operator.Credential{Actor: "owner", Token: operator.Random()}, Reader: operator.Credential{Actor: "observer", Token: operator.Random()}}
	body, _ := json.MarshalIndent(cfg, "", "  ")
	_, err = file.Write(append(body, '\n'))
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err != nil || closeErr != nil {
		fmt.Fprintln(os.Stderr, "credential file write failed")
		os.Exit(1)
	}
	fmt.Println("Created private operator/read-only credentials. Set OPERATOR_AUTH_FILE to this file; use its operator token in the local sign-in form.")
}

// Explicit trusted local setup. HTTP never accepts repository or archive paths.
package main

import (
	"context"
	"flag"
	"fmt"
	"forgeswarm/internal/sandbox"
	"os"
	"time"
)

func main() {
	registry := flag.String("registry", ".cache/sandbox-fixtures", "trusted fixture registry")
	repository := flag.String("repository", "", "vetted local Git repository")
	revision := flag.String("revision", "", "full immutable Git commit ID")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if *repository == "" || flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "repository and full revision are required")
		os.Exit(2)
	}
	id, err := sandbox.RegisterGit(ctx, *registry, *repository, *revision)
	if err != nil {
		fmt.Fprintln(os.Stderr, "fixture registration rejected:", err)
		os.Exit(1)
	}
	fmt.Println(id)
}

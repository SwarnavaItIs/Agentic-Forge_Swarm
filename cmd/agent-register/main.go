package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"forgeswarm/internal/agent"
	"os"
	"time"
)

func main() {
	repository := flag.String("repository", "", "operator-vetted local Git repository")
	rev := flag.String("revision", "", "full immutable commit")
	registry := flag.String("registry", ".cache/sandbox-fixtures", "snapshot registry")
	catalog := flag.String("catalog", ".cache/agent-catalog", "operator catalog")
	profile := flag.String("profile", agent.Profile, "operator-controlled sum-v1 or math-v1 profile")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	r, err := agent.RegisterProfile(ctx, *registry, *catalog, *repository, *rev, *profile)
	if err != nil {
		fmt.Fprintln(os.Stderr, "controlled repository registration rejected")
		os.Exit(1)
	}
	if json.NewEncoder(os.Stdout).Encode(r) != nil {
		os.Exit(1)
	}
}

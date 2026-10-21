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
	helper := flag.String("helper", "bin/sandbox-helper-linux", "trusted Linux helper binary")
	flag.Parse()
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	tag, err := sandbox.PrepareImage(ctx, *helper)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println(tag)
}

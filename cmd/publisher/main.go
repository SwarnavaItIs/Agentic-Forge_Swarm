// Publisher is an operator-started trusted process; workers/models have no
// equivalent RPC/tool. It never merges, rebases, force-pushes or deletes branches.
package main

import (
	"context"
	"errors"
	"flag"
	"forgeswarm/internal/config"
	"forgeswarm/internal/github"
	"forgeswarm/internal/postgres"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("publisher_stopped", "error_class", "configuration_or_publication_failure")
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	once := flag.Bool("once", false, "Process exactly the selected approved operation")
	approval := flag.String("approval", "", "Approved request UUID; mandatory with -once")
	repository := flag.Int64("repository-id", 0, "Exact configured GitHub repository ID")
	confirm := flag.Bool("confirm-publish", false, "Explicit operator authorization to publish the approved draft")
	readOnly := flag.Bool("reconcile-only", false, "Discover external effects without creating any remote resources")
	flag.Parse()
	if !*confirm && !*readOnly {
		return errors.New("explicit -confirm-publish or -reconcile-only required")
	}
	client, err := github.Load(os.Getenv("GITHUB_CONFIG_FILE"))
	if err != nil {
		return err
	}
	if *repository != client.Repository().ID {
		return errors.New("configured repository ID confirmation required")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	pc, err := cfg.PoolConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return err
	}
	defer pool.Close()
	store := postgres.New(pool)
	if err = store.Ready(ctx); err != nil {
		return err
	}
	id := uuid.Nil
	if *once {
		approvalID, e := uuid.Parse(*approval)
		if e != nil || approvalID == uuid.Nil {
			return errors.New("approval UUID required")
		}
		a, e := store.Approval(ctx, approvalID)
		if e != nil {
			return e
		}
		if a.Operation == nil || a.Binding.Repository != client.Repository() {
			return errors.New("approved operation missing or wrong repository")
		}
		id = a.Operation.ID
	} else if *approval != "" {
		return errors.New("-approval requires -once")
	}
	for {
		call, cancel := context.WithTimeout(ctx, 45*time.Second)
		worked, e := store.PublishOne(call, client, id, *readOnly)
		cancel()
		if e != nil {
			logger.Error("publication_step_failed", "error_class", "durable_or_remote_state_unavailable")
			if *once {
				return e
			}
		}
		if *once {
			logger.Info("publication_observed", "operation_id", id, "processed", worked, "test_demo", client.TestDemo())
			return nil
		}
		timer := time.NewTimer(time.Second)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

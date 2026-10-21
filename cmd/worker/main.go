package main

import (
	"context"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/broker"
	"forgeswarm/internal/config"
	"forgeswarm/internal/rpc"
	"forgeswarm/internal/sandbox"
	"forgeswarm/internal/worker"
	"github.com/google/uuid"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if run(log) != nil {
		log.Error("worker_stopped", "error_class", "configuration_or_startup_failure")
		os.Exit(1)
	}
}
func run(log *slog.Logger) error {
	c, err := config.DistributedLoad()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var driver *sandbox.Docker
	if os.Getenv("SANDBOX_ENABLED") == "true" {
		installation, e := uuid.Parse(os.Getenv("SANDBOX_INSTALLATION_ID"))
		if e != nil {
			return e
		}
		driver, err = sandbox.NewDocker(ctx, installation, os.Getenv("SANDBOX_FIXTURE_REGISTRY"), os.Getenv("SANDBOX_HELPER"))
		if err != nil {
			log.Error("sandbox_runtime_unavailable", "error_class", "unsupported_runtime_or_unprepared_image", "reason", err.Error())
			return err
		}
		log.Info("sandbox_runtime_verified", "isolation_mode", driver.Mode(), "helper_sha256", driver.HelperHash(), "scope", "vetted_dependency_free_go_fixtures")
	} else if value := os.Getenv("SANDBOX_ENABLED"); value != "" && value != "false" {
		return sandbox.ErrPolicy
	}
	conn, err := rpc.Dial(c)
	if err != nil {
		return err
	}
	defer conn.Close()
	connect, cancel := context.WithTimeout(ctx, 5*time.Second)
	b, err := broker.Open(connect, c)
	cancel()
	if err != nil {
		return err
	}
	defer b.Close()
	log.Info("worker_started", "capacity", c.Capacity)
	return worker.RunWithSandbox(ctx, workerv1.NewCoordinatorClient(conn), b, c, log, driver)
}

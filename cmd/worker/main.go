package main

import (
	"context"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/broker"
	"forgeswarm/internal/config"
	"forgeswarm/internal/rpc"
	"forgeswarm/internal/worker"
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
	return worker.Run(ctx, workerv1.NewCoordinatorClient(conn), b, c, log)
}

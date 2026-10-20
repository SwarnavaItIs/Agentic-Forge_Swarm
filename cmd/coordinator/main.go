package main

import (
	"context"
	"forgeswarm/api/worker/v1"
	"forgeswarm/internal/config"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/rpc"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/grpc"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if run(log) != nil {
		log.Error("coordinator_stopped", "error_class", "configuration_or_serve_failure")
		os.Exit(1)
	}
}
func run(log *slog.Logger) error {
	base, err := config.Load()
	if err != nil {
		return err
	}
	c, err := config.DistributedLoad()
	if err != nil {
		return err
	}
	keys, err := c.WorkerKeys()
	if err != nil {
		return err
	}
	opts, err := rpc.ServerOptions(c, keys)
	if err != nil {
		return err
	}
	pc, err := base.PoolConfig()
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(context.Background(), pc)
	if err != nil {
		return err
	}
	defer pool.Close()
	store, err := postgres.NewConfigured(pool, c.Limits, base.RecoveryPolicy)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	listener, err := net.Listen("tcp", c.RPCAddr)
	if err != nil {
		return err
	}
	server := grpc.NewServer(opts...)
	workerv1.RegisterCoordinatorServer(server, rpc.New(store))
	result := make(chan error, 1)
	go func() { result <- server.Serve(listener) }()
	log.Info("coordinator_started")
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
	}
	done := make(chan struct{})
	go func() { server.GracefulStop(); close(done) }()
	timer := time.NewTimer(c.Shutdown)
	defer timer.Stop()
	select {
	case <-done:
	case <-timer.C:
		server.Stop()
	}
	log.Info("coordinator_drained")
	return nil
}

package main

import (
	"context"
	"forgeswarm/internal/broker"
	"forgeswarm/internal/config"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/relay"
	"github.com/jackc/pgx/v5/pgxpool"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if run(log) != nil {
		log.Error("relay_stopped", "error_class", "configuration_or_startup_failure")
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pc, err := base.PoolConfig()
	if err != nil {
		return err
	}
	pool, err := pgxpool.NewWithConfig(ctx, pc)
	if err != nil {
		return err
	}
	defer pool.Close()
	connect, cancel := context.WithTimeout(ctx, 5e9)
	b, err := broker.Open(connect, c)
	cancel()
	if err != nil {
		return err
	}
	defer b.Close()
	log.Info("relay_started")
	err = relay.Run(ctx, postgres.New(pool), b, c.RelayBatch, c.RelayLease, c.Poll, log)
	log.Info("relay_drained")
	return err
}

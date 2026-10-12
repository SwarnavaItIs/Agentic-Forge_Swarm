package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"forgeswarm/internal/config"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/scheduler"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("scheduler_stopped", "error_class", "configuration_or_startup_failure")
		os.Exit(1)
	}
}
func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	poolCfg, err := cfg.PoolConfig()
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	logger.Info("scheduler_started", "batch_size", cfg.SchedulerBatchSize, "interval", cfg.SchedulerInterval.String())
	err = scheduler.Run(ctx, postgres.New(pool), scheduler.Settings{BatchSize: cfg.SchedulerBatchSize, Interval: cfg.SchedulerInterval, ScanTimeout: cfg.SchedulerScanTimeout}, logger)
	logger.Info("scheduler_drained")
	return err
}

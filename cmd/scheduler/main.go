package main

import (
	"context"
	"forgeswarm/internal/telemetry/setup"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"forgeswarm/internal/agent"
	"forgeswarm/internal/config"
	"forgeswarm/internal/postgres"
	"forgeswarm/internal/scheduler"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	stopTelemetry := setup.Init("scheduler")
	defer stopTelemetry()
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
	store, err := postgres.NewConfigured(pool, cfg.ExecutionLimits, cfg.RecoveryPolicy)
	if err != nil {
		return err
	}
	if value := os.Getenv("APPROVAL_TTL"); value != "" {
		ttl, e := time.ParseDuration(value)
		if e != nil {
			return e
		}
		if e = store.ConfigureApprovalTTL(ttl); e != nil {
			return e
		}
	}
	if os.Getenv("AGENT_ENABLED") == "true" {
		pricing, e := agent.LoadPricing(os.Getenv("AGENT_PRICING_FILE"))
		if e != nil {
			return e
		}
		if e = store.ConfigureAgent(pricing); e != nil {
			return e
		}
	}
	err = scheduler.Run(ctx, store, scheduler.Settings{BatchSize: cfg.SchedulerBatchSize, Interval: cfg.SchedulerInterval, ScanTimeout: cfg.SchedulerScanTimeout}, logger)
	logger.Info("scheduler_drained")
	return err
}

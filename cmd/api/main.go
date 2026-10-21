package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"forgeswarm/internal/config"
	"forgeswarm/internal/httpapi"
	"forgeswarm/internal/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("api_stopped", "error_class", "startup_or_serve_failure")
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		logger.Error("invalid_config", "reason", err.Error())
		return err
	}
	poolCfg, err := cfg.PoolConfig()
	if err != nil {
		return err
	}
	// Lazy pool initialization permits liveness even during a dependency outage.
	pool, err := pgxpool.NewWithConfig(context.Background(), poolCfg)
	if err != nil {
		return err
	}
	defer pool.Close()
	store, err := postgres.NewConfigured(pool, cfg.ExecutionLimits, cfg.RecoveryPolicy)
	if err != nil {
		return err
	}
	api := httpapi.New(store, logger, cfg.RequestTimeout, cfg.ReadyTimeout).WithAgentCatalog(os.Getenv("AGENT_CATALOG"))
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: api.Handler(), ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout: cfg.ReadTimeout, WriteTimeout: cfg.WriteTimeout, IdleTimeout: cfg.IdleTimeout, MaxHeaderBytes: 16 << 10}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.ListenAndServe() }()
	logger.Info("api_started", "address", cfg.HTTPAddr)
	select {
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-signals.Done():
		api.Stop()
		logger.Info("api_draining")
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			_ = server.Close()
			return err
		}
	}
	return nil
}

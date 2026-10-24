package main

import (
	"context"
	"errors"
	"forgeswarm/internal/telemetry/setup"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"forgeswarm/internal/agent"
	"forgeswarm/internal/config"
	"forgeswarm/internal/github"
	"forgeswarm/internal/httpapi"
	"forgeswarm/internal/operator"
	"forgeswarm/internal/postgres"
	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	stopTelemetry := setup.Init("api")
	defer stopTelemetry()
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
	if os.Getenv("TELEMETRY_ENABLED") == "true" {
		registration, e := store.RegisterOperationalMetrics()
		if e == nil {
			defer registration.Unregister()
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
	api := httpapi.New(store, logger, cfg.RequestTimeout, cfg.ReadyTimeout).WithAgentCatalog(os.Getenv("AGENT_CATALOG"))
	authPath, githubPath := os.Getenv("OPERATOR_AUTH_FILE"), os.Getenv("GITHUB_CONFIG_FILE")
	if authPath != "" || githubPath != "" {
		if !httpapi.LoopbackAddress(cfg.HTTPAddr) || authPath == "" {
			return errors.New("authenticated controls require loopback and OPERATOR_AUTH_FILE")
		}
		forbidden := []string{os.Getenv("AGENT_RUNTIME_TOKEN"), os.Getenv("WORKER_TOKEN")}
		if path := os.Getenv("RPC_KEYS_FILE"); path != "" {
			keys, e := (config.Distributed{KeysFile: path}).WorkerKeys()
			if e != nil {
				return e
			}
			for _, v := range keys {
				forbidden = append(forbidden, v)
			}
		}
		auth, e := operator.Load(authPath, forbidden)
		if e != nil {
			return e
		}
		api.WithOperator(auth)
		if githubPath != "" {
			if os.Getenv("COLLAB_ENABLED") != "true" {
				return errors.New("GitHub import requires collaboration enabled")
			}
			client, e := github.Load(githubPath)
			if e != nil {
				return e
			}
			if client.OperatorCredentialConflict(auth) {
				return errors.New("GitHub service and operator credentials must differ")
			}
			api.WithGitHub(client)
		}
	}
	var handler http.Handler = api.Handler()
	if os.Getenv("COLLAB_ENABLED") == "true" || os.Getenv("WEB_DIR") != "" || authPath != "" {
		if !httpapi.LoopbackAddress(cfg.HTTPAddr) {
			return errors.New("dashboard and collaboration controls require local loopback")
		}
		rounds := 2
		if value := os.Getenv("COLLAB_MAX_ROUNDS"); value != "" {
			rounds, err = strconv.Atoi(value)
			if err != nil || rounds < 0 || rounds > 2 {
				return errors.New("COLLAB_MAX_ROUNDS must be 0-2")
			}
		}
		registry := os.Getenv("SANDBOX_FIXTURE_REGISTRY")
		if registry == "" {
			registry = ".cache/sandbox-fixtures"
		}
		if os.Getenv("COLLAB_ENABLED") == "true" {
			api.WithCollaboration(registry, rounds)
		}
		handler = httpapi.PrivateUI(api.Handler(), os.Getenv("WEB_DIR"))
	}
	server := &http.Server{Addr: cfg.HTTPAddr, Handler: handler, ReadHeaderTimeout: cfg.ReadHeaderTimeout,
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

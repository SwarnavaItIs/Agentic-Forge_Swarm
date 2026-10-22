// Package scheduler owns a real process boundary; domain decisions and durable
// transitions remain reusable in workflow and postgres. No execution occurs.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"forgeswarm/internal/execution"
	"forgeswarm/internal/postgres"
)

type Store interface {
	ReconcileBatch(context.Context, int) (postgres.ScanResult, error)
}
type Settings struct {
	BatchSize             int
	Interval, ScanTimeout time.Duration
}

// Run reconciles immediately on startup and once per bounded interval. Recovery
// comes solely from PostgreSQL; no graph/readiness cache or leader mutex exists.
func Run(ctx context.Context, store Store, settings Settings, logger *slog.Logger) error {
	if settings.BatchSize < 1 || settings.BatchSize > 100 || settings.Interval < 50*time.Millisecond || settings.Interval > time.Minute || settings.ScanTimeout < 50*time.Millisecond || settings.ScanTimeout > 30*time.Second {
		return errors.New("invalid scheduler settings")
	}
	for {
		if ctx.Err() != nil {
			return nil
		}
		scanCtx, cancel := context.WithTimeout(ctx, settings.ScanTimeout)
		result, err := store.ReconcileBatch(scanCtx, settings.BatchSize)
		if err == nil {
			if collaboration, ok := store.(interface {
				ReconcileCollaboration(context.Context, int) error
			}); ok {
				err = collaboration.ReconcileCollaboration(scanCtx, settings.BatchSize)
			}
		}
		cancel()
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			if errors.Is(err, execution.ErrInvariant) {
				logger.Error("recovery_consistency_violation", "error_class", "ownership_consistency_violation")
				return err
			}
			logger.Error("scheduler_scan_failed", "error_class", "database_or_state_unavailable")
		} else {
			// Counts only; no task IDs or workflow IDs become metric labels.
			logger.Info("scheduler_scan", "workflows", result.Workflows, "transitions", result.Transitions)
		}
		timer := time.NewTimer(settings.Interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
	}
}

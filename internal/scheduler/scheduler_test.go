package scheduler

import (
	"context"
	"errors"
	"forgeswarm/internal/postgres"
	"io"
	"log/slog"
	"testing"
	"time"
)

type storeFunc func(context.Context, int) (postgres.ScanResult, error)

func (f storeFunc) ReconcileBatch(ctx context.Context, batch int) (postgres.ScanResult, error) {
	return f(ctx, batch)
}
func TestStartupBoundedScanAndGracefulShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	fake := storeFunc(func(scan context.Context, batch int) (postgres.ScanResult, error) {
		calls++
		if batch != 2 {
			t.Fatal("batch setting ignored")
		}
		deadline, ok := scan.Deadline()
		if !ok || time.Until(deadline) > time.Second {
			t.Fatal("unbounded scan")
		}
		<-scan.Done()
		cancel()
		return postgres.ScanResult{}, scan.Err()
	})
	start := time.Now()
	err := Run(ctx, fake, Settings{2, time.Second, 50 * time.Millisecond}, slog.New(slog.NewJSONHandler(io.Discard, nil)))
	if err != nil || calls != 1 || time.Since(start) > time.Second {
		t.Fatal("startup/shutdown", err, calls)
	}
}
func TestOutageRetriesAndInvalidSettings(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	fake := storeFunc(func(context.Context, int) (postgres.ScanResult, error) {
		calls++
		if calls == 2 {
			cancel()
		}
		return postgres.ScanResult{}, errors.New("outage")
	})
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	if err := Run(ctx, fake, Settings{1, 50 * time.Millisecond, time.Second}, logger); err != nil || calls != 2 {
		t.Fatal("reconciliation stopped after outage")
	}
	if err := Run(ctx, fake, Settings{0, time.Second, time.Second}, logger); err == nil {
		t.Fatal("invalid config")
	}
}

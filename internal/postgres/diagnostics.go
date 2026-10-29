package postgres

import (
	"context"
	"forgeswarm/internal/workflow"
	"time"
)

type Diagnostics struct {
	Operational   OperationalMetrics `json:"operational"`
	Pool          map[string]any     `json:"pool"`
	Cleanup       int64              `json:"cleanup_backlog"`
	Artifacts     int64              `json:"artifacts"`
	ArtifactBytes int64              `json:"artifact_bytes"`
	Pricing       map[string]any     `json:"configured_pricing"`
}

func (s *Store) Diagnostics(ctx context.Context) (Diagnostics, error) {
	ctx, cancel := context.WithTimeout(ctx, 700*time.Millisecond)
	defer cancel()
	data, err := s.OperationalSnapshot(ctx)
	if err != nil {
		return Diagnostics{}, err
	}
	stat := s.pool.Stat()
	out := Diagnostics{Operational: data, Pool: map[string]any{"acquired": stat.AcquiredConns(), "idle": stat.IdleConns(), "total": stat.TotalConns(), "max": stat.MaxConns(), "acquire_count": stat.AcquireCount(), "acquire_seconds": stat.AcquireDuration().Seconds(), "empty_acquire_count": stat.EmptyAcquireCount(), "cancelled_acquire_count": stat.CanceledAcquireCount()}}
	if p := s.agentPricing; p != nil {
		// Only public cost/model policy. Operator notes and source strings never
		// enter responses. A live admission guard checks the actual server cap.
		out.Pricing = map[string]any{"provider": p.Provider, "model": p.Model, "input_microusd_per_million": p.Input, "output_microusd_per_million": p.Output, "maximum_estimate_microusd": p.Maximum}
	}
	err = s.pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM task_attempts WHERE cleanup_required),(SELECT count(*) FROM sandbox_artifacts),(SELECT COALESCE(sum(octet_length(body)),0) FROM sandbox_artifacts)`).Scan(&out.Cleanup, &out.Artifacts, &out.ArtifactBytes)
	if err != nil {
		return Diagnostics{}, workflow.ErrUnavailable
	}
	return out, nil
}

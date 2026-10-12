package relay

import (
	"context"
	"forgeswarm/internal/postgres"
	"github.com/google/uuid"
	"log/slog"
	"time"
)

type Store interface {
	ClaimOutbox(context.Context, uuid.UUID, int, time.Duration) ([]postgres.Dispatch, error)
	MarkSent(context.Context, uuid.UUID, uuid.UUID) error
}
type Publisher interface {
	Publish(context.Context, string, []byte) error
}

func Step(ctx context.Context, s Store, p Publisher, id uuid.UUID, batch int, lease time.Duration) (int, error) {
	items, err := s.ClaimOutbox(ctx, id, batch, lease)
	if err != nil {
		return 0, err
	}
	sent := 0
	for _, item := range items {
		// ClaimOutbox already committed: no DB lock remains during publish/ack.
		if err = p.Publish(ctx, item.ID.String(), item.Payload); err != nil {
			return sent, err
		}
		if err = s.MarkSent(ctx, id, item.ID); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}
func Run(ctx context.Context, s Store, p Publisher, batch int, lease, poll time.Duration, logger *slog.Logger) error {
	id := uuid.New()
	for ctx.Err() == nil {
		step, cancel := context.WithTimeout(ctx, 2*time.Second)
		n, err := Step(step, s, p, id, batch, lease)
		cancel()
		if err != nil {
			logger.Warn("relay_step_failed", "error_class", "database_or_broker_unavailable")
		} else {
			logger.Info("relay_step", "sent", n)
		}
		timer := time.NewTimer(poll)
		select {
		case <-ctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}
	return nil
}

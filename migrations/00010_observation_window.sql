-- +goose Up
-- Read-only telemetry windows use a stable transaction timestamp. This index
-- bounds their event scan without changing lifecycle/lease clock semantics.
CREATE INDEX workflow_events_observed_at_idx ON workflow_events(occurred_at, workflow_id);

-- +goose Down
DROP INDEX workflow_events_observed_at_idx;

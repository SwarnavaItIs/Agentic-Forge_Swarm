-- +goose Up
-- Correlation only. No authorization, request normalization or idempotency hash
-- depends on these fields. Existing workflows and deliveries remain valid.
ALTER TABLE workflows ADD COLUMN traceparent text NOT NULL DEFAULT '' CHECK(traceparent='' OR traceparent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$');
ALTER TABLE outbox ADD COLUMN traceparent text NOT NULL DEFAULT '' CHECK(traceparent='' OR traceparent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$');

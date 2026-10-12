-- +goose Up
CREATE TABLE workflows (
    id uuid PRIMARY KEY NOT NULL,
    name text NOT NULL CHECK (char_length(name) BETWEEN 1 AND 128),
    status text NOT NULL CHECK (status IN ('PENDING', 'CANCELLED')),
    version bigint NOT NULL CHECK (version > 0),
    creation_key text NOT NULL UNIQUE CHECK (creation_key ~ '^[A-Za-z0-9._:-]{1,128}$'),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    request_schema_version integer NOT NULL CHECK (request_schema_version = 1),
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);

CREATE TABLE tasks (
    id uuid PRIMARY KEY NOT NULL,
    workflow_id uuid NOT NULL REFERENCES workflows(id) ON DELETE RESTRICT,
    task_key text NOT NULL CHECK (task_key ~ '^[a-z][a-z0-9_-]{0,63}$'),
    position integer NOT NULL CHECK (position >= 0),
    kind text NOT NULL CHECK (kind = 'noop'),
    status text NOT NULL CHECK (status IN ('PENDING', 'CANCELLED')),
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE (workflow_id, task_key),
    UNIQUE (workflow_id, position)
);

CREATE TABLE workflow_events (
    workflow_id uuid NOT NULL REFERENCES workflows(id) ON DELETE RESTRICT,
    sequence bigint NOT NULL CHECK (sequence > 0),
    event_type text NOT NULL CHECK (event_type IN ('workflow.created', 'workflow.cancelled')),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload) = 'object' AND octet_length(payload::text) <= 4096),
    occurred_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    PRIMARY KEY (workflow_id, sequence)
);

-- Index coverage: workflow PK serves details/locking, creation_key UNIQUE serves
-- retries. Task composite UNIQUE indexes cover child updates, ordered details,
-- and FK checks. Event PK covers FK checks and exclusive ordered cursor reads.
-- No additional indexes or speculative tables are needed in stage 01.

-- +goose Down
DROP TABLE workflow_events;
DROP TABLE tasks;
DROP TABLE workflows;

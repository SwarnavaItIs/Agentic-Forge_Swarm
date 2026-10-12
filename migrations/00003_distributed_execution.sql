-- +goose Up
-- Stage 02 never creates actual RUNNING ownership. Refuse to invent attempts
-- for manually inserted RUNNING rows during an upgrade.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM tasks WHERE status='RUNNING') THEN
        RAISE EXCEPTION 'unowned stage-02 RUNNING rows require repair before upgrade';
    END IF;
END $$;
-- +goose StatementEnd
ALTER TABLE workflows DROP CONSTRAINT workflows_status_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_status_check CHECK (status IN ('PENDING','RUNNING','CANCELLED','SUCCEEDED','FAILED'));
ALTER TABLE workflows DROP CONSTRAINT workflows_request_schema_version_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_request_schema_version_check CHECK (request_schema_version IN (1,2,3));
ALTER TABLE tasks DROP CONSTRAINT tasks_kind_check;
ALTER TABLE tasks ADD COLUMN delay_ms integer NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD CONSTRAINT tasks_diagnostic_check CHECK ((kind='noop' AND delay_ms=0) OR (kind='delay' AND delay_ms BETWEEN 1 AND 30000));
ALTER TABLE tasks ADD COLUMN dispatch_generation bigint NOT NULL DEFAULT 0 CHECK (dispatch_generation>=0);
ALTER TABLE tasks ADD COLUMN attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count>=0);
ALTER TABLE tasks ADD COLUMN current_attempt_id uuid;
ALTER TABLE tasks ADD CONSTRAINT tasks_running_claim_check CHECK (status<>'RUNNING' OR current_attempt_id IS NOT NULL);
CREATE TABLE workers (
    id text PRIMARY KEY NOT NULL CHECK (id ~ '^[a-z][a-z0-9_-]{0,63}$'),
    instance_id uuid NOT NULL,
    capabilities text[] NOT NULL CHECK (cardinality(capabilities) BETWEEN 1 AND 2 AND capabilities <@ ARRAY['noop','delay']::text[]),
    capacity integer NOT NULL CHECK (capacity BETWEEN 1 AND 100),
    heartbeat_at timestamptz NOT NULL,
    draining boolean NOT NULL DEFAULT false
);
CREATE TABLE task_attempts (
    id uuid PRIMARY KEY NOT NULL,
    workflow_id uuid NOT NULL,
    task_id uuid NOT NULL,
    attempt_number integer NOT NULL CHECK (attempt_number>0),
    worker_id text NOT NULL REFERENCES workers(id) ON DELETE RESTRICT,
    worker_instance_id uuid NOT NULL,
    claim_request_id uuid NOT NULL,
    generation bigint NOT NULL CHECK (generation>0),
    started_at timestamptz NOT NULL,
    lease_expires_at timestamptz NOT NULL,
    finished_at timestamptz,
    state text NOT NULL CHECK (state IN ('RUNNING','SUCCEEDED','FAILED','LOST','CANCELLED')),
    outcome_digest bytea CHECK (outcome_digest IS NULL OR octet_length(outcome_digest)=32),
    result_metadata jsonb CHECK (result_metadata IS NULL OR (jsonb_typeof(result_metadata)='object' AND octet_length(result_metadata::text)<=4096)),
    FOREIGN KEY (workflow_id,task_id) REFERENCES tasks(workflow_id,id) ON DELETE RESTRICT,
    UNIQUE (task_id,id), UNIQUE (task_id,attempt_number), UNIQUE (task_id,generation), UNIQUE (worker_id,claim_request_id),
    CHECK (lease_expires_at>started_at),
    CHECK ((state='RUNNING' AND finished_at IS NULL) OR (state<>'RUNNING' AND finished_at IS NOT NULL)),
    CHECK ((state IN ('SUCCEEDED','FAILED') AND outcome_digest IS NOT NULL AND result_metadata IS NOT NULL) OR (state IN ('RUNNING','LOST','CANCELLED') AND outcome_digest IS NULL AND result_metadata IS NULL))
);
ALTER TABLE tasks ADD CONSTRAINT tasks_current_attempt_fk FOREIGN KEY (id,current_attempt_id) REFERENCES task_attempts(task_id,id) ON DELETE RESTRICT;
CREATE INDEX attempts_worker_running ON task_attempts(worker_id,lease_expires_at) WHERE state='RUNNING';
CREATE TABLE outbox (
    id uuid PRIMARY KEY NOT NULL,
    task_id uuid NOT NULL REFERENCES tasks(id) ON DELETE RESTRICT,
    dispatch_generation bigint NOT NULL CHECK (dispatch_generation>0),
    schema_version integer NOT NULL CHECK (schema_version=1),
    payload jsonb NOT NULL CHECK (jsonb_typeof(payload)='object' AND octet_length(payload::text)<=4096),
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    state text NOT NULL DEFAULT 'PENDING' CHECK (state IN ('PENDING','SENT')),
    relay_id uuid, relay_lease_expires_at timestamptz, sent_at timestamptz,
    UNIQUE (task_id,dispatch_generation),
    CHECK ((relay_id IS NULL)=(relay_lease_expires_at IS NULL)),
    CHECK ((state='SENT')=(sent_at IS NOT NULL))
);
CREATE INDEX outbox_pending ON outbox(available_at,id) WHERE state='PENDING';
-- Existing READY tasks need their first notification, without rewriting their
-- public version, timestamps, old hashes or immutable definitions.
UPDATE tasks SET dispatch_generation=1 WHERE status='READY';
INSERT INTO outbox(id,task_id,dispatch_generation,schema_version,payload)
SELECT gen_random_uuid(),id,1,1,jsonb_build_object('schema_version',1,'task_id',id,'dispatch_generation',1) FROM tasks WHERE status='READY';
CREATE INDEX workflows_active_scan ON workflows(scheduler_checked_at NULLS FIRST,created_at,id) WHERE status IN ('PENDING','RUNNING');
ALTER TABLE workflow_events DROP CONSTRAINT workflow_events_event_type_check;
ALTER TABLE workflow_events ADD CONSTRAINT workflow_events_event_type_check CHECK (event_type IN ('workflow.created','workflow.cancelled','task.ready','task.blocked','workflow.succeeded','workflow.failed','workflow.running','task.claimed','task.succeeded','task.failed','attempt.lost'));
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION protect_task_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' THEN
        IF ROW(NEW.id,NEW.workflow_id,NEW.task_key,NEW.position,NEW.kind,NEW.delay_ms,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.id,OLD.workflow_id,OLD.task_key,OLD.position,OLD.kind,OLD.delay_ms,OLD.created_at) THEN
            RAISE EXCEPTION 'task definitions are immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP='DELETE' THEN RAISE EXCEPTION 'task definitions are immutable' USING ERRCODE='23514'; END IF;
    PERFORM id FROM workflows WHERE id=NEW.workflow_id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM workflow_events WHERE workflow_id=NEW.workflow_id AND event_type='workflow.created') THEN
        RAISE EXCEPTION 'workflow definitions are finalized' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
-- This forward migration is deliberately irreversible: attempts and dispatch
-- history cannot safely be represented by stage 02. No destructive Down path.

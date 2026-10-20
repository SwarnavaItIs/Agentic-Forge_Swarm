-- +goose Up
ALTER TABLE workflows DROP CONSTRAINT workflows_status_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_status_check CHECK (status IN ('PENDING','RUNNING','CANCELLING','CANCELLED','SUCCEEDED','FAILED'));
ALTER TABLE workflows DROP CONSTRAINT workflows_request_schema_version_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_request_schema_version_check CHECK (request_schema_version IN (1,2,3,4));
ALTER TABLE workflows ADD COLUMN deadline_at timestamptz NOT NULL DEFAULT (clock_timestamp()+interval '10 minutes');
ALTER TABLE workflows ADD COLUMN terminal_reason text NOT NULL DEFAULT '' CHECK (terminal_reason IN ('','explicit_cancel','deadline_exceeded'));
ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check CHECK (status IN ('PENDING','READY','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED','BLOCKED','CANCELLED'));
ALTER TABLE tasks ADD COLUMN max_attempts integer NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 10);
ALTER TABLE tasks ADD COLUMN deadline_at timestamptz NOT NULL DEFAULT (clock_timestamp()+interval '2 minutes');
ALTER TABLE tasks ADD COLUMN next_attempt_at timestamptz;
ALTER TABLE tasks ADD COLUMN ready_since timestamptz;
ALTER TABLE tasks ADD COLUMN last_error_class text NOT NULL DEFAULT '' CHECK (last_error_class IN ('','transient_infrastructure','worker_lost','permission_denied','invalid_configuration','unsupported_task','diagnostic_failed','deadline_exceeded','explicit_cancel'));
ALTER TABLE tasks ADD CONSTRAINT tasks_retry_due_check CHECK ((status='RETRY_WAIT')=(next_attempt_at IS NOT NULL));
ALTER TABLE tasks ADD CONSTRAINT tasks_retry_deadline_check CHECK (next_attempt_at IS NULL OR next_attempt_at<deadline_at);
-- Existing active records receive one bounded upgrade grace period; hashes,
-- receipts, public timestamps and accepted outcomes remain untouched.
UPDATE tasks t SET deadline_at=LEAST(t.deadline_at,w.deadline_at), ready_since=CASE WHEN t.status='READY' THEN clock_timestamp() END FROM workflows w WHERE w.id=t.workflow_id;
ALTER TABLE task_attempts ADD COLUMN cancel_requested_at timestamptz;
ALTER TABLE task_attempts ADD COLUMN stop_reason text NOT NULL DEFAULT '' CHECK (stop_reason IN ('','explicit_cancel','deadline_exceeded'));
ALTER TABLE task_attempts ADD COLUMN cleanup_required boolean NOT NULL DEFAULT false;
ALTER TABLE task_attempts ADD COLUMN last_error_class text NOT NULL DEFAULT '' CHECK (last_error_class IN ('','transient_infrastructure','worker_lost','permission_denied','invalid_configuration','unsupported_task','diagnostic_failed','deadline_exceeded','explicit_cancel'));
UPDATE task_attempts SET last_error_class='diagnostic_failed' WHERE state='FAILED';
ALTER TABLE outbox ADD COLUMN publish_revision integer NOT NULL DEFAULT 0 CHECK (publish_revision>=0);
ALTER TABLE outbox DROP CONSTRAINT outbox_state_check;
ALTER TABLE outbox ADD CONSTRAINT outbox_state_check CHECK (state IN ('PENDING','SENT','OBSOLETE'));
-- Retain but retire old unsent notifications whose work is already claimed,
-- terminal or superseded. Valid READY notifications retain their generation.
UPDATE outbox o SET state='OBSOLETE',relay_id=NULL,relay_lease_expires_at=NULL
FROM tasks t JOIN workflows w ON w.id=t.workflow_id
WHERE o.task_id=t.id AND o.state='PENDING' AND
 (t.status<>'READY' OR t.dispatch_generation<>o.dispatch_generation OR w.status NOT IN ('PENDING','RUNNING'));
CREATE INDEX tasks_retry_due ON tasks(workflow_id,next_attempt_at,id) WHERE status='RETRY_WAIT';
CREATE INDEX attempts_active_expiry ON task_attempts(lease_expires_at,workflow_id,id) WHERE state='RUNNING';
CREATE INDEX tasks_active_ownership ON tasks(workflow_id,id) WHERE status='RUNNING';
CREATE INDEX tasks_ready_age ON tasks(ready_since,workflow_id,id) WHERE status='READY';
CREATE INDEX workflows_recovery_scan ON workflows(scheduler_checked_at NULLS FIRST,created_at,id) WHERE status IN ('PENDING','RUNNING','CANCELLING');
ALTER TABLE workflow_events DROP CONSTRAINT workflow_events_event_type_check;
ALTER TABLE workflow_events ADD CONSTRAINT workflow_events_event_type_check CHECK (event_type IN ('workflow.created','workflow.cancelled','task.ready','task.blocked','workflow.succeeded','workflow.failed','workflow.running','task.claimed','task.succeeded','task.failed','attempt.lost','task.retry_wait','workflow.cancelling','attempt.stop_requested','attempt.stopped','task.cancelled','task.dispatch_requeued'));
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION protect_task_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='UPDATE' THEN
        IF ROW(NEW.id,NEW.workflow_id,NEW.task_key,NEW.position,NEW.kind,NEW.delay_ms,NEW.max_attempts,NEW.deadline_at,NEW.created_at)
           IS DISTINCT FROM ROW(OLD.id,OLD.workflow_id,OLD.task_key,OLD.position,OLD.kind,OLD.delay_ms,OLD.max_attempts,OLD.deadline_at,OLD.created_at) THEN
            RAISE EXCEPTION 'task definitions and budgets are immutable' USING ERRCODE='23514';
        END IF;
        IF OLD.status IN ('SUCCEEDED','FAILED','BLOCKED','CANCELLED') AND NEW.status<>OLD.status THEN
            RAISE EXCEPTION 'terminal task cannot be redriven' USING ERRCODE='23514';
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
CREATE OR REPLACE FUNCTION protect_workflow_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.id,NEW.name,NEW.creation_key,NEW.request_hash,NEW.request_schema_version,NEW.deadline_at,NEW.created_at)
       IS DISTINCT FROM ROW(OLD.id,OLD.name,OLD.creation_key,OLD.request_hash,OLD.request_schema_version,OLD.deadline_at,OLD.created_at) THEN
        RAISE EXCEPTION 'workflow definition and deadline are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE FUNCTION protect_accepted_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.id,NEW.workflow_id,NEW.task_id,NEW.attempt_number,NEW.worker_id,NEW.worker_instance_id,NEW.claim_request_id,NEW.generation,NEW.started_at)
       IS DISTINCT FROM ROW(OLD.id,OLD.workflow_id,OLD.task_id,OLD.attempt_number,OLD.worker_id,OLD.worker_instance_id,OLD.claim_request_id,OLD.generation,OLD.started_at) THEN
        RAISE EXCEPTION 'attempt identity is immutable' USING ERRCODE='23514';
    END IF;
    IF OLD.state IN ('SUCCEEDED','FAILED') AND ROW(NEW.state,NEW.outcome_digest,NEW.result_metadata,NEW.finished_at,NEW.started_at,NEW.lease_expires_at,NEW.worker_id,NEW.worker_instance_id,NEW.task_id,NEW.generation,NEW.last_error_class)
       IS DISTINCT FROM ROW(OLD.state,OLD.outcome_digest,OLD.result_metadata,OLD.finished_at,OLD.started_at,OLD.lease_expires_at,OLD.worker_id,OLD.worker_instance_id,OLD.task_id,OLD.generation,OLD.last_error_class) THEN
        RAISE EXCEPTION 'accepted outcome is immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER immutable_accepted_outcome BEFORE UPDATE ON task_attempts FOR EACH ROW EXECUTE FUNCTION protect_accepted_outcome();
-- +goose StatementEnd
-- Forward only: persisted recovery/cancellation evidence has no lossless v3 form.

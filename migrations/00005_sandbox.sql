-- +goose Up
ALTER TABLE workflows DROP CONSTRAINT workflows_request_schema_version_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_request_schema_version_check CHECK (request_schema_version IN (1,2,3,4,5));
ALTER TABLE tasks ADD COLUMN sandbox_input jsonb;
ALTER TABLE tasks DROP CONSTRAINT tasks_diagnostic_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_execution_input_check CHECK (
 (kind='noop' AND delay_ms=0 AND sandbox_input IS NULL) OR
 (kind='delay' AND delay_ms BETWEEN 1 AND 30000 AND sandbox_input IS NULL) OR
 (kind='sandbox_go' AND delay_ms=0 AND sandbox_input IS NOT NULL AND jsonb_typeof(sandbox_input)='object'
  AND sandbox_input->>'schema_version'='1' AND sandbox_input->>'fixture_id' ~ '^[0-9a-f]{64}$'
  AND sandbox_input->>'command' IN ('build','test') AND sandbox_input ?& ARRAY['schema_version','fixture_id','command']
  AND sandbox_input - ARRAY['schema_version','fixture_id','command'] = '{}'::jsonb));
ALTER TABLE workers DROP CONSTRAINT workers_capabilities_check;
ALTER TABLE workers ADD CONSTRAINT workers_capabilities_check CHECK (cardinality(capabilities) BETWEEN 1 AND 3 AND capabilities <@ ARRAY['noop','delay','sandbox_go']::text[]);
CREATE TABLE sandbox_commands (
 attempt_id uuid PRIMARY KEY REFERENCES task_attempts(id) ON DELETE RESTRICT,
 request_id uuid NOT NULL UNIQUE,
 workflow_id uuid NOT NULL,
 task_id uuid NOT NULL,
 generation bigint NOT NULL CHECK (generation>0),
 input jsonb NOT NULL CHECK (jsonb_typeof(input)='object' AND octet_length(input::text)<=1024),
 authorized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 FOREIGN KEY (workflow_id,task_id) REFERENCES tasks(workflow_id,id),
 FOREIGN KEY (task_id,attempt_id) REFERENCES task_attempts(task_id,id)
);
CREATE TABLE sandbox_executions (
 attempt_id uuid PRIMARY KEY REFERENCES sandbox_commands(attempt_id),
 metadata jsonb NOT NULL CHECK (jsonb_typeof(metadata)='object' AND octet_length(metadata::text)<=8192)
);
-- Small local PostgreSQL byte store: content and accepted references commit
-- atomically, avoiding a filesystem/DB dual-write or unreferenced upload queue.
-- No cloud storage abstraction until a larger artifact workload needs one.
CREATE TABLE sandbox_artifacts (
 id uuid PRIMARY KEY,
 attempt_id uuid NOT NULL REFERENCES sandbox_executions(attempt_id),
 workflow_id uuid NOT NULL,
 task_id uuid NOT NULL,
 generation bigint NOT NULL CHECK (generation>0),
 kind text NOT NULL CHECK (kind IN ('stdout','stderr','patch','test_result')),
 media_type text NOT NULL CHECK (media_type='text/plain; charset=utf-8'),
 base_revision text NOT NULL CHECK (base_revision ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'),
 sha256 bytea NOT NULL CHECK (octet_length(sha256)=32),
 content_length integer NOT NULL CHECK (content_length BETWEEN 0 AND 8192),
 body bytea NOT NULL CHECK (octet_length(body)<=8192),
 truncated boolean NOT NULL,
 UNIQUE(attempt_id,kind),
 CHECK (content_length=octet_length(body)),
 FOREIGN KEY (workflow_id,task_id) REFERENCES tasks(workflow_id,id),
 FOREIGN KEY (task_id,attempt_id) REFERENCES task_attempts(task_id,id)
);
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION protect_task_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.id,NEW.workflow_id,NEW.task_key,NEW.position,NEW.kind,NEW.delay_ms,NEW.max_attempts,NEW.deadline_at,NEW.sandbox_input,NEW.created_at)
   IS DISTINCT FROM ROW(OLD.id,OLD.workflow_id,OLD.task_key,OLD.position,OLD.kind,OLD.delay_ms,OLD.max_attempts,OLD.deadline_at,OLD.sandbox_input,OLD.created_at)
  THEN RAISE EXCEPTION 'task definitions and budgets are immutable' USING ERRCODE='23514'; END IF;
  IF OLD.status IN ('SUCCEEDED','FAILED','BLOCKED','CANCELLED') AND NEW.status<>OLD.status THEN RAISE EXCEPTION 'terminal task cannot be redriven' USING ERRCODE='23514'; END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'task definitions are immutable' USING ERRCODE='23514'; END IF;
 PERFORM id FROM workflows WHERE id=NEW.workflow_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM workflow_events WHERE workflow_id=NEW.workflow_id AND event_type='workflow.created') THEN RAISE EXCEPTION 'workflow definitions are finalized' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION sandbox_command_owned() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM task_attempts a JOIN tasks t ON t.id=a.task_id JOIN workflows w ON w.id=a.workflow_id
 WHERE a.id=NEW.attempt_id AND a.workflow_id=NEW.workflow_id AND a.task_id=NEW.task_id AND a.generation=NEW.generation
 AND t.current_attempt_id=a.id AND t.dispatch_generation=a.generation AND a.state='RUNNING' AND t.status='RUNNING'
 AND w.status='RUNNING' AND a.lease_expires_at>clock_timestamp() AND a.cancel_requested_at IS NULL
 AND t.deadline_at>clock_timestamp() AND w.deadline_at>clock_timestamp() AND t.sandbox_input=NEW.input)
 THEN RAISE EXCEPTION 'sandbox command is not currently owned' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER owned_sandbox_command BEFORE INSERT ON sandbox_commands FOR EACH ROW EXECUTE FUNCTION sandbox_command_owned();
CREATE FUNCTION sandbox_execution_accepted() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM task_attempts a JOIN tasks t ON t.id=a.task_id WHERE a.id=NEW.attempt_id
 AND a.state IN ('SUCCEEDED','FAILED') AND t.current_attempt_id=a.id AND t.dispatch_generation=a.generation)
 THEN RAISE EXCEPTION 'sandbox execution has no accepted current outcome' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER accepted_sandbox_execution BEFORE INSERT ON sandbox_executions FOR EACH ROW EXECUTE FUNCTION sandbox_execution_accepted();
CREATE FUNCTION sandbox_artifact_owned() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM task_attempts a WHERE a.id=NEW.attempt_id AND a.workflow_id=NEW.workflow_id AND a.task_id=NEW.task_id AND a.generation=NEW.generation AND a.state IN ('SUCCEEDED','FAILED'))
 THEN RAISE EXCEPTION 'artifact attempt identity mismatch' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER owned_sandbox_artifact BEFORE INSERT ON sandbox_artifacts FOR EACH ROW EXECUTE FUNCTION sandbox_artifact_owned();
CREATE FUNCTION sandbox_evidence_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'sandbox evidence is immutable' USING ERRCODE='23514'; END $$;
CREATE TRIGGER immutable_sandbox_commands BEFORE UPDATE OR DELETE ON sandbox_commands FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER immutable_sandbox_executions BEFORE UPDATE OR DELETE ON sandbox_executions FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER immutable_sandbox_artifacts BEFORE UPDATE OR DELETE ON sandbox_artifacts FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
-- +goose StatementEnd
-- Forward only: accepted sandbox evidence has no lossless stage-04 form.

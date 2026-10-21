-- +goose Up
ALTER TABLE workflows DROP CONSTRAINT workflows_request_schema_version_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_request_schema_version_check CHECK(request_schema_version BETWEEN 1 AND 6);
ALTER TABLE tasks ADD COLUMN agent_input jsonb;
ALTER TABLE tasks DROP CONSTRAINT tasks_execution_input_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_execution_input_check CHECK (
 (kind='noop' AND delay_ms=0 AND sandbox_input IS NULL AND agent_input IS NULL) OR
 (kind='delay' AND delay_ms BETWEEN 1 AND 30000 AND sandbox_input IS NULL AND agent_input IS NULL) OR
 (kind IN ('sandbox_go','agent_go') AND delay_ms=0 AND sandbox_input IS NOT NULL
 AND jsonb_typeof(sandbox_input)='object' AND sandbox_input->>'schema_version'='1'
 AND sandbox_input->>'fixture_id' ~ '^[0-9a-f]{64}$' AND sandbox_input->>'command' IN ('build','test')
 AND sandbox_input ?& ARRAY['schema_version','fixture_id','command']
 AND sandbox_input - ARRAY['schema_version','fixture_id','command']='{}'::jsonb
 AND ((kind='sandbox_go' AND agent_input IS NULL) OR
 (kind='agent_go' AND agent_input IS NOT NULL AND jsonb_typeof(agent_input)='object'
 AND octet_length(agent_input::text)<=4096 AND agent_input->>'schema_version'='1'
 AND agent_input->>'repository_id'=sandbox_input->>'fixture_id'
 AND agent_input->>'base_revision' ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'
 AND length(agent_input->>'task_description') BETWEEN 1 AND 2000
 AND agent_input->>'policy'='controlled-go-v1' AND agent_input->>'budget'='small-v1'
 AND agent_input->>'test_profile'='sum-v1' AND sandbox_input->>'command'='test'
 AND agent_input ?& ARRAY['schema_version','repository_id','base_revision','task_description','policy','budget','test_profile']
 AND agent_input - ARRAY['schema_version','repository_id','base_revision','task_description','policy','budget','test_profile']='{}'::jsonb))));
ALTER TABLE workers DROP CONSTRAINT workers_capabilities_check;
ALTER TABLE workers ADD CONSTRAINT workers_capabilities_check CHECK(cardinality(capabilities) BETWEEN 1 AND 4 AND capabilities <@ ARRAY['noop','delay','sandbox_go','agent_go']::text[]);

-- JSON null and numeric strings must not pass SQL CHECK's unknown truth value
-- or ->> text coercion. Forward-only strengthening also covers legacy inputs.
ALTER TABLE tasks ADD CONSTRAINT tasks_execution_json_types CHECK (
 (sandbox_input IS NULL OR (
  jsonb_typeof(sandbox_input->'schema_version')='number' AND
  jsonb_typeof(sandbox_input->'fixture_id')='string' AND
  jsonb_typeof(sandbox_input->'command')='string') IS TRUE) AND
 (agent_input IS NULL OR (
  jsonb_typeof(agent_input->'schema_version')='number' AND
  jsonb_typeof(agent_input->'repository_id')='string' AND
  jsonb_typeof(agent_input->'base_revision')='string' AND
  jsonb_typeof(agent_input->'task_description')='string' AND
  jsonb_typeof(agent_input->'policy')='string' AND
  jsonb_typeof(agent_input->'budget')='string' AND
  jsonb_typeof(agent_input->'test_profile')='string') IS TRUE));

CREATE TABLE agent_sessions (
 attempt_id uuid PRIMARY KEY REFERENCES task_attempts(id),
 generation bigint NOT NULL CHECK(generation>0),
 base_sha256 text NOT NULL CHECK(base_sha256 ~ '^[0-9a-f]{64}$'),
 workspace_sha256 text NOT NULL CHECK(workspace_sha256 ~ '^[0-9a-f]{64}$'),
 deadline_at timestamptz NOT NULL,
 turns integer NOT NULL DEFAULT 0 CHECK(turns BETWEEN 0 AND 12),
 tools integer NOT NULL DEFAULT 0 CHECK(tools BETWEEN 0 AND 24),
 denials integer NOT NULL DEFAULT 0 CHECK(denials BETWEEN 0 AND 2),
 tokens integer NOT NULL DEFAULT 0 CHECK(tokens BETWEEN 0 AND 200000),
 estimated_microusd bigint NOT NULL DEFAULT 0 CHECK(estimated_microusd>=0),
 pricing jsonb NOT NULL CHECK(octet_length(pricing::text)<=2048),
 pending_model uuid,
 pending_tool uuid
);
CREATE TABLE agent_steps (
 attempt_id uuid NOT NULL REFERENCES agent_sessions(attempt_id),
 request_id uuid NOT NULL,
 phase text NOT NULL CHECK(phase IN ('begin','end')),
 generation bigint NOT NULL CHECK(generation>0),
 name text NOT NULL CHECK(length(name) BETWEEN 1 AND 64),
 input_sha256 text NOT NULL CHECK(input_sha256 ~ '^[0-9a-f]{64}$'),
 output_sha256 text NOT NULL,
 workspace_sha256 text NOT NULL CHECK(workspace_sha256 ~ '^[0-9a-f]{64}$'),
 after_workspace_sha256 text NOT NULL,
 status text NOT NULL CHECK(length(status)<=32),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(attempt_id,request_id,phase)
);
CREATE TABLE model_calls (
 attempt_id uuid NOT NULL REFERENCES agent_sessions(attempt_id),
 request_id uuid NOT NULL,
 phase text NOT NULL CHECK(phase IN ('begin','end')),
 generation bigint NOT NULL CHECK(generation>0),
 input_sha256 text NOT NULL CHECK(input_sha256 ~ '^[0-9a-f]{64}$'),
 output_sha256 text NOT NULL,
 prompt_token_upper integer NOT NULL CHECK(prompt_token_upper BETWEEN 0 AND 30000),
 reserved_tokens integer NOT NULL CHECK(reserved_tokens>0),
 estimated_microusd bigint NOT NULL CHECK(estimated_microusd>=0),
 metadata jsonb NOT NULL CHECK(octet_length(metadata::text)<=2048),
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(attempt_id,request_id,phase)
);
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION protect_task_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP='UPDATE' THEN
  IF ROW(NEW.id,NEW.workflow_id,NEW.task_key,NEW.position,NEW.kind,NEW.delay_ms,NEW.max_attempts,NEW.deadline_at,NEW.sandbox_input,NEW.agent_input,NEW.created_at)
   IS DISTINCT FROM ROW(OLD.id,OLD.workflow_id,OLD.task_key,OLD.position,OLD.kind,OLD.delay_ms,OLD.max_attempts,OLD.deadline_at,OLD.sandbox_input,OLD.agent_input,OLD.created_at)
  THEN RAISE EXCEPTION 'task definitions and budgets are immutable' USING ERRCODE='23514'; END IF;
  IF OLD.status IN ('SUCCEEDED','FAILED','BLOCKED','CANCELLED') AND NEW.status<>OLD.status THEN RAISE EXCEPTION 'terminal task cannot be redriven' USING ERRCODE='23514'; END IF;
  RETURN NEW;
 END IF;
 IF TG_OP='DELETE' THEN RAISE EXCEPTION 'task definitions are immutable' USING ERRCODE='23514'; END IF;
 PERFORM id FROM workflows WHERE id=NEW.workflow_id FOR UPDATE;
 IF EXISTS(SELECT 1 FROM workflow_events WHERE workflow_id=NEW.workflow_id AND event_type='workflow.created') THEN RAISE EXCEPTION 'workflow definitions are finalized' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE FUNCTION agent_event_owned() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NOT EXISTS(SELECT 1 FROM task_attempts a JOIN tasks t ON t.id=a.task_id JOIN workflows w ON w.id=a.workflow_id
 WHERE a.id=NEW.attempt_id AND a.generation=NEW.generation AND t.kind='agent_go'
 AND t.current_attempt_id=a.id AND t.dispatch_generation=a.generation AND a.state='RUNNING' AND t.status='RUNNING'
 AND w.status='RUNNING' AND a.lease_expires_at>clock_timestamp() AND a.cancel_requested_at IS NULL
 AND t.deadline_at>clock_timestamp() AND w.deadline_at>clock_timestamp())
 THEN RAISE EXCEPTION 'agent event has no current live attempt' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER owned_agent_steps BEFORE INSERT ON agent_steps FOR EACH ROW EXECUTE FUNCTION agent_event_owned();
CREATE TRIGGER owned_model_calls BEFORE INSERT ON model_calls FOR EACH ROW EXECUTE FUNCTION agent_event_owned();
CREATE TRIGGER immutable_agent_steps BEFORE UPDATE OR DELETE ON agent_steps FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER immutable_model_calls BEFORE UPDATE OR DELETE ON model_calls FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
-- +goose StatementEnd

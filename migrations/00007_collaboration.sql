-- +goose Up
ALTER TABLE workflows DROP CONSTRAINT workflows_request_schema_version_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_request_schema_version_check CHECK(request_schema_version BETWEEN 1 AND 7);
ALTER TABLE tasks DROP CONSTRAINT tasks_execution_input_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_execution_input_check CHECK ((
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
    AND octet_length(agent_input->>'task_description') BETWEEN 1 AND 2000
    AND agent_input->>'policy'='controlled-go-v1' AND agent_input->>'budget'='small-v1'
    AND sandbox_input->>'command'='test'
    AND agent_input ?& ARRAY['schema_version','repository_id','base_revision','task_description','policy','budget','test_profile']
    AND agent_input - ARRAY['schema_version','repository_id','base_revision','task_description','policy','budget','test_profile','role']='{}'::jsonb
    AND ((NOT agent_input ? 'role' AND agent_input->>'test_profile'='sum-v1') OR
     (agent_input->>'test_profile'='math-v1' AND jsonb_typeof(agent_input->'role')='object'
      AND agent_input->'role'->>'schema_version'='1'
      AND agent_input->'role'->>'name' IN ('planner','developer','integration','reviewer')
      AND jsonb_typeof(agent_input->'role'->'write_scope')='array'
      AND jsonb_array_length(agent_input->'role'->'write_scope')<=3)))))));
-- Keep the established JSON-type constraint authoritative for legacy inputs;
-- role-specific fields receive their own explicit null/type guard.
ALTER TABLE tasks ADD CONSTRAINT tasks_collaboration_role_json CHECK (
 (agent_input IS NULL OR NOT agent_input ? 'role') OR (
  jsonb_typeof(agent_input->'role')='object'
  AND jsonb_typeof(agent_input->'role'->'schema_version')='number'
  AND jsonb_typeof(agent_input->'role'->'name')='string'
  AND jsonb_typeof(agent_input->'role'->'version')='number'
  AND (agent_input->'role'->>'version')::integer BETWEEN 1 AND 3
  AND jsonb_typeof(agent_input->'role'->'task_key')='string'
  AND jsonb_typeof(agent_input->'role'->'write_scope')='array'
  AND agent_input->'role'->'write_scope' <@ '["add.go","double.go","score.go"]'::jsonb
  AND jsonb_typeof(agent_input->'role'->'test_profile')='string'
  AND jsonb_typeof(agent_input->'role'->'max_calls')='number'
  AND (agent_input->'role'->>'max_calls')::integer BETWEEN 1 AND 8) IS TRUE);
ALTER TABLE workflow_events DROP CONSTRAINT workflow_events_event_type_check;
ALTER TABLE workflow_events ADD CONSTRAINT workflow_events_event_type_check CHECK(event_type IN ('workflow.created','workflow.cancelled','task.ready','task.blocked','workflow.succeeded','workflow.failed','workflow.running','task.claimed','task.succeeded','task.failed','attempt.lost','task.retry_wait','workflow.cancelling','attempt.stop_requested','attempt.stopped','task.cancelled','task.dispatch_requeued','project.state'));

CREATE TABLE collab_projects (
 root_id uuid PRIMARY KEY REFERENCES workflows(id),
 job jsonb NOT NULL CHECK(octet_length(job::text)<=4096),
 status text NOT NULL CHECK(status IN ('PLANNING','EXECUTING','SUCCEEDED','FAILED','NEEDS_HUMAN','CANCELLING','CANCELLED')),
 reason text NOT NULL DEFAULT '',
 round integer NOT NULL DEFAULT 0 CHECK(round BETWEEN 0 AND 2),
 max_rounds integer NOT NULL CHECK(max_rounds BETWEEN 0 AND 2),
 maximum_microusd bigint NOT NULL CHECK(maximum_microusd>0),
 deadline_at timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE collab_workflows (
 workflow_id uuid PRIMARY KEY REFERENCES workflows(id),
 root_id uuid NOT NULL REFERENCES collab_projects(root_id),
 version integer NOT NULL CHECK(version BETWEEN 1 AND 3),
 phase text NOT NULL CHECK(phase IN ('planning','execution')),
 parent_version integer,
 base_snapshot jsonb NOT NULL CHECK(octet_length(base_snapshot::text)<=16384),
 base_sha256 text NOT NULL CHECK(base_sha256 ~ '^[0-9a-f]{64}$'),
 UNIQUE(root_id,version,phase)
);
CREATE TABLE collab_plans (
 root_id uuid NOT NULL REFERENCES collab_projects(root_id),
 version integer NOT NULL CHECK(version BETWEEN 1 AND 3),
 schema_version integer NOT NULL CHECK(schema_version=1),
 digest text NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'),
 body jsonb NOT NULL CHECK(octet_length(body::text)<=8192),
 planner_attempt uuid NOT NULL REFERENCES task_attempts(id),
 policy text NOT NULL CHECK(policy='collaborative-go-v1'),
 PRIMARY KEY(root_id,version), UNIQUE(root_id,version,digest)
);
CREATE TABLE collab_executions (
 root_id uuid NOT NULL,
 version integer NOT NULL,
 plan_digest text NOT NULL,
 child_id uuid NOT NULL UNIQUE REFERENCES workflows(id),
 PRIMARY KEY(root_id,version),
 FOREIGN KEY(root_id,version,plan_digest) REFERENCES collab_plans(root_id,version,digest)
);
CREATE TABLE collab_attempt_inputs (
 attempt_id uuid PRIMARY KEY REFERENCES task_attempts(id),
 generation bigint NOT NULL CHECK(generation>0),
 work jsonb NOT NULL CHECK(octet_length(work::text)<=32768),
 input_sha256 text NOT NULL CHECK(input_sha256 ~ '^[0-9a-f]{64}$')
);
CREATE TABLE collab_results (
 attempt_id uuid PRIMARY KEY REFERENCES collab_attempt_inputs(attempt_id),
 generation bigint NOT NULL CHECK(generation>0),
 role text NOT NULL CHECK(role IN ('planner','developer','integration','reviewer')),
 accepted boolean NOT NULL,
 body jsonb NOT NULL CHECK(octet_length(body::text)<=32768),
 artifact_sha256 text NOT NULL CHECK(artifact_sha256 ~ '^[0-9a-f]{64}$'),
 candidate_sha256 text NOT NULL CHECK(candidate_sha256 ~ '^[0-9a-f]{64}$'),
 test_sha256 text NOT NULL,
 recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE collab_integrations (
 root_id uuid NOT NULL REFERENCES collab_projects(root_id),
 version integer NOT NULL,
 work jsonb NOT NULL CHECK(octet_length(work::text)<=32768),
 candidate_sha256 text NOT NULL CHECK(candidate_sha256 ~ '^[0-9a-f]{64}$'),
 PRIMARY KEY(root_id,version)
);
CREATE INDEX workflows_cursor_idx ON workflows(created_at,id);
-- +goose StatementBegin
CREATE FUNCTION protect_collab_project_policy() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.root_id,NEW.job,NEW.max_rounds,NEW.maximum_microusd,NEW.deadline_at,NEW.created_at) IS DISTINCT FROM ROW(OLD.root_id,OLD.job,OLD.max_rounds,OLD.maximum_microusd,OLD.deadline_at,OLD.created_at)
 THEN RAISE EXCEPTION 'project policy and budget are pinned' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER pinned_collab_project_policy BEFORE UPDATE ON collab_projects FOR EACH ROW EXECUTE FUNCTION protect_collab_project_policy();
CREATE TRIGGER owned_collab_inputs BEFORE INSERT ON collab_attempt_inputs FOR EACH ROW EXECUTE FUNCTION agent_event_owned();
CREATE TRIGGER owned_collab_results BEFORE INSERT ON collab_results FOR EACH ROW EXECUTE FUNCTION agent_event_owned();
CREATE TRIGGER immutable_collab_workflows BEFORE UPDATE OR DELETE ON collab_workflows FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER immutable_collab_plans BEFORE UPDATE OR DELETE ON collab_plans FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER immutable_collab_executions BEFORE UPDATE OR DELETE ON collab_executions FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER immutable_collab_inputs BEFORE UPDATE OR DELETE ON collab_attempt_inputs FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER immutable_collab_results BEFORE UPDATE OR DELETE ON collab_results FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER immutable_collab_integrations BEFORE UPDATE OR DELETE ON collab_integrations FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
-- +goose StatementEnd
-- Forward only: immutable plan and candidate history has no lossless old form.

-- +goose Up
ALTER TABLE workflows DROP CONSTRAINT workflows_status_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_status_check CHECK (status IN ('PENDING','CANCELLED','SUCCEEDED','FAILED'));
ALTER TABLE workflows DROP CONSTRAINT workflows_request_schema_version_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_request_schema_version_check CHECK (request_schema_version IN (1,2));
ALTER TABLE workflows ADD COLUMN last_event_sequence bigint NOT NULL DEFAULT 1 CHECK (last_event_sequence > 0);
ALTER TABLE workflows ADD COLUMN scheduler_checked_at timestamptz;
-- The migration holds workflows' DDL lock; stage-01 writers lock/update their
-- parent before emitting events. MAX is used only for this serialized upgrade,
-- never to allocate runtime events. Legacy request hashes/versions stay intact.
UPDATE workflows w SET last_event_sequence = COALESCE((SELECT max(sequence) FROM workflow_events WHERE workflow_id=w.id),1);
ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check CHECK (status IN ('PENDING','READY','RUNNING','SUCCEEDED','FAILED','BLOCKED','CANCELLED'));
ALTER TABLE tasks ADD CONSTRAINT tasks_workflow_id_id_key UNIQUE (workflow_id,id);
CREATE TABLE task_dependencies (
    workflow_id uuid NOT NULL,
    task_id uuid NOT NULL,
    depends_on_task_id uuid NOT NULL,
    PRIMARY KEY (workflow_id,task_id,depends_on_task_id),
    FOREIGN KEY (workflow_id,task_id) REFERENCES tasks(workflow_id,id) ON DELETE RESTRICT,
    FOREIGN KEY (workflow_id,depends_on_task_id) REFERENCES tasks(workflow_id,id) ON DELETE RESTRICT,
    CHECK (task_id <> depends_on_task_id)
);
CREATE INDEX task_dependencies_predecessor ON task_dependencies(workflow_id,depends_on_task_id);
CREATE INDEX workflows_scheduler_scan ON workflows(scheduler_checked_at NULLS FIRST,created_at,id) WHERE status='PENDING';
ALTER TABLE workflow_events DROP CONSTRAINT workflow_events_event_type_check;
ALTER TABLE workflow_events ADD CONSTRAINT workflow_events_event_type_check CHECK (event_type IN ('workflow.created','workflow.cancelled','task.ready','task.blocked','workflow.succeeded','workflow.failed'));

-- Definitions/edges are finalized by workflow.created in their creation tx.
-- Runtime may change task state, never definitions. Future plan versions need
-- a separately versioned finalized plan, not mutation of this executing graph.
-- +goose StatementBegin
CREATE FUNCTION protect_task_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP = 'UPDATE' THEN
        IF ROW(NEW.id,NEW.workflow_id,NEW.task_key,NEW.position,NEW.kind,NEW.created_at)
            IS DISTINCT FROM ROW(OLD.id,OLD.workflow_id,OLD.task_key,OLD.position,OLD.kind,OLD.created_at) THEN
            RAISE EXCEPTION 'task definitions are immutable' USING ERRCODE='23514';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN RAISE EXCEPTION 'task definitions are immutable' USING ERRCODE='23514'; END IF;
    PERFORM id FROM workflows WHERE id=NEW.workflow_id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM workflow_events WHERE workflow_id=NEW.workflow_id AND event_type='workflow.created') THEN
        RAISE EXCEPTION 'workflow definitions are finalized' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE FUNCTION protect_dependency() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP <> 'INSERT' THEN RAISE EXCEPTION 'dependencies are immutable' USING ERRCODE='23514'; END IF;
    PERFORM id FROM workflows WHERE id=NEW.workflow_id FOR UPDATE;
    IF EXISTS(SELECT 1 FROM workflow_events WHERE workflow_id=NEW.workflow_id AND event_type='workflow.created') THEN
        RAISE EXCEPTION 'workflow dependencies are finalized' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
CREATE FUNCTION protect_workflow_definition() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF ROW(NEW.id,NEW.name,NEW.creation_key,NEW.request_hash,NEW.request_schema_version,NEW.created_at)
        IS DISTINCT FROM ROW(OLD.id,OLD.name,OLD.creation_key,OLD.request_hash,OLD.request_schema_version,OLD.created_at) THEN
        RAISE EXCEPTION 'workflow definitions are immutable' USING ERRCODE='23514';
    END IF;
    RETURN NEW;
END $$;
-- +goose StatementEnd
CREATE TRIGGER immutable_task_definition BEFORE INSERT OR UPDATE OR DELETE ON tasks FOR EACH ROW EXECUTE FUNCTION protect_task_definition();
CREATE TRIGGER immutable_dependency BEFORE INSERT OR UPDATE OR DELETE ON task_dependencies FOR EACH ROW EXECUTE FUNCTION protect_dependency();
CREATE TRIGGER immutable_workflow_definition BEFORE UPDATE ON workflows FOR EACH ROW EXECUTE FUNCTION protect_workflow_definition();

-- +goose Down
-- Reject downgrade once new states/requests/edges exist; never erase history or
-- reinterpret version-2 hashes as version 1. Unused stage-02 schema can roll back.
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS(SELECT 1 FROM task_dependencies) OR EXISTS(SELECT 1 FROM workflows WHERE request_schema_version<>1 OR status NOT IN ('PENDING','CANCELLED')) OR EXISTS(SELECT 1 FROM tasks WHERE status NOT IN ('PENDING','CANCELLED')) OR EXISTS(SELECT 1 FROM workflow_events WHERE event_type NOT IN ('workflow.created','workflow.cancelled')) THEN
        RAISE EXCEPTION 'stage-02 data prevents lossless downgrade';
    END IF;
END $$;
-- +goose StatementEnd
DROP TRIGGER immutable_workflow_definition ON workflows;
DROP TRIGGER immutable_dependency ON task_dependencies;
DROP TRIGGER immutable_task_definition ON tasks;
DROP FUNCTION protect_workflow_definition();
DROP FUNCTION protect_dependency();
DROP FUNCTION protect_task_definition();
DROP TABLE task_dependencies;
ALTER TABLE tasks DROP CONSTRAINT tasks_workflow_id_id_key;
ALTER TABLE tasks DROP CONSTRAINT tasks_status_check;
ALTER TABLE tasks ADD CONSTRAINT tasks_status_check CHECK (status IN ('PENDING','CANCELLED'));
DROP INDEX workflows_scheduler_scan;
ALTER TABLE workflows DROP COLUMN scheduler_checked_at;
ALTER TABLE workflows DROP COLUMN last_event_sequence;
ALTER TABLE workflows DROP CONSTRAINT workflows_status_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_status_check CHECK (status IN ('PENDING','CANCELLED'));
ALTER TABLE workflows DROP CONSTRAINT workflows_request_schema_version_check;
ALTER TABLE workflows ADD CONSTRAINT workflows_request_schema_version_check CHECK (request_schema_version=1);
ALTER TABLE workflow_events DROP CONSTRAINT workflow_events_event_type_check;
ALTER TABLE workflow_events ADD CONSTRAINT workflow_events_event_type_check CHECK (event_type IN ('workflow.created','workflow.cancelled'));

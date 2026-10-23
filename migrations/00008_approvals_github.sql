-- +goose Up
ALTER TABLE collab_projects DROP CONSTRAINT collab_projects_status_check;
ALTER TABLE collab_projects ADD CONSTRAINT collab_projects_status_check CHECK(status IN ('PLANNING','EXECUTING','SUCCEEDED','FAILED','NEEDS_HUMAN','CANCELLING','CANCELLED','WAITING_FOR_APPROVAL','PUBLISHING','PUBLISHED'));
CREATE TABLE github_imports (
 root_id uuid PRIMARY KEY REFERENCES collab_projects(root_id),
 delivery_id text UNIQUE NOT NULL CHECK(length(delivery_id) BETWEEN 1 AND 128),
 payload_sha256 text NOT NULL CHECK(payload_sha256 ~ '^[0-9a-f]{64}$'),
 actor text NOT NULL CHECK(length(actor) BETWEEN 1 AND 100),
 prepared jsonb NOT NULL CHECK(octet_length(prepared::text)<=16384),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE approvals (
 id uuid PRIMARY KEY,
 root_id uuid NOT NULL REFERENCES github_imports(root_id),
 plan_version integer NOT NULL CHECK(plan_version BETWEEN 1 AND 3),
 version integer NOT NULL DEFAULT 1 CHECK(version=1),
 binding jsonb NOT NULL CHECK(octet_length(binding::text)<=8192),
 binding_sha256 text NOT NULL CHECK(binding_sha256 ~ '^[0-9a-f]{64}$'),
 status text NOT NULL CHECK(status IN ('PENDING','APPROVED','REJECTED','EXPIRED','INVALIDATED','CANCELLED')),
 expires_at timestamptz NOT NULL,
 actor text NOT NULL DEFAULT '' CHECK(length(actor)<=100),
 decided_at timestamptz,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 UNIQUE(root_id,plan_version)
);
CREATE INDEX approvals_expiry_idx ON approvals(expires_at) WHERE status IN ('PENDING','APPROVED');
CREATE TABLE external_operations (
 id uuid PRIMARY KEY,
 approval_id uuid UNIQUE NOT NULL REFERENCES approvals(id),
 root_id uuid NOT NULL REFERENCES github_imports(root_id),
 binding_sha256 text NOT NULL CHECK(binding_sha256 ~ '^[0-9a-f]{64}$'),
 branch text UNIQUE NOT NULL CHECK(branch='forgeswarm/op-'||id::text),
 status text NOT NULL CHECK(status IN ('prepared','in_flight','uncertain','confirmed','failed','needs_review','cancelled','manual_reconciliation')),
 attempts integer NOT NULL DEFAULT 0 CHECK(attempts BETWEEN 0 AND 6),
 next_attempt_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 head_commit text NOT NULL DEFAULT '',
 pr_number bigint NOT NULL DEFAULT 0,
 pr_url text NOT NULL DEFAULT '' CHECK(length(pr_url)<=512),
 reason text NOT NULL DEFAULT '' CHECK(length(reason)<=128),
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX external_operations_due_idx ON external_operations(next_attempt_at) WHERE status IN ('prepared','in_flight','uncertain');
-- +goose StatementBegin
CREATE FUNCTION protect_approval_binding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.id,NEW.root_id,NEW.plan_version,NEW.version,NEW.binding,NEW.binding_sha256,NEW.expires_at,NEW.created_at) IS DISTINCT FROM ROW(OLD.id,OLD.root_id,OLD.plan_version,OLD.version,OLD.binding,OLD.binding_sha256,OLD.expires_at,OLD.created_at)
 THEN RAISE EXCEPTION 'approval binding is immutable' USING ERRCODE='23514'; END IF;
 IF OLD.status<>'PENDING' AND (NEW.actor,NEW.decided_at) IS DISTINCT FROM (OLD.actor,OLD.decided_at)
 THEN RAISE EXCEPTION 'approval decision history is immutable' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_approval_binding BEFORE UPDATE ON approvals FOR EACH ROW EXECUTE FUNCTION protect_approval_binding();
CREATE FUNCTION protect_external_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF ROW(NEW.id,NEW.approval_id,NEW.root_id,NEW.binding_sha256,NEW.branch,NEW.created_at) IS DISTINCT FROM ROW(OLD.id,OLD.approval_id,OLD.root_id,OLD.binding_sha256,OLD.branch,OLD.created_at)
 THEN RAISE EXCEPTION 'publication identity is immutable' USING ERRCODE='23514'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER immutable_external_identity BEFORE UPDATE ON external_operations FOR EACH ROW EXECUTE FUNCTION protect_external_identity();
CREATE TRIGGER immutable_github_imports BEFORE UPDATE OR DELETE ON github_imports FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER retain_approvals BEFORE DELETE ON approvals FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
CREATE TRIGGER retain_external_operations BEFORE DELETE ON external_operations FOR EACH ROW EXECUTE FUNCTION sandbox_evidence_immutable();
-- +goose StatementEnd
-- Forward only: approval identities and external effects cannot be undone.

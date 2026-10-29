"""Owned real-service final checks. Success always comes from persisted state.

All automated approvals are restricted to the local TEST_DEMO repository 42.
Never import this module to authorize a real repository or paid model call.
"""
import concurrent.futures
import hashlib
import json
import subprocess
import time
import uuid

import dev
import local_platform
import performance

EVIDENCE = local_platform.EVIDENCE


def save(name, value):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE/name).write_text(json.dumps(value, indent=2)+"\n")


def delay(milliseconds=15000):
    return {"name": "owned bounded recovery", "timeout_ms": 120000, "tasks": [{"key": "slow", "kind": "delay", "delay_ms": milliseconds, "timeout_ms": 100000, "max_attempts": 3}]}


def running(session, identifier):
    dev.poll(lambda: any(n["status"] == "RUNNING" for n in session.graph(identifier)["nodes"]), "persisted active attempt", timeout=30)


def attempts(session, identifier):
    result = []
    for task in session.graph(identifier)["nodes"]:
        code, value = session.request(f'/v1/workflows/{identifier}/tasks/{task["id"]}/attempts?limit=10')
        assert code == 200
        result += value["attempts"]
    return result


def sandbox_cleanup(session):
    return int(session.sql("SELECT count(*) FROM task_attempts a JOIN sandbox_commands c ON c.attempt_id=a.id WHERE a.cleanup_required"))


def recreate_worker(session, suffix):
    session.close_client()
    session.run("stop", "runtime_"+suffix, timeout=35, quiet=True)
    session.run("up", "-d", "--no-deps", "--force-recreate", "worker_"+suffix, "runtime_"+suffix, timeout=60, quiet=True)


def recover_worker(session, pause=False):
    session.up(1)
    identifier = session.submit(delay())
    running(session, identifier)
    original = attempts(session, identifier)
    assert original and original[0]["state"] == "RUNNING"
    if pause:
        session.run("pause", "worker_a", quiet=True)
        try:
            # Lease is six seconds; observe the durable fence, not a local
            # assumed expiry. The paused process cannot renew or acknowledge.
            dev.poll(lambda: any(a["state"] == "LOST" for a in attempts(session, identifier)), "database lease fence while worker paused", timeout=25)
        finally:
            session.run("unpause", "worker_a", quiet=True)
    else:
        session.run("kill", "-s", "SIGKILL", "worker_a", quiet=True)
    recreate_worker(session, "a")
    graph = session.settle(identifier)
    records = attempts(session, identifier)
    assert graph["status"] == "SUCCEEDED", graph
    assert any(a["state"] == "LOST" for a in records) and sum(a["state"] == "SUCCEEDED" for a in records) == 1
    assert len(records) <= 3 and all(a.get("generation", 0) >= 1 for a in records)
    return {"workflow_id": identifier, "fault": "pause_past_lease" if pause else "worker_SIGKILL", "attempts": records, "status": graph["status"]}


def import_project(session):
    config = json.loads((session.assets/"github.json").read_text())
    assert config["credential_mode"] == "TEST_DEMO" and config["repository"]["id"] == 42 and config["test_api_url"] == "http://127.0.0.1:9088"
    code, value = session.request("/v1/github/import", "POST", {"issue_number": 1}, "stage10-issue-"+uuid.uuid4().hex)
    assert code in {201, 202}, (code, value)
    identifier = value["id"]
    def waiting():
        code, project = session.request("/v1/projects/"+identifier)
        assert code == 200
        assert project["status"] not in {"FAILED", "NEEDS_HUMAN"}, project
        return project["status"] == "WAITING_FOR_APPROVAL"
    dev.poll(waiting, "actual protected candidate and review awaiting approval", timeout=150)
    return identifier


def approve_fake(session, identifier):
    code, value = session.request("/v1/approvals?root="+identifier)
    assert code == 200 and len(value["items"]) == 1
    approval = value["items"][0]
    binding = approval["binding"]
    assert approval["status"] == "PENDING" and binding["test_demo"] and binding["repository"]["id"] == 42
    for field in ["candidate_sha256", "patch_sha256", "test_evidence_sha256", "review_sha256"]:
        assert len(binding[field]) == 64
    decision = {"expected_version": approval["version"], "binding_sha256": approval["binding_sha256"], "candidate_sha256": binding["candidate_sha256"], "decision": "approve", "acknowledge_sensitive": False}
    assert session.request("/v1/approvals/"+approval["id"], "POST", decision, reader=True)[0] == 403
    wrong = decision | {"candidate_sha256": "0"*64}
    assert session.request("/v1/approvals/"+approval["id"], "POST", wrong)[0] == 409
    assert session.request("/v1/approvals/"+approval["id"], "POST", decision)[0] == 200
    dev.poll(lambda: session.request("/v1/projects/"+identifier)[1]["status"] == "PUBLISHED", "controlled fake publication", timeout=45)
    return session.request("/v1/approvals/"+approval["id"])[1]


def demo(session):
    diamond = {"name": "parallel diagnostic diamond", "tasks": [{"key": "root", "kind": "noop"}, {"key": "left", "kind": "delay", "delay_ms": 800, "depends_on": ["root"]}, {"key": "right", "kind": "delay", "delay_ms": 800, "depends_on": ["root"]}, {"key": "join", "kind": "noop", "depends_on": ["left", "right"]}]}
    session.up(2)
    identifier = session.submit(diamond)
    assert session.settle(identifier)["status"] == "SUCCEEDED"
    rows = attempts(session, identifier)
    # Prove actual overlap from persisted start/end times, not just DAG shape.
    long = [a for a in rows if (a["finished_at"] > a["started_at"])]
    import datetime
    parsed = [(datetime.datetime.fromisoformat(a["started_at"].replace("Z", "+00:00")), datetime.datetime.fromisoformat(a["finished_at"].replace("Z", "+00:00"))) for a in long]
    overlap = any(max(a[0], b[0]) < min(a[1], b[1]) for i, a in enumerate(parsed) for b in parsed[i+1:])
    assert overlap, "two-worker demonstration did not overlap tasks"
    recovery = recover_worker(session)
    session.up(2)
    root = import_project(session)
    approval = approve_fake(session, root)
    assert session.graph(identifier)["status"] == "SUCCEEDED"
    save("demo.json", {"at": dev.timestamp(), "mode": "scripted_TEST_DEMO; real Docker protected tests and controlled fake GitHub HTTP", "diamond_id": identifier, "parallel_attempt_overlap": overlap, "recovery": recovery, "project_id": root, "approval": approval, "actual_pr": False, "paid_calls": False})
    print("Live local dashboard project: "+session.base+"/#workflow/"+root, flush=True)
    return root


def faults(session):
    proofs = [recover_worker(session, pause=True)]
    completed = session.submit(delay(100))
    assert session.settle(completed)["status"] == "SUCCEEDED"
    for role in ["scheduler", "relay", "broker", "db"]:
        identifier = session.submit(delay(2500))
        session.run("kill", "-s", "SIGKILL", role, quiet=True)
        time.sleep(2)
        session.run("up", "-d", "--no-deps", role, timeout=45, quiet=True)
        session.close_client()
        dev.poll(lambda: session.request("/health/ready", auth=False)[0] == 200, "readiness after "+role, timeout=30)
        graph = session.settle(identifier)
        records = attempts(session, identifier)
        assert graph["status"] in {"SUCCEEDED", "FAILED"} and len(records) <= 3
        assert session.graph(completed)["status"] == "SUCCEEDED"
        proofs.append({"fault": role+"_SIGKILL_restart", "id": identifier, "status": graph["status"], "attempts": records})
    # Terminate an actual controlled compiler sandbox, scoped to this workflow.
    fixture = next((session.assets/"fixtures").glob("*.json")).stem
    # This public fixture is a package-main test library without func main;
    # go test compiles it correctly, whereas go build would intentionally fail.
    body = {"name": "owned sandbox termination", "timeout_ms": 120000, "tasks": [{"key": "test", "kind": "sandbox_go", "max_attempts": 2, "sandbox": {"schema_version": 1, "fixture_id": fixture, "command": "test"}}]}
    identifier = session.submit(body)
    container = []
    def sandbox():
        _, text = dev.run(["docker", "ps", "-q", "--filter", "label=forgeswarm.installation="+str(uuid.UUID(session.owner)), "--filter", "label=forgeswarm.workflow="+identifier, "--filter", "label=forgeswarm.resource=sandbox-v1"], quiet=True)
        container[:] = text.split()
        return bool(container)
    dev.poll(sandbox, "owned active sandbox to terminate", timeout=30)
    assert len(container) == 1
    dev.run(["docker", "kill", container[0]], label="owned-sandbox-kill")
    graph = session.settle(identifier)
    assert graph["status"] in {"FAILED", "SUCCEEDED"}
    fresh = session.submit(body)
    assert session.settle(fresh)["status"] == "SUCCEEDED"
    # Stage04 deliberately retains LOST diagnostic stop uncertainty when its
    # process dies before an acknowledgment. Do not forge an acknowledgment or
    # erase flags with SQL. Actual sandbox cleanup must independently converge.
    dev.poll(lambda: sandbox_cleanup(session) == 0, "sandbox cleanup converged", timeout=30)
    outstanding = session.request("/v1/diagnostics")[1]["database"]["cleanup_backlog"]
    proofs.append({"fault": "active_sandbox_kill", "workflow_id": identifier, "status": graph["status"], "fresh_workflow_id": fresh, "fresh_status": "SUCCEEDED", "failure_is_not_relabelled_success": True})
    save("faults.json", {"at": dev.timestamp(), "proofs": proofs, "stale_fence_contract": "Prior real PostgreSQL/RPC Stage04 tests submit the old generation and assert rejection; paused/killed attempts here must remain LOST.", "completed_preserved": completed, "sandbox_cleanup_backlog": 0, "all_cleanup_backlog": outstanding, "outstanding_cleanup_semantics": "LOST diagnostic attempts retain unacknowledged stop uncertainty. Outcomes remain fenced. No SQL reset or fabricated stop acknowledgment is used."})


def soak(session):
    session.up(2)
    observer = performance.Observer(session)
    observer.start()
    identifiers, rejected = [], 0
    start = time.monotonic()
    try:
        for index in range(120):
            if observer.failure:
                raise RuntimeError(observer.failure)
            body = {"name": "bounded soak", "tasks": [{"key": "a", "kind": "delay", "delay_ms": 20}]}
            code, value = session.request("/v1/workflows", "POST", body, "soak-"+uuid.uuid4().hex)
            if code == 201:
                identifiers.append(value["id"])
            else:
                assert code == 429
                rejected += 1
            time.sleep(max(0, start+(index+1)*.5-time.monotonic()))
        graphs = performance.observe_workflows(session, identifiers)
    finally:
        observer.stop()
    assert len(graphs) == len(identifiers) and all(v["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"} for v in graphs.values())
    samples = [v for v in observer.samples if "api" in v]
    assert len(samples) >= 5 and not observer.failure
    first, last = samples[0]["api"], samples[-1]["api"]
    # Generous finite regression bounds after warmup; never claim an infinite
    # soak or zero growth from a one-minute observation.
    assert last["goroutines"] <= first["goroutines"]+32
    assert last["heap_inuse_bytes"] <= first["heap_inuse_bytes"]+64*1024*1024
    assert last["database"]["pool"]["total"] <= last["database"]["pool"]["max"]
    assert last["database"]["cleanup_backlog"] == first["database"]["cleanup_backlog"] and samples[-1]["sandboxes"] == 0 and sandbox_cleanup(session) == 0
    assert last["database"]["artifacts"] == first["database"]["artifacts"] and last["database"]["artifact_bytes"] == first["database"]["artifact_bytes"]
    assert len(samples[-1]["containers"]) <= len(samples[0]["containers"])+1
    save("soak.json", {"at": dev.timestamp(), "duration_seconds": time.monotonic()-start, "submitted": 120, "rate_per_second": 2, "accepted": len(identifiers), "rejected_429": rejected, "outcomes": {state: sum(v["status"] == state for v in graphs.values()) for state in ["SUCCEEDED", "FAILED", "CANCELLED"]}, "samples": observer.samples, "limitation": "60-second diagnostic soak; finite generous growth bounds, not proof of leak freedom or artifact retention under sustained coding workloads"})


def restore(session):
    # Real demo creates artifacts before backup. Stop trusted roles to prevent
    # split-brain when recovering the copied database; source DB stays intact.
    active = session.submit(delay())
    running(session, active)
    session.close_client()
    session.run("stop", "runtime_a", "runtime_b", "worker_a", "worker_b", "scheduler", "coordinator", "relay", "publisher", "api", timeout=60, quiet=True)
    before = session.sql("SELECT json_build_object('workflows',(SELECT count(*) FROM workflows),'artifacts',(SELECT count(*) FROM sandbox_artifacts),'bodies',(SELECT COALESCE(json_agg(encode(sha256(body),'hex') ORDER BY id),'[]') FROM sandbox_artifacts))")
    expected = json.loads(before)
    assert expected["artifacts"] > 0, "backup must contain actual execution artifacts"
    db = session.container("db")
    backup = session.path/"database.dump"
    with backup.open("wb") as output:
        result = subprocess.run(["docker", "exec", db, "pg_dump", "-U", "forgeswarm", "-d", "forgeswarm", "-Fc", "--no-owner", "--no-acl"], stdout=output, stderr=subprocess.PIPE, timeout=45)
    assert result.returncode == 0 and backup.stat().st_size < 64 << 20
    restored = "forgeswarm_restore_"+uuid.uuid4().hex[:12]
    session.sql("CREATE DATABASE "+restored)
    with backup.open("rb") as source:
        result = subprocess.run(["docker", "exec", "-i", db, "pg_restore", "-U", "forgeswarm", "-d", restored, "--no-owner", "--no-acl", "--exit-on-error"], stdin=source, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=45)
    assert result.returncode == 0
    assert json.loads(session.sql("SELECT json_build_object('workflows',(SELECT count(*) FROM workflows),'artifacts',(SELECT count(*) FROM sandbox_artifacts),'bodies',(SELECT COALESCE(json_agg(encode(sha256(body),'hex') ORDER BY id),'[]') FROM sandbox_artifacts))", restored)) == expected
    secret = session.assets/"database-url"
    old = secret.read_text()
    assert "/forgeswarm?" in old
    secret.write_text(old.replace("/forgeswarm?", "/"+restored+"?"))
    session.up(2)
    graph = session.settle(active)
    assert graph["status"] == "SUCCEEDED" and any(v["state"] == "LOST" for v in attempts(session, active))
    assert int(session.sql("SELECT count(*) FROM workflows", "forgeswarm")) == expected["workflows"]
    assert json.loads(session.sql("SELECT COALESCE(json_agg(encode(sha256(body),'hex') ORDER BY id),'[]') FROM sandbox_artifacts", restored)) == expected["bodies"]
    save("restore.json", {"at": dev.timestamp(), "backup_sha256": hashlib.sha256(backup.read_bytes()).hexdigest(), "backup_bytes": backup.stat().st_size, "source_preserved": True, "separate_restored_database": restored, "expected": expected, "recovered_workflow_id": active, "recovered_status": graph["status"], "artifacts_rehashed": True, "private_backup_reference": str(backup.relative_to(dev.ROOT))})


def browser(session):
    # Existing Stage08 browser gate tests escaped source/events, reader/operator
    # permissions, exact binding decisions and observed fake publication. The
    # Stage10 transport is separately verified against its installation CA.
    import stage08
    stage08.browser()

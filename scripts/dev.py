"""Cross-platform stage-01 commands. Python 3.10+, Go 1.26.3, Docker required.

Only uniquely named, labelled verification containers are deleted. Development
Compose storage is persistent, and db-down never uses --volumes. Verification
evidence is sanitized, ignored by Git, bounded per command, and replaced on rerun.
"""

import contextlib
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
import uuid

# Browser/test reporters contain Unicode; Windows redirected stdout may otherwise
# default to cp1252 and turn a passing command into a reporting exception.
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

ROOT = Path(__file__).resolve().parent.parent
IMAGE = "postgres:17.11-bookworm@sha256:66aafa11cf15800a3c94763f7e11d1e7b2e37e5e84bc4ce027cb6e1e4bfaf4df"
GO_IMAGE = "golang:1.26.3-bookworm@sha256:386d475a660466863d9f8c766fec64d7fdad3edac2c6a05020c09534d71edb4b"
BIN = ROOT / "bin" / ("api.exe" if os.name == "nt" else "api")
EVIDENCE = ROOT / "artifacts/verification/stage-01"
ENV = os.environ.copy()
ENV.update({"GOCACHE": str(ROOT / ".cache/go-build"), "GOMODCACHE": str(ROOT / ".cache/go-mod"),
            "GOPATH": str(ROOT / ".cache/gopath"), "GOTMPDIR": str(ROOT / ".cache/go-tmp"),
            "GOENV": "off", "GOTOOLCHAIN": "local"})
COMMANDS = []
RECORD = False


def timestamp():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def sanitize(text):
    text = re.sub(r"postgres(?:ql)?://[^\s\"']+", "[REDACTED_DATABASE_URL]", text)
    text = re.sub(r"(?i)(password\s*=\s*)([^\s\"']+)", r"\1[REDACTED]", text)
    text = re.sub(r"(?i)(Bearer\s+)[A-Za-z0-9._:-]+",r"\1[REDACTED]",text)
    text = re.sub(r"(?i)((?:AGENT_RUNTIME_TOKEN|TEST_AGENT_RUNTIME_TOKEN|OPENAI_API_KEY|NATS_TOKEN|WORKER_TOKEN)=)[^\s\"',]+", r"\1[REDACTED]", text)
    return text[-131072:]


def run(args, timeout=180, env=None, check=True, label=None, quiet=False):
    started = timestamp()
    try:
        result = subprocess.run([str(a) for a in args], cwd=ROOT, env=env or ENV,
                                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                timeout=timeout, encoding="utf-8", errors="replace")
        output, code = sanitize(result.stdout), result.returncode
    except (OSError, subprocess.TimeoutExpired) as exc:
        output, code = sanitize(str(exc)), 124
    if RECORD:
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        path = EVIDENCE / f"{len(COMMANDS):03d}-{label or Path(str(args[0])).stem}.log"
        path.write_text(output, encoding="utf-8")
        COMMANDS.append({"command": sanitize(subprocess.list2cmdline([str(a) for a in args])),
                         "started_at": started, "finished_at": timestamp(), "exit": code,
                         "required": check,
                         "evidence_path": path.relative_to(ROOT).as_posix()})
    if not quiet and output:
        print(output, end="" if output.endswith("\n") else "\n", flush=True)
    if check and code:
        raise RuntimeError(f"{label or args[0]} failed (exit {code})")
    return code, output


def poll(check, description, timeout=40):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.2)
    raise RuntimeError(f"timed out waiting for {description}")


@contextlib.contextmanager
def database():
    # Never accept DATABASE_URL or user-owned container names for verification.
    run(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=30, label="docker")
    token = uuid.uuid4().hex
    name, dbname = f"forgeswarm-stage01-{token}", f"forgeswarm_stage01_{token}"
    network = f"forgeswarm-network-{token}"
    try:
        run(["docker", "network", "create", "--label", f"forgeswarm.verification={token}", network], label="test-network")
        run(["docker", "run", "-d", "--name", name, "--label", f"forgeswarm.verification={token}",
             "--platform", "linux/amd64", "--network", network, "--network-alias", "db", "-p", "127.0.0.1::5432", "-e", "POSTGRES_USER=forgeswarm",
             "-e", "POSTGRES_PASSWORD=forgeswarm_test", "-e", f"POSTGRES_DB={dbname}", IMAGE],
            timeout=240, label="disposable-db")
        def is_ready():
            code, _ = run(["docker", "exec", name, "pg_isready", "-U", "forgeswarm", "-d", dbname],
                          timeout=5, check=False, quiet=True, label="db-poll")
            return code == 0
        poll(is_ready, "disposable PostgreSQL")
        _, port = run(["docker", "port", name, "5432/tcp"], label="db-port", quiet=True)
        port = port.strip().rsplit(":", 1)[1]
        url = f"postgres://forgeswarm:forgeswarm_test@127.0.0.1:{port}/{dbname}?sslmode=disable"
        env = ENV | {"TEST_DATABASE_URL": url, "TEST_RESOURCE_TOKEN": token, "DATABASE_URL": url, "TEST_DOCKER_NETWORK": network}
        yield env, name, is_ready
    finally:
        # Only delete the resource if its unpredictable ownership token matches.
        code, owner = run(["docker", "inspect", "--format", '{{ index .Config.Labels "forgeswarm.verification" }}', name],
                          check=False, timeout=15, quiet=True, label="db-ownership")
        if code == 0 and owner.strip() == token:
            run(["docker", "rm", "-f", "-v", name], timeout=30, label="db-cleanup")
        code, owner = run(["docker", "network", "inspect", "--format", '{{ index .Labels "forgeswarm.verification" }}', network],
                          check=False, timeout=15, quiet=True, label="network-ownership")
        if code == 0 and owner.strip() == token:
            run(["docker", "network", "rm", network], timeout=30, label="network-cleanup")


def build():
    BIN.parent.mkdir(exist_ok=True)
    run(["go", "build", "-o", BIN, "./cmd/api"], label="build-api")
    run(["go", "build", "-o", BIN.parent / ("migrate.exe" if os.name == "nt" else "migrate"), "./cmd/migrate"], label="build-migrate")
    if (ROOT / "cmd/scheduler").exists():
        run(["go", "build", "-o", BIN.parent / ("scheduler.exe" if os.name == "nt" else "scheduler"), "./cmd/scheduler"], label="build-scheduler")
    for name in ["coordinator","relay","worker","sandbox-fixture","sandbox-prepare","publisher","github-test-server","operator-init","evaluate"]:
        if (ROOT / "cmd" / name).exists():run(["go","build","-o",BIN.parent / (name+".exe" if os.name=="nt" else name),"./cmd/"+name],label="build-"+name)


def race(args, env=None, container=None, label="race"):
    if os.name != "nt":
        return run(["go", "test", "-race", *args], env=env, timeout=300, label=label)
    # The installed Windows compiler may be 32-bit; use the pinned Linux Go
    # toolchain for real race detection, rather than weakening the check.
    token = uuid.uuid4().hex
    name = f"forgeswarm-go-{token}"
    docker = ["docker", "run", "--rm", "--name", name, "--label", f"forgeswarm.verification={token}", "--platform", "linux/amd64",
              "--mount", f"type=bind,source={ROOT},target=/src", "-w", "/src",
              "-e", "GOCACHE=/src/.cache/linux-build", "-e", "GOMODCACHE=/src/.cache/go-mod",
              "-e", "GOTELEMETRY=off", "-e", "GOTOOLCHAIN=local"]
    if container:
        docker += ["--network", env.get("TEST_GO_NETWORK",env["TEST_DOCKER_NETWORK"])]
        test_url = re.sub(r"127\.0\.0\.1:\d+/", "db:5432/", env["TEST_DATABASE_URL"])
        docker += ["-e", f"TEST_DATABASE_URL={test_url}", "-e", f"TEST_RESOURCE_TOKEN={env['TEST_RESOURCE_TOKEN']}"]
        if "TEST_NATS_URL" in env:docker += ["-e","TEST_NATS_URL","-e","TEST_NATS_TOKEN","-e","TEST_NATS_NAMESPACE"]
    try:
        return run([*docker, GO_IMAGE, "go", "test", "-race", "-timeout=120s", *args],env=env,timeout=300, label=label)
    finally:
        code, owner = run(["docker", "inspect", "--format", '{{ index .Config.Labels "forgeswarm.verification" }}', name],
                          check=False, timeout=15, quiet=True, label="go-ownership")
        if code == 0 and owner.strip() == token:
            run(["docker", "rm", "-f", name], timeout=30, label="go-cleanup")


def integration():
    with database() as (env, container, _):
        race(["-tags=integration", "-count=1", "-timeout=120s", "-v", "./tests/integration"],
             env=env, container=container, label="integration")


def http(base, path, method="GET", data=None, key=None):
    headers = {"Content-Type": "application/json"} if data is not None else {}
    if key:
        headers["Idempotency-Key"] = key
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        response = urllib.request.urlopen(req, timeout=3)
    except urllib.error.HTTPError as exc:
        response = exc
    with response:
        return response.status, json.loads(response.read()), dict(response.headers)


def smoke():
    if not BIN.exists():
        build()
    with database() as (env, container, is_ready):
        run(["go", "run", "./cmd/migrate", "up"], env=env, label="smoke-migrate")
        run(["go", "run", "./cmd/migrate", "status"], env=env, label="smoke-migration-status")
        linux_bin = ROOT / "bin/api-linux"
        run(["go", "build", "-o", linux_bin, "./cmd/api"], env=ENV | {"GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"}, label="smoke-build")
        base = None
        process = None
        log_path = EVIDENCE / "smoke-api.log" if RECORD else ROOT / ".cache/smoke-api.log"
        log_path.parent.mkdir(parents=True, exist_ok=True)
        log_path.write_text("", encoding="utf-8")
        started = timestamp()
        def start_api():
            nonlocal base
            p = "forgeswarm-api-" + uuid.uuid4().hex
            url = re.sub(r"127\.0\.0\.1:\d+/", "db:5432/", env["DATABASE_URL"])
            run(["docker", "run", "-d", "--name", p, "--label", f"forgeswarm.verification={env['TEST_RESOURCE_TOKEN']}",
                 "--platform", "linux/amd64", "--network", env["TEST_DOCKER_NETWORK"], "-p", "127.0.0.1::8080",
                 "--mount", f"type=bind,source={ROOT / 'bin'},target=/app,readonly",
                 "-e", f"DATABASE_URL={url}", "-e", "HTTP_ADDR=0.0.0.0:8080", "-e", "REQUEST_TIMEOUT=1s",
                 "-e", "READY_TIMEOUT=300ms", "-e", "CONNECT_TIMEOUT=300ms", GO_IMAGE, "/app/api-linux"], label="api-start")
            _, published = run(["docker", "port", p, "8080/tcp"], quiet=True, label="api-port")
            port = published.strip().rsplit(":", 1)[1]
            base = f"http://127.0.0.1:{port}"
            def ready():
                try:
                    return http(base, "/health/ready")[0] == 200
                except (OSError, ValueError):
                    return False
            try:
                poll(ready, "API readiness")
            except RuntimeError:
                remove_api(p)
                raise
            return p
        def remove_api(p):
            code, owner = run(["docker", "inspect", "--format", '{{ index .Config.Labels "forgeswarm.verification" }}', p],
                              check=False, quiet=True, label="api-ownership")
            if code == 0 and owner.strip() == env["TEST_RESOURCE_TOKEN"]:
                run(["docker", "rm", "-f", p], label="api-cleanup")
        def stop_api(p):
            run(["docker", "stop", "--time", "15", p], timeout=25, label="api-sigterm")
            _, exit_code = run(["docker", "inspect", "--format", "{{.State.ExitCode}}", p], quiet=True, label="api-exit")
            _, logs = run(["docker", "logs", p], quiet=True, label="api-logs")
            with log_path.open("a", encoding="utf-8") as output:
                output.write(logs)
            remove_api(p)
            if exit_code.strip() != "0" or '"msg":"api_draining"' not in logs:
                raise RuntimeError("API did not handle SIGTERM and drain cleanly")
        try:
            process = start_api()
            body = b'{"name":"Smoke workflow","tasks":[{"key":"diagnostic","kind":"noop"}]}'
            code, receipt, headers = http(base, "/v1/workflows", "POST", body, "smoke-retry")
            assert code == 201 and set(receipt) == {"id", "created_at"}
            path = headers.get("Location")
            assert path == "/v1/workflows/" + receipt["id"]
            # Receipt is deliberately not used until the old process has exited.
            stop_api(process)
            process = start_api()
            code, retry, _ = http(base, "/v1/workflows", "POST", body, "smoke-retry")
            assert code == 201 and retry == receipt, "restart changed creation receipt"
            code, detail, _ = http(base, path)
            assert code == 200 and detail["tasks"][0]["status"] == "PENDING"
            code, cancellation, _ = http(base, path + "/cancel", "POST", b"")
            assert code == 200 and cancellation["version"] == 2
            _, cancelled, _ = http(base, path)
            assert http(base, path + "/cancel", "POST", b"")[1] == cancellation
            assert http(base, path)[1] == cancelled, "repeat cancellation mutated timestamps"
            assert http(base, "/v1/workflows", "POST", body, "smoke-retry")[1] == receipt
            _, events, _ = http(base, path + "/events?after=0&limit=1")
            assert len(events["events"]) == 1 and events["next_after"] == 1
            assert http(base, path + "/events?after=1")[1]["next_after"] == 2
            run(["docker", "stop", "--time", "5", container], timeout=15, label="smoke-outage")
            assert http(base, "/health/live")[0] == 200
            assert http(base, "/health/ready")[0] == 503
            assert http(base, path)[0] == 503
            run(["docker", "start", container], label="smoke-recovery")
            poll(is_ready, "PostgreSQL recovery")
            poll(lambda: http(base, "/health/ready")[0] == 200, "API readiness recovery")
            assert http(base, path)[1] == cancelled, "DB restart lost records"
            stop_api(process)
            print("smoke: six endpoints, API restart, no-op cancellation, DB outage/recovery and clean shutdown passed", flush=True)
            if RECORD:
                COMMANDS.append({"command": "smoke HTTP/restart/outage assertions", "started_at": started, "finished_at": timestamp(), "exit": 0,
                                 "required": True,
                                 "evidence_path": log_path.relative_to(ROOT).as_posix()})
        finally:
            if process:
                remove_api(process)
            if log_path.exists():
                log_path.write_text(sanitize(log_path.read_text(encoding="utf-8", errors="replace")), encoding="utf-8")


def fingerprint():
    paths = [ROOT / f for f in ["go.mod", "go.sum", ".env.example", ".gitignore", ".gitattributes", "Makefile", "compose.yaml"]]
    for folder in ["cmd", "internal", "migrations", "tests", "scripts", ".github","api","config","agent-runtime","web"]:
        paths.extend(p for p in (ROOT / folder).rglob("*") if p.is_file() and not any(part in {"__pycache__", ".pytest_cache", ".mypy_cache", ".ruff_cache", ".venv", ".cache", "node_modules", "dist", "test-results", "playwright-report"} or part.startswith("pytest-cache-files-") for part in p.parts))
    digest = hashlib.sha256()
    for path in sorted(paths):
        if path.is_file():
            digest.update(path.relative_to(ROOT).as_posix().encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest()


def progress(status, failure=None):
    path = ROOT / ".forgeswarm/progress.json"
    path.parent.mkdir(exist_ok=True)
    document = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"schema_version": 1, "stages": {}}
    if document.get("schema_version") != 1:
        raise RuntimeError("unsupported progress schema; refusing to overwrite")
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=ROOT, capture_output=True, text=True)
    dirty = subprocess.run(["git", "status", "--porcelain", "--", "."], cwd=ROOT, capture_output=True, text=True)
    stage = document.setdefault("stages", {}).setdefault("01", {})
    stage.update({"stage_number": 1, "title": "Durable workflow foundation", "status": status,
        "updated_at": timestamp(), "implemented_capabilities": ["six HTTP endpoints", "versioned normalized idempotency",
        "atomic workflow/task/event creation", "row-locked idempotent cancellation", "snapshot details", "event cursor pagination",
        "bounded readiness and request contexts", "graceful shutdown", "explicit Goose migrations", "disposable PostgreSQL verification"],
        "acceptance_criteria": {key: ("passed" if status == "verified_local" else "unverified") for key in
            ["six_endpoints", "concurrency_and_conflicts", "event_failure_rollback", "replay_after_pool_restart_and_cancellation",
             "cancel_noop_and_consistent_details", "sql_constraints", "blocked_query_cancellation_and_cleanup", "api_restart",
             "db_outage_liveness_readiness_recovery", "format_vet_unit_race_build_integration_smoke"]},
        "blockers": [failure] if failure else [], "external_verification": {"live_models": "not_applicable", "cloud": "not_requested"},
        "reference_docs": ["https://pkg.go.dev/github.com/jackc/pgx/v5", "https://pkg.go.dev/github.com/jackc/pgx/v5/pgxpool",
            "https://www.postgresql.org/docs/current/transaction-iso.html", "https://pkg.go.dev/net/http",
            "https://pressly.github.io/goose/documentation/provider/", "https://hub.docker.com/_/postgres"],
        "decisions": ["Empty project initialized as a standalone Git repository; unrelated parent files preserved",
            "Task definitions do not execute; no attempt/scheduler/broker/agent/frontend",
            "Normalization v1 frozen by golden hash; future fields require explicit retry compatibility",
            "Workflow version is event sequence only for stage-01 mutations; future history requires a serialized allocator",
            "Task unique indexes and event primary key cover workflow FK/update/order predicates; no redundant indexes",
            "Verification owns uniquely named labelled disposable containers; development storage never destroyed",
            "Generated sanitized logs replaced on rerun and ignored; progress and evidence excluded from fingerprint",
            "Official pgx, PostgreSQL, net/http, Goose and Docker image documentation consulted",
            "Windows race tests use pinned Linux Go image; smoke runs Linux API containers to verify real SIGTERM draining"],
        "sources": ["cmd/api/main.go:run", "internal/workflow/workflow.go:HashForVersion", "internal/postgres/store.go:Create,Cancel,Details,Events,Ready",
            "internal/httpapi/api.go:Handler", "internal/config/config.go:Parse", "migrations/00001_workflows.sql",
            "tests/integration/workflow_test.go", "scripts/dev.py", ".github/workflows/stage-01.yaml"],
        "validation_commands": COMMANDS, "git": {"revision": revision.stdout.strip() if revision.returncode == 0 else None,
            "dirty": bool(dirty.stdout.strip()), "scope": "."},
        "fingerprint": {"algorithm": "sha256", "value": fingerprint(), "excludes": [".forgeswarm", "artifacts/verification", ".cache", "bin", "__pycache__"]},
        "next_stage": 2 if status == "verified_local" else 1})
    if status == "verified_local":
        stage["blockers"] = []
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(document, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / "results.json").write_text(json.dumps({"status": status, "failure": failure, "commands": COMMANDS}, indent=2) + "\n", encoding="utf-8")


def fmt_check():
    paths = sorted(str(p.relative_to(ROOT)) for folder in ["cmd", "internal", "migrations", "tests","api"] for p in (ROOT / folder).rglob("*.go"))
    _, output = run(["gofmt", "-l", *paths], label="format")
    if output.strip():
        raise RuntimeError("format check failed; run fmt")


def main():
    global RECORD
    if len(sys.argv)>1 and sys.argv[1] in {"observability-up","eval-fixture","eval-real","verify-stage-09"}:
        import stage09
        stage09.command(sys.argv[1]);return
    if len(sys.argv)>1 and sys.argv[1] in {"verify-stage-08","github-contract-test","github-live-demo"}:
        import stage08
        stage08.command(sys.argv[1]);return
    if len(sys.argv)>1 and sys.argv[1] in {"verify-stage-07","collaboration-test","collaboration-fixture","dashboard-test"}:
        import stage07
        stage07.command(sys.argv[1]);return
    if len(sys.argv)>1 and sys.argv[1] in {"verify-stage-06","agent-test","agent-demo-fixture","agent-runtime-prepare"}:
        import stage06
        stage06.command(sys.argv[1]);return
    os.chdir(ROOT)
    (ROOT / ".cache/go-tmp").mkdir(parents=True, exist_ok=True)
    command = sys.argv[1] if len(sys.argv) == 2 else "help"
    if command in {"verify-stage-05","sandbox-prepare","sandbox-fixture","sandbox-demo","sandbox-test"}:
        import stage05
        stage05.command(command)
        return
    if command=="verify-stage-04":
        import stage04
        stage04.verify()
        return
    if command=="verify-stage-03":
        import stage03
        stage03.verify()
        return
    if command in {"proto-check","proto-generate"}:
        import proto
        proto.generate(command=="proto-check")
        return
    if command == "verify-stage-02":
        import stage02
        stage02.verify()
        return
    if command == "help":
        print("Stage 09: observability-up; eval-fixture; guarded eval-real; verify-stage-09.\n"
              "Local OTLP endpoint 127.0.0.1:4317; enable TELEMETRY_ENABLED=true on trusted services. Grafana 3000, traces 16686, Prometheus 9095.\n"
              "Migrate additive 00009 before upgrading roles. Config/metric units/buckets: config/observability/definitions.json. IDs never become labels.\n"
              "Eight frozen Go tasks/four configs; shared 12-call/24-tool/200000-token/90s task budget. Fixture results are plumbing only.\n"
              "Raw manifests/JSONL/CSV and independent summary/HTML remain under ignored artifacts/verification/stage-09.\n"
              "Live eval requires existing session authorization, EVAL_AUTHORIZATION_FILE with total/per-run caps and exact catalog/pricing hashes, EVAL_CONFIRM=existing-session-authorized-spend and configured runtime/pricing.\n"
              "Runtime must explicitly set AGENT_EVAL_ENABLED=true. No paid call is authorized by configuration alone. Missing usage/cost stays unknown; uncertain charges retain reservations.\n\n"
              "Stage 08: github-contract-test; verify-stage-08 (full Stage 07 gate plus real approval/GitHub HTTP/browser tests).\n"
              "Migrate additive 00008 and upgrade trusted services together. Existing migrations and normalization v1-v7 stay unchanged.\n"
              "Generate ignored credentials: go run ./cmd/operator-init -file .cache/operator-auth.json; set OPERATOR_AUTH_FILE on API only.\n"
              "POST /v1/session {token}: 1h HttpOnly SameSite=Strict cookie; mutations require returned csrf in X-CSRF-Token plus same Origin. Trusted CLI may use Bearer instead.\n"
              "All /v1 reads, artifacts and controls require identity when configured; read_only cannot mutate. Persisted GitHub imports fail closed if auth configuration disappears.\n"
              "GITHUB_CONFIG_FILE is API/publisher only: copy config/github.example.json under .cache, configure one numeric repository ID, owner/name/target and trigger actor/label.\n"
              "Prefer repository-scoped installation_token; explicit development_token supported. Required permissions: metadata/issues read, contents/pull requests write. Token refresh is operator-managed; 401/403 stop.\n"
              "POST /v1/github/import {issue_number}+Idempotency-Key, or HMAC SHA-256 signed issues:labeled /v1/github/webhook. Delivery UUID/raw SHA-256 dedup is durable.\n"
              "Preparation pins commit/tree and verifies bounded root blobs (controlled math-v1, 4 KiB). No host checkout, hooks, LFS, submodules, arbitrary URLs or redirects.\n"
              "Imported projects enter aggregate WAITING_FOR_APPROVAL with completed DAGs/no worker lease. Scheduler APPROVAL_TTL=1s..24h (default 24h), pinned using DB clock.\n"
              "GET /v1/approvals?root=UUID&after=UUID (20/page); GET/POST /v1/approvals/{id}. Decision fields: expected_version,binding_sha256,candidate_sha256,decision,acknowledge_sensitive. Actor derives from auth.\n"
              "Bindings pin repo/target/base/plan/candidate/tree/patch/test/review/action/policy. Sensitive paths are flagged and outside current writable fixture policy. New base/content requires a fresh reviewed import/approval.\n"
              "Trusted publisher: go run ./cmd/publisher -repository-id ID -confirm-publish; -once -approval UUID selects one intent; -reconcile-only discovers without new writes.\n"
              "Stable operation/branch/commit/PR marker; no update/force refs, merge, deploy or resource deletion. Uncertain outcomes reconcile first; six retries honor durable reset guidance, then manual reconciliation.\n"
              "Cancellation/expiry prevents new writes but cannot roll back an in-flight external effect. Observed branch/PR and drift stay visible; old approval cannot authorize a new base.\n"
              "github-live-demo requires GITHUB_CONFIG_FILE, GITHUB_LIVE_APPROVAL_ID, GITHUB_LIVE_REPOSITORY_ID and GITHUB_LIVE_CONFIRM=publish-approved-draft, plus actual session authorization.\n"
              "Single-owner loopback only; no browser localStorage secrets. TEST_DEMO API URL is numeric loopback only; live API is fixed api.github.com over HTTPS, REST version 2026-03-10.\n\n"
              "Stage 07: collaboration-fixture; collaboration-test; dashboard-test; verify-stage-07 (includes full Stage 06/05/04 gate).\n"
              "POST /v1/projects + Idempotency-Key: same controlled job fields as Stage 06, test_profile=math-v1. Operator CLI: agent-register -profile math-v1.\n"
              "COLLAB_ENABLED=true; COLLAB_MAX_ROUNDS=0..2 (default 2); AGENT_ENABLED=true on API, coordinator and scheduler; worker needs configured runtime.\n"
              "Migration 00007 is additive and normalization v1-v6 stays frozen; upgrade all roles together.\n"
              "Planner is read-only; strict plan caps 4 developers/8 edges, declared agent_go capability, file scopes and fixed profiles. Trusted finalizer inserts mandatory integrate/review tasks in a new child DAG.\n"
              "POST /v1/projects/{id}/plans/{version}/finalize + {digest}: same version/digest returns the existing child; different content requires a new immutable correction version. Scheduler recovers finalization automatically.\n"
              "Developers use private in-memory snapshots and confined Docker tests. Dependent work receives a deterministic merge of exact accepted predecessor artifacts.\n"
              "math-v1: add.go/Add(a,b)=a+b, double.go/Double(a)=2*a, score.go/Score(a,b)=3*a+b. Only one pure arithmetic function per file, 4 KiB total/8 files.\n"
              "Conservative three-way enclosing-hunk merge rejects overlapping edits/Git-style conflicts; no host patch/Git execution and no silent winner. Combined tests can reject semantic conflicts despite separate profile passes.\n"
              "Review is read-only and candidate-bound. Rejection starts a fresh immutable plan/execution version; max corrections default 2. Conflicts/test failures and exhausted repairs end NEEDS_HUMAN.\n"
              "One pinned project allowance across roles/rounds: dated configured maximum estimate, 96 calls, 800000 tokens, 10m deadline; task/attempt limits also apply. Unknown usage retains reservations.\n"
              "GET /v1/workflows?after=UUID&limit=20; /v1/workflows/{id}/summary and /candidates; GET /v1/projects/{id} exposes versions and aggregate state.\n"
              "Dashboard: Node 24.16.0, npm ci --prefix web --ignore-scripts; npm run dev --prefix web (127.0.0.1:5173) or build and set WEB_DIR=web/dist on the local API.\n"
              "Dashboard/control hosting is local loopback only, with host/origin checks. No unauthenticated public controls or automatic GitHub publication.\n"
              "GET /v1/workflows/{id}/stream supports Last-Event-ID or initial ?after=sequence. At-least-once UI delivery; deduplicate by workflow/sequence.\n"
              "SSE: durable pages <=32, max 64 clients, 5s heartbeat, 1s slow-writer deadline, 30s reconnect rotation. Retention gaps return event_retention_gap or explicit stream event; refresh detail/cursor. Disconnect never cancels execution.\n"
              "Frontend retains 100 escaped events; cancellation uses REST and reports pending until persisted state/cleanup settles. No untrusted HTML rendering.\n"
              "Stage 06: agent-demo-fixture; agent-runtime-prepare; agent-test; verify-stage-06 (includes Stage 05/04 real regressions).\n"
              "Python 3.12.10 + uv 0.13.0; locked dependencies, lint/type/pytest and reproducible Go/Python protobuf.\n"
              "Operator: go run ./cmd/agent-register -repository PATH -revision FULL_COMMIT; catalog/snapshot defaults under .cache.\n"
              "POST /v1/jobs + Idempotency-Key: schema_version=1, repository_id, base_revision, task_description, policy=controlled-go-v1, budget=small-v1, test_profile=sum-v1.\n"
              "Only sum.go is writable, via exact conditional replacement. Tree/read/literal search/status/diff are bounded; no model shell.\n"
              "Immutable helper evaluates Add(int,int) against six cases outside writable agent source. Final text cannot establish success.\n"
              "sum-v1 requires one pure Add arithmetic expression; imports/init/calls/extra declarations and compiler directives are denied.\n"
              "Limits: 12 model turns across task attempts, 24 tools/attempt, 2 denials, 2 schema repairs/429 retries; 200000 tokens/task.\n"
              "Per attempt wall 90s, fixed overall job task deadline 120s; context 12 KiB, observation 4 KiB, patch 8 KiB, output 1024 tokens.\n"
              "Pricing config pins dated input/output estimates and maximum (default USD 0.50); reserve before call, unknown usage retains estimate.\n"
              "Retries start pinned base and retain prior budget charges. Interrupted edits are never blindly reapplied.\n"
              "Migrate 00006 and upgrade all roles together; legacy normalization v1-v5 is preserved.\n"
              "Runtime: local separately confined read-only nonroot Docker image, no repository mounts/socket/DB/broker keys; loopback authenticated RPC only.\n"
              "Use scripts/stage06.py runtime(image) as executable TEST/DEMO configuration reference; AGENT_PROVIDER=scripted_TEST_DEMO needs AGENT_TEST_DEMO=true.\n"
              "Worker/coordinator AGENT_ENABLED=true; API AGENT_CATALOG; coordinator AGENT_PRICING_FILE; worker runtime address/token and explicit AGENT_INSECURE_LOCAL=true.\n"
              "OpenAI adapter defaults disabled. Live runtime needs its own OPENAI_API_KEY, AGENT_PROVIDER=openai, AGENT_LIVE_ENABLED=true and existing explicit spending authorization.\n"
              "Scripted evidence proves deterministic infrastructure, not live model capability/success rates. No paid calls in verification.\n"
              "Stage 05: verify-stage-05; sandbox-prepare; sandbox-fixture / sandbox-demo; sandbox-test.\n"
              "Vetted small dependency-free Go 1.26.3 fixtures, Linux cgroup v2 Docker; no host repository commands.\n"
              "sandbox-fixture registers the bundled demo; custom inputs: go run ./cmd/sandbox-fixture -repository PATH -revision FULL_COMMIT.\n"
              "Set SANDBOX_ENABLED=true, stable SANDBOX_INSTALLATION_ID, registry/helper paths before run-worker.\n"
              "Migrate 00005 and upgrade all services together; v5 adds sandbox_go plus sandbox:{schema_version:1,fixture_id:ID,command:build|test}.\n"
              "Default per command: 1 CPU, 512 MiB memory/no swap, 64 PIDs, 60s, 256 MiB workspace + 64 MiB tmp + 1 MiB shm.\n"
              "Network off, nonroot, read-only image/harness, default seccomp, capabilities dropped; actual rootless/rootful mode recorded.\n"
              "Accepted artifacts: local PostgreSQL byte store, 8 KiB maximum each, hashes/truncation, ID-only JSON/base64 retrieval.\n"
              "GET /v1/workflows/{id}/artifacts/{artifact_id}; execution metadata appears in task attempt inspection.\n"
              "Docker shares its kernel; no complete hostile-code isolation. Runtime failure retains cleanup uncertainty and denies publication.\n"
              "ForgeSwarm Stages 01/02/03: make <command> or python scripts/dev.py <command>\n"
              "Stage 04: verify-stage-04; RETRY_WAIT persists capped jittered retry due times.\n"
              "Migrate 00004 and upgrade API/coordinator/schedulers/workers together; old binaries cannot enforce new policies.\n"
              "Defaults: workflow deadline 10m, task deadline 2m from creation, 3 attempts; retries never extend them.\n"
              "POST timeout_ms (100-86400000) on workflow/task, task max_attempts (1-10); zero means default; normalization v4 preserves v1-v3.\n"
              "MAX_WORKFLOWS=100, MAX_TASKS_PER_WORKFLOW=100, MAX_QUEUED_TASKS=10000, MAX_READY_TASKS=1000.\n"
              "New admissions alone return 429 + Retry-After: 1; replay/cancellation/health/results remain available.\n"
              "All services sharing a database must use the same admission/concurrency settings.\n"
              "Active cancel enters CANCELLING, signals stop on renew, settles after stop acknowledgment or lease fence.\n"
              "Lease fencing does not prove physical termination; cleanup_required is retained until acknowledged; sandbox teardown is Stage 05.\n"
              "READY resends every 5s retain generation and change transport revision; capability wait is bounded to 30s.\n"
              "GET /v1/workflows/{id}/tasks/{task_id} and /attempts?after=0&limit=10 expose bounded diagnostics.\n"
              "Manual redrive is deferred/read-only; accepted outcomes and terminal tasks cannot be rewritten.\n"
              "FIFO/age scans rotate; persistent lock contention has no strict fairness guarantee.\n"
              "Local broker: docker compose --profile execution up -d broker; INSECURE_LOCAL=true and nats://127.0.0.1:4222 only for loopback.\n"
              "Stage 03: verify-stage-03, proto-check/proto-generate, run-coordinator, run-relay, run-worker.\n"
              "Migrate first, then run API/coordinator/scheduler/relay and >=2 workers.\n"
              "Workers require WORKER_TOKEN matching RPC_KEYS_FILE identities; NATS_TOKEN is required.\n"
              "RPC/NATS require verified TLS; INSECURE_LOCAL=true permits loopback addresses only.\n"
              "Defaults: 2 slots/worker, 4 total, 3 attempts, 3s leases; only noop/delay (1-30000ms).\n"
              "Normalization v1/v2 hashes stay frozen; delay input uses v3.\n"
              "Outbox + file-backed JetStream provide at-least-once dispatch with fenced results.\n"
              "Single-node broker disk loss is recovered by Stage-04 READY republishing when PostgreSQL survives; no replication/throughput claim.\n"
              "help, db-up, db-down (keeps data), migrate-up, migrate-status, run, fmt, vet, test,\n"
              "test-race, test-integration (requires Docker; never skips), build, smoke, verify-stage-01,\n"
              "verify-stage-02, run-scheduler\n"
              "Go 1.26.3, Python 3.10+, Docker Linux engine required; Linux hosts also need a C compiler.\n"
              "Windows race tests use a pinned Linux Go container; smoke tests run Linux API containers.\n"
              "Set env vars from .env.example; API does not automatically load .env or migrate.\n"
              "Stage 02: migrate-up, upgrade API, then run-scheduler; never mix the old API with the new scheduler.\n"
              "depends_on is a set; dependency order is ignored, task display order stays supplied.\n"
              "Graph GET returns task_id -> depends_on_task_id (requires success), max 100 nodes/1000 edges.\n"
              "PENDING -> READY means eligibility only; no tasks execute in Stage 02.\n"
              "Parent-row locks serialize mutation/history; SKIP LOCKED has no strict fairness guarantee.\n"
              "SCHEDULER_BATCH_SIZE=16 (1-100), SCHEDULER_INTERVAL=1s (50ms-1m), SCHEDULER_SCAN_TIMEOUT=5s (50ms-30s).\n"
              "Stage 03 claims enter RUNNING; accepted outcomes use authenticated worker RPCs.\n"
              "Test-only diamond fixture tests/fixtures/diamond.json is demonstrated in verify-stage-02\n"
              "using explicit terminal seeds in uniquely owned disposable databases only.\n"
              "Creation keys have local single-user scope and survive for the record lifetime.\n"
              "Verification uses random isolated resources and leaves dev storage intact.\n"
              "Evidence under artifacts/verification/stage-01 is sanitized, bounded to 128 KiB/log,\n"
              "ignored and replaced on rerun. Stage 02 records separate stage-02 evidence and preserves stage-01 history.\n"
              "Export evidence externally for longer retention. No providers, cloud provisioning or GitHub writes.")
        return
    if command == "verify-stage-01":
        if not __debug__:
            raise RuntimeError("verification requires Python assertions; disable optimization")
        if not EVIDENCE.resolve().is_relative_to(ROOT.resolve()):
            raise RuntimeError("evidence path is outside the project")
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        # Replace only known generated files from the previous run, never recurse.
        old_logs = list(EVIDENCE.glob("[0-9][0-9][0-9]-*.log"))
        for old in old_logs + [EVIDENCE / "smoke-api.log", EVIDENCE / "results.json"]:
            if old.is_file() and not old.is_symlink():
                old.unlink()
        RECORD = True
        progress("in_progress")
        try:
            fmt_check()
            run([sys.executable, "-m", "py_compile", "scripts/dev.py"], label="python")
            run(["go", "vet", "./..."], label="vet")
            run(["go", "test", "-count=1", "./..."], label="unit")
            race(["-count=1", "./..."])
            build()
            run(["docker", "compose", "config", "--quiet"], label="compose")
            integration()
            smoke()
        except (RuntimeError, AssertionError) as exc:
            progress("blocked", str(exc))
            raise
        progress("verified_local")
        print("stage 01 verified_local; see .forgeswarm/progress.json and artifacts/verification/stage-01/results.json")
    elif command == "fmt":
        paths = sorted(str(p.relative_to(ROOT)) for folder in ["cmd", "internal", "migrations", "tests","api"] for p in (ROOT / folder).rglob("*.go"))
        run(["gofmt", "-w", *paths])
    elif command == "test-race":
        race(["-count=1", "./..."])
    elif command in {"vet", "test"}:
        args = ["go", "vet", "./..."] if command == "vet" else ["go", "test", "-count=1"] + (["-race"] if command == "test-race" else []) + ["./..."]
        run(args)
    elif command == "build":
        build()
    elif command == "test-integration":
        integration()
    elif command == "smoke":
        if not __debug__:
            raise RuntimeError("smoke requires Python assertions; disable optimization")
        smoke()
    elif command == "run-scheduler":
        raise SystemExit(subprocess.call(["go", "run", "./cmd/scheduler"], cwd=ROOT, env=ENV))
    elif command in {"run-coordinator","run-relay","run-worker"}:
        raise SystemExit(subprocess.call(["go","run","./cmd/"+command.removeprefix("run-")],cwd=ROOT,env=ENV))
    elif command == "run":
        # Inherit terminal signals for the actual API process.
        raise SystemExit(subprocess.call(["go", "run", "./cmd/api"], cwd=ROOT, env=ENV))
    elif command in {"db-up", "db-down"}:
        run(["docker", "compose", "up", "-d", "--wait", "--wait-timeout", "60"] if command == "db-up" else ["docker", "compose", "down"])
    elif command in {"migrate-up", "migrate-status"}:
        run(["go", "run", "./cmd/migrate", command.removeprefix("migrate-")])
    else:
        raise RuntimeError(f"unknown command: {command}")


if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, AssertionError) as error:
        print(f"verification failed: {error}", file=sys.stderr)
        sys.exit(1)

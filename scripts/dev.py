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
        docker += ["--network", env["TEST_DOCKER_NETWORK"]]
        test_url = re.sub(r"127\.0\.0\.1:\d+/", "db:5432/", env["TEST_DATABASE_URL"])
        docker += ["-e", f"TEST_DATABASE_URL={test_url}", "-e", f"TEST_RESOURCE_TOKEN={env['TEST_RESOURCE_TOKEN']}"]
    try:
        return run([*docker, GO_IMAGE, "go", "test", "-race", "-timeout=120s", *args], timeout=300, label=label)
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
    paths = [ROOT / f for f in ["go.mod", "go.sum", ".env.example", ".gitignore", "Makefile", "compose.yaml"]]
    for folder in ["cmd", "internal", "migrations", "tests", "scripts", ".github"]:
        paths.extend(p for p in (ROOT / folder).rglob("*") if p.is_file() and "__pycache__" not in p.parts)
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
    paths = sorted(str(p.relative_to(ROOT)) for folder in ["cmd", "internal", "migrations", "tests"] for p in (ROOT / folder).rglob("*.go"))
    _, output = run(["gofmt", "-l", *paths], label="format")
    if output.strip():
        raise RuntimeError("format check failed; run fmt")


def main():
    global RECORD
    os.chdir(ROOT)
    (ROOT / ".cache/go-tmp").mkdir(parents=True, exist_ok=True)
    command = sys.argv[1] if len(sys.argv) == 2 else "help"
    if command == "verify-stage-02":
        import stage02
        stage02.verify()
        return
    if command == "help":
        print("ForgeSwarm Stages 01/02: make <command> or python scripts/dev.py <command>\n"
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
              "RUNNING transitions are reserved for Stage 03 claiming, with no completion endpoint.\n"
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
        paths = sorted(str(p.relative_to(ROOT)) for folder in ["cmd", "internal", "migrations", "tests"] for p in (ROOT / folder).rglob("*.go"))
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

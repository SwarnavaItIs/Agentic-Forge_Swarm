"""Bounded local collaboration/dashboard gate; never calls a paid provider."""
import contextlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import sys
import time
import uuid

import dev
import stage03
import stage05
import stage06

EVIDENCE = dev.ROOT / "artifacts/verification/stage-07"
TAGS = "integration,stage03,stage04,stage05,stage06,stage07"
CRITERIA = ["stage06_and_prior_regressions", "strict_plan_role_and_merge_policy", "immutable_finalization_restart_and_family_budget", "parallel_developers_and_exact_predecessor_snapshot", "real_combined_candidate_protected_tests_and_review", "conflicts_and_bounded_correction_versions", "durable_sse_cursors_bounds_and_slow_clients", "python_contracts_and_frontend_types_build_components", "real_local_dashboard_browser_smoke"]
NPM = shutil.which("npm") or "npm"
NODE = shutil.which("node") or "node"
WEB_ENV = dev.ENV | {"NODE_USE_SYSTEM_CA": "1", "npm_config_registry": "https://registry.npmjs.org", "npm_config_cache": str(dev.ROOT / ".cache/npm"), "npm_config_fetch_retries": "0", "npm_config_fetch_timeout": "30000", "PLAYWRIGHT_BROWSERS_PATH": str(dev.ROOT / ".cache/playwright"), "PLAYWRIGHT_DOWNLOAD_CONNECTION_TIMEOUT": "45000"}


def fixture(registry=None, catalog=None, folder=None):
    folder = folder or dev.ROOT / ".cache/collaboration-demo-repository"
    registry = registry or dev.ROOT / ".cache/sandbox-fixtures"
    catalog = catalog or dev.ROOT / ".cache/agent-catalog"
    marker = folder / ".forgeswarm-owned"
    if folder.exists() and (folder.is_symlink() or not marker.is_file() or marker.read_text() != "stage07-demo-v1"):
        raise RuntimeError("refusing a non-owned collaboration fixture")
    folder.mkdir(parents=True, exist_ok=True)
    marker.write_text("stage07-demo-v1")
    git = ["git", "-C", folder, "-c", "core.hooksPath=" + str(dev.ROOT / ".cache/no-hooks"), "-c", "core.fsmonitor=false", "-c", "core.autocrlf=false"]
    env = dev.ENV | {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0"}
    if not (folder / ".git").exists():
        dev.run([*git, "init"], env=env, quiet=True, label="collab-fixture-init")
    source = dev.ROOT / "tests/fixtures/collaboration-go"
    for name in ["go.mod", "add.go", "double.go", "score.go", "README.txt"]:
        (folder / name).write_bytes((source / name).read_bytes())
    dev.run([*git, "add", "--", "go.mod", "add.go", "double.go", "score.go", "README.txt"], env=env, quiet=True, label="collab-fixture-index")
    dev.run([*git, "-c", "user.name=ForgeSwarm Fixture", "-c", "user.email=fixture@localhost", "commit", "--allow-empty", "-m", "Controlled multi-role TEST/DEMO fixture"], env=env, quiet=True, label="collab-fixture-commit")
    _, revision = dev.run([*git, "rev-parse", "HEAD"], env=env, quiet=True, label="collab-fixture-revision")
    _, registered = dev.run(["go", "run", "./cmd/agent-register", "-profile", "math-v1", "-registry", registry, "-catalog", catalog, "-repository", folder, "-revision", revision.strip()], env=env, quiet=True, label="collab-trusted-registration")
    registration = json.loads(registered)
    return {"schema_version": 1, "repository_id": registration["repository_id"], "base_revision": registration["base_revision"], "task_description": "Fix Add and Double independently, then fix dependent Score using their exact predecessor source.", "policy": "controlled-go-v1", "budget": "small-v1", "test_profile": "math-v1"}


def frontend():
    _, version = dev.run([NODE, "--version"], label="node-version")
    if version.strip() != "v24.16.0":
        raise RuntimeError("Stage 07 verification pins Node 24.16.0")
    dev.run([NPM, "ci", "--prefix", "web", "--ignore-scripts", "--no-audit", "--no-fund"], env=WEB_ENV, timeout=180, label="frontend-locked-install")
    dev.run([NPM,"run","format:check","--prefix","web"],env=WEB_ENV,label="frontend-format")
    dev.run([NPM, "run", "test", "--prefix", "web"], env=WEB_ENV, label="frontend-components")
    dev.run([NPM, "run", "build", "--prefix", "web"], env=WEB_ENV, label="frontend-types-build")
    dev.run([NODE, "web/node_modules/@playwright/test/cli.js", "install", "chromium"], env=WEB_ENV, timeout=240, label="pinned-browser-install")


def database_tests():
    with stage03.infrastructure() as (env, db, broker, healthy):
        dev.race(["-tags=" + TAGS, "-count=1", "-timeout=180s", "-v", "./tests/integration", "-run", "TestCollaboration"], env=env, container=db, label="collaboration-db-race")


def real_tests(image):
    with stage06.runtime(image) as settings:
        with stage03.infrastructure() as (env, db, broker, healthy):
            if os.name == "nt":
                _, port = dev.run(["docker", "port", broker, "4222/tcp"], quiet=True, label="collaboration-broker-port")
                env = env | {"TEST_NATS_URL": "nats://127.0.0.1:" + port.strip().rsplit(":", 1)[1]}
            env = env | {k: v for k, v in settings.items() if k.startswith("TEST_AGENT_")}
            stage05.native_tests(["-tags=" + TAGS + ",real_sandbox", "./tests/integration", "-run", "TestCollaboration"], env, "collaboration-real-workers")


def free_port():
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return str(server.getsockname()[1])


@contextlib.contextmanager
def local_services(settings, trusted_settings=None, control=None, telemetry=False):
    with stage03.infrastructure() as (env, db, broker, healthy):
        private = dev.ROOT / ".cache/stage07" / uuid.uuid4().hex
        private.mkdir(parents=True)
        registry, catalog = private / "fixtures", private / "catalog"
        job = fixture(registry, catalog, private / "repository")
        _, port = dev.run(["docker", "port", broker, "4222/tcp"], quiet=True, label="browser-broker-port")
        keys = {"worker_a": uuid.uuid4().hex + uuid.uuid4().hex}
        (private / "keys.json").write_text(json.dumps(keys))
        http_port, rpc_port = free_port(), free_port()
        env = env | {"DATABASE_URL": env["TEST_DATABASE_URL"], "HTTP_ADDR": "127.0.0.1:" + http_port, "RPC_ADDR": "127.0.0.1:" + rpc_port, "INSECURE_LOCAL": "true", "RPC_KEYS_FILE": str(private / "keys.json"), "NATS_URL": "nats://127.0.0.1:" + port.strip().rsplit(":", 1)[1], "NATS_TOKEN": env["TEST_NATS_TOKEN"], "WORKER_CAPACITY": "2", "WORKER_MAX_CAPACITY": "2", "TOTAL_CAPACITY": "2", "SCHEDULER_INTERVAL": "100ms", "SCHEDULER_SCAN_TIMEOUT": "2s", "SANDBOX_ENABLED": "true", "SANDBOX_INSTALLATION_ID": str(uuid.uuid4()), "SANDBOX_FIXTURE_REGISTRY": str(registry), "SANDBOX_HELPER": str(stage05.HELPER), "AGENT_ENABLED": "true", "AGENT_CATALOG": str(catalog), "AGENT_PRICING_FILE": str(dev.ROOT / "config/agent-pricing.json"), "AGENT_RUNTIME_ADDR": settings["TEST_AGENT_RUNTIME_ADDR"], "AGENT_RUNTIME_TOKEN": settings["TEST_AGENT_RUNTIME_TOKEN"], "AGENT_INSECURE_LOCAL": "true", "COLLAB_ENABLED": "true", "COLLAB_MAX_ROUNDS": "2", "WEB_DIR": str(dev.ROOT / "web/dist")}
        dev.run([dev.BIN.parent / ("migrate.exe" if os.name == "nt" else "migrate"), "up"], env=env, label="browser-additive-migrate")
        if telemetry:
            env.update({"TELEMETRY_ENABLED":"true","TELEMETRY_SAMPLE_RATIO":"1","OTEL_EXPORTER_OTLP_ENDPOINT":"127.0.0.1:4317"})
        processes, logs = [], []
        try:
            roles = ["api", "scheduler", "coordinator", "relay", "worker"] + (["publisher"] if trusted_settings else [])
            for role in roles:
                log_path = private / (role + ".log")
                log = log_path.open("wb")
                role_env = {k:v for k,v in env.items() if k not in {"GITHUB_CONFIG_FILE","OPERATOR_AUTH_FILE","GITHUB_TOKEN","GH_TOKEN","OPENAI_API_KEY"}}
                if telemetry:
                    role_env["TELEMETRY_SERVICE_NAME"]="worker_a" if role=="worker" else role
                if role == "worker":
                    role_env["WORKER_TOKEN"] = keys["worker_a"]
                if trusted_settings and role in {"api", "publisher"}:
                    role_env.update(trusted_settings)
                    if role == "publisher":
                        role_env.pop("OPERATOR_AUTH_FILE", None)
                args = [str(dev.BIN.parent / (role + (".exe" if os.name == "nt" else "")))]
                if role == "publisher":
                    args += ["-repository-id","42","-confirm-publish"]
                process = subprocess.Popen(args, cwd=dev.ROOT, env=role_env, stdout=log, stderr=subprocess.STDOUT, creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
                processes.append(process); logs.append((role, log_path, log))
                if control is not None and role=="worker":
                    control.update({"worker":process,"args":args,"env":role_env,"log":log,"processes":processes})
            base = "http://127.0.0.1:" + http_port
            def ready():
                try:
                    return dev.http(base, "/health/ready")[0] == 200 and all(p.poll() is None for p in processes)
                except OSError:
                    return False
            dev.poll(ready, "local private dashboard readiness")
            yield base, job
        finally:
            for process in reversed(processes):
                if process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill(); process.wait(timeout=5)
            _, containers=dev.run(["docker","ps","-aq","--no-trunc","--filter","label=forgeswarm.installation="+env["SANDBOX_INSTALLATION_ID"],"--filter","label=forgeswarm.resource=sandbox-v1"],quiet=True,label="browser-owned-sandbox-scan")
            for container in containers.split():
                _, inspection=dev.run(["docker","inspect",container],quiet=True,label="browser-owned-sandbox-check")
                labels=json.loads(inspection)[0]["Config"]["Labels"]
                if labels.get("forgeswarm.installation")==env["SANDBOX_INSTALLATION_ID"] and labels.get("forgeswarm.resource")=="sandbox-v1" and labels.get("forgeswarm.worker")=="worker_a":
                    dev.run(["docker","rm","-f",container],label="browser-owned-sandbox-cleanup")
            EVIDENCE.mkdir(parents=True, exist_ok=True)
            for role, path, log in logs:
                log.close()
                (EVIDENCE / ("browser-" + role + ".log")).write_text(dev.sanitize(path.read_text(encoding="utf-8", errors="replace")))


def browser_smoke(image):
    with stage06.runtime(image) as settings:
        with local_services(settings) as (base, job):
            code, receipt, _ = dev.http(base, "/v1/projects", "POST", json.dumps(job).encode(), "browser-collab-" + uuid.uuid4().hex)
            assert code == 202, (code, receipt)
            project = None
            def completed():
                nonlocal project
                code, project, _ = dev.http(base, "/v1/projects/" + receipt["id"])
                if code != 200 or project["status"] in {"FAILED", "NEEDS_HUMAN"}:
                    raise AssertionError(project)
                return project["status"] == "SUCCEEDED"
            dev.poll(completed, "real multi-role dashboard fixture", timeout=80)
            execution = next(v["workflow_id"] for v in project["versions"] if v["phase"] == "execution")
            env = WEB_ENV | {"TEST_DASHBOARD_URL": base, "TEST_COLLAB_ROOT": receipt["id"], "TEST_COLLAB_EXECUTION": execution, "TEST_SCREENSHOT": str(EVIDENCE / "dashboard.png")}
            dev.run([NPM, "run", "test:browser", "--prefix", "web"], env=env, timeout=120, label="real-dashboard-browser")
            proof = {"provider": "scripted_TEST_DEMO", "root_id": receipt["id"], "execution_id": execution, "status": project["status"], "versions": len(project["plans"]), "live_model": "unverified", "verified_at": dev.timestamp()}
            (EVIDENCE / "browser-demo.json").write_text(json.dumps(proof, indent=2) + "\n")


def progress(status, results, error=None):
    path = dev.ROOT / ".forgeswarm/progress.json"
    document = json.loads(path.read_text())
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=dev.ROOT, text=True).strip()
    document["stages"].setdefault("07", {}).update({"stage_number": 7, "title": "Validated planning, review, patch integration and dashboard", "status": status, "updated_at": dev.timestamp(), "implemented_capabilities": ["immutable normalized plans and idempotent child finalization", "read-only planner/reviewer and scoped developer gateway", "private workspaces and deterministic three-way integration with protected combined tests", "candidate-bound review and bounded immutable correction versions", "shared durable role/round spending and token budget", "private React dashboard and durable cursor SSE"], "acceptance_criteria": {key: results.get(key, "unverified") for key in CRITERIA}, "blockers": [str(error)] if error else [], "decisions": ["Plan preparation and each child execution are distinct immutable DAGs; original accepted outputs are never reset.", "Mandatory integration math-v1 tests and review are appended by trusted finalization policy before admitting the child, not added to a running DAG.", "Controlled math-v1 permits only dependency-free pure arithmetic Add/Double/Score in three scoped files. Conservative enclosing-hunk three-way merge rejects overlaps rather than silently choosing patches.", "Protected tests run against the exact combined content digest. A separate read-only reviewer call adds a check, not proof of independent security review.", "Every role and correction version shares a pinned monetary allowance, 96 turns and 800000 token ceiling; task/attempt limits also apply. Default maximum two correction rounds.", "SSE polls durable sequence pages of at most 32 events; 64 client limit, 1s write deadline, 5s heartbeat and 30s reconnect rotation. UI deduplicates workflow/sequence and retains at most 100 events; retention gaps are explicit.", "Unauthenticated dashboard/control surface binds only to local loopback and rejects rebinding hosts/cross-origin requests. No public hosting or GitHub publication tools."], "sources": ["internal/agent/plans.go", "internal/agent/roles.go", "internal/postgres/collaboration.go", "internal/postgres/collaboration_recovery.go", "internal/postgres/collaboration_budget.go", "internal/sandbox/math.go", "internal/httpapi/stream.go", "web", "migrations/00007_collaboration.sql", "scripts/stage07.py"], "validation_commands": dev.COMMANDS, "evidence_index": "artifacts/verification/stage-07/results.json", "evidence_retention": "generated ignored latest bounded sanitized logs; no secrets or full model prompts", "git": {"revision": revision, "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=dev.ROOT)), "scope": "."}, "fingerprint": {"algorithm": "sha256", "value": dev.fingerprint(), "excludes": [".forgeswarm", "artifacts/verification", ".cache", "bin", "node_modules", "dist", "__pycache__"]}, "limitations": ["Scripted TEST/DEMO verification only; live OpenAI capability and comparative performance remain unverified.", "Controlled local pure-function math fixtures only; arbitrary Git patches/repositories and remote ingestion remain unsupported.", "Docker shares the host kernel; confinement is not complete hostile-code isolation. Runtime RPC remains local authenticated plaintext.", "Reviewer rejection creates a new version; overlap or combined-test failure ends needs-human. No automatic GitHub publication.", "Monetary amounts are dated configured estimates; missing usage retains reservations and remains explicitly unknown/estimated."], "reference_docs": ["https://developers.openai.com/api/docs/guides/structured-outputs", "https://react.dev/reference/react-dom/components/common", "https://vite.dev/guide/", "https://playwright.dev/docs/test-webserver", "https://pkg.go.dev/net/http#ResponseController", "https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events"], "external_verification": {"paid_provider": "unverified; no paid calls", "performance": "not_measured", "cloud": "not_requested"}, "next_stage": 8 if status == "verified_local" else 7})
    path.write_text(json.dumps(document, indent=2) + "\n")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / "results.json").write_text(json.dumps({"status": status, "criteria": results, "failure": str(error) if error else None, "commands": dev.COMMANDS}, indent=2) + "\n")


def verify():
    if not __debug__:
        raise RuntimeError("verification requires assertions")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    for path in EVIDENCE.glob("[0-9][0-9][0-9]-*.log"):
        if path.is_file() and not path.is_symlink():
            path.unlink()
    dev.EVIDENCE, dev.RECORD = EVIDENCE, True
    dev.COMMANDS.clear(); results = {}
    progress("in_progress", results)
    try:
        dev.run([sys.executable, "scripts/dev.py", "verify-stage-06"], timeout=1500, label="stage06-prerequisites-and-regressions")
        results[CRITERIA[0]] = "passed"
        stage06.python_checks()
        frontend()
        results[CRITERIA[1]] = results[CRITERIA[6]] = results[CRITERIA[7]] = "passed"
        database_tests(); results[CRITERIA[2]] = "passed"
        image = stage06.prepare_runtime()
        real_tests(image)
        results[CRITERIA[3]] = results[CRITERIA[4]] = results[CRITERIA[5]] = "passed"
        browser_smoke(image); results[CRITERIA[8]] = "passed"
        dev.run(["git", "diff", "--check"], label="diff-check")
    except (RuntimeError, AssertionError, OSError, ValueError) as error:
        progress("implemented_unverified", results, error)
        raise
    progress("verified_local", results)
    print("stage 07 verified_local; scripted multi-role infrastructure; no live AI capability/performance claim", flush=True)


def command(name):
    if name == "verify-stage-07":
        verify()
    elif name == "collaboration-fixture":
        print(json.dumps(fixture(), indent=2))
    elif name == "collaboration-test":
        database_tests(); stage05.prepare(); real_tests(stage06.prepare_runtime())
    elif name == "dashboard-test":
        frontend(); browser_smoke(stage06.prepare_runtime())

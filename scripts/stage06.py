"""Stage 06: deterministic infrastructure verification; never calls a paid model."""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import uuid

import dev
import stage03
import stage05

EVIDENCE = dev.ROOT / "artifacts/verification/stage-06"
PYTHON = dev.ROOT / ".cache/agent-venv" / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
ENV = dev.ENV | {"UV_CACHE_DIR": str(dev.ROOT / ".cache/uv"),
                 "UV_PROJECT_ENVIRONMENT": str(dev.ROOT / ".cache/agent-venv"),
                 "MYPY_CACHE_DIR": str(dev.ROOT / ".cache/mypy"),
                 "RUFF_CACHE_DIR": str(dev.ROOT / ".cache/ruff"),
                 "PYTHONDONTWRITEBYTECODE": "1"}
BASE = "python:3.12.10-slim-bookworm@sha256:fd95fa221297a88e1cf49c55ec1828edd7c5a428187e67b5d1805692d11588db"
CRITERIA = ["stage05_and_stage04_prerequisites", "pinned_python_lint_type_pytest",
            "go_python_contract_and_proto_reproducibility", "strict_tools_injection_and_bounds",
            "durable_reservations_usage_and_fences", "registered_jobs_and_legacy_hashes",
            "real_protected_fixture_patch", "failing_candidate_never_succeeds",
            "runtime_and_worker_crash_restart", "prior_go_race_build_and_sandbox_regressions"]


def setup():
    _, version = dev.run(["uv", "--version"], label="uv-version")
    if not version.startswith("uv 0.13.0 "):
        raise RuntimeError("verification requires pinned uv 0.13.0")
    dev.run(["uv", "--system-certs", "sync", "--project", "agent-runtime", "--locked", "--python", "3.12.10"], env=ENV, label="python-locked-environment")


def proto(check=True):
    target = dev.ROOT / ".cache/agent-proto" if check else dev.ROOT / "agent-runtime/generated"
    target.mkdir(parents=True, exist_ok=True)
    dev.run([PYTHON, "-m", "grpc_tools.protoc", "-Iapi/agent/v1", "--python_out=" + str(target),
             "--pyi_out=" + str(target), "--grpc_python_out=" + str(target), "api/agent/v1/agent.proto"], env=ENV, label="python-protobuf")
    path = target / "agent_pb2_grpc.py"
    # grpcio-tools emits an absolute sibling import; make it package-relative in
    # both generation/check paths. No handwritten protocol implementation.
    path.write_bytes(path.read_bytes().replace(b"import agent_pb2 as agent__pb2", b"from . import agent_pb2 as agent__pb2"))
    if check:
        for name in ["agent_pb2.py", "agent_pb2.pyi", "agent_pb2_grpc.py"]:
            if (target / name).read_bytes() != (dev.ROOT / "agent-runtime/generated" / name).read_bytes():
                raise RuntimeError("Python protobuf differs: " + name)


def python_checks():
    setup()
    proto()
    for args, label in [(["ruff", "format", "--check", "agent-runtime"], "python-format"),
                        (["ruff", "check", "agent-runtime"], "python-lint"),
                        (["mypy", "--config-file", "agent-runtime/pyproject.toml", "agent-runtime/forge_agent"], "python-type"),
                        (["pytest", "-q", "-o", "cache_dir=" + str(dev.ROOT / ".cache/pytest"), "agent-runtime/tests"], "python-pytest")]:
        dev.run([PYTHON, "-m", *args], env=ENV, label=label)


def prepare_runtime():
    # Fetch hash-verified Linux wheels with the host's trusted certificate store.
    # Docker build is offline and receives only explicitly copied trusted files.
    dev.run(["uv", "export", "--project", "agent-runtime", "--locked", "--no-dev", "--no-emit-project",
             "--output-file", "agent-runtime/requirements.lock"], env=ENV, quiet=True, label="runtime-lock-export")
    wheels = dev.ROOT / ".cache/agent-wheels"
    wheels.mkdir(parents=True, exist_ok=True)
    dev.run([sys.executable, "-m", "pip", "download", "--dest", wheels, "--platform", "manylinux2014_x86_64",
             "--python-version", "3.12", "--implementation", "cp", "--abi", "cp312", "--only-binary=:all:",
             "--require-hashes", "-r", "agent-runtime/requirements.lock"], timeout=240, label="runtime-hashed-wheels")
    folder = dev.ROOT / ".cache/agent-image-context"
    # A fresh unique directory avoids stale files without any recursive delete.
    folder = folder / uuid.uuid4().hex
    folder.mkdir(parents=True)
    for name in ["forge_agent", "generated"]:
        (folder / name).mkdir()
        for source in (dev.ROOT / "agent-runtime" / name).glob("*.py"):
            shutil.copyfile(source, folder / name / source.name)
    for name in ["Dockerfile", "requirements.lock"]:
        shutil.copyfile(dev.ROOT / "agent-runtime" / name, folder / name)
    shutil.copyfile(dev.ROOT / "config/agent-pricing.json", folder / "pricing.json")
    (folder / "wheels").mkdir()
    for source in wheels.glob("*.whl"):
        shutil.copyfile(source, folder / "wheels" / source.name)
    manifest = hashlib.sha256()
    for source in sorted(folder.rglob("*")):
        if source.is_file():
            manifest.update(source.relative_to(folder).as_posix().encode() + b"\0" + source.read_bytes())
    tag = "forgeswarm-agent-runtime:" + manifest.hexdigest()
    code, _ = dev.run(["docker", "image", "inspect", BASE], check=False, quiet=True, label="python-base")
    if code:
        dev.run(["docker", "pull", BASE], timeout=240, label="python-base-prepare")
    dev.run(["docker", "build", "--platform", "linux/amd64", "--network", "none", "--pull=false", "-t", tag, folder], timeout=240, label="runtime-image-build")
    _, output = dev.run(["docker", "image", "inspect", tag], quiet=True, label="runtime-image-id")
    return json.loads(output)[0]["Id"]


@contextlib.contextmanager
def runtime(image):
    token, resource = secrets.token_urlsafe(36), uuid.uuid4().hex
    name = "forgeswarm-agent-test-" + resource
    env = ENV | {"AGENT_RUNTIME_TOKEN": token}
    try:
        blank_proxies = [value for key in ["HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy"] for value in ["-e", key + "="]]
        dev.run(["docker", "run", "-d", "--name", name, "--label", "forgeswarm.verification=" + resource,
                 "--platform", "linux/amd64", "--pull=never", "--read-only", "--user", "10002:10002",
                 "--cap-drop=ALL", "--security-opt=no-new-privileges=true", "--memory=256m", "--memory-swap=256m",
                 "--cpus=1", "--pids-limit=64", "--log-driver=none", "--tmpfs=/tmp:rw,noexec,nosuid,nodev,size=16777216",
                 "-p", "127.0.0.1::9091", "-e", "AGENT_RUNTIME_TOKEN", "-e", "AGENT_INSECURE_LOCAL=true",
                 "-e", "AGENT_LISTEN=0.0.0.0:9091", "-e", "AGENT_PROVIDER=scripted_TEST_DEMO", "-e", "AGENT_TEST_DEMO=true", *blank_proxies, image], env=env, label="test-runtime")
        _, inspection = dev.run(["docker", "inspect", name], quiet=True, label="runtime-confinement")
        actual = json.loads(inspection)[0]
        assert actual["HostConfig"]["ReadonlyRootfs"] and not actual["HostConfig"]["Binds"]
        assert actual["Config"]["User"] == "10002:10002" and actual["HostConfig"]["CapDrop"] == ["ALL"]
        assert not any(x.startswith(("DATABASE_URL=", "NATS_", "WORKER_TOKEN=", "OPENAI_API_KEY=")) for x in actual["Config"]["Env"])
        _, port = dev.run(["docker", "port", name, "9091/tcp"], quiet=True, label="runtime-port")
        address = "127.0.0.1:" + port.strip().rsplit(":", 1)[1]
        yield env | {"TEST_AGENT_RUNTIME_ADDR": address, "TEST_AGENT_RUNTIME_TOKEN": token,
                     "TEST_AGENT_RUNTIME_CONTAINER": name, "TEST_AGENT_RUNTIME_OWNER": resource}
    finally:
        code, output = dev.run(["docker", "inspect", name], quiet=True, check=False, label="runtime-owned")
        if code == 0 and json.loads(output)[0]["Config"]["Labels"].get("forgeswarm.verification") == resource:
            dev.run(["docker", "rm", "-f", name], label="runtime-cleanup")


def database_tests():
    with stage03.infrastructure() as (env, db, broker, healthy):
        dev.race(["-tags=integration,stage03,stage04,stage05,stage06", "-count=1", "-timeout=180s", "-v", "./tests/integration", "-run", "TestAgent"], env=env, container=db, label="agent-db-race")


def real_tests(image):
    with runtime(image) as settings:
        with stage03.infrastructure() as (env, db, broker, healthy):
            if os.name == "nt":
                _, port = dev.run(["docker", "port", broker, "4222/tcp"], quiet=True, label="agent-broker-port")
                env = env | {"TEST_NATS_URL": "nats://127.0.0.1:" + port.strip().rsplit(":", 1)[1]}
            env = env | {k: v for k, v in settings.items() if k.startswith("TEST_AGENT_")}
            stage05.native_tests(["-tags=integration,stage03,stage04,stage05,stage06,real_sandbox", "./tests/integration", "-run", "TestAgent"], env, "agent-real-runtime-worker")


def progress(status, results, error=None):
    path = dev.ROOT / ".forgeswarm/progress.json"
    document = json.loads(path.read_text())
    revision = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=dev.ROOT, text=True).strip()
    document["stages"].setdefault("06", {}).update({
        "stage_number": 6, "title": "A bounded coding agent with real tool enforcement", "status": status,
        "updated_at": dev.timestamp(), "implemented_capabilities": ["strict versioned controlled jobs/catalog", "authenticated bounded Python next-action RPC",
            "OpenAI Responses adapter and explicit TEST/DEMO scripted provider", "Go policy, conditional edits, deterministic retrieval, finite loop",
            "durable budget reservations, digests and exact-candidate protected sandbox evaluation"],
        "acceptance_criteria": {key: results.get(key, "unverified") for key in CRITERIA},
        "blockers": [str(error)] if error else [],
        "decisions": ["Agent source is a bounded Go-owned in-memory file map; only sum.go is writable for controlled sum-v1. No host checkout commands.",
            "Protected function evaluator/cases are compiled into the immutable helper, outside writable source. Repository tests and model prose cannot grant success.",
            "Provider calls reserve budgets before RPC. Unknown/failed usage retains conservative reservation. Repair and bounded 429 retry calls count separately.",
            "Interrupted attempts restart from pinned base; no arbitrary edit/provider replay. Tool identity conflicts deny effects.",
            "Python runtime uses a digest-pinned, read-only nonroot container without repository mounts, daemon socket or control-plane credentials.",
            "Initial runtime deployment is local authenticated RPC with loopback host publication; remote runtime TLS deployment is not implemented.",
            "Migration 00006 is additive; normalization v1-v5 remains frozen. Upgrade services together after migration."],
        "sources": ["internal/agent", "internal/postgres/agent.go", "internal/worker/agent.go", "internal/sandbox/protected.go", "agent-runtime", "api/agent/v1", "migrations/00006_agent.sql", "scripts/stage06.py", "tests/integration/agent_test.go"],
        "validation_commands": dev.COMMANDS, "evidence_index": "artifacts/verification/stage-06/results.json",
        "evidence_retention": "latest logs replaced; sanitized output bounded to 128 KiB; generated evidence ignored",
        "git": {"revision": revision, "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=dev.ROOT)), "scope": "."},
        "fingerprint": {"algorithm": "sha256", "value": dev.fingerprint(), "excludes": [".forgeswarm", "artifacts/verification", ".cache", "bin", "__pycache__"]},
        "external_verification": {"live_provider": "unverified; no paid call or spending authorization", "model_capability": "not_measured; scripted fixture proves infrastructure only", "cloud": "not_requested"},
        "reference_docs": ["https://developers.openai.com/api/docs/guides/structured-outputs", "https://developers.openai.com/api/docs/models/gpt-5.4-mini", "https://grpc.io/docs/languages/python/basics/", "https://pydantic.dev/docs/validation/latest/concepts/strict_mode/"],
        "next_stage": 7 if status == "verified_local" else 6,
    })
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
    dev.COMMANDS.clear()
    results = {}
    progress("in_progress", results)
    try:
        dev.run([sys.executable, "scripts/dev.py", "verify-stage-05"], timeout=1200, label="stage05-regressions")
        results[CRITERIA[0]] = results[CRITERIA[-1]] = "passed"
        python_checks()
        results[CRITERIA[1]] = results[CRITERIA[2]] = results[CRITERIA[3]] = "passed"
        database_tests()
        results[CRITERIA[4]] = results[CRITERIA[5]] = "passed"
        image = prepare_runtime()
        real_tests(image)
        results[CRITERIA[6]] = results[CRITERIA[7]] = results[CRITERIA[8]] = "passed"
        dev.run(["git", "diff", "--check"], label="diff-check")
    except (RuntimeError, AssertionError, OSError, ValueError) as error:
        progress("implemented_unverified", results, error)
        raise
    progress("verified_local", results)
    print("stage 06 verified_local; scripted infrastructure only; live model execution unverified", flush=True)


def command(name):
    if name == "verify-stage-06":
        verify()
    elif name == "agent-test":
        python_checks()
        database_tests()
        stage05.prepare()
        real_tests(prepare_runtime())
    elif name == "agent-demo-fixture":
        fixture()
    elif name == "agent-runtime-prepare":
        setup()
        print(prepare_runtime())


def fixture():
    folder = dev.ROOT / ".cache/agent-demo-repository"
    marker = folder / ".forgeswarm-owned"
    if folder.exists() and (folder.is_symlink() or not marker.is_file() or marker.read_text() != "stage06-demo-v1"):
        raise RuntimeError("refusing a non-owned demo repository")
    folder.mkdir(parents=True, exist_ok=True)
    marker.write_text("stage06-demo-v1")
    git = ["git", "-C", folder, "-c", "core.hooksPath=" + str(dev.ROOT / ".cache/no-hooks"), "-c", "core.fsmonitor=false", "-c", "core.autocrlf=false"]
    env = dev.ENV | {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0"}
    if not (folder / ".git").exists():
        dev.run([*git, "init"], env=env, label="agent-fixture-init")
    source = dev.ROOT / "tests/fixtures/agent-go"
    for name in ["go.mod", "sum.go", "README.txt"]:
        (folder / name).write_bytes((source / name).read_bytes())
    dev.run([*git, "add", "--", "go.mod", "sum.go", "README.txt"], env=env, label="agent-fixture-index")
    dev.run([*git, "-c", "user.name=ForgeSwarm Fixture", "-c", "user.email=fixture@localhost", "commit", "--allow-empty", "-m", "Controlled buggy Add fixture"], env=env, label="agent-fixture-commit")
    _, rev = dev.run([*git, "rev-parse", "HEAD"], env=env, quiet=True, label="agent-fixture-revision")
    _, registered = dev.run(["go", "run", "./cmd/agent-register", "-repository", folder, "-revision", rev.strip()], quiet=True, label="agent-fixture-registration")
    data = json.loads(registered)
    value = {"schema_version": 1, "repository_id": data["repository_id"], "base_revision": data["base_revision"],
             "test_profile": "sum-v1", "task_description": "Fix Add to return the sum of both integers.",
             "policy": "controlled-go-v1", "budget": "small-v1"}
    print(json.dumps(value, indent=2), flush=True)
    return value

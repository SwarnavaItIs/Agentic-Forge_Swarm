"""Mandatory real Linux Docker gate; unavailable controls never become a skip."""
import hashlib
import json
import os
import subprocess
import sys
import dev
import stage03

EVIDENCE = dev.ROOT / "artifacts/verification/stage-05"
HELPER = dev.ROOT / "bin/sandbox-helper-linux"
CRITERIA = ["stage04_failover_and_coordination_regressions", "format_vet_unit_race_build_proto",
            "trusted_registration_frozen_hashes_and_additive_migration", "real_build_test_compile_failure_and_nonzero_exit",
            "kernel_cpu_memory_pid_and_storage_enforcement", "network_metadata_and_control_plane_denied",
            "workspace_archive_symlink_output_and_artifact_bounds", "timeout_descendant_teardown",
            "real_worker_cancellation_lease_loss_and_fresh_attempt", "transactional_current_attempt_artifacts_and_read_only_ids",
            "worker_process_crash_restart_scoped_cleanup", "visible_retryable_cleanup_failure"]


def prepare():
    # Explicit preparation may fetch this one pinned trusted image; runtime
    # execution itself uses --pull=never, GOPROXY=off and no network.
    code, _ = dev.run(["docker", "image", "inspect", dev.GO_IMAGE], check=False, quiet=True, label="prepared-base")
    if code:
        dev.run(["docker", "pull", dev.GO_IMAGE], timeout=240, label="prepare-pinned-toolchain")
    HELPER.parent.mkdir(exist_ok=True)
    dev.run(["go", "build", "-trimpath", "-buildvcs=false", "-o", HELPER, "./cmd/sandbox-helper"],
            env=dev.ENV | {"GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"}, label="sandbox-helper-build")
    dev.run(["go", "run", "./cmd/sandbox-prepare", "-helper", HELPER], timeout=150, label="sandbox-image-prepare")
    digest = hashlib.sha256(HELPER.read_bytes()).hexdigest()
    _, text = dev.run(["docker", "image", "inspect", "forgeswarm-execution:" + digest], quiet=True, label="prepared-image-id")
    image = json.loads(text)[0]["Id"]
    _, text = dev.run(["docker", "info", "--format", "{{json .}}"], quiet=True, label="runtime-inspection")
    actual = json.loads(text)
    runtime = {k: actual[k] for k in ["ServerVersion", "OSType", "KernelVersion", "CgroupVersion", "SecurityOptions",
                                     "MemoryLimit", "SwapLimit", "CpuCfsQuota", "PidsLimit"]}
    runtime.update({"execution_image": image, "base_image": dev.GO_IMAGE, "helper_sha256": digest,
                    "isolation_mode": "rootless_linux" if "name=rootless" in actual["SecurityOptions"] else "rootful_linux",
                    "hard_storage": "256 MiB workspace + 64 MiB temporary + 1 MiB shm tmpfs ceilings; read-only image; daemon logs disabled",
                    "apparmor": "daemon default retained where supported; no profile disabled"})
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / "runtime.json").write_text(json.dumps(runtime, indent=2) + "\n")
    return runtime


def fixture():
    """An explicitly owned local demo repository, never an HTTP host path."""
    folder = dev.ROOT / ".cache/sandbox-demo-repository"
    marker = folder / ".forgeswarm-owned"
    if folder.exists() and (folder.is_symlink() or not marker.is_file() or marker.read_text() != "stage05-demo-v1"):
        raise RuntimeError("refusing to replace a non-owned demo repository")
    folder.mkdir(parents=True, exist_ok=True)
    marker.write_text("stage05-demo-v1")
    env = dev.ENV | {"GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": os.devnull, "GIT_TERMINAL_PROMPT": "0"}
    git = ["git", "-C", folder, "-c", "core.hooksPath=" + str(dev.ROOT / ".cache/no-hooks"), "-c", "core.fsmonitor=false", "-c", "core.autocrlf=false"]
    if not (folder / ".git").exists():
        dev.run([*git, "init"], env=env, label="fixture-init")
    source = dev.ROOT / "tests/fixtures/sandbox-go"
    for name in ["go.mod", "sum.go", "sum_test.go"]:
        (folder / name).write_bytes((source / name).read_bytes())
    dev.run([*git, "add", "--", "go.mod", "sum.go", "sum_test.go"], env=env, label="fixture-index")
    dev.run([*git, "-c", "user.name=ForgeSwarm Fixture", "-c", "user.email=fixture@localhost", "commit", "--allow-empty", "-m", "Vetted dependency-free Go fixture"], env=env, label="fixture-commit")
    _, revision = dev.run([*git, "rev-parse", "HEAD"], env=env, quiet=True, label="fixture-revision")
    _, fixture_id = dev.run(["go", "run", "./cmd/sandbox-fixture", "-repository", folder, "-revision", revision.strip()], quiet=True, label="fixture-registration")
    value = {"name": "sandbox-fixture", "tasks": [{"key": "test", "kind": "sandbox_go", "sandbox": {"schema_version": 1, "fixture_id": fixture_id.strip(), "command": "test"}}]}
    print(json.dumps(value, indent=2), flush=True)
    return value


def native_tests(args, env=None, label="sandbox-docker"):
    # Windows' installed C compiler is 32-bit. Baseline unit and DB race tests
    # use the pinned Linux toolchain; real Docker orchestration uses the native
    # CLI here. Linux CI also races the actual Docker/process tests.
    race = [] if os.name == "nt" else ["-race"]
    settings = (env or dev.ENV) | {"TEST_SANDBOX_HELPER": str(HELPER)}
    dev.run(["go", "test", *race, "-count=1", "-timeout=300s", "-v", *args], env=settings, timeout=360, label=label)


def real_workers(pattern="TestSandbox"):
    with stage03.infrastructure() as (env, db, broker, healthy):
        # stage03.race uses broker-container networking on Windows. These tests
        # run on the native execution gateway instead, using the published port.
        if os.name == "nt":
            _, port = dev.run(["docker", "port", broker, "4222/tcp"], quiet=True, label="native-broker-port")
            env = env | {"TEST_NATS_URL": "nats://127.0.0.1:" + port.strip().rsplit(":", 1)[1]}
        native_tests(["-tags=integration,stage03,stage04,stage05,real_sandbox", "./tests/integration", "-run", pattern], env, "sandbox-real-workers")


def progress(status, results, failure=None):
    path = dev.ROOT / ".forgeswarm/progress.json"
    doc = json.loads(path.read_text())
    if doc.get("schema_version") != 1:
        raise RuntimeError("unsupported progress schema")
    revision = subprocess.run(["git", "rev-parse", "HEAD"], cwd=dev.ROOT, capture_output=True, text=True)
    dirty = subprocess.run(["git", "status", "--porcelain"], cwd=dev.ROOT, capture_output=True, text=True)
    runtime_path = EVIDENCE / "runtime.json"
    runtime = json.loads(runtime_path.read_text()) if runtime_path.exists() else {"status": "unverified"}
    doc.setdefault("stages", {}).setdefault("05", {}).update({
        "stage_number": 5, "title": "Constrained Docker execution and artifacts", "status": status, "updated_at": dev.timestamp(),
        "implemented_capabilities": ["v5 sandbox_go fixture input; immutable additive migration 00005", "trusted local Git-object registration without checkout/filter/hooks/submodules/LFS",
            "offline immutable prepared image selected by SHA-256 ID", "per-attempt tmpfs hard ceilings and Linux cgroup v2 limits",
            "Go-owned authorization, renewal, stop/removal and scoped restart reconciliation", "transactional bounded artifacts in local PostgreSQL byte store; ID-only retrieval"],
        "acceptance_criteria": {key: results.get(key, "unverified") for key in CRITERIA},
        "blockers": [dev.sanitize(str(failure))] if failure else [], "runtime": runtime,
        "decisions": [
            "Initial scope is vetted, dependency-free Go 1.26.3 fixtures, at most 128 files / 256 KiB / 64 KiB per file. Fixture registration is a trusted local CLI; HTTP accepts only registered IDs and build/test enums.",
            "Git cat-file/ls-tree read commit objects. No host checkout, hooks, filters, LFS, submodules, lazy object fetch or repository-controlled host command. Archive input rejects links/devices/traversal/duplicates.",
            "Explicit preparation uses the digest-pinned Go base and fixed trusted offline harness/cache. The worker resolves helper-hash-labelled image to an immutable SHA-256 ID and executes with --pull=never. Missing modules fail offline.",
            "The immutable helper/harness live on the read-only image. Before repository execution, the helper verifies actual UID/capabilities/seccomp, cgroup limits, loopback-only networking, read-only root and exact tmpfs capacities. Each attempt copies trusted cache into its own tmpfs, charged to the same memory/storage ceilings. Go build/test temporaries execute inside the assigned workspace.",
            "Nonroot UID 10001; all capabilities dropped; no-new-privileges; default seccomp and available AppArmor retained. No network, host mounts/namespaces/socket/home/SSH/cloud credentials. Docker proxy defaults explicitly blanked; child environment is fixed.",
            "Storage is enforced by size-limited tmpfs, not periodic size checks: workspace 256 MiB, /tmp 64 MiB, shm 1 MiB. Root is read-only and Docker daemon logging is disabled. Trusted image/helper storage is fixed preparation overhead. Memory and swap limits are equal.",
            "RunCommand validates the full server-derived image/argv/env/workspace/policy contract. Stable request UUID is derived from attempt ID. PostgreSQL authorization is checked before create and before start; ambiguous renewal failure cancels execution.",
            "Stop/removal uses a fresh bounded cleanup context, verifies every installation/workflow/task/attempt/instance/generation label and confirms daemon absence. New worker registration precedes reconciliation, which is partitioned by authenticated worker ID and skips active local attempts.",
            "An immutable PID 1 watchdog bounds the whole helper to 63s before input and command timeout + 2s after input, including preparation/collection. It remains effective if the gateway dies. Confined Linux file opens are nonblocking so a FIFO replacement cannot stall collection.",
            "Durable cleanup_required is set before resources can start. Reconciliation handles crash-before-create and remove-before-ack gaps through a bounded DB cleanup page. Runtime failures retain uncertainty; the gateway cannot publish results without confirmed cleanup.",
            "Startup reconciliation must succeed before polling. Failed cleanup suspends new sandbox work until a successful retry; a monotonically counted failure prevents a concurrent older scan from reopening admission.",
            "Artifacts use a small local PostgreSQL byte store (8 KiB each, at most four). Bytes, SHA-256, size, media type, base revision and attempt/generation FKs commit with the fenced accepted outcome, avoiding filesystem/DB dual writes. No speculative cloud adapter.",
            "Stdout/stderr retain bounded prefixes (4 KiB each); discarded bytes are drained and truncation explicit. Patch reads use os.Root plus regular-file/handle identity checks; symlink/path races cannot escape confinement. Test logs are untrusted diagnostics, not authoritative acceptance tests.",
            "Normalization v1-v4 encodings remain unchanged; nonempty sandbox fields conflict with old replay hashes. Existing diagnostics remain available with sandbox execution disabled by default.",
            "Real Docker testing caught read-only-root docker-cp rejection, a non-executable Go temporary directory and Go 1.26 test build-failure output on stdout. The prepared image, workspace temporary directory and classification checks cover these findings. The initial Stage-04 baseline passed. Added columns exposed earlier snapshot tests depending on a planner join order: their volatile gate also blocked cancellation writers. Gates now target the reader connection explicitly, preserving every snapshot assertion; unknown-version checks advance from v5 to v6."],
        "limitations": ["Constrained Docker shares the execution host kernel; it is not complete hostile-code isolation. Only vetted fixtures are authorized; no arbitrary repositories or dependency preparation scripts.",
            "Actual mode is recorded; this environment uses rootful Docker Desktop's Linux VM with default seccomp. No privileges/sysctls were changed; rootless remains preferred where configured.",
            "Host/daemon failure can delay physical teardown despite a DB fence. Cleanup uncertainty stays visible and restart/periodic reconciliation retries; stale results/artifacts remain denied.",
            "A paused/crashed gateway cannot observe lease loss immediately. The helper lifetime ceiling and restart reconciliation bound residual execution; DB expiry still rejects stale outputs. Teardown cannot be guaranteed while the execution host/daemon is unavailable.",
            "Storage retention follows accepted workflow records; no deletion API or cloud object store yet. Windows real orchestration tests run natively without race instrumentation; unit/DB races run Linux, and Linux CI races real Docker/process tests.",
            "No paid providers, model calls, cloud provisioning, hostile-code isolation claim or throughput benchmark."],
        "sources": ["internal/sandbox", "internal/worker/sandbox.go", "internal/worker/worker.go", "internal/postgres/sandbox.go", "migrations/00005_sandbox.sql",
            "api/worker/v1/worker.proto", "internal/httpapi/api.go", "cmd/sandbox-fixture", "cmd/sandbox-prepare", "cmd/sandbox-helper", "tests/integration/sandbox_test.go",
            "tests/integration/sandbox_process_test.go", "scripts/stage05.py", ".github/workflows/stage-05.yaml"],
        "validation_commands": dev.COMMANDS, "evidence_index": "artifacts/verification/stage-05/results.json",
        "evidence_retention": "latest run replaced; sanitized logs bounded to 128 KiB; generated evidence ignored by Git",
        "git": {"revision": revision.stdout.strip(), "dirty": bool(dirty.stdout), "scope": "."},
        "fingerprint": {"algorithm": "sha256", "value": dev.fingerprint(), "excludes": [".forgeswarm", "artifacts/verification", ".cache", "bin", "__pycache__"]},
        "external_verification": {"live_models": "not_applicable", "cloud": "not_requested", "performance": "not_measured", "linux_ci": "configured; remote execution not claimed"},
        "reference_docs": ["https://docs.docker.com/engine/storage/tmpfs/", "https://docs.docker.com/engine/containers/resource_constraints/", "https://docs.docker.com/engine/security/seccomp/",
            "https://pkg.go.dev/os#Root", "https://docs.kernel.org/admin-guide/cgroup-v2.html", "https://pkg.go.dev/syscall#Statfs", "https://git-scm.com/docs/git-ls-tree", "https://git-scm.com/docs/git-cat-file"], "next_stage": 6 if status == "verified_local" else 5})
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(doc, indent=2) + "\n")
    temporary.replace(path)
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / "results.json").write_text(json.dumps({"status": status, "failure": dev.sanitize(str(failure)) if failure else None,
                                                   "criteria": results, "commands": dev.COMMANDS}, indent=2) + "\n")


def verify():
    if not __debug__:
        raise RuntimeError("verification requires assertions")
    if not EVIDENCE.resolve().is_relative_to(dev.ROOT.resolve()):
        raise RuntimeError("evidence outside workspace")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    for path in [*EVIDENCE.glob("[0-9][0-9][0-9]-*.log"), EVIDENCE / "results.json", EVIDENCE / "runtime.json"]:
        if path.is_file() and not path.is_symlink():
            path.unlink()
    dev.EVIDENCE = EVIDENCE
    dev.RECORD = True
    dev.COMMANDS.clear()
    results = {}
    progress("in_progress", results)
    try:
        # Prior process failover remains mandatory after shared worker/store edits.
        dev.run([sys.executable, "scripts/dev.py", "verify-stage-04"], timeout=1200, label="stage04-regressions")
        results[CRITERIA[0]] = results[CRITERIA[1]] = "passed"
        dev.run([sys.executable, "-m", "py_compile", "scripts/stage05.py"], label="python-stage05")
        prepare()
        native_tests(["-tags=sandbox", "./internal/sandbox"])
        for index in [3, 4, 5, 6, 7, 11]:
            results[CRITERIA[index]] = "passed"
        with stage03.infrastructure() as (env, db, broker, healthy):
            dev.race(["-tags=integration,stage03,stage04,stage05", "-count=1", "-timeout=180s", "-v", "./tests/integration", "-run", "TestSandbox"], env=env, container=db, label="sandbox-db-race")
        results[CRITERIA[9]] = "passed"
        fixture()
        results[CRITERIA[2]] = "passed"
        real_workers()
        results[CRITERIA[8]] = results[CRITERIA[10]] = "passed"
        dev.run(["git", "diff", "--check"], label="diff-check")
        if any(results.get(key) != "passed" for key in CRITERIA):
            raise RuntimeError("mandatory stage-05 criterion unverified")
    except (RuntimeError, AssertionError, OSError, ValueError) as error:
        progress("implemented_unverified", results, error)
        raise
    progress("verified_local", results)
    print("stage 05 verified_local; see progress and stage-05 evidence", flush=True)


def command(name):
    if name == "verify-stage-05":
        verify()
    elif name == "sandbox-prepare":
        prepare()
    elif name == "sandbox-fixture":
        fixture()
    elif name == "sandbox-test":
        prepare()
        native_tests(["-tags=sandbox", "./internal/sandbox"])
        real_workers()
    elif name == "sandbox-demo":
        fixture()
        prepare()
        real_workers("TestSandboxProductionWorkerAcceptedArtifactsAndLostReply")

"""Final local gate. No cloud provisioning, paid calls, or real GitHub writes.

Demo order: health -> parallel diamond -> owned worker crash/recovery -> real
protected candidate -> durable exact approval -> controlled fake draft. Live
publication is the separate guarded Stage 08 command after operator approval.
"""
import json
import hashlib
import os
import subprocess
import sys

import dev

EVIDENCE = dev.ROOT / "artifacts/verification/stage-10"
CRITERIA = ["all_prior_stage_gates", "authenticated_https_complete_compose",
            "deterministic_demo_and_browser", "faults_and_bounded_soak",
            "migrations_backup_restore", "performance_matrix_and_worker_comparison",
            "query_plan_before_after", "security_scans", "private_aws_config"]


def record(status, criteria, error=None):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    path = dev.ROOT / ".forgeswarm/progress.json"
    document = json.loads(path.read_text())
    stage = document["stages"].setdefault("10", {})
    stage.update({"stage_number": 10, "title": "Reproducible performance, deployment readiness and final hardening",
                  "status": status, "updated_at": dev.timestamp(),
                  "acceptance_criteria": {key: criteria.get(key, "unverified") for key in CRITERIA},
                  "blockers": [str(error)] if error else [],
                  "implemented_capabilities": ["TLS authenticated private Compose profile", "bounded k6 diagnostic workload matrix", "aggregate DB query/pool tracing without SQL text", "patched Go toolchain and reachable dependencies", "disposable EXPLAIN ANALYZE optimization"],
                  "sources": ["scripts/stage10.py", "scripts/local_platform.py", "scripts/performance.py", "scripts/database_checks.py", "config/local/compose.json", "performance/scenarios.json", "internal/telemetry/dbtrace", "migrations/00010_observation_window.sql"],
                  "validation_commands": list(dev.COMMANDS),
                  "fingerprint": {"algorithm": "sha256", "value": dev.fingerprint(), "excludes": [".forgeswarm", "artifacts", ".cache", "bin", "node_modules", "dist"]},
                  "git": {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=dev.ROOT, text=True).strip(), "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=dev.ROOT))},
                  "external_verification": {"paid_model": "unverified; no calls", "real_github_demo": "unverified; controlled fake only", "aws_deployed": False},
                  "limitations": ["Single owner; Docker socket grants trusted workers host authority; shared kernel is not hostile code isolation.", "Local diagnostic samples do not establish production capacity or model quality.", "No paid model run, actual demo PR, or AWS provisioning is authorized/configured."],
                  "evidence_index": "artifacts/verification/stage-10/results.json",
                  "evidence_retention": "Ignored sanitized logs bounded to 128 KiB per command; latest gate and at most three owned benchmark runs; private credentials/backups remain under ignored .cache.",
                  "next_step": "final explanation/interview prompt after all local gates pass"})
    path.write_text(json.dumps(document, indent=2)+"\n")
    (EVIDENCE / "results.json").write_text(json.dumps({"status": status, "criteria": criteria, "error": str(error) if error else None, "commands": dev.COMMANDS}, indent=2)+"\n")


def verify(resume=False):
    if not __debug__:
        raise RuntimeError("verification requires Python assertions")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    dev.EVIDENCE, dev.RECORD = EVIDENCE, True
    dev.COMMANDS.clear()
    results = {}
    source = dev.fingerprint()
    checkpoint_path = EVIDENCE/"prior-gate-checkpoints.json"
    checkpoints = {"schema_version": 1, "source_sha256": source, "gates": {}}
    if resume and checkpoint_path.exists():
        previous = json.loads(checkpoint_path.read_text())
        if previous.get("schema_version") != 1 or previous.get("source_sha256") != source:
            raise RuntimeError("Source changed since checkpoints; run verify-all for fresh verification")
        checkpoints = previous
    def digest(path):
        return hashlib.sha256(path.read_bytes()).hexdigest()
    record("in_progress", results)
    try:
        # Stage 09 includes 04..08; Stage 04 shares lower-level tests, but 01..03
        # acceptance scripts also contain distinct restart and transport checks.
        for number in [1, 2, 3, 9]:
            key = str(number)
            saved = checkpoints["gates"].get(key)
            if saved:
                for item in saved["evidence"]:
                    path = (dev.ROOT/item["path"]).resolve()
                    if not path.is_relative_to((dev.ROOT/"artifacts/verification").resolve()) or path.is_symlink() or not path.is_file() or digest(path) != item["sha256"]:
                        raise RuntimeError("Checkpoint evidence changed; run verify-all")
                if json.loads((dev.ROOT/f"artifacts/verification/stage-{number:02d}/results.json").read_text())["status"] != "verified_local":
                    raise RuntimeError("Prior gate no longer verified; run verify-all")
                print(f"Reusing source/evidence-verified Stage {number:02d} checkpoint from {saved['finished_at']}", flush=True)
                dev.COMMANDS.append(saved["command_record"])
            else:
                dev.run([sys.executable, "scripts/dev.py", f"verify-stage-{number:02d}"], timeout=7200, label=f"stage-{number:02d}-regressions")
                if dev.fingerprint() != source:
                    raise RuntimeError("Source changed during verification; rerun on frozen inputs")
                command_record = dev.COMMANDS[-1]
                paths = [dev.ROOT/command_record["evidence_path"]]+sorted(p for p in (dev.ROOT/f"artifacts/verification/stage-{number:02d}").rglob("*") if p.is_file())
                if len(paths) > 5000 or any(p.is_symlink() for p in paths):
                    raise RuntimeError("Unexpected checkpoint evidence scope")
                checkpoints["gates"][key] = {"finished_at": dev.timestamp(), "command_record": command_record, "evidence": [{"path": p.relative_to(dev.ROOT).as_posix(), "sha256": digest(p)} for p in paths]}
                checkpoint_path.write_text(json.dumps(checkpoints, indent=2)+"\n")
        results[CRITERIA[0]] = "passed"
        import observability
        observability.owned()
        dev.run(["docker", "compose", "-f", observability.COMPOSE, "down"], timeout=60, label="stop-owned-prior-telemetry-keep-volumes")
        import database_checks
        assert database_checks.query_plan()["goal_met"]
        results["query_plan_before_after"] = "passed"
        dev.run([sys.executable, "-m", "unittest", "discover", "-s", "scripts", "-p", "test_stage10.py"], label="final-policy-unit-tests")
        import stage10_security
        stage10_security.run()
        results["security_scans"] = "passed"
        import aws_local
        aws_local.validate()
        results["private_aws_config"] = "passed_offline_only"
        import local_platform
        import stage06
        import stage10_checks
        import performance
        runtime_image = stage06.prepare_runtime()
        assert stage06.prepare_runtime() == runtime_image, "identical runtime preparation invalidated an active image ID"
        with local_platform.isolated() as session:
            session.up(2)
            results["authenticated_https_complete_compose"] = "passed"
            stage10_checks.demo(session)
            stage10_checks.browser(session)
            results["deterministic_demo_and_browser"] = "passed"
            stage10_checks.faults(session)
            stage10_checks.soak(session)
            results["faults_and_bounded_soak"] = "passed"
            stage10_checks.restore(session)
            results["migrations_backup_restore"] = "passed"
            performance.run(session)
            results["performance_matrix_and_worker_comparison"] = "passed"
        dev.run(["git", "diff", "--check"], label="final-diff-check")
        if dev.fingerprint() != source:
            raise RuntimeError("Source changed during final checks; rerun on frozen inputs")
    except Exception as error:
        record("implemented_unverified", results, error)
        raise
    record("verified_local", results)
    print("Stage 10 verified locally; paid providers, actual demo PR and AWS deployment remain unverified.", flush=True)


def command(name):
    if name in {"verify-stage-10", "verify-all", "verify-resume"}:
        verify(resume=name == "verify-resume")
    elif name == "demo-live":
        import live_demo
        live_demo.run()
    elif name == "deployment-validate":
        import aws_local
        aws_local.validate()
    elif name == "deployment-preview":
        print((dev.ROOT / "deploy/aws/operations.json").read_text())
    elif name in {"demo", "platform-up", "platform-down", "performance"}:
        import local_platform
        if name == "platform-down":
            pointer = local_platform.PRIVATE/"local.json"
            if not pointer.exists():
                print("No owned persistent local platform is registered.")
                return
            session = local_platform.Session(local_platform.PRIVATE/json.loads(pointer.read_text())["owner"])
            session.stop()
            return
        session = local_platform.create(stable=True)
        session.up(2 if name == "demo" else 1)
        if name == "demo":
            import stage10_checks
            stage10_checks.demo(session)
        elif name == "performance":
            import performance
            performance.run(session)
        ports = session.document["ports"]
        print(json.dumps({"app": session.base+"/", "operator_credentials_file": str(session.assets/"operator-auth.json"), "private_ca": str(session.assets/"tls/ca.pem"), "grafana": f"http://127.0.0.1:{ports['GRAFANA_PORT']}/d/forgeswarm-stage09", "traces": f"http://127.0.0.1:{ports['JAEGER_PORT']}/", "persistent_owner": session.owner}, indent=2))
        session.close_client()

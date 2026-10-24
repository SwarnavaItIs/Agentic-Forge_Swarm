"""Real local telemetry and clearly labelled controlled evaluation."""
import json
import os
import re
import subprocess
import sys
import uuid

import dev
import eval_report
import observability
import stage05

EVIDENCE = dev.ROOT / "artifacts/verification/stage-09"
CRITERIA = ["stage08_and_prior_regressions", "sanitized_async_rpc_context_and_retry_links",
            "durable_metrics_success_retry_cancel_loss_and_bounded_cardinality",
            "collector_outage_does_not_stop_workflows", "actual_failure_diagnosis_in_local_backend",
            "eight_task_catalog_and_protected_sandbox_authority", "anti_tamper_wrong_base_timeout_omission",
            "equal_all_role_budgets_and_persistent_spending_caps", "raw_denominators_aggregation_uncertainty_repeatability",
            "format_vet_race_build_python_frontend_and_additive_migration"]


def record(status, criteria, error=None):
    path = dev.ROOT / ".forgeswarm/progress.json"
    document = json.loads(path.read_text())
    assert document["schema_version"] == 1
    stage = document["stages"].setdefault("09", {})
    stage.update({
        "stage_number": 9, "title": "Observability and controlled agent evaluation",
        "status": status, "updated_at": dev.timestamp(),
        "implemented_capabilities": [
            "bounded local OpenTelemetry HTTP/gRPC/broker/attempt/model/tool/sandbox instrumentation",
            "durable traceparent-only handoff and separate linked attempt traces",
            "short read-only operational SQL and 24h duration projections",
            "pinned local Collector/Prometheus/Grafana/Jaeger; no vendor export",
            "eight frozen Go tasks: four development/four holdout covering all six required task areas",
            "immutable helper tests, exact candidate/base proof and anti-tamper policy",
            "four role configurations sharing total task budgets and authority",
            "guarded real adapter and restart-persistent total/per-run spending ledger",
            "raw JSONL/CSV manifests, independent aggregation, uncertainty and HTML report"],
        "acceptance_criteria": {key: criteria.get(key, "unverified") for key in CRITERIA},
        "blockers": [dev.sanitize(str(error))] if error else [],
        "decisions": [
            "Telemetry is optional and local-only. Correlation metadata never authorizes execution.",
            "256-span nonblocking queues, 500ms export deadlines, 25% default sampling and 256 metric series/instrument. Local verification explicitly samples 100%.",
            "No baggage/tracestate, prompts, source, credentials, URLs or raw error text. IDs are for logs/spans only.",
            "Durable rolling-24h state/duration gauges differ from process-local counters/histograms. Failed/cancelled outcomes count; empty populations are omitted.",
            "Additive migration 00009 adds correlation fields; prior migrations and normalization hashes remain unchanged.",
            "The controlled evaluation profile uses the Go gateway and Docker executor separately from production math-v1 permissions.",
            "Frozen tests remain in the immutable helper, outside model context/writable trees. Exact test events plus package success are required.",
            "Every planning/development/review/repair call shares the original per-task budget. Full workflow permits one bounded correction.",
            "Scripted oracle results validate plumbing only. Holdout membership and prompts are frozen before live reporting.",
            "Unknown usage/cost remains null. Uncertain paid calls retain conservative reservations. No paid evaluation is authorized.",
            "Intended denominators come from a prewritten manifest; missing records are failures with integrity errors."],
        "prerequisite_audit": {
            "initial_gate": "Go checks/build/race and DB recovery passed; old recovery smoke hit a post-outage HTTP timeout.",
            "rerun": "Unchanged isolated worker-loss/restart/cancellation acceptance and real issue-to-approved-controlled-draft pipeline passed before changes.",
            "evidence": ["artifacts/verification/stage-09/prerequisite-recovery", "artifacts/verification/stage-09/prerequisite-pipeline"]},
        "review_findings": [
            "A syntactically valid body-less counter method reproduced a validator panic. Validation now rejects it as mutex_protocol and has a regression test.",
            "Passing evaluation spans were incorrectly marked ERROR. Success status is corrected; exact outcome counters/histograms and broker first/redelivery denominators are regression-tested.",
            "Two concurrent logical runtime clients reproduced one RESOURCE_EXHAUSTED rejection in 200 valid calls. Bounded RPC admission now allows four requests while the provider semaphore remains two; actual gRPC regressions verify repeat calls, provider concurrency and fifth-request rejection.",
            "RPC failure journals retain only a bounded gRPC status code; private error descriptions are excluded and regression-tested.",
            "The single-developer phase originally stopped at eight of its declared twelve calls. It now can consume the entire shared total budget; a twelve-call regression covers the baseline.",
            "A Unicode fixture exposed invalid trusted test-source quoting. All protected Go test sources are now parsed in a catalog regression; scripted reruns use the corrected frozen catalog.",
            "Initial HTTP and coordinator deadline failures were not reproduced in isolated unchanged checks; no assertions or execution deadlines were relaxed.",
            "Jaeger v2.22 service discovery uses /api/v3/services; actual metric names were checked against Prometheus before dashboard verification."],
        "sources": ["internal/telemetry", "internal/postgres/telemetry.go", "internal/agent/telemetry.go",
                    "agent-runtime/forge_agent/telemetry.py", "migrations/00009_observability.sql",
                    "internal/evalcatalog", "internal/evaluation", "internal/sandbox/evaluation.go",
                    "cmd/evaluate", "config/observability", "scripts/eval_report.py", "scripts/stage09_demo.py"],
        "theory_to_code": {
            "async_correlation_vs_authorization": ["internal/telemetry/telemetry.go", "internal/telemetry/rpctrace"],
            "bounded_observability": ["internal/telemetry/setup", "internal/postgres/telemetry.go"],
            "leakage_and_test_authority": ["internal/evalcatalog", "internal/sandbox/evaluation.go"],
            "fair_budgets_and_denominators": ["internal/evaluation/engine.go", "scripts/eval_report.py"],
            "uncertain_cost": ["internal/evaluation/spending.go"]},
        "validation_commands": dev.COMMANDS,
        "fingerprint": {"algorithm": "sha256", "value": dev.fingerprint(), "excludes": [".forgeswarm", "artifacts/verification", ".cache", "bin", "node_modules", "dist"]},
        "git": {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=dev.ROOT, text=True).strip(),
                "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=dev.ROOT))},
        "evidence_index": "artifacts/verification/stage-09/results.json",
        "evidence_retention": "Latest bounded sanitized numbered logs; at most three owned fixture folders; credentials stay ignored under .cache.",
        "external_verification": {"paid_eval": "unverified; no calls", "real_agent_quality": "unmeasured", "unequal_budget": "unmeasured", "cloud": "not_requested"},
        "limitations": [
            "Eight tiny dependency-free tasks and four holdouts; finite tests do not establish general correctness.",
            "Scripted results are plumbing evidence only; every real-provider role comparison remains unmeasured.",
            "Sampling/queues can drop traces. Jaeger is transient and capped at 2000 traces; process death may leave incomplete traces.",
            "DB observation times out after 400ms; failure emits database_up=0 and omits projected values.",
            "Docker shares the host kernel; vetted controlled fixtures only, not complete hostile-code isolation.",
            "Paid crash reservations and ledger locks require explicit manual reconciliation. Dated estimates are not billing.",
            "Small-sample intervals are exploratory; repeated trials share tasks. No quality or performance advantage is claimed."],
        "reference_docs": ["https://opentelemetry.io/docs/languages/go/exporters/",
                           "https://opentelemetry.io/docs/languages/python/propagation/",
                           "https://prometheus.io/docs/prometheus/latest/configuration/configuration/",
                           "https://grafana.com/docs/grafana/latest/administration/provisioning/",
                           "https://www.jaegertracing.io/docs/2.22/getting-started/",
                           "https://developers.openai.com/api/docs/guides/structured-outputs"],
        "next_stage": 10 if status == "verified_local" else 9})
    path.write_text(json.dumps(document, indent=2)+"\n")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE/"results.json").write_text(json.dumps({"status": status, "criteria": criteria, "failure": dev.sanitize(str(error)) if error else None, "commands": dev.COMMANDS}, indent=2)+"\n")


def retain_evaluations():
    folders = sorted(EVIDENCE.glob("eval-fixture-*"), key=lambda p: p.stat().st_mtime, reverse=True)
    for folder in folders[2:]:
        if folder.is_symlink() or folder.resolve().parent != EVIDENCE.resolve() or not (folder/".owned").is_file():
            continue
        if (folder/".owned").read_text() != "forgeswarm-evaluation-fixture-v1":
            continue
        allowed = {"manifest.json", "runs.jsonl", "runs.csv", "summary.json", "index.html", ".owned"}
        for path in folder.iterdir():
            if path.is_file() and not path.is_symlink() and (path.name in allowed or re.fullmatch(r"[0-9a-f]{64}\.proof\.json", path.name)):
                path.unlink()
        if not any(folder.iterdir()):
            folder.rmdir()


def fixture(task=None):
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    retain_evaluations()
    folder = EVIDENCE/("eval-fixture-"+uuid.uuid4().hex)
    args = ["go", "run", "./cmd/evaluate", "-mode", "fixture", "-out", folder, "-helper", stage05.HELPER]
    if task:
        args += ["-task", task]
    dev.run(args, timeout=1200, label="protected-scripted-evaluation")
    (folder/".owned").write_text("forgeswarm-evaluation-fixture-v1")
    summary = eval_report.report(folder)
    assert not summary["integrity_errors"] and summary["intended_runs"] == summary["observed_runs"]
    assert all(v["successes"] == v["intended_runs"] for v in summary["configurations"].values()), "scripted oracle failed protected tests"
    if not task:
        (EVIDENCE/"latest-evaluation.json").write_text(json.dumps({"folder": folder.relative_to(dev.ROOT).as_posix(), "summary": summary}, indent=2)+"\n")
    return folder


def verify():
    if not __debug__:
        raise RuntimeError("assertions required")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    for path in EVIDENCE.glob("[0-9][0-9][0-9]-*.log"):
        if path.is_file() and not path.is_symlink():
            path.unlink()
    dev.EVIDENCE, dev.RECORD = EVIDENCE, True
    dev.COMMANDS.clear()
    criteria = {}
    record("in_progress", criteria)
    try:
        dev.run([sys.executable, "scripts/dev.py", "verify-stage-08"], timeout=3000, label="stage08-and-prior-regressions")
        for index in [0, 1, 2, 9]:
            criteria[CRITERIA[index]] = "passed"
        dev.run([sys.executable, "-m", "unittest", "discover", "-s", "scripts", "-p", "test_eval_report.py"], label="independent-aggregation")
        criteria[CRITERIA[6]] = criteria[CRITERIA[7]] = "passed"
        fixture()
        first, second = fixture("add"), fixture("add")
        def normalized(folder):
            records = [json.loads(v) for v in (folder/"runs.jsonl").read_text().splitlines()]
            for value in records:
                for key in ["started_at", "ended_at", "duration_seconds", "trace_id"]:
                    value.pop(key)
            return sorted(records, key=lambda v: v["run_id"])
        assert normalized(first) == normalized(second), "scripted protected replay differs"
        criteria[CRITERIA[5]] = criteria[CRITERIA[8]] = "passed"
        observability.up()
        import stage09_demo
        stage09_demo.run()
        criteria[CRITERIA[3]] = criteria[CRITERIA[4]] = "passed"
        dev.run([sys.executable, "-m", "py_compile", "scripts/stage09.py", "scripts/stage09_demo.py", "scripts/observability.py", "scripts/eval_report.py"], label="python-stage09")
        dev.run(["git", "diff", "--check"], label="diff-check")
    except (RuntimeError, AssertionError, OSError, ValueError) as error:
        record("implemented_unverified", criteria, error)
        raise
    record("verified_local", criteria)
    print("Stage 09 verified_local. Real local telemetry and SCRIPTED plumbing only; no paid evaluation or quality comparison.", flush=True)


def command(name):
    if name == "observability-up":
        observability.up()
    elif name == "eval-fixture":
        stage05.prepare()
        print("Fixture report: "+str(fixture()/"index.html"), flush=True)
    elif name == "eval-real":
        authorization = os.environ.get("EVAL_AUTHORIZATION_FILE")
        if not authorization or os.environ.get("EVAL_CONFIRM") != "existing-session-authorized-spend":
            raise RuntimeError("Paid evaluation disabled. Requires already-authorized total/per-run caps in EVAL_AUTHORIZATION_FILE, runtime credentials/pricing and EVAL_CONFIRM=existing-session-authorized-spend. No paid calls made.")
        folder = EVIDENCE/("eval-live-"+uuid.uuid4().hex)
        dev.run(["go", "run", "./cmd/evaluate", "-mode", "live", "-authorization", authorization, "-confirm-authorized-spending", "-out", folder, "-helper", stage05.HELPER], timeout=1200, label="explicit-authorized-evaluation")
        eval_report.report(folder)
    elif name == "verify-stage-09":
        verify()

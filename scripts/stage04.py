"""Stage-04 acceptance uses only isolated labelled disposable resources."""
import json
import subprocess
import sys
import dev
import proto
import stage03

EVIDENCE=dev.ROOT / "artifacts/verification/stage-04"
CRITERIA=["prior_stage_regressions_and_frozen_hashes","additive_upgrade_and_immutable_budgets","one_recovery_for_two_reapers",
          "persisted_capped_jitter_retry_and_exhaustion","permanent_error_classification","fixed_task_and_workflow_deadlines",
          "cancellation_stop_handshake_and_result_retention","lease_boundary_races_and_stale_tool_result_fences",
          "transactional_workflow_queue_ready_capacity_limits","120_concurrent_admissions_replays_and_drain_paths",
          "abandoned_relay_and_lost_notification_recovery","bounded_capability_wait_and_terminal_inspection",
          "visible_consistency_failures","process_kill_retry_restart_deadline_and_dependency_outage",
          "format_vet_unit_race_build_and_proto_reproducibility"]

def progress(status,failure=None):
    path=dev.ROOT / ".forgeswarm/progress.json"
    document=json.loads(path.read_text())
    if document.get("schema_version")!=1:raise RuntimeError("unsupported progress schema")
    revision=subprocess.run(["git","rev-parse","HEAD"],cwd=dev.ROOT,capture_output=True,text=True)
    dirty=subprocess.run(["git","status","--porcelain"],cwd=dev.ROOT,capture_output=True,text=True)
    document.setdefault("stages",{}).setdefault("04",{}).update({"stage_number":4,"title":"Recovery, retries, cancellation, and backpressure","status":status,"updated_at":dev.timestamp(),
        "implemented_capabilities":["additive migration 00004; normalization v4 with frozen v1-v3 inputs","persisted max attempts, jittered retry due time, fixed overall deadlines and last error",
            "bounded startup/periodic recovery and visible ownership consistency checks","CANCELLING stop acknowledgment or lease-fence settlement with residual cleanup evidence",
            "diagnostic tool authorization and fenced results","transactional admission, READY limit and bounded capability mismatch handling",
            "same-generation resend with distinct transport publication revision","bounded task/attempt inspection; manual redrive deferred"],
        "acceptance_criteria":{c:"passed" if status=="verified_local" else "unverified" for c in CRITERIA},"blockers":[dev.sanitize(str(failure))] if failure else [],
        "decisions":["PostgreSQL clock after ownership locks governs expiry/deadline predicates. Heartbeats never revive leases.",
            "Admission lock -> workflow -> task -> worker -> attempt. Scheduler READY admission uses the same global lock; no DB locks span broker/RPC waits.",
            "Capacity is derived from durable RUNNING attempts, including stop-requested attempts until acknowledged or reaped. No decrement counter can run twice.",
            "All instances sharing a database must use the same operator admission/concurrency configuration. Admission is serialized for correctness; throughput is unmeasured.",
            "Retry time uses capped exponential equal jitter [cap/2,cap]; the sampled DB-time due date is stored once. RETRY_WAIT occupies no worker, long-lived transaction or timer.",
            "Only transient_infrastructure/worker_lost retry. Permissions/configuration/unsupported/diagnostic failure are permanent. Explicit cancellation and deadline never retry.",
            "New request timeout_ms fields (100ms-24h) and task max_attempts (1-10) require normalization v4; zero means omitted/default. Legacy v1-v3 hashes and receipts stay unchanged.",
            "Persisted deadlines are measured from accepted creation, capped per workflow and immutable. Defaults are workflow 10m/task 2m/3 attempts; old rows get one bounded migration grace window.",
            "CANCELLING denies new claims, tools and results. Renew signals stop without extending the lease. Settle on diagnostic stop acknowledgment or expired fenced ownership; retain cleanup_required when physical termination is unproven.",
            "Acknowledging an old LOST attempt only clears its own cleanup flag with an audit event; it cannot change replacement outcomes or grant ownership. Cleanup audit events may advance a terminal workflow revision while its accepted outcome remains immutable.",
            "Outbox task/generation uniqueness stays intact. READY resends reuse the logical generation but increase publish_revision, avoiding broker duplicate-window suppression after message loss.",
            "Publish/fetch failures re-ensure configured JetStream streams and durable consumer under bounded contexts. Real broker tests delete both streams/consumer, then recover through existing client handles and same-generation dispatch.",
            "Cancellation, terminal failure/completion and superseded generations mark unsent outbox rows OBSOLETE in the same transaction. Retained audit rows are excluded from the relay queue, preventing create/cancel cycles from bypassing queue bounds.",
            "FIFO/age ordering and rotating workflow scans are initial policy; locks and per-workflow serialization provide no strict fairness or throughput guarantee.",
            "Malformed notifications use bounded digest-only broker quarantine; exhausted logical tasks keep bounded classified DB attempts/events and read-only diagnostics.",
            "Manual redrive, provider fallback and model/code repair loops are deferred. Future application-loop budgets must remain separate from infrastructure retry budgets.",
            "Stage-03 immediate requeue/cancellation assumptions intentionally superseded by RETRY_WAIT/CANCELLING; earlier creation/cancellation timestamps, payloads and hash contracts retained.",
            "The initial Docker-off failure was environmental; full Stage-03 baseline passed before changes. Stage-04 process tests then exposed a latent Stage-03 worker renewal-cancellation race: normal completion cancelled an in-flight renewal and falsely abandoned ownership. Fixed and covered by a controlled real-RPC renewal-reply regression.",
            "Regression tests caught and repaired no-active cancellation timestamp/payload compatibility during this implementation; the original assertions remain enforced."],
        "limitations":["A DB fence is not proof that an old physical process or external side effect stopped. Stage 05 must enforce sandbox teardown and tool-side fencing.",
            "Single-node file-backed JetStream survives process restart with retained volume, not node/disk loss; notification loss is recoverable from surviving PostgreSQL within configured resend/deadline budgets.",
            "No manual redrive endpoint, provider fallback, model calls, cloud deployment, hostile-code isolation or measured throughput."],
        "sources":["migrations/00004_recovery_policy.sql","internal/execution/policy.go","internal/postgres/recovery.go","internal/postgres/stop.go",
            "internal/postgres/execution.go","internal/postgres/store.go","internal/postgres/inspection.go","internal/config/recovery.go","internal/worker/worker.go",
            "api/worker/v1/worker.proto","internal/httpapi/api.go","tests/integration/recovery_test.go","scripts/stage04.py","scripts/stage03.py"],
        "validation_commands":[c for c in dev.COMMANDS if any(s in c["command"] for s in ["gofmt","py_compile","proto","go vet","go test","go build","compose config","assertions","diff --check"])],
        "evidence_index":"artifacts/verification/stage-04/results.json","process_observations":"artifacts/verification/stage-04/process-results.json",
        "evidence_retention":"latest run replaced; bounded 128 KiB sanitized logs; evidence ignored by Git",
        "git":{"revision":revision.stdout.strip(),"dirty":bool(dirty.stdout),"scope":"."},
        "fingerprint":{"algorithm":"sha256","value":dev.fingerprint(),"excludes":[".forgeswarm","artifacts/verification",".cache","bin","__pycache__"]},
        "external_verification":{"live_models":"not_applicable","cloud":"not_requested","performance":"smoke recovery observations only; no throughput claim"},
        "reference_docs":["https://www.postgresql.org/docs/17/functions-datetime.html","https://www.postgresql.org/docs/17/explicit-locking.html",
            "https://docs.nats.io/learn/jetstream/pull-consumers","https://grpc.io/docs/guides/status-codes/"],"next_stage":5 if status=="verified_local" else 4})
    temporary=path.with_suffix(".tmp");temporary.write_text(json.dumps(document,indent=2)+"\n");temporary.replace(path)
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    (EVIDENCE/"results.json").write_text(json.dumps({"status":status,"failure":dev.sanitize(str(failure)) if failure else None,"commands":dev.COMMANDS},indent=2)+"\n")

def verify():
    if not __debug__:raise RuntimeError("verification requires assertions")
    if not EVIDENCE.resolve().is_relative_to(dev.ROOT.resolve()):raise RuntimeError("evidence outside workspace")
    EVIDENCE.mkdir(parents=True,exist_ok=True)
    for p in [*EVIDENCE.glob("[0-9][0-9][0-9]-*.log"),EVIDENCE/"results.json",EVIDENCE/"process-results.json"]:
        if p.is_file() and not p.is_symlink():p.unlink()
    dev.EVIDENCE=EVIDENCE;dev.RECORD=True;dev.COMMANDS.clear();progress("in_progress")
    try:
        dev.fmt_check();dev.run([sys.executable,"-m","py_compile","scripts/dev.py","scripts/stage02.py","scripts/stage03.py","scripts/stage04.py","scripts/proto.py"],label="python")
        proto.generate(True)
        dev.run(["go","vet","./..."],label="vet");dev.run(["go","test","-count=1","./..."],label="unit");dev.race(["-count=1","./..."])
        dev.build();dev.run(["docker","compose","config","--quiet"],label="compose")
        with stage03.infrastructure() as (env,db,broker,healthy):
            dev.race(["-tags=integration,stage03,stage04","-count=1","-timeout=120s","-v","./tests/integration"],env=env,container=db,label="stage04-integration")
        dev.smoke()
        previous=stage03.EVIDENCE;stage03.EVIDENCE=EVIDENCE
        try:stage03.smoke(recovery=True)
        finally:stage03.EVIDENCE=previous
        dev.run(["git","diff","--check"],label="diff-check")
    except (RuntimeError,AssertionError,OSError,ValueError) as error:progress("implemented_unverified",error);raise
    progress("verified_local");print("stage 04 verified_local; see progress and stage-04 evidence",flush=True)

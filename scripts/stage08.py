"""Stage 08 deterministic local approval/GitHub gate; no actual GitHub writes."""
import json
import os
import subprocess
import sys

import dev
import stage03
import stage05
import stage06
import stage07

EVIDENCE = dev.ROOT / "artifacts/verification/stage-08"
CRITERIA = ["stage07_and_prior_regressions", "authenticated_operator_reader_worker_boundaries",
            "signed_bounded_allowlisted_issue_and_delivery_dedup",
            "durable_exact_approval_restart_expiry_decision_and_cancel_races",
            "serialized_publication_uncertain_reconciliation_drift_and_retry_bounds",
            "real_issue_to_protected_candidate_to_fake_draft",
            "dashboard_login_exact_approval_and_read_only_browser",
            "additive_migration_legacy_hashes_format_vet_race_build"]


def record(status, criteria, error=None):
    path = dev.ROOT / ".forgeswarm/progress.json"
    document = json.loads(path.read_text())
    stage = document["stages"].setdefault("08", {})
    stage.update({"stage_number": 8, "title": "Durable approvals and GitHub integration",
                  "status": status, "updated_at": dev.timestamp(),
                  "acceptance_criteria": {key: criteria.get(key, "unverified") for key in CRITERIA},
                  "blockers": [str(error)] if error else [], "validation_commands": dev.COMMANDS,
                  "evidence_index": "artifacts/verification/stage-08/results.json",
                  "evidence_retention": "Latest bounded sanitized ignored logs replaced on rerun; credentials/prompts never retained.",
                  "fingerprint": {"algorithm": "sha256", "value": dev.fingerprint(),
                                  "excludes": [".forgeswarm", "artifacts/verification", ".cache", "bin", "node_modules", "dist"]},
                  "git": {"revision": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=dev.ROOT, text=True).strip(),
                          "dirty": bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=dev.ROOT))},
                  "external_verification": {"live_github_pr": "unverified; no actual test PR authorized/configured",
                                            "paid_provider": "unverified; no paid calls", "performance": "not_measured"},
                  "reference_docs": ["https://docs.github.com/en/webhooks/using-webhooks/validating-webhook-deliveries",
                                     "https://docs.github.com/en/rest/git/refs", "https://docs.github.com/en/rest/git/trees",
                                     "https://docs.github.com/en/rest/git/commits", "https://docs.github.com/en/rest/pulls/pulls",
                                     "https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api"],
                  "next_stage": 9 if status == "verified_local" else 8})
    path.write_text(json.dumps(document, indent=2) + "\n")
    EVIDENCE.mkdir(parents=True, exist_ok=True)
    (EVIDENCE / "results.json").write_text(json.dumps({"status": status, "criteria": criteria, "failure": str(error) if error else None, "commands": dev.COMMANDS}, indent=2) + "\n")


def contracts(image=None, pattern="TestGitHub"):
    image = image or stage06.prepare_runtime()
    with stage06.runtime(image) as settings:
        with stage03.infrastructure() as (env, db, broker, healthy):
            if os.name == "nt":
                _, port = dev.run(["docker", "port", broker, "4222/tcp"], quiet=True, label="github-native-broker-port")
                env = env | {"TEST_NATS_URL": "nats://127.0.0.1:" + port.strip().rsplit(":", 1)[1]}
            env = env | {k: v for k, v in settings.items() if k.startswith("TEST_AGENT_")}
            stage05.native_tests(["-tags=" + stage07.TAGS + ",real_sandbox", "./tests/integration", "-run", pattern], env, "github-real-services-contracts")


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
    record("in_progress", results)
    try:
        dev.run([sys.executable, "scripts/dev.py", "verify-stage-07"], timeout=1800, label="stage07-and-prior-regressions")
        results[CRITERIA[0]] = results[CRITERIA[1]] = results[CRITERIA[2]] = results[CRITERIA[7]] = "passed"
        contracts()
        results[CRITERIA[3]] = results[CRITERIA[4]] = results[CRITERIA[5]] = "passed"
        browser()
        results[CRITERIA[6]] = "passed"
        dev.run(["git", "diff", "--check"], label="diff-check")
    except (RuntimeError, AssertionError, OSError, ValueError) as error:
        record("implemented_unverified", results, error)
        raise
    record("verified_local", results)
    print("stage 08 verified_local; controlled GitHub HTTP fixture only; no actual GitHub PR or paid model call", flush=True)


def browser():
    # Defined below with the native local-service harness.
    import stage08_browser
    stage08_browser.run()


def command(name):
    if name == "verify-stage-08":
        verify()
    elif name == "github-contract-test":
        EVIDENCE.mkdir(parents=True, exist_ok=True)
        dev.EVIDENCE, dev.RECORD = EVIDENCE, True
        dev.run(["go", "test", "-count=1", "./internal/github", "./internal/httpapi"], label="github-http-auth-contracts")
        stage05.prepare()
        contracts()
    elif name == "github-live-demo":
        approval = os.environ.get("GITHUB_LIVE_APPROVAL_ID", "")
        repo = os.environ.get("GITHUB_LIVE_REPOSITORY_ID", "")
        if not approval or not repo or not os.environ.get("GITHUB_CONFIG_FILE") or os.environ.get("GITHUB_LIVE_CONFIRM") != "publish-approved-draft":
            raise RuntimeError("Live demo requires configured credentials/repository plus GITHUB_LIVE_APPROVAL_ID, GITHUB_LIVE_REPOSITORY_ID and GITHUB_LIVE_CONFIRM=publish-approved-draft. Approval must already be recorded by an authenticated operator.")
        dev.run(["go", "run", "./cmd/publisher", "-once", "-approval", approval, "-repository-id", repo, "-confirm-publish"], timeout=60, label="explicit-approved-live-draft")

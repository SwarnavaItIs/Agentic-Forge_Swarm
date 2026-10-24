"""Independently recompute evaluation denominators from immutable raw records."""
import collections
import hashlib
import html
import json
import math
from pathlib import Path
import random
import statistics


def sha(value):
    return hashlib.sha256(json.dumps(value, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def quantiles(values):
    values = sorted(values)
    if not values:
        return {"samples": 0, "p50": None, "p95": None, "max": None}
    def percentile(q):
        i = (len(values)-1)*q
        lo, hi = math.floor(i), math.ceil(i)
        return values[lo]+(values[hi]-values[lo])*(i-lo)
    return {"samples": len(values), "p50": percentile(.5), "p95": percentile(.95), "max": values[-1]}


def wilson(success, total):
    if not total:
        return [None, None]
    z = 1.96
    p = success/total
    denominator = 1+z*z/total
    middle = (p+z*z/(2*total))/denominator
    half = z*math.sqrt(p*(1-p)/total+z*z/(4*total*total))/denominator
    return [max(0, middle-half), min(1, middle+half)]


def aggregate(manifest, records):
    planned = {item["RunID"]: item for item in manifest["runs"]}
    if len(planned) != len(manifest["runs"]):
        raise ValueError("duplicate planned run")
    observed = {}
    errors = []
    specs = {item["id"]: item for item in manifest["catalog"]}
    groups = collections.defaultdict(list)
    for record in records:
        run_id = record["run_id"]
        if run_id not in planned or run_id in observed:
            raise ValueError("unexpected/duplicate run cannot be silently dropped")
        intended = planned[run_id]
        if record["task_id"] != intended["TaskID"] or record["config_id"] != intended["ConfigID"] or record["trial"] != intended["Trial"]:
            raise ValueError("result identity differs from plan")
        if record["catalog_sha256"] != manifest["catalog_sha256"] or record["base_commit"] != specs[record["task_id"]]["base_commit"] or record["budget"] != manifest["budget"]:
            raise ValueError("base/catalog/budget mismatch")
        if record["model_calls_attempted"] > manifest["budget"]["model_calls"] or record["reserved_tokens"] > manifest["budget"]["reserved_token_ceiling"]:
            raise ValueError("record exceeded shared budget")
        if record["outcome"] == "passed":
            proof = record["protected_test_result"]
            if proof["status"] != "passed" or proof["candidate_sha256"] != record["candidate_sha256"] or proof["base_commit"] != record["base_commit"] or not proof["sha256"] or record["policy_violations"]:
                raise ValueError("unverified success claim")
        observed[run_id] = record
    for run_id, intended in planned.items():
        record = observed.get(run_id)
        if record is None:
            errors.append({"run_id": run_id, "reason": "omitted_result"})
            record = {"run_id": run_id, "task_id": intended["TaskID"], "config_id": intended["ConfigID"], "trial": intended["Trial"], "outcome": "omitted_result", "duration_seconds": None, "estimated_cost_microusd": None, "input_tokens": None, "output_tokens": None, "reviews": 0, "review_rejections": 0, "operator_interventions": 0}
        groups[intended["ConfigID"]].append(record)
    configurations = {}
    for config, rows in sorted(groups.items()):
        successes = sum(r["outcome"] == "passed" for r in rows)
        review_count = sum(r["reviews"] for r in rows)
        by_task = collections.defaultdict(list)
        for row in rows:
            by_task[row["task_id"]].append(row)
        configurations[config] = {
            "successes": successes, "intended_runs": len(rows), "task_trial_pass_rate": successes/len(rows),
            "task_trial_wilson_95_exploratory": wilson(successes, len(rows)),
            "task_success_counts": {key: {"successes": sum(r["outcome"] == "passed" for r in task_rows), "intended_trials": len(task_rows)} for key, task_rows in sorted(by_task.items())},
            "tasks_with_any_success": sum(any(r["outcome"] == "passed" for r in task_rows) for task_rows in by_task.values()),
            "task_denominator": len(by_task),
            "split_success_counts": {split: {"successes": sum(r["outcome"] == "passed" for r in rows if specs[r["task_id"]].get("split", "unknown") == split), "intended_runs": sum(specs[r["task_id"]].get("split", "unknown") == split for r in rows)} for split in sorted({specs[r["task_id"]].get("split", "unknown") for r in rows})},
            "outcomes": dict(collections.Counter(r["outcome"] for r in rows)),
            "wall_seconds_all_observed_outcomes": quantiles([r["duration_seconds"] for r in rows if r["duration_seconds"] is not None]),
            "estimated_cost_microusd": quantiles([r["estimated_cost_microusd"] for r in rows if r["estimated_cost_microusd"] is not None]),
            "unknown_cost_runs": sum(r["estimated_cost_microusd"] is None for r in rows),
            "unknown_usage_runs": sum(r["input_tokens"] is None or r["output_tokens"] is None for r in rows),
            "review_rejections": sum(r["review_rejections"] for r in rows), "review_denominator": review_count,
            "review_rejection_rate": sum(r["review_rejections"] for r in rows)/review_count if review_count else None,
            "operator_intervention_run_rate": sum(r["operator_interventions"] > 0 for r in rows)/len(rows),
            "end_to_end_failure_rate": (len(rows)-successes)/len(rows),
        }
    paired = {}
    baseline = groups.get("developer", [])
    baseline_by = {(r["task_id"], r["trial"]): r for r in baseline}
    for config, rows in groups.items():
        if config == "developer":
            continue
        differences = collections.defaultdict(list)
        for record in rows:
            other = baseline_by.get((record["task_id"], record["trial"]))
            if other:
                differences[record["task_id"]].append(int(record["outcome"] == "passed")-int(other["outcome"] == "passed"))
        task_differences = [statistics.mean(x) for x in differences.values()]
        if task_differences:
            rng = random.Random(4109)
            samples = sorted(statistics.mean(rng.choices(task_differences, k=len(task_differences))) for _ in range(1000))
            paired[config] = {"paired_tasks": len(task_differences), "mean_task_pass_difference": statistics.mean(task_differences), "task_cluster_bootstrap_95_exploratory": [samples[25], samples[974]]}
    live = manifest["mode"] == "live"
    return {"schema_version": 1, "mode": manifest["mode"], "catalog_sha256": manifest["catalog_sha256"], "intended_runs": len(planned), "observed_runs": len(observed), "integrity_errors": errors, "configurations": configurations, "paired_vs_developer": paired,
            "quality_evidence": "real_provider_observations_small_controlled_sample" if live else "SCRIPTED_PLUMBING_ONLY_NOT_AGENT_QUALITY",
            "limitations": ["Small controlled tasks; no general software-engineering quality claim.", "Repeated trials share tasks; Wilson intervals are descriptive and may understate dependence.", "Paired bootstrap uses task clusters; tiny samples/identical outcomes can yield degenerate intervals.", "Failures/timeouts/infrastructure faults/budget exhaustion/omissions remain in denominators.", "Unknown costs are null, never zero. Fixture usage is synthetic and fixture cost is unknown.", "Run-order seed controls scheduling only; exact live-model reproducibility is not claimed."],
            "unmeasured_comparisons": [] if live else ["All real-provider role comparisons", "Paid-model quality/cost distributions", "Unequal-budget comparisons"]}


def report(folder):
    folder = Path(folder)
    manifest = json.loads((folder/"manifest.json").read_text())
    records = [json.loads(line) for line in (folder/"runs.jsonl").read_text().splitlines() if line]
    for row in records:
        proof = row["protected_test_result"]
        digest = proof["sha256"]
        if not digest:
            if row["outcome"] == "passed":
                raise ValueError("successful result omitted protected proof")
            continue
        if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
            raise ValueError("invalid proof identity")
        body = (folder/(digest+".proof.json")).read_bytes()
        if hashlib.sha256(body).hexdigest() != digest:
            raise ValueError("protected proof artifact changed")
        authority = json.loads(body)
        if authority["candidate_sha256"] != proof["candidate_sha256"] or authority["base_revision"] != row["base_commit"] or authority["catalog_sha256"] != manifest["catalog_sha256"]:
            raise ValueError("protected proof artifact pin mismatch")
        if row["outcome"] == "passed":
            spec = next(t for t in manifest["catalog"] if t["id"] == row["task_id"])
            if not authority["success"] or authority["authority"] != "immutable-helper" or sorted(authority["passed_tests"]) != sorted(spec["required_tests"]):
                raise ValueError("fake/skipped/incomplete protected success")
    summary = aggregate(manifest, records)
    (folder/"summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    rows = []
    for name, values in summary["configurations"].items():
        rows.append(f'<tr><td>{html.escape(name)}</td><td>{values["successes"]}/{values["intended_runs"]}</td><td>{values["task_trial_pass_rate"]:.1%}</td><td>{values["unknown_cost_runs"]}</td></tr>')
    page = '<!doctype html><meta charset="utf-8"><title>ForgeSwarm controlled evaluation</title><style>body{font:16px system-ui;max-width:1000px;margin:3rem auto;padding:1rem;background:#101827;color:#e5edf7}table{border-collapse:collapse;width:100%}td,th{padding:1rem;border-bottom:1px solid #536172;text-align:left}strong{color:#f5c56c}pre{white-space:pre-wrap}</style>'
    page += '<h1>Controlled evaluation</h1><p><strong>'+html.escape(summary["quality_evidence"])+'</strong></p>'
    page += f'<p>Intended runs: {summary["intended_runs"]}. Observed: {summary["observed_runs"]}. Missing results: {len(summary["integrity_errors"])}.</p>'
    page += '<table><tr><th>Configuration</th><th>Passed/intended</th><th>Pass rate</th><th>Unknown cost runs</th></tr>'+''.join(rows)+'</table>'
    page += '<p>Machine-readable details and uncertainty: summary.json; raw authority: manifest.json, runs.jsonl and protected proof files.</p><pre>'+html.escape('\n'.join(summary["limitations"]))+'</pre>'
    (folder/"index.html").write_text(page, encoding="utf-8")
    return summary


if __name__ == "__main__":
    import sys
    value = report(sys.argv[1])
    print(json.dumps({"intended": value["intended_runs"], "observed": value["observed_runs"], "integrity_errors": value["integrity_errors"]}))

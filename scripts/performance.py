"""Bounded k6 infrastructure measurements; all failures remain in denominators."""
import collections
import concurrent.futures
import csv
import datetime
import html
import json
import os
from pathlib import Path
import platform as host_platform
import re
import subprocess
import threading
import time
import urllib.request
import urllib.parse
import uuid

import dev
import eval_report
import local_platform

SPEC = json.loads((dev.ROOT/"performance/scenarios.json").read_text())
K6 = "grafana/k6:2.1.0@sha256:65c920dc067d5e2e00befbf982af6ad6ad0117034e8b1c65817c7975c52d4669"
EVIDENCE = dev.ROOT/"artifacts/verification/stage-10"


def bytes_value(text):
    match = re.fullmatch(r"([0-9.]+)\s*([KMGT]?i?B)", text.strip())
    if not match:
        raise ValueError("unexpected Docker memory units")
    powers = {"B": 1, "kB": 1000, "KB": 1000, "MB": 1000**2, "GB": 1000**3, "KiB": 1024, "MiB": 1024**2, "GiB": 1024**3, "TiB": 1024**4}
    return int(float(match[1])*powers[match[2]])


class Observer:
    def __init__(self, session):
        self.session = session
        self.samples = []
        self.stopped = threading.Event()
        self.failure = None
        self.high_cpu_since = None
        if not hasattr(session, "engine_cpus"):
            _, text = dev.run(["docker", "info", "--format", "{{.NCPU}}"], quiet=True, label="resource-cpu-denominator")
            session.engine_cpus = int(text.strip())
        self.cpus = session.engine_cpus
        self.thread = threading.Thread(target=self.observe, daemon=True)

    def observe(self):
        def command(args):
            result = subprocess.run([str(v) for v in args], env=dev.ENV, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=8)
            if result.returncode or len(result.stdout) > 131072:
                raise RuntimeError("bounded resource observation failed")
            return result.stdout
        while not self.stopped.is_set():
            try:
                values = command(["docker", "ps", "-q", "--filter", "label=com.docker.compose.project="+self.session.project])
                identifiers = values.split()
                sandboxes = command(["docker", "ps", "-q", "--filter", "label=forgeswarm.installation="+str(uuid.UUID(self.session.owner)), "--filter", "label=forgeswarm.resource=sandbox-v1"])
                identifiers += sandboxes.split()
                if not identifiers or len(identifiers) > 32:
                    raise RuntimeError("resource scope exceeds configured bounds")
                values = command(["docker", "stats", "--no-stream", "--format", "{{json .}}", *identifiers])
                data = [json.loads(row) for row in values.splitlines()]
                rss = sum(bytes_value(row["MemUsage"].split("/")[0]) for row in data)
                cpu = sum(float(row["CPUPerc"].removesuffix("%")) for row in data)
                code, diagnostic = self.session.request("/v1/diagnostics", reader=True)
                assert code == 200
                sample = {"at": dev.timestamp(), "container_rss_bytes": rss, "container_cpu_percent_one_core_units": cpu, "containers": data, "sandboxes": len(sandboxes.split()), "api": diagnostic}
                self.samples.append(sample)
                if cpu/self.cpus > SPEC["stop_conditions"]["normalized_owned_cpu_percent_10_seconds"]:
                    self.high_cpu_since = self.high_cpu_since or time.monotonic()
                    if time.monotonic()-self.high_cpu_since >= 10:
                        self.failure = "predeclared sustained CPU stop condition"
                        return
                else:
                    self.high_cpu_since = None
                if rss > SPEC["stop_conditions"]["owned_container_rss_bytes"] or sample["sandboxes"] > SPEC["stop_conditions"]["owned_sandboxes"]:
                    self.failure = "predeclared resource stop condition"
                    return
            except (RuntimeError, OSError, AssertionError, ValueError, subprocess.TimeoutExpired) as error:
                self.samples.append({"at": dev.timestamp(), "observation_error": type(error).__name__})
            self.stopped.wait(SPEC["resource_sample_seconds"])

    def start(self):
        self.thread.start()

    def stop(self):
        self.stopped.set()
        self.thread.join(timeout=12)
        if self.thread.is_alive():
            raise RuntimeError("resource observer did not stop")


def k6(session, count, folder):
    folder.mkdir(parents=True)
    # Local Linux k6 shares the owned API network namespace. SSL_CERT_FILE is
    # honored by the Linux Go verifier; no insecureSkipTLSVerify is used.
    namespace = session.container("github-test")
    env = dev.ENV|{"BENCH_TOKEN": session.tokens["operator"]["token"], "BENCH_COUNT": str(count), "BENCH_RUN": uuid.uuid4().hex, "BENCH_URL": "https://127.0.0.1:8443", "BENCH_HOST": "127.0.0.1:"+str(session.document["ports"]["API_PORT"])}
    args = ["docker", "run", "--rm", "--name", "forgeswarm-k6-"+env["BENCH_RUN"], "--label", "forgeswarm.performance="+session.owner, "--platform", "linux/amd64", "--pull=never", "--network", "container:"+namespace, "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges=true", "--memory=192m", "--cpus=0.5", "--pids-limit=64", "--user", "0:0", "--tmpfs=/tmp:rw,nosuid,nodev,size=16777216", "--mount", "type=bind,source="+str(dev.ROOT/"performance")+",target=/scenarios,readonly", "--mount", "type=bind,source="+str(folder)+",target=/results", "--mount", "type=bind,source="+str(session.assets/"tls/ca.pem")+",target=/ca.pem,readonly", "-e", "SSL_CERT_FILE=/ca.pem", "-e", "K6_NO_USAGE_REPORT=true"]
    for key in ["BENCH_TOKEN", "BENCH_COUNT", "BENCH_RUN", "BENCH_URL", "BENCH_HOST"]:
        args += ["-e", key]
    args += [K6, "run", "--no-usage-report", "--log-format=json", "--log-output=file=/results/submissions.log", "--out=json=/results/k6-raw.jsonl", "/scenarios/diagnostic.js"]
    started = time.monotonic()
    exit_code, _ = dev.run(args, env=env, timeout=80, check=False, quiet=True, label="bounded-k6-submissions")
    records = []
    for line in (folder/"submissions.log").read_text().splitlines():
        value = json.loads(line)
        message = value.get("msg", "")
        if message.startswith('{"schema_version":1'):
            records.append(json.loads(message))
    assert len({v["iteration"] for v in records}) == len(records), "duplicate result"
    assert all(0 <= v["iteration"] < count for v in records)
    # A failed/ambiguous response may still have committed. The disposable DB is
    # the independent source of truth; never silently lose that admission.
    statement = "SELECT COALESCE(json_agg(json_build_object('id',id,'key',creation_key)),'[]') FROM workflows WHERE creation_key LIKE 'bench:"+env["BENCH_RUN"]+":%'"
    admitted = json.loads(session.sql(statement))
    accepted = {v["id"] for v in admitted}
    assert {v["workflow_id"] for v in records if v["status"] == 201}.issubset(accepted)
    assert all(len(v["id"]) == 36 for v in admitted)
    return {"intended": count, "exit": exit_code, "records": records, "admitted": admitted, "accepted": sorted(accepted), "generator_seconds": time.monotonic()-started, "run_key": env["BENCH_RUN"]}


def observe_workflows(session, identifiers):
    graphs = {}
    deadline = time.monotonic()+SPEC["drain_max_seconds"]
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        while time.monotonic() < deadline:
            pending = [value for value in identifiers if graphs.get(value, {}).get("status") not in {"SUCCEEDED", "FAILED", "CANCELLED"}]
            if not pending:
                return graphs
            for identifier, value in zip(pending, pool.map(session.graph, pending)):
                graphs[identifier] = value
            time.sleep(.25)
    return graphs


def task_evidence(session, graphs):
    rows, queue = [], []
    for identifier, graph in graphs.items():
        code, page = session.request("/v1/workflows/"+identifier+"/events?limit=100", reader=True)
        assert code == 200
        events = page.get("events", page.get("items", []))
        ready = {}
        for event in events:
            payload = event.get("payload", {})
            if event.get("type", event.get("event_type")) == "task.ready":
                ready[(payload["task_id"], payload["generation"])] = datetime.datetime.fromisoformat(event["occurred_at"].replace("Z", "+00:00"))
        for task in graph["nodes"]:
            code, attempts = session.request(f'/v1/workflows/{identifier}/tasks/{task["id"]}/attempts?limit=10', reader=True)
            assert code == 200
            for attempt in attempts["attempts"]:
                key = (task["id"], attempt["generation"])
                if key in ready:
                    wait = (datetime.datetime.fromisoformat(attempt["started_at"].replace("Z", "+00:00"))-ready[key]).total_seconds()
                    assert wait >= 0
                    queue.append(wait)
                rows.append({"workflow_id": identifier, "task_id": task["id"], "logical_status": task["status"], "attempt": attempt})
    return rows, queue


def exported_histograms(session):
    expression = '{__name__=~"forgeswarm_operation_duration_seconds_(bucket|count|sum)|forgeswarm_database_(query|acquire)_duration_seconds_(bucket|count|sum)"}'
    url = "http://127.0.0.1:"+str(session.document["ports"]["PROMETHEUS_PORT"])+"/api/v1/query?"+urllib.parse.urlencode({"query": expression})
    with urllib.request.urlopen(url, timeout=5) as response:
        value = json.load(response)
    assert value["status"] == "success"
    return value["data"]["result"]


def histogram_deltas(before, after):
    def key(metric):
        return tuple(sorted(metric.items()))
    previous = {key(row["metric"]): float(row["value"][1]) for row in before}
    groups = collections.defaultdict(lambda: {"count": 0, "sum_seconds": 0, "cumulative_buckets": collections.defaultdict(float)})
    for row in after:
        metric = row["metric"]
        name = metric["__name__"]
        kind = "scheduler_scan" if metric.get("operation") == "scheduler.scan" else "database_query" if "database_query" in name else "pool_acquire" if "database_acquire" in name else None
        if kind is None:
            continue
        delta = float(row["value"][1])-previous.get(key(metric), 0)
        if delta < 0:
            raise ValueError("process counters reset inside the measured phase")
        group = groups[kind]
        if name.endswith("_count"):
            group["count"] += delta
        elif name.endswith("_sum"):
            group["sum_seconds"] += delta
        else:
            group["cumulative_buckets"][metric["le"]] += delta
    return {name: dict(value)|{"mean_seconds": value["sum_seconds"]/value["count"] if value["count"] else None, "interpretation": "actual exported process histograms; phase boundaries have up to one scrape/export interval of lag"} for name, value in groups.items()}


def point(session, count, workers, folder):
    session.up(workers)
    warmup = k6(session, 5, folder/"warmup")
    warm = observe_workflows(session, warmup["accepted"])
    assert all(v["status"] == "SUCCEEDED" for v in warm.values()) and len(warm) == 5
    time.sleep(SPEC["warmup_settle_seconds"])
    before_histograms = exported_histograms(session)
    observer = Observer(session)
    observer.start()
    started = time.monotonic()
    try:
        submitted = k6(session, count, folder/"measured")
        graphs = observe_workflows(session, submitted["accepted"])
        duration = time.monotonic()-started
    finally:
        observer.stop()
    rows, queue = task_evidence(session, graphs)
    outcomes = collections.Counter(v["status"] for v in graphs.values())
    statuses = collections.Counter(str(v["status"]) for v in submitted["records"])
    complete = sum(outcomes.get(state, 0) for state in ["SUCCEEDED", "FAILED", "CANCELLED"])
    observed = [value for value in observer.samples if "api" in value]
    assert observed, "resource measurements unavailable; no successful benchmark claim"
    final = session.request("/v1/diagnostics", reader=True)[1]
    after_histograms = exported_histograms(session)
    result = {"schema_version": 1, "scenario_version": SPEC["version"], "intended_submissions": count, "observed_responses": len(submitted["records"]), "omitted_results": count-len(submitted["records"]), "http_statuses": dict(statuses), "database_accepted": len(submitted["accepted"]), "initial_201": statuses.get("201", 0), "rejections_429": statuses.get("429", 0), "workflow_outcomes": dict(outcomes), "api_ms_all_observed_statuses": eval_report.quantiles([v["duration_ms"] for v in submitted["records"]]), "api_p99_ms": percentile([v["duration_ms"] for v in submitted["records"]], .99), "queue_delay_seconds": eval_report.quantiles(queue), "queue_delay_samples": len(queue), "attempts": len(rows), "logical_tasks": sum(len(v["nodes"]) for v in graphs.values()), "attempts_per_second": len(rows)/duration, "logical_tasks_per_second": sum(len(v["nodes"]) for v in graphs.values())/duration, "wall_seconds_including_observation": duration, "workers": workers, "slots_per_worker": 1, "resource_samples": len(observed), "peak_owned_container_rss_bytes": max((v["container_rss_bytes"] for v in observed), default=None), "final_diagnostics": final, "stop_condition": observer.failure, "goals": SPEC["predeclared_goals"], "goal_results": {"admission_p95": percentile([v["duration_ms"] for v in submitted["records"]], .95) <= 250, "admission_p99": percentile([v["duration_ms"] for v in submitted["records"]], .99) <= 1000, "accepted_terminal": complete == len(submitted["accepted"]), "cleanup": final["database"]["cleanup_backlog"] == 0}, "limitations": ["One host, bounded diagnostic diamond, closed-loop submissions; not a production arrival-rate estimate.", "API latency includes 429 and failed responses; missing latencies stay unknown. Throughput wall time includes observer overhead.", "Completed tasks are distinct from attempts. Warmup data is excluded from workload denominators."]}
    result["api_ms_all_observed_statuses"]["p99"] = result["api_p99_ms"]
    result["task_outcomes"] = dict(collections.Counter(task["status"] for value in graphs.values() for task in value["nodes"]))
    result["attempt_outcomes"] = dict(collections.Counter(row["attempt"]["state"] for row in rows))
    result["process_latencies"] = histogram_deltas(before_histograms, after_histograms)
    assert len(queue) == len(rows), "missing generation-bound queue measurements"
    assert all(result["process_latencies"].get(name, {}).get("count", 0) > 0 for name in ["scheduler_scan", "database_query", "pool_acquire"]), "required process timing samples unavailable"
    leases, pending, ages = [], [], []
    for sample in observed:
        points = {(p["Name"], p["Label"]): p["Value"] for p in sample["api"]["database"]["operational"]["Points"]}
        leases.append(points.get(("leases", "active"), 0))
        pending.append(points.get(("outbox", "pending"), 0))
        ages.append(points.get(("outbox_oldest_seconds", "pending"), 0))
    result["sampled_worker_slot_utilization"] = sum(leases)/len(leases)/workers if leases else None
    result["peak_sampled_outbox_pending"] = max(pending, default=None)
    result["peak_sampled_outbox_age_seconds"] = max(ages, default=None)
    result["resource_sampling_note"] = "At least one second between collections; actual timestamps include Docker stats latency. Short bursts may be missed."
    result["outstanding_sandbox_cleanup"] = int(session.sql("SELECT count(*) FROM task_attempts a JOIN sandbox_commands c ON c.attempt_id=a.id WHERE a.cleanup_required"))
    result["database_contention_after_drain"] = json.loads(session.sql("SELECT json_build_object('sessions',(SELECT COALESCE(json_agg(x),'[]') FROM (SELECT state,wait_event_type,count(*) FROM pg_stat_activity WHERE datname=current_database() GROUP BY state,wait_event_type) x),'locks',(SELECT COALESCE(json_agg(x),'[]') FROM (SELECT locktype,granted,count(*) FROM pg_locks WHERE database=(SELECT oid FROM pg_database WHERE datname=current_database()) GROUP BY locktype,granted) x))"))
    result["contention_limitation"] = "Aggregate snapshot after drain; no query text/session identifiers; transient lock waits may be missed. Pool waits and query latency are additionally captured by actual process histograms."
    (folder/"histogram-boundaries.json").write_text(json.dumps({"before": before_histograms, "after": after_histograms}, indent=2)+"\n")
    for name, value in [("result.json", result), ("submissions.json", submitted), ("workflows.json", graphs), ("attempts.json", rows), ("resources.json", observer.samples)]:
        (folder/name).write_text(json.dumps(value, indent=2)+"\n")
    with (folder/"submissions.csv").open("w", newline="") as output:
        writer = csv.DictWriter(output, fieldnames=["iteration", "status", "duration_ms", "workflow_id", "payload_bytes", "started_at_ms", "ended_at_ms", "error_code"])
        writer.writeheader()
        writer.writerows({key: value[key] for key in writer.fieldnames} for value in submitted["records"])
    assert submitted["exit"] == 0 and len(submitted["records"]) == count and observer.failure is None
    # The all-cleanup-zero goal stays unmet if preceding crash checks left
    # honest diagnostic stop uncertainty. Do not erase it to make a chart green.
    assert result["goal_results"]["accepted_terminal"] and result["outstanding_sandbox_cleanup"] == 0
    print("Measured", count, "submitted; workers", workers, "accepted", len(graphs), "429", statuses.get("429", 0), "p95 ms", result["api_ms_all_observed_statuses"]["p95"], flush=True)
    return result


def percentile(values, fraction):
    if not values:
        return float("inf")
    values = sorted(values)
    index = (len(values)-1)*fraction
    low = int(index)
    return values[low]+(values[min(low+1, len(values)-1)]-values[low])*(index-low)


def run(session):
    code, _ = dev.run(["docker", "image", "inspect", K6], check=False, quiet=True, label="pinned-k6-image")
    if code:
        dev.run(["docker", "pull", K6], timeout=180, label="prepare-pinned-k6")
    folder = EVIDENCE/("perf-"+uuid.uuid4().hex)
    folder.mkdir(parents=True)
    (folder/".owned").write_text("forgeswarm-benchmark-v1")
    _, engine = dev.run(["docker", "info", "--format", "{{json .}}"], quiet=True, label="performance-host-facts")
    actual = json.loads(engine)
    session.engine_cpus = actual["NCPU"]
    metadata = {"schema_version": 1, "started_at": dev.timestamp(), "git": dev.run(["git", "rev-parse", "HEAD"], quiet=True)[1].strip(), "dirty": bool(dev.run(["git", "status", "--porcelain"], quiet=True)[1]), "fingerprint": dev.fingerprint(), "host_os": host_platform.platform(), "host_cpu": host_platform.processor(), "host_logical_cpus": os.cpu_count(), "engine": {key: actual[key] for key in ["ServerVersion", "KernelVersion", "OSType", "NCPU", "MemTotal", "CgroupVersion"]}, "images": session.document["images"], "k6_image": K6, "spec": SPEC, "model": "not_applicable_no_model_workload", "repetitions": 1}
    (folder/"manifest.json").write_text(json.dumps(metadata, indent=2)+"\n")
    results = []
    for count, workers in [(10, 1), (50, 1), (100, 1), (500, 1), (50, 2)]:
        results.append(point(session, count, workers, folder/(str(count)+"-workers-"+str(workers))))
    (folder/"summary.json").write_text(json.dumps({"results": results, "finished_at": dev.timestamp(), "not_measured": ["live model throughput", "production capacity", "multi-host performance", "other worker-count/load combinations"]}, indent=2)+"\n")
    rows = ''.join('<tr><td>'+str(v["intended_submissions"])+'</td><td>'+str(v["workers"])+'</td><td>'+str(v["database_accepted"])+'</td><td>'+str(v["rejections_429"])+'</td><td>'+html.escape(str(v["api_ms_all_observed_statuses"]["p95"]))+'</td><td>'+html.escape(json.dumps(v["goal_results"]))+'</td></tr>' for v in results)
    (folder/"index.html").write_text('<!doctype html><meta charset="utf-8"><title>ForgeSwarm infrastructure measurements</title><style>body{font:16px system-ui;max-width:1100px;margin:3rem auto}td,th{padding:.6rem;border-bottom:1px solid #aaa;text-align:left}</style><h1>Measured local infrastructure</h1><p>Diagnostic fixtures only. One trial per point; no production or model-capacity claim. Goals remain goals when unmet.</p><table><tr><th>Submitted</th><th>Workers</th><th>Accepted</th><th>429</th><th>p95 ms (all statuses)</th><th>Goals met</th></tr>'+rows+'</table><p>Raw JSON/CSV, sample counts and environment are retained beside this report.</p>')
    (EVIDENCE/"latest-performance.json").write_text(json.dumps({"folder": folder.relative_to(dev.ROOT).as_posix()}, indent=2)+"\n")
    retain()
    return folder


def retain():
    """Retain three exact owned runs; no recursive shell deletes or symlinks."""
    runs = sorted(EVIDENCE.glob("perf-*"), key=lambda p: p.stat().st_mtime, reverse=True)
    for folder in runs[3:]:
        if folder.is_symlink() or not folder.resolve().is_relative_to(EVIDENCE.resolve()):
            raise RuntimeError("foreign benchmark path")
        marker = folder/".owned"
        if not marker.is_file() or marker.read_text() != "forgeswarm-benchmark-v1":
            continue
        paths = list(folder.rglob("*"))
        if len(paths) > 500 or any(path.is_symlink() or not path.resolve().is_relative_to(folder.resolve()) for path in paths):
            raise RuntimeError("unexpected benchmark retention scope")
        for path in sorted(paths, key=lambda p: len(p.parts), reverse=True):
            if path.is_file():
                path.unlink()
            elif path.is_dir():
                path.rmdir()
        folder.rmdir()

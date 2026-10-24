"""Owned real services: diagnose worker loss, and prove collector outage isolation."""
import json
import os
import subprocess
import urllib.request
import uuid

import dev
import observability
import stage06
import stage07

EVIDENCE = dev.ROOT / "artifacts/verification/stage-09"


def fetch(url):
    with urllib.request.urlopen(url, timeout=3) as response:
        return json.load(response)


def run():
    image = stage06.prepare_runtime()
    control = {}
    with stage06.runtime(image, telemetry=True) as settings:
        with stage07.local_services(settings, control=control, telemetry=True) as (base, job):
            code, receipt, _ = dev.http(base, "/v1/projects", "POST", json.dumps(job).encode(), "telemetry-pipeline-"+uuid.uuid4().hex)
            assert code == 202
            project = {}
            def complete_project():
                nonlocal project
                code, project, _ = dev.http(base, "/v1/projects/"+receipt["id"])
                assert code == 200
                if project["status"] in {"FAILED", "NEEDS_HUMAN"}:
                    diagnostic = {"project": project, "candidates": []}
                    for version in project.get("versions", []):
                        path = "/v1/workflows/"+version["workflow_id"]+"/candidates"
                        diagnostic["candidates"].append(dev.http(base, path)[1])
                        graph_value=dev.http(base,"/v1/workflows/"+version["workflow_id"]+"/graph")[1]
                        for node in graph_value["nodes"]:
                            page=dev.http(base,"/v1/workflows/"+version["workflow_id"]+"/tasks/"+node["id"]+"/attempts")[1]
                            diagnostic.setdefault("attempts",[]).append(page)
                    diagnostic["usage"]=dev.http(base,"/v1/workflows/"+receipt["id"]+"/summary")[1]
                    EVIDENCE.mkdir(parents=True, exist_ok=True)
                    (EVIDENCE/"diagnosis-failed-project.json").write_text(json.dumps(diagnostic,indent=2)[:131072])
                    raise AssertionError(project["reason"])
                return project["status"] == "SUCCEEDED"
            dev.poll(complete_project, "instrumented protected collaboration", timeout=90)
            def graph(identifier):
                code, value, _ = dev.http(base, "/v1/workflows/"+identifier+"/graph")
                assert code == 200
                return value
            body = json.dumps({"name": "controlled telemetry worker loss", "tasks": [{"key": "delay", "kind": "delay", "delay_ms": 25000, "max_attempts": 2}]}).encode()
            code, failure, _ = dev.http(base, "/v1/workflows", "POST", body, "telemetry-loss-"+uuid.uuid4().hex)
            assert code == 201
            dev.poll(lambda: any(v["status"] == "RUNNING" for v in graph(failure["id"])["nodes"]), "selected fault durable claim")
            task = graph(failure["id"])["nodes"][0]["id"]
            attempts_path = f'/v1/workflows/{failure["id"]}/tasks/{task}/attempts'
            attempts = dev.http(base, attempts_path)[1]["attempts"]
            assert len(attempts) == 1 and attempts[0]["state"] == "RUNNING"
            lost = attempts[0]
            # Exact Popen handle created by this context, never a scanned PID.
            worker = control["worker"]
            assert worker.poll() is None
            worker.kill()
            worker.wait(timeout=10)
            replacement = subprocess.Popen(control["args"], cwd=dev.ROOT, env=control["env"],
                                           stdout=control["log"], stderr=subprocess.STDOUT,
                                           creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
            control["processes"].append(replacement)
            dev.poll(lambda: graph(failure["id"])["status"] == "SUCCEEDED", "worker loss replacement outcome", timeout=90)
            attempts = dev.http(base, attempts_path)[1]["attempts"]
            assert len(attempts) == 2 and {v["state"] for v in attempts} == {"LOST", "SUCCEEDED"}
            control["log"].flush()
            # Find the trace by a UUID-only structured worker log, then verify it
            # exists in the actual backend. An abruptly killed trace is partial.
            records = []
            with open(control["log"].name, encoding="utf-8", errors="replace") as source:
                for line in source:
                    try:
                        records.append(json.loads(line))
                    except ValueError:
                        continue
            matches = [r for r in records if r.get("attempt_id") == lost["id"] and r.get("msg") == "diagnostic_started"]
            assert len(matches) == 1 and len(matches[0]["trace_id"]) == 32
            trace_id = matches[0]["trace_id"]
            traces = {}
            def observed_trace():
                nonlocal traces
                traces = fetch("http://127.0.0.1:16686/api/traces/"+trace_id)
                return bool(traces.get("data")) and bool(traces["data"][0].get("spans"))
            dev.poll(observed_trace, "actual worker-loss trace backend", timeout=30)
            span_attempts = [tag.get("value") for trace in traces["data"] for span in trace.get("spans", []) for tag in span.get("tags", []) if tag.get("key") == "attempt.id"]
            assert lost["id"] in span_attempts, "claim span must bind actual lost attempt"
            # Stop only this repository's Collector. State commits, RPC and
            # execution must continue while exports fail under bounded queues.
            observability.collector("stop")
            try:
                code, outage, _ = dev.http(base, "/v1/workflows", "POST", json.dumps({"name": "collector unavailable", "tasks": [{"key": "noop", "kind": "noop"}]}).encode(), "collector-outage-"+uuid.uuid4().hex)
                assert code == 201
                dev.poll(lambda: graph(outage["id"])["status"] == "SUCCEEDED", "workflow commits with Collector unavailable", timeout=40)
            finally:
                observability.collector("start")
            dev.poll(lambda: observability.ready("http://127.0.0.1:13133/"), "Collector restart")
            metrics = ""
            def observed_metrics():
                nonlocal metrics
                with urllib.request.urlopen("http://127.0.0.1:9464/metrics", timeout=3) as response:
                    metrics = response.read(2 << 20).decode()
                return "forgeswarm_state_expired_attempts" in metrics and "forgeswarm_state_retries" in metrics and "forgeswarm_database_up" in metrics
            dev.poll(observed_metrics, "real operational metrics after exporter recovery", timeout=30)
            for forbidden in ["task_id=", "workflow_id=", "attempt_id=", "Bearer ", "OPENAI_API_KEY", "task_description"]:
                assert forbidden not in metrics, "sensitive or unbounded metric label"
            services = fetch("http://127.0.0.1:16686/api/v3/services")["services"]
            assert "forgeswarm-python-runtime" in services, "Python provider spans missing from local backend"
            EVIDENCE.mkdir(parents=True, exist_ok=True)
            proof = {"verified_at": dev.timestamp(), "mode": "actual_local_services_scripted_TEST_DEMO",
                     "pipeline_root": receipt["id"], "pipeline_status": project["status"],
                     "failure": "controlled abrupt worker process loss", "workflow": failure["id"],
                     "lost_attempt": lost["id"], "replacement_attempt": next(v["id"] for v in attempts if v["state"] == "SUCCEEDED"),
                     "trace_id": trace_id, "trace_url": "http://127.0.0.1:16686/trace/"+trace_id,
                     "trace_is_partial_after_process_kill": True, "backend_span_count": sum(len(t.get("spans", [])) for t in traces["data"]),
                     "attempt_binding_present": True, "collector_outage_workflow_status": "SUCCEEDED",
                     "python_provider_spans_observed": True, "metric_labels_bounded": True,
                     "actual_github_write": False, "paid_provider_calls": False}
            (EVIDENCE/"failure-diagnosis.json").write_text(json.dumps(proof, indent=2)+"\n")
            # Metrics contain only fixed labels/values, bounded to a 128KiB tail.
            (EVIDENCE/"metrics.txt").write_text(metrics[-131072:])
            (EVIDENCE/"trace.json").write_text(json.dumps(traces, indent=2)[:131072]+"\n")
            print("Verified actual worker-loss trace/metrics and successful execution during Collector outage.", flush=True)

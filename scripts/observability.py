"""Explicit local-only monitoring stack; no workflow data is sent to vendors."""
import json
import os
from pathlib import Path
import secrets
import urllib.request

import dev

COMPOSE = dev.ROOT / "config/observability/compose.yaml"
PROJECT = "forgeswarm-observability"


def command(*args):
    return ["docker", "compose", "-f", COMPOSE, "-p", PROJECT, *args]


def owned():
    _, values = dev.run(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project="+PROJECT], quiet=True, label="observability-ownership-scan")
    for identifier in values.splitlines():
        _, raw = dev.run(["docker", "inspect", identifier], quiet=True, label="observability-owner")
        labels = json.loads(raw)[0]["Config"]["Labels"]
        configured = labels.get("com.docker.compose.project.config_files", "")
        if str(COMPOSE).replace("\\", "/").lower() != configured.replace("\\", "/").lower():
            raise RuntimeError("refusing another project's monitoring containers")


def ready(url):
    try:
        with urllib.request.urlopen(url, timeout=2) as response:
            return response.status == 200
    except OSError:
        return False


def up():
    private = dev.ROOT / ".cache/observability"
    private.mkdir(parents=True, exist_ok=True)
    password = private / "grafana-password"
    if not password.exists():
        descriptor = os.open(password, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "w") as target:
            target.write(secrets.token_urlsafe(32))
    owned()
    dev.run(command("config", "--quiet"), label="observability-compose-config")
    dev.run(command("up", "-d"), timeout=300, label="observability-start")
    for name, url in {"collector": "http://127.0.0.1:13133/", "jaeger": "http://127.0.0.1:16686/", "prometheus": "http://127.0.0.1:9095/-/ready", "grafana": "http://127.0.0.1:3000/api/health"}.items():
        dev.poll(lambda: ready(url), name+" local readiness", timeout=60)
    print("Local Grafana: http://127.0.0.1:3000/d/forgeswarm-stage09 ; Jaeger: http://127.0.0.1:16686 ; Prometheus: http://127.0.0.1:9095 . Enable TELEMETRY_ENABLED=true in trusted services; OTEL_EXPORTER_OTLP_ENDPOINT=127.0.0.1:4317. Anonymous Grafana is read-only; administrator password stays in .cache/observability/grafana-password.", flush=True)


def collector(action):
    if action not in {"stop", "start"}:
        raise ValueError("invalid collector action")
    owned()
    dev.run(command(action, "collector"), timeout=30, label="controlled-collector-"+action)

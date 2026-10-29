"""Guarded real-provider project admission. Never automatically approves a PR.

The operator must already have explicit session spending/repository authority,
configure a live runtime and the existing fixed GitHub adapter, and supply exact
pricing/repository/total caps. The entire project maximum is retained before an
HTTP request; crashes/ambiguous responses never release that reservation.
Publication uses github-live-demo only after a human's durable exact approval.
"""
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import urllib.error
import urllib.parse
import urllib.request
import uuid

import dev


def validate(authorization, pricing, github, now=None):
    now = now or datetime.datetime.now(datetime.timezone.utc)
    if authorization.get("session_authorized") is not True or not re.fullmatch(r"[0-9a-f]{32}", authorization["id"]):
        raise ValueError("existing explicit session authorization required")
    if datetime.datetime.fromisoformat(authorization["expires_at"].replace("Z", "+00:00")) <= now:
        raise ValueError("spending authorization expired")
    if github["credential_mode"] not in {"installation_token", "development_token"}:
        raise ValueError("real repository credential mode required")
    if github["repository"] != authorization["repository"]:
        raise ValueError("exact numeric repository/owner/name/branch mismatch")
    maximum = pricing["maximum_estimate_microusd"]
    if not 0 < maximum <= authorization["max_project_estimate_microusd"] <= authorization["max_total_estimate_microusd"] <= 10000000:
        raise ValueError("explicit bounded total/project cost caps required")
    return maximum


def run():
    if os.environ.get("LIVE_DEMO_CONFIRM") != "existing-session-authorized-spend" or os.environ.get("LIVE_RUNTIME_PROVIDER") != "openai":
        raise RuntimeError("Live demo disabled. Requires existing explicit spending/repository authorization, configured live OpenAI runtime, LIVE_RUNTIME_PROVIDER=openai and LIVE_DEMO_CONFIRM=existing-session-authorized-spend. No paid call made.")
    def read(name):
        path = Path(os.environ.get(name, ""))
        if not path.is_file() or path.stat().st_size > 65536:
            raise ValueError("missing/bounded private input: "+name)
        return path.read_bytes()
    authorization = json.loads(read("LIVE_DEMO_AUTHORIZATION_FILE"))
    pricing_bytes = read("AGENT_PRICING_FILE")
    pricing = json.loads(pricing_bytes)
    github = json.loads(read("GITHUB_CONFIG_FILE"))
    maximum = validate(authorization, pricing, github)
    if hashlib.sha256(pricing_bytes).hexdigest() != authorization["pricing_sha256"]:
        raise ValueError("pricing changed; fresh authorization required")
    endpoint = urllib.parse.urlsplit(os.environ.get("LIVE_DEMO_URL", ""))
    if endpoint.scheme not in {"http", "https"} or endpoint.hostname not in {"127.0.0.1", "localhost", "::1"} or endpoint.username or endpoint.password or endpoint.path not in {"", "/"} or endpoint.query or endpoint.fragment:
        raise ValueError("owned loopback operator endpoint required; use verified TLS SSM forwarding for private AWS")
    credentials = json.loads(read("OPERATOR_AUTH_FILE"))
    context = ssl.create_default_context(cafile=os.environ.get("LIVE_DEMO_CA_FILE"))
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPSHandler(context=context))
    policy_request = urllib.request.Request(endpoint.geturl().rstrip("/")+"/v1/diagnostics", headers={"Authorization": "Bearer "+credentials["operator"]["token"]})
    with opener.open(policy_request, timeout=5) as response:
        body = response.read(65537)
        if response.status != 200 or len(body) > 65536:
            raise ValueError("server policy unavailable")
        policy = json.loads(body)["database"]["configured_pricing"]
    if not policy or any(policy.get(key) != pricing[key] for key in ["provider", "model", "input_microusd_per_million", "output_microusd_per_million", "maximum_estimate_microusd"]):
        raise ValueError("actual server pricing/project cap differs; no admission or paid call made")
    ledger = dev.ROOT/".cache/live-demo"/authorization["id"]
    ledger.mkdir(parents=True, mode=0o700, exist_ok=True)
    lock = ledger/"reservation.lock"
    # Interrupted reservations remain locked: manual reconciliation is required.
    with lock.open("x") as stream:
        stream.write("uncertain spend must be reconciled before removing this lock\n")
    records = ledger/"reservations.jsonl"
    previous = [json.loads(line) for line in records.read_text().splitlines()] if records.exists() else []
    if sum(row["reserved_estimate_microusd"] for row in previous)+maximum > authorization["max_total_estimate_microusd"]:
        lock.unlink()
        raise ValueError("authorized total allowance exhausted")
    key = "live-demo-"+uuid.uuid4().hex
    reservation = {"at": dev.timestamp(), "idempotency_key": key, "reserved_estimate_microusd": maximum, "repository": authorization["repository"], "pricing_sha256": authorization["pricing_sha256"], "usage": "unknown_retained_maximum", "provider_identity": "operator configuration; actual calls require independent model ledger evidence"}
    with records.open("a") as stream:
        stream.write(json.dumps(reservation)+"\n")
        stream.flush()
        os.fsync(stream.fileno())
    request = urllib.request.Request(endpoint.geturl().rstrip("/")+"/v1/github/import", data=json.dumps({"issue_number": int(os.environ["GITHUB_LIVE_ISSUE_NUMBER"])}).encode(), headers={"Authorization": "Bearer "+credentials["operator"]["token"], "Content-Type": "application/json", "Idempotency-Key": key}, method="POST")
    with opener.open(request, timeout=10) as response:
        body = response.read(65537)
        if response.status not in {201, 202} or len(body) > 65536:
            raise ValueError("bounded admission failed; reservation retained")
        receipt = json.loads(body)
    (ledger/(key+".receipt.json")).write_text(json.dumps(receipt, indent=2)+"\n")
    lock.unlink()
    print(json.dumps({"workflow_id": receipt["id"], "maximum_reserved_estimate_microusd": maximum, "ledger": str(ledger), "next": "Observe actual candidate/protected tests/review; inspect provider ledger. Human approves exact candidate in the dashboard, then run github-live-demo with that approval/repository ID.", "publication_performed": False, "model_quality_claim": False}, indent=2))

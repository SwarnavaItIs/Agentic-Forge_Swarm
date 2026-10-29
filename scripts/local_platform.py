"""Owned local HTTPS platform. Only the trusted executor receives Docker access.

Preparation builds trusted code; execution images stay offline and immutable.
Secrets are file-injected and never printed. Existing development volumes are
not touched. Stable sessions can be restarted; verification uses fresh owners.
"""
import contextlib
import hashlib
import json
import os
from pathlib import Path
import re
import queue
import secrets
import shutil
import socket
import ssl
import subprocess
import threading
import time
import urllib.error
import urllib.request
import urllib.parse
import uuid

import dev
import stage05
import stage06
import stage07

COMPOSE = dev.ROOT / "config/local/compose.json"
PRIVATE = dev.ROOT / ".cache/platform"
EVIDENCE = dev.ROOT / "artifacts/verification/stage-10"
DOCKER_CLI = "docker:29.5.2-cli@sha256:9ba8e32bfc35a2c7ae2feb1e3241b2778ae21dee80f4dcd31d04e1cfdea86ea2"


def fingerprint():
    digest = hashlib.sha256()
    for folder in ["cmd", "internal", "api", "migrations", "agent-runtime/forge_agent", "agent-runtime/generated", "web/src"]:
        for path in sorted((dev.ROOT/folder).rglob("*")):
            if path.is_file() and "__pycache__" not in path.parts:
                digest.update(path.relative_to(dev.ROOT).as_posix().encode()+b"\0"+path.read_bytes())
    for name in ["go.mod", "go.sum", "config/local/Dockerfile", "config/agent-pricing.json", "agent-runtime/requirements.lock", "web/package-lock.json"]:
        digest.update((dev.ROOT/name).read_bytes())
    return digest.hexdigest()


def prepare():
    PRIVATE.mkdir(parents=True, exist_ok=True)
    record = PRIVATE/"prepared.json"
    current = fingerprint()
    if record.exists():
        value = json.loads(record.read_text())
        if value["input_sha256"] == current:
            available = True
            for image in [value["platform_image"], value["runtime_image"], value["execution_image"]]:
                code, _ = dev.run(["docker", "image", "inspect", "--format", "{{.Id}}", image], check=False, quiet=True, label="prepared-role-image")
                available &= code == 0
            if available:
                return value
    dev.build()
    if not (dev.ROOT/"web/node_modules/@playwright/test").is_dir():
        stage07.frontend()
    else:
        # An existing dist directory is not evidence it matches current source.
        dev.run([stage07.NPM, "run", "build", "--prefix", "web"], env=stage07.WEB_ENV, label="current-dashboard-types-build")
    execution = stage05.prepare()
    runtime = stage06.prepare_runtime()
    context = PRIVATE/("image-"+uuid.uuid4().hex)
    binaries = context/"bin"
    binaries.mkdir(parents=True)
    dev.run(["go", "build", "-trimpath", "-buildvcs=false", "-o", str(binaries)+os.sep, "./cmd/..."], env=dev.ENV|{"GOOS": "linux", "GOARCH": "amd64", "CGO_ENABLED": "0"}, timeout=240, label="trusted-linux-role-build")
    shutil.copyfile(stage05.HELPER, binaries/"sandbox-helper-linux")
    shutil.copytree(dev.ROOT/"web/dist", context/"web")
    shutil.copyfile(dev.ROOT/"config/agent-pricing.json", context/"agent-pricing.json")
    shutil.copyfile(dev.ROOT/"config/local/Dockerfile", context/"Dockerfile")
    code, _ = dev.run(["docker", "image", "inspect", DOCKER_CLI], check=False, quiet=True, label="trusted-cli-image")
    if code:
        dev.run(["docker", "pull", DOCKER_CLI], timeout=240, label="trusted-cli-pinned-pull")
    tag = "forgeswarm-platform:"+current
    dev.run(["docker", "build", "--platform", "linux/amd64", "--network", "none", "--pull=false", "-t", tag, context], timeout=180, label="trusted-role-image")
    value = {"input_sha256": current, "platform_image": tag, "runtime_image": runtime, "execution_image": execution["execution_image"], "go_toolchain": "1.26.10", "prepared_at": dev.timestamp()}
    record.write_text(json.dumps(value, indent=2)+"\n")
    return value


def port():
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return server.getsockname()[1]


class Session:
    def __init__(self, path):
        self.path = Path(path).resolve()
        if not self.path.is_relative_to(PRIVATE.resolve()) or self.path.is_symlink():
            raise RuntimeError("unexpected local session location")
        self.document = json.loads((self.path/"session.json").read_text())
        self.owner = self.document["owner"]
        if not re.fullmatch(r"[0-9a-f]{32}", self.owner):
            raise RuntimeError("invalid session owner")
        self.project = self.document["project"]
        if self.project != "forgeswarm-local-"+self.owner[:12]:
            raise RuntimeError("unexpected project owner")
        self.assets = self.path/"assets"
        self.context = ssl.create_default_context(cafile=str(self.assets/"tls/ca.pem"))
        self.base = "https://127.0.0.1:"+str(self.document["ports"]["API_PORT"])
        self.tokens = json.loads((self.assets/"operator-auth.json").read_text())
        self.client = None
        self.client_lock = threading.Lock()

    def command(self, *args):
        return ["docker", "compose", "-f", COMPOSE, "--env-file", self.path/"compose.env", "-p", self.project, "--profile", "local", "--profile", "two-workers", *args]

    def run(self, *args, **kwargs):
        self.owned()
        return dev.run(self.command(*args), **kwargs)

    def identifiers(self):
        _, text = dev.run(["docker", "ps", "-aq", "--filter", "label=com.docker.compose.project="+self.project], quiet=True, label="local-owner-scan")
        return text.split()

    def owned(self):
        identifiers = self.identifiers()
        if not identifiers:
            return
        _, text = dev.run(["docker", "inspect", "--format", "{{json .Config.Labels}}", *identifiers], quiet=True, label="local-owner-check")
        for line in text.splitlines():
            labels = json.loads(line)
            configured = labels.get("com.docker.compose.project.config_files", "").replace("\\", "/").lower()
            if labels.get("com.docker.compose.project") != self.project or COMPOSE.as_posix().lower() != configured:
                raise RuntimeError("refusing a foreign container")

    def container(self, role):
        self.owned()
        _, values = dev.run(self.command("ps", "-aq", role), quiet=True, label="local-role-id")
        identifiers = values.split()
        if len(identifiers) != 1:
            raise RuntimeError("expected exactly one owned role: "+role)
        return identifiers[0]

    def up(self, workers=1):
        if workers not in {1, 2}:
            raise ValueError("one or two bounded workers required")
        roles = ["db", "broker", "seed", "migrate", "github-test", "api", "scheduler", "coordinator", "relay", "publisher", "collector", "jaeger", "prometheus", "grafana", "worker_a", "runtime_a"]
        if workers == 2:
            roles += ["worker_b", "runtime_b"]
        self.run("up", "-d", *roles, timeout=240, label="complete-private-platform")
        if workers == 1:
            self.run("stop", "runtime_b", "worker_b", timeout=40, check=False, label="single-worker-point")
        dev.poll(lambda: self.ready(workers), "authenticated TLS platform and fresh worker registration", timeout=90)

    def ready(self, workers):
        try:
            if self.request("/health/ready", auth=False)[0] != 200:
                return False
            values = self.request("/v1/diagnostics")[1]["database"]["operational"]["Points"]
            if not any(p["Name"] == "workers" and p["Label"] == "active" and p["Value"] == workers for p in values):
                return False
            for suffix in (["a", "b"] if workers == 2 else ["a"]):
                identifier = self.container("runtime_"+suffix)
                _, status = dev.run(["docker", "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{end}}", identifier], quiet=True, label="confined-runtime-health")
                if status.strip() != "healthy":
                    return False
            return True
        except (OSError, KeyError, ValueError):
            return False

    def request(self, route, method="GET", data=None, key=None, reader=False, auth=True):
        headers = {}
        if auth:
            headers["Authorization"] = "Bearer "+self.tokens["reader" if reader else "operator"]["token"]
        if key:
            headers["Idempotency-Key"] = key
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.base+route, data=None if data is None else json.dumps(data).encode(), headers=headers, method=method)
        if os.name == "nt":
            # Windows antivirus may replace even loopback certificates. Verify
            # the installation CA in Linux instead of disabling validation.
            with self.client_lock:
                if self.client is None or self.client.poll() is not None:
                    namespace = self.container("api")
                    args = ["docker", "run", "--rm", "-i", "--label", "forgeswarm.tls-client="+self.owner, "--network", "container:"+namespace, "--read-only", "--cap-drop=ALL", "--security-opt=no-new-privileges=true", "--memory=96m", "--cpus=.25", "--pids-limit=32", "--mount", "type=bind,source="+str(self.assets/"tls/ca.pem")+",target=/ca.pem,readonly", "--mount", "type=bind,source="+str(dev.ROOT/"scripts/tls_probe_client.py")+",target=/client.py,readonly", "--entrypoint", "python", self.document["images"]["runtime_image"], "-u", "/client.py"]
                    self.client = subprocess.Popen(args, cwd=dev.ROOT, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, text=True, encoding="utf-8", creationflags=subprocess.CREATE_NO_WINDOW)
                    self.client_lines = queue.Queue(maxsize=1)
                    process, lines = self.client, self.client_lines
                    def read_response():
                        while process.poll() is None:
                            line = process.stdout.readline(150000)
                            if not line:
                                break
                            try:
                                lines.put(line, timeout=2)
                            except queue.Full:
                                break
                    threading.Thread(target=read_response, daemon=True).start()
                self.client.stdin.write(json.dumps({"route": route, "host": "127.0.0.1:"+str(self.document["ports"]["API_PORT"]), "headers": headers, "method": method, "data": data})+"\n")
                self.client.stdin.flush()
                try:
                    value = json.loads(self.client_lines.get(timeout=15))
                except queue.Empty:
                    self.client.terminate()
                    self.client.wait(timeout=5)
                    self.client = None
                    raise OSError("bounded CA-verified client response deadline") from None
                if "transport_error" in value:
                    raise OSError("CA-verified container client: "+value["transport_error"])
                return value["code"], value["data"]
        try:
            response = urllib.request.urlopen(request, context=self.context, timeout=5)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            return response.status, json.load(response)

    def graph(self, identifier):
        code, value = self.request("/v1/workflows/"+identifier+"/graph", reader=True)
        assert code == 200
        return value

    def submit(self, body):
        code, value = self.request("/v1/workflows", "POST", body, "stage10-"+uuid.uuid4().hex)
        assert code == 201, (code, value)
        return value["id"]

    def settle(self, identifier, timeout=120):
        value = {}
        def terminal():
            nonlocal value
            value = self.graph(identifier)
            return value["status"] in {"SUCCEEDED", "FAILED", "CANCELLED"}
        dev.poll(terminal, "owned workflow terminal state", timeout=timeout)
        return value

    def sql(self, statement, database=None):
        if database is None:
            # Restore switches only this installation's mounted URL. Observe
            # the same database as its services; explicit source comparisons
            # still select the preserved original database by name.
            database = urllib.parse.urlsplit((self.assets/"database-url").read_text()).path.lstrip("/")
        if not re.fullmatch(r"forgeswarm(?:_restore_[0-9a-f]{12})?", database):
            raise ValueError("unexpected disposable database")
        _, value = self.run("exec", "-T", "db", "psql", "-X", "-U", "forgeswarm", "-d", database, "-At", "-v", "ON_ERROR_STOP=1", "-c", statement, quiet=True, label="owned-db-observation")
        return value.strip()

    def stop(self):
        self.close_client()
        self.owned()
        # Stop creators before enumerating detached sandbox resources. Inspect
        # both ownership labels before every removal; never prune the daemon.
        self.run("stop", "runtime_a", "runtime_b", "worker_a", "worker_b", timeout=60, quiet=True, label="stop-owned-execution-creators")
        installation = str(uuid.UUID(self.owner))
        _, values = dev.run(["docker", "ps", "-aq", "--no-trunc", "--filter", "label=forgeswarm.installation="+installation, "--filter", "label=forgeswarm.resource=sandbox-v1"], quiet=True, label="owned-sandbox-teardown-scan")
        identifiers = values.split()
        if len(identifiers) > 32 or any(not re.fullmatch(r"[0-9a-f]{64}", value) for value in identifiers):
            raise RuntimeError("unexpected owned sandbox teardown scope")
        for identifier in identifiers:
            _, labels = dev.run(["docker", "inspect", "--format", "{{json .Config.Labels}}", identifier], quiet=True, label="owned-sandbox-teardown-check")
            labels = json.loads(labels)
            if labels.get("forgeswarm.installation") != installation or labels.get("forgeswarm.resource") != "sandbox-v1":
                raise RuntimeError("refusing foreign sandbox teardown")
            dev.run(["docker", "rm", "-f", identifier], timeout=20, label="owned-sandbox-teardown")
        # No --volumes: named data volumes are handled only after label checks.
        self.run("down", "--timeout", "15", timeout=90, label="owned-platform-stop")

    def cleanup(self):
        self.stop()
        _, values = dev.run(["docker", "volume", "ls", "-q", "--filter", "label=com.docker.compose.project="+self.project], quiet=True, label="owned-volume-scan")
        for volume in values.split():
            _, actual = dev.run(["docker", "volume", "inspect", "--format", "{{json .Labels}}", volume], quiet=True, label="owned-volume-check")
            if json.loads(actual).get("com.docker.compose.project") != self.project:
                raise RuntimeError("foreign volume")
            dev.run(["docker", "volume", "rm", volume], label="disposable-volume-cleanup")

    def close_client(self):
        with self.client_lock:
            if self.client is not None:
                self.client.stdin.close()
                try:
                    self.client.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    self.client.terminate()
                    self.client.wait(timeout=5)
                self.client = None


def create(stable=False):
    prepared = prepare()
    pointer = PRIVATE/"local.json"
    if stable and pointer.exists():
        session = Session(PRIVATE/json.loads(pointer.read_text())["owner"])
        if session.document["images"] != prepared:
            session.owned()
            environment = session.path/"compose.env"
            body = environment.read_text()
            for key, value in [("PLATFORM_IMAGE", prepared["platform_image"]), ("RUNTIME_IMAGE", prepared["runtime_image"])]:
                body = re.sub(r"^"+key+r"=.*$", key+"="+json.dumps(value), body, flags=re.MULTILINE)
            environment.write_text(body)
            session.document["images"] = prepared
            (session.path/"session.json").write_text(json.dumps(session.document, indent=2)+"\n")
        return session
    owner = uuid.uuid4().hex
    path = PRIVATE/owner
    assets = path/"assets"
    assets.mkdir(parents=True, mode=0o700)
    os.chmod(path, 0o700)
    values = {key: port() for key in ["API_PORT", "OTLP_PORT", "METRICS_PORT", "COLLECTOR_HEALTH_PORT", "JAEGER_PORT", "PROMETHEUS_PORT", "GRAFANA_PORT"]}
    document = {"schema_version": 1, "owner": owner, "project": "forgeswarm-local-"+owner[:12], "ports": values, "images": prepared, "created_at": dev.timestamp(), "stable": stable}
    def secret(name, value):
        # Parent directories are private. Per-role bind secrets need to be
        # readable by the unprivileged service UID; they are never in an image.
        (assets/name).write_text(value)
        os.chmod(assets/name, 0o644)
    password, broker, runtime = (secrets.token_hex(32) for _ in range(3))
    secret("db-password", password)
    secret("database-url", "postgres://forgeswarm:"+password+"@db:5432/forgeswarm?sslmode=verify-full&sslrootcert=/run/secrets/ca")
    secret("nats-token", broker)
    secret("runtime-token", runtime)
    secret("grafana-password", secrets.token_urlsafe(32))
    keys = {"worker_"+suffix: secrets.token_hex(32) for suffix in ["a", "b"]}
    secret("worker-keys.json", json.dumps(keys))
    for suffix in ["a", "b"]:
        secret("worker-"+suffix+"-token", keys["worker_"+suffix])
    secret("operator-auth.json", json.dumps({"operator": {"actor": "local-owner", "token": secrets.token_hex(32)}, "reader": {"actor": "local-observer", "token": secrets.token_hex(32)}}))
    secret("github.json", json.dumps({"repository": {"id": 42, "owner": "fixture-owner", "name": "math-fixture", "target_branch": "main"}, "credential_mode": "TEST_DEMO", "token": "TEST_DEMO_NO_REAL_GITHUB_CREDENTIAL", "webhook_secret": "TEST_DEMO_WEBHOOK_SECRET_NOT_FOR_REAL_GITHUB", "trigger_label": "forgeswarm", "trigger_actor": "fixture-owner", "test_api_url": "http://127.0.0.1:9088"}))
    dev.run([dev.BIN.parent/("tlsgen.exe" if os.name == "nt" else "tlsgen"), "-out", assets/"tls"], label="installation-private-ca")
    for key in (assets/"tls").glob("*-key.pem"):
        if key.name != "ca-key.pem":
            os.chmod(key, 0o644)
    job = stage07.fixture(assets/"fixtures", assets/"catalog", path/"repository")
    for folder in [assets/"fixtures", assets/"catalog"]:
        os.chmod(folder, 0o755)
        for file in folder.iterdir():
            os.chmod(file, 0o644)
    (assets/"nats.conf").write_text('listen: "0.0.0.0:4222"\nhttp: "127.0.0.1:8222"\nauthorization { token: $NATS_TOKEN }\ntls { cert_file: "/run/secrets/broker_cert", key_file: "/run/secrets/broker_key", timeout: 2 }\njetstream { store_dir: "/data", max_memory_store: 32MB, max_file_store: 256MB }\n')
    (path/"job.json").write_text(json.dumps(job, indent=2)+"\n")
    (path/"session.json").write_text(json.dumps(document, indent=2)+"\n")
    _, gid = dev.run(["docker", "run", "--rm", "--network", "none", "--mount", "type=bind,source=/var/run/docker.sock,target=/var/run/docker.sock", "--entrypoint", "stat", DOCKER_CLI, "-c", "%g", "/var/run/docker.sock"], quiet=True, label="trusted-executor-socket-group")
    if not gid.strip().isdigit():
        raise RuntimeError("cannot determine trusted Docker group")
    environment = {"PLATFORM_IMAGE": prepared["platform_image"], "RUNTIME_IMAGE": prepared["runtime_image"], "ASSETS_DIR": assets.as_posix(), "INSTALLATION_ID": str(uuid.UUID(owner)), "DOCKER_GID": gid.strip()}|{key: str(value) for key, value in values.items()}
    (path/"compose.env").write_text("\n".join(key+"="+json.dumps(value) for key, value in environment.items())+"\n")
    session = Session(path)
    dev.run(session.command("config", "--quiet"), label="complete-compose-validated")
    if stable:
        pointer.write_text(json.dumps({"owner": owner})+"\n")
    return session


@contextlib.contextmanager
def isolated():
    session = create()
    try:
        yield session
    finally:
        session.cleanup()

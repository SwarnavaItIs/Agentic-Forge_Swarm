"""Prepared-AMI host launcher; no package installs or repository execution.

Input is the reviewed role-specific release.json on the prepared AMI. S3 bundle
identity and role separation are verified before any image load or service start.
Run by root on the two trusted private hosts, never inside a code sandbox.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tarfile

CONTROL = {"db-password", "database-url", "nats-token", "operator-auth.json", "github.json", "worker-keys.json", "grafana-password", "tls/ca.pem", "tls/api.pem", "tls/api-key.pem", "tls/coordinator.pem", "tls/coordinator-key.pem", "tls/broker.pem", "tls/broker-key.pem", "tls/db.pem", "tls/db-key.pem"}
EXECUTION = {"nats-token", "runtime-token", "worker-a-token", "tls/ca.pem"}


def run(args, **kwargs):
    return subprocess.run(args, check=True, timeout=180, stdout=subprocess.PIPE, stderr=subprocess.PIPE, **kwargs).stdout


def validate(manifest, role):
    if role not in {"control", "execution"} or manifest["role"] != role:
        raise ValueError("role mismatch")
    if not re.fullmatch(r"[0-9a-f]{32}", manifest["owner"]) or not re.fullmatch(r"[0-9a-f]{64}", manifest["release_sha256"]):
        raise ValueError("identity/hash invalid")
    if not re.fullmatch(r"arn:aws:secretsmanager:[a-z0-9-]+:[0-9]{12}:secret:[A-Za-z0-9/_+=.@-]+", manifest["secret_arn"]):
        raise ValueError("exact secret ARN required")
    if manifest.get("provider", "scripted_TEST_DEMO") != "scripted_TEST_DEMO":
        raise ValueError("live deployment requires separate egress/cost review")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--role", required=True, choices=["control", "execution"])
    parser.add_argument("--manifest", default="/opt/forgeswarm/release.json")
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    validate(manifest, args.role)
    root = Path("/var/lib/forgeswarm")
    root.mkdir(mode=0o700, exist_ok=True)
    os.chmod(root, 0o700)
    archive = root/"release.tar"
    run(["aws", "s3api", "get-object", "--bucket", manifest["release_bucket"], "--key", manifest["release_key"], str(archive)])
    if archive.stat().st_size > 4*1024**3:
        raise ValueError("release size limit")
    with archive.open("rb") as stream:
        if hashlib.file_digest(stream, "sha256").hexdigest() != manifest["release_sha256"]:
            raise ValueError("release hash mismatch")
    release = root/("release-"+manifest["release_sha256"])
    release.mkdir(mode=0o700, exist_ok=True)
    with tarfile.open(archive) as tar:
        members = tar.getmembers()
        if len(members) > 5000 or sum(x.size for x in members) > 8*1024**3 or any(not (x.isfile() or x.isdir()) or x.name.startswith("/") or ".." in Path(x.name).parts for x in members):
            raise ValueError("release archive bounds")
        tar.extractall(release, filter="data")
    current = root/"current"
    if current.exists() and (not current.is_symlink() or not current.resolve().is_relative_to(root)):
        raise ValueError("foreign current release path")
    link = root/"current.next"
    if link.exists() or link.is_symlink():
        raise ValueError("unfinished release switch requires operator inspection")
    link.symlink_to(release, target_is_directory=True)
    link.replace(current)
    # Exact allowlist: execution secret cannot accidentally carry control keys.
    raw = run(["aws", "secretsmanager", "get-secret-value", "--secret-id", manifest["secret_arn"], "--query", "SecretString", "--output", "text"])
    if len(raw) > 65536:
        raise ValueError("secret document limit")
    secrets = json.loads(raw)
    if set(secrets) != (CONTROL if args.role == "control" else EXECUTION):
        raise ValueError("role secret allowlist mismatch")
    if args.role == "control":
        github = json.loads(secrets["github.json"])
        if github["credential_mode"] != "TEST_DEMO" or github["repository"]["id"] != 42:
            raise ValueError("this offline deployment profile supports controlled demo only")
    assets = root/"secrets"
    assets.mkdir(mode=0o700, exist_ok=True)
    for name, value in secrets.items():
        path = assets/name
        path.parent.mkdir(mode=0o700, exist_ok=True)
        if not isinstance(value, str) or len(value) > 32768:
            raise ValueError("secret value bounds")
        path.write_text(value)
        os.chmod(path, 0o644)  # Private parent; service bind UID must read it.
    shared = root/"shared-assets"
    shared.mkdir(mode=0o750, exist_ok=True)
    if not re.fullmatch(r"fs-[a-f0-9]+", manifest["registry_id"]) or not re.fullmatch(r"fsap-[a-f0-9]+", manifest["registry_access_point"]):
        raise ValueError("exact shared registry identities required")
    if not os.path.ismount(shared):
        options = "tls,iam,accesspoint="+manifest["registry_access_point"]+("\x2cro" if args.role == "execution" else "")
        run(["mount", "-t", "efs", "-o", options, manifest["registry_id"]+":/", str(shared)])
    images = release/(args.role+"-images.tar")
    run(["docker", "load", "--input", str(images)])
    # A role-specific reviewed Compose document is part of the hashed release.
    # Inspect effective config before starting: no worker on control, and no
    # control role/credential mount on the execution host.
    compose = ["docker", "compose", "-f", str(release/(args.role+".json")), "--env-file", str(release/(args.role+".env")), "-p", "forgeswarm-"+manifest["owner"][:12]]
    config = json.loads(run([*compose, "config", "--format", "json"]))
    services = set(config["services"])
    if args.role == "execution":
        if services != {"worker_a", "runtime_a"}:
            raise ValueError("execution host contains control services")
        for service in config["services"].values():
            if any(key in service.get("environment", {}) for key in ["DATABASE_URL", "DATABASE_URL_FILE", "GITHUB_CONFIG_FILE", "OPERATOR_AUTH_FILE"]):
                raise ValueError("execution control secret/config denied")
        worker = config["services"]["worker_a"]["environment"]
        if worker["RPC_ADDR"] != manifest["control_private_ip"]+":9090" or worker["NATS_URL"] != "tls://"+manifest["control_private_ip"]+":4222":
            raise ValueError("release control endpoint does not match deployment")
    elif services & {"worker_a", "worker_b", "runtime_a", "runtime_b"}:
        raise ValueError("worker cannot run on control host")
    for service in config["services"].values():
        if any(not str(p.get("host_ip", "")).startswith("127.") and p["target"] not in {4222, 9090} for p in service.get("ports", [])):
            raise ValueError("unexpected exposed port")
    run([*compose, "up", "-d", "--wait", "--wait-timeout", "90"])
    if args.role == "control":
        run(["systemctl", "is-enabled", "forgeswarm-backup.timer"])
    print("Private role started; inspect health with the operator CA and identity.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        raise SystemExit("private host bootstrap failed; inspect owned configuration without printing secrets")

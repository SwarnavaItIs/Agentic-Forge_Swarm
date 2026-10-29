"""Build an offline two-host release; does not upload, provision or execute it.

Example: python deploy/aws/package.py --session .cache/platform/<owner>
--control-ip 10.0.1.10 --out .cache/aws-release-<unique>.tar
TLS certificate SANs and the prepared AMI remain separately reviewed inputs.
"""
import argparse
import copy
import hashlib
import ipaddress
import json
from pathlib import Path
import shutil
import sys
import tarfile
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[2]/"scripts"))
import dev
import local_platform

CONTROL_ROLES = {"db", "broker", "seed", "migrate", "github-test", "api", "scheduler", "coordinator", "relay", "publisher"}
EXECUTION_ROLES = {"worker_a", "runtime_a"}
CURRENT = "/var/lib/forgeswarm/current"


def configuration(resolved, role, control_ip):
    if not ipaddress.ip_address(control_ip).is_private or ipaddress.ip_address(control_ip).is_loopback:
        raise ValueError("private control IP required")
    value = copy.deepcopy(resolved)
    selected = CONTROL_ROLES if role == "control" else EXECUTION_ROLES
    value["services"] = {key: item for key, item in value["services"].items() if key in selected}
    used_secrets = set()
    for name, service in value["services"].items():
        service.pop("profiles", None)
        if "depends_on" in service:
            service["depends_on"] = {key: item for key, item in service["depends_on"].items() if key in selected}
        env = service.get("environment", {})
        env["TELEMETRY_ENABLED"] = "false"
        env["TOTAL_CAPACITY"] = "1"
        if name == "api":
            env["UI_ORIGIN"] = "https://127.0.0.1:8443"
        if name == "github-test":
            service["ports"] = [{"target": 8443, "published": "8443", "host_ip": "127.0.0.1", "protocol": "tcp"}]
        for key in ["DATABASE_URL", "DATABASE_URL_FILE", "OPERATOR_AUTH_FILE", "GITHUB_CONFIG_FILE", "RPC_KEYS_FILE"]:
            if role == "execution":
                env.pop(key, None)
        if role == "execution":
            env["RPC_ADDR"] = control_ip+":9090"
            env["NATS_URL"] = "tls://"+control_ip+":4222"
            if name == "worker_a":
                service["networks"] = {"edge": {}}
        for mount in service.get("volumes", []):
            if mount["target"] == "/assets":
                mount.update(type="bind", source="/var/lib/forgeswarm/shared-assets", read_only=role == "execution")
            elif mount["target"] in {"/seed/fixtures", "/seed/catalog"}:
                mount["source"] = CURRENT+mount["target"].replace("/seed", "")
            elif mount["target"] == "/etc/postgresql/pg_hba.conf":
                mount["source"] = CURRENT+"/pg_hba.conf"
            elif mount["target"] == "/etc/nats/secure.conf":
                mount["source"] = CURRENT+"/nats.conf"
        if name in {"broker", "coordinator"}:
            service["networks"] = {"default": {}, "edge": {}}
            port = 4222 if name == "broker" else 9090
            service["ports"] = [{"target": port, "published": str(port), "host_ip": control_ip, "protocol": "tcp"}]
        for secret in service.get("secrets", []):
            used_secrets.add(secret["source"] if isinstance(secret, dict) else secret)
    value["secrets"] = {key: item for key, item in value["secrets"].items() if key in used_secrets}
    raw = json.loads(local_platform.COMPOSE.read_text())["secrets"]
    for key, item in value["secrets"].items():
        item["file"] = raw[key]["file"].replace("${ASSETS_DIR}", "/var/lib/forgeswarm/secrets")
    value["volumes"] = {key: item for key, item in value.get("volumes", {}).items() if key in {"state", "messages"}}
    for item in value["volumes"].values():
        item.pop("name", None)
    # No shared cross-host Docker networks: only verified TLS to host-private
    # RPC/broker ports. Only the fixed trusted executor mounts the Docker socket.
    value["networks"] = {"default": {"internal": True}, "edge": {}}
    value.pop("name", None)
    return value


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--session", required=True)
    parser.add_argument("--control-ip", required=True)
    parser.add_argument("--out", required=True)
    args = parser.parse_args()
    session = local_platform.Session(args.session)
    out = Path(args.out).resolve()
    if not out.is_relative_to((dev.ROOT/".cache").resolve()) or out.exists():
        raise ValueError("new ignored output under .cache required")
    _, text = dev.run(session.command("config", "--format", "json"), quiet=True)
    resolved = json.loads(text)
    context = dev.ROOT/".cache"/("aws-package-"+uuid.uuid4().hex)
    context.mkdir(mode=0o700)
    for role in ["control", "execution"]:
        config = configuration(resolved, role, args.control_ip)
        (context/(role+".json")).write_text(json.dumps(config, indent=2)+"\n")
        (context/(role+".env")).write_text("")
        images = sorted({item["image"] for item in config["services"].values()})
        if role == "execution":
            images.append(session.document["images"]["execution_image"])
        dev.run(["docker", "image", "save", "--output", context/(role+"-images.tar"), *images], timeout=240, label="offline-role-image-bundle")
    for folder in ["fixtures", "catalog"]:
        shutil.copytree(session.assets/folder, context/folder)
    shutil.copyfile(session.assets/"nats.conf", context/"nats.conf")
    shutil.copyfile(dev.ROOT/"config/local/pg_hba.conf", context/"pg_hba.conf")
    with tarfile.open(out, "x") as tar:
        for path in sorted(context.rglob("*")):
            if path.is_file():
                tar.add(path, arcname=path.relative_to(context).as_posix(), recursive=False)
    with out.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    print(json.dumps({"release": str(out), "sha256": digest, "bytes": out.stat().st_size, "owner": session.owner, "control_private_ip": args.control_ip, "cloud_deployed": False}, indent=2))


if __name__ == "__main__":
    main()

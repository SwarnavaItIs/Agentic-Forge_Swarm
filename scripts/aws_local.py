"""Offline CloudFormation syntax/policy checks; never invokes an AWS API."""
import copy
import json
import os
from pathlib import Path
import shutil
import importlib.util
import uuid

import dev


def validate():
    folder = dev.ROOT/"artifacts/verification/stage-10/deployment"
    folder.mkdir(parents=True, exist_ok=True)
    template = dev.ROOT/"deploy/aws/private.json"
    lint = dev.ROOT/".cache/stage10-tools"/("Scripts/cfn-lint.exe" if os.name == "nt" else "bin/cfn-lint")
    dev.run([lint, template], label="cfn-lint-offline")
    candidates = list((dev.ROOT/".cache/stage10-tools/guard").rglob("cfn-guard.exe" if os.name == "nt" else "cfn-guard"))
    guard = shutil.which("cfn-guard") or (str(candidates[0]) if len(candidates) == 1 else None)
    if guard is None:
        raise RuntimeError("Install pinned cfn-guard 3.2.1 (cargo install cfn-guard --version 3.2.1 --locked); see config/security-tools.json")
    args = [guard, "validate", "--rules", dev.ROOT/"deploy/aws/private.guard", "--data", template, "--show-summary", "fail"]
    dev.run(args, label="cfn-guard-offline")
    original = json.loads(template.read_text())
    for name, change in [("public-ip", lambda v: v["Resources"]["ControlHost"]["Properties"]["NetworkInterfaces"][0].update(AssociatePublicIpAddress=True)), ("metadata", lambda v: v["Resources"]["ExecutionHost"]["Properties"]["MetadataOptions"].update(HttpTokens="optional")), ("unencrypted", lambda v: v["Resources"]["Registry"]["Properties"].update(Encrypted=False))]:
        value = copy.deepcopy(original)
        change(value)
        path = folder/("negative-"+name+".json")
        path.write_text(json.dumps(value))
        negative = list(args)
        negative[negative.index("--data")+1] = path
        code, _ = dev.run(negative, check=False, quiet=True, label="guard-denies-"+name)
        assert code != 0, "policy bypass accepted"
    assert original["Resources"]["ControlRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"][2]["Resource"] == {"Ref": "ControlSecretArn"}
    assert original["Resources"]["ExecutionRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"][2]["Resource"] == {"Ref": "ExecutionSecretArn"}
    # Validate both generated host Compose documents without running them or
    # reading credentials. Placeholder images are never pulled/executed.
    spec = importlib.util.spec_from_file_location("aws_release_configuration", dev.ROOT/"deploy/aws/package.py")
    package = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(package)
    environment = dev.ENV | {"PLATFORM_IMAGE": "configuration-only:never-run", "RUNTIME_IMAGE": "configuration-only:never-run", "ASSETS_DIR": str(dev.ROOT/".cache/configuration-only-no-secrets"), "INSTALLATION_ID": str(uuid.UUID("a"*32)), "DOCKER_GID": "0"}
    _, text = dev.run(["docker", "compose", "-f", dev.ROOT/"config/local/compose.json", "--profile", "local", "--profile", "two-workers", "config", "--format", "json"], env=environment, quiet=True, label="resolve-release-config-only")
    resolved = json.loads(text)
    for role in ["control", "execution"]:
        config = package.configuration(resolved, role, "10.0.1.10")
        path = folder/(role+"-configuration-only.json")
        path.write_text(json.dumps(config, indent=2)+"\n")
        dev.run(["docker", "compose", "-f", path, "-p", "forgeswarm-config-only", "config", "--quiet"], label="validate-"+role+"-compose-only")
        services = config["services"]
        if role == "execution":
            assert set(services) == {"worker_a", "runtime_a"}
            assert not {"database_url", "operator_auth", "github_config", "rpc_keys"} & set(config["secrets"])
            assert "DATABASE_URL_FILE" not in services["worker_a"]["environment"]
            assert all(mount["read_only"] for mount in services["worker_a"]["volumes"] if mount["target"] == "/assets")
        else:
            assert not {"worker_a", "runtime_a", "worker_b", "runtime_b"} & set(services)
            assert services["api"]["environment"]["UI_ORIGIN"] == "https://127.0.0.1:8443"
    result = {"at": dev.timestamp(), "cfn_lint": "passed", "cfn_guard": "passed", "negative_policy_cases": 3, "aws_api_calls": 0, "cloud_deployed": False, "unverified": ["account permissions and IAM simulation", "AMI/bootstrap and EFS mount on EC2", "network endpoints and regional quotas", "actual cloud backup restore", "cost estimate and spending authorization"], "costs": "Two EC2 hosts, retained EBS, EFS storage/backup and S3 backup/version storage incur ongoing regional charges; no estimate or budget authorized. Inspect https://aws.amazon.com/efs/pricing/ and the target-region pricing calculator before provisioning."}
    (folder/"validation.json").write_text(json.dumps(result, indent=2)+"\n")
    return result

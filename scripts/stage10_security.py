"""Established dependency scanners. Never equate govulncheck JSON exit 0 to clean."""
import hashlib
import json
import os
from pathlib import Path
import ssl
import subprocess
import sys

import dev
import stage07


def run():
    folder = dev.ROOT / "artifacts/verification/stage-10/security"
    folder.mkdir(parents=True, exist_ok=True)
    tools = dev.ROOT / ".cache/stage10-bin"
    tools.mkdir(parents=True, exist_ok=True)
    binary = tools / ("govulncheck.exe" if os.name == "nt" else "govulncheck")
    dev.run(["go", "install", "golang.org/x/vuln/cmd/govulncheck@v1.9.0"], env=dev.ENV | {"GOBIN": str(tools)}, timeout=240, label="pinned-govulncheck")
    value = subprocess.run([str(binary), "-json", "./..."], cwd=dev.ROOT, env=dev.ENV, capture_output=True, text=True, encoding="utf-8", errors="replace", timeout=240)
    assert len(value.stdout) < 4 << 20
    (folder/"go-raw.jsonl").write_text(value.stdout, encoding="utf-8")
    decoder, data, findings = json.JSONDecoder(), value.stdout, []
    while data.strip():
        item, end = decoder.raw_decode(data.lstrip())
        data = data.lstrip()[end:]
        if "finding" in item:
            findings.append(item["finding"])
    reachable = sorted({v["osv"] for v in findings if v.get("trace") and v["trace"][0].get("function")})
    residual = sorted({v["osv"] for v in findings} - set(reachable))
    assert value.returncode == 0 and not reachable, "reachable Go vulnerabilities; inspect structured evidence"
    audits = {}
    for label, args in [("npm-runtime", ["--omit=dev"]), ("npm-all", [])]:
        code, output = dev.run([stage07.NPM, "audit", "--json", "--prefix", "web", *args], env=stage07.WEB_ENV, timeout=120, check=False, quiet=True, label=label)
        result = json.loads(output)
        audits[label] = result
        (folder/(label+".json")).write_text(json.dumps(result, indent=2)+"\n")
        assert code == 0 and not result.get("metadata", {}).get("vulnerabilities", {}).get("total"), label
    venv = dev.ROOT/".cache/stage10-tools"
    python = venv/("Scripts/python.exe" if os.name == "nt" else "bin/python")
    if not python.exists():
        dev.run([sys.executable, "-m", "venv", venv], label="isolated-security-tools")
    cert = venv/"system-ca.pem"
    cert.write_text("".join(ssl.DER_cert_to_PEM_cert(x) for x in ssl.create_default_context().get_ca_certs(binary_form=True)))
    environment = dev.ENV | {"REQUESTS_CA_BUNDLE": str(cert)}
    dev.run([python, "-m", "pip", "install", "pip-audit==2.10.1", "cfn-lint==1.58.0"], env=environment, timeout=180, label="pinned-audit-tools")
    code, output = dev.run([python, "-m", "pip_audit", "-r", "agent-runtime/requirements.lock", "--disable-pip", "--no-deps", "-f", "json"], env=environment, timeout=120, check=False, quiet=True, label="python-runtime-audit")
    result = json.loads(output[output.index("{"):])
    (folder/"python-runtime.json").write_text(json.dumps(result, indent=2)+"\n")
    assert code == 0 and not any(v.get("vulns") for v in result["dependencies"])
    # Check only secret signatures in repository-owned source/config. Report
    # paths/hashes, never matching bytes. Generated fixture strings are public.
    import re
    paths = subprocess.check_output(["git", "ls-files", "-co", "--exclude-standard"], cwd=dev.ROOT, text=True).splitlines()
    forbidden = re.compile(rb"(?:-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----|AKIA[A-Z0-9]{16}|ghp_[A-Za-z0-9]{36}|sk-proj-[A-Za-z0-9_-]{40,})")
    secret_paths = [p for p in paths if (dev.ROOT/p).is_file() and forbidden.search((dev.ROOT/p).read_bytes())]
    assert not secret_paths, "potential tracked secret signatures: " + ", ".join(secret_paths)
    report = {"checked_at": dev.timestamp(), "fingerprint": dev.fingerprint(), "reachable_go": reachable, "module_only_go": residual, "npm": {key: value["metadata"]["vulnerabilities"] for key, value in audits.items()}, "python_runtime_vulnerabilities": 0, "configuration_paths_scanned": len(paths), "secret_signature_paths": secret_paths, "limitations": ["Module-only Go issues remain listed even if packages are not imported.", "No OS/image-package vulnerability scanner was available; those vulnerabilities remain unverified.", "Signature scanning cannot establish absence of secrets or all configuration defects."]}
    (folder/"summary.json").write_text(json.dumps(report, indent=2)+"\n")
    return report

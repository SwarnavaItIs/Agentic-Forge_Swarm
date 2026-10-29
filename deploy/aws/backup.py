"""Trusted daily host backup. Versioned S3 retention is defined in private.json."""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import subprocess
import uuid


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    args = parser.parse_args()
    manifest = json.loads(Path(args.manifest).read_text())
    if manifest["role"] != "control" or len(manifest["owner"]) != 32:
        raise ValueError("control owner required")
    project = "forgeswarm-"+manifest["owner"][:12]
    ids = subprocess.check_output(["docker", "ps", "-q", "--filter", "label=com.docker.compose.project="+project, "--filter", "label=com.docker.compose.service=db"], text=True, timeout=10).split()
    if len(ids) != 1:
        raise ValueError("one owned database required")
    path = Path("/var/lib/forgeswarm")/("backup-"+uuid.uuid4().hex+".dump")
    with path.open("xb") as output:
        subprocess.run(["docker", "exec", ids[0], "pg_dump", "-U", "forgeswarm", "-d", manifest.get("database", "forgeswarm"), "-Fc", "--no-owner", "--no-acl"], stdout=output, stderr=subprocess.DEVNULL, check=True, timeout=120)
    with path.open("rb") as stream:
        digest = hashlib.file_digest(stream, "sha256").hexdigest()
    key = manifest["owner"]+"/"+datetime.datetime.now(datetime.timezone.utc).strftime("%Y/%m/%d/")+path.name
    subprocess.run(["aws", "s3api", "put-object", "--bucket", manifest["backup_bucket"], "--key", key, "--body", str(path), "--server-side-encryption", "AES256", "--metadata", "sha256="+digest], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, timeout=180)
    path.unlink()  # Exact newly created file only, after successful upload.


if __name__ == "__main__":
    main()

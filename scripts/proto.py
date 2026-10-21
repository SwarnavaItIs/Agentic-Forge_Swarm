"""Pinned compiler/plugins; regeneration is byte-for-byte, without repo writes in check mode."""
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import sys
import urllib.request
import zipfile
import dev

VERSION="35.0"
SHA={"win64":"d1cede9e308cc3eb072392af1c02ccae4bdd3d2f374ec2970dbd8cdfdaa91363",
     "linux-x86_64":"a45cda0989c17dd950db55f6fbe1e5814c50fda08e87aa422980ac1f89dddbbc"}
def generate(check=True):
    target=dev.ROOT / ".cache/proto-generated"
    target.mkdir(parents=True,exist_ok=True)
    compiler=shutil.which("protoc")
    if not compiler or subprocess.check_output([compiler,"--version"]).decode().strip()!=f"libprotoc {VERSION}":
        platform="win64" if os.name=="nt" else "linux-x86_64"
        tools=dev.ROOT / ".cache" / f"protoc-{VERSION}-{platform}"
        exe=tools / "bin" / ("protoc.exe" if os.name=="nt" else "protoc")
        if not exe.exists():
            url=f"https://github.com/protocolbuffers/protobuf/releases/download/v{VERSION}/protoc-{VERSION}-{platform}.zip"
            data=urllib.request.urlopen(url,timeout=60).read(20<<20)
            if hashlib.sha256(data).hexdigest()!=SHA[platform]:raise RuntimeError("protoc archive checksum mismatch")
            archive=dev.ROOT / ".cache/protoc.zip";archive.write_bytes(data)
            with zipfile.ZipFile(archive) as z:
                for name in z.namelist():
                    if not (tools/name).resolve().is_relative_to(tools.resolve()):raise RuntimeError("invalid protoc archive path")
                z.extractall(tools)
            if os.name!="nt":exe.chmod(0o755)
        compiler=str(exe)
    plugins=dev.ROOT / ".cache/tools";plugins.mkdir(parents=True,exist_ok=True)
    env=dev.ENV | {"GOBIN":str(plugins),"PATH":str(plugins)+os.pathsep+os.environ["PATH"]}
    for package,version in [("google.golang.org/protobuf/cmd/protoc-gen-go","v1.36.10"),("google.golang.org/grpc/cmd/protoc-gen-go-grpc","v1.5.1")]:
        dev.run(["go","install",package+"@"+version],env=env,label="proto-plugin")
    out=target if check else dev.ROOT
    dev.run([compiler,"--go_out="+str(out),"--go_opt=module=forgeswarm","--go-grpc_out="+str(out),"--go-grpc_opt=module=forgeswarm","api/worker/v1/worker.proto","api/agent/v1/agent.proto"],env=env,label="proto-generate")
    if check:
        for name in ["worker.pb.go","worker_grpc.pb.go"]:
            if (out/"api/worker/v1"/name).read_bytes()!=(dev.ROOT/"api/worker/v1"/name).read_bytes():raise RuntimeError("generated protobuf differs: "+name)
        for name in ["agent.pb.go","agent_grpc.pb.go"]:
            if (out/"api/agent/v1"/name).read_bytes()!=(dev.ROOT/"api/agent/v1"/name).read_bytes():raise RuntimeError("generated protobuf differs: "+name)
        print("pinned Protobuf regeneration matches committed sources",flush=True)
if __name__=="__main__":generate(len(sys.argv)==1 or sys.argv[1]!="write")

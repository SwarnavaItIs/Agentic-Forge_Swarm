"""Authenticated HTTPS for owned Linux API acceptance containers.

Supersedes the earlier anonymous 0.0.0.0 HTTP harness assumption. Curl verifies
the generated CA inside the namespace; antivirus interception on a Windows host
cannot cause tests to disable certificate checking. Tokens travel on stdin.
"""
import json
import socket
import subprocess
import uuid
import secrets

import dev


class Transport:
    def __init__(self, owner):
        self.owner, self.container = owner, None
        self.path = dev.ROOT/".cache"/("private-acceptance-"+uuid.uuid4().hex)
        self.path.mkdir(mode=0o700)
        dev.run(["go", "run", "./cmd/tlsgen", "-out", self.path/"tls"], label="acceptance-private-ca")
        self.token = secrets.token_hex(32)
        (self.path/"auth.json").write_text(json.dumps({"operator": {"actor": "acceptance", "token": self.token}, "reader": {"actor": "observer", "token": secrets.token_hex(32)}}))
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            self.port = sock.getsockname()[1]
        self.base = "https://127.0.0.1:"+str(self.port)

    def arguments(self):
        return ["-p", f"127.0.0.1:{self.port}:8080", "--mount", f"type=bind,source={self.path},target=/http-private,readonly", "-e", "HTTP_TLS_CERT=/http-private/tls/api.pem", "-e", "HTTP_TLS_KEY=/http-private/tls/api-key.pem", "-e", "OPERATOR_AUTH_FILE=/http-private/auth.json", "-e", "UI_ORIGIN="+self.base]

    def bind(self, container):
        _, owner = dev.run(["docker", "inspect", "--format", '{{ index .Config.Labels "forgeswarm.verification" }}', container], quiet=True, label="private-api-owner")
        if owner.strip() != self.owner:
            raise RuntimeError("foreign acceptance API")
        self.container = container

    def request(self, base, path, method="GET", data=None, key=None):
        if base != self.base or self.container is None:
            raise ValueError("unexpected acceptance endpoint")
        config = ['url = '+json.dumps('https://127.0.0.1:8080'+path), 'cacert = "/http-private/tls/ca.pem"', 'max-time = 3', 'silent', 'show-error', 'include', 'request = '+json.dumps(method), 'header = '+json.dumps('Host: '+base.removeprefix('https://')), 'header = '+json.dumps('Authorization: Bearer '+self.token)]
        if key:
            config.append('header = '+json.dumps('Idempotency-Key: '+key))
        if data is not None:
            config += ['header = "Content-Type: application/json"', 'data-binary = '+json.dumps(data.decode())]
        value = subprocess.run(['docker', 'exec', '-i', self.container, 'curl', '--config', '-'], input='\n'.join(config)+'\n', capture_output=True, text=True, encoding='utf-8', timeout=8)
        if value.returncode or len(value.stdout) > 262144:
            raise OSError("bounded owned HTTPS acceptance transport failed")
        head, body = value.stdout.split('\n\n', 1)
        headers = {key.title(): value for key, value in (line.split(': ', 1) for line in head.splitlines()[1:] if ': ' in line)}
        return int(head.split()[1]), json.loads(body), headers

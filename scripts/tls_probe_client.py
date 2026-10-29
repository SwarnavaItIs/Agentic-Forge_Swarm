"""Line-bounded trusted HTTPS client for hosts that intercept private TLS.

Runs in an owned API network namespace with only the public CA mounted. Requests
and bearer credentials travel on stdin, never argv, Docker env, or log files.
No shell, provider, arbitrary host, redirect or certificate bypass is available.
"""
import json
import ssl
import sys
import urllib.error
import urllib.request


def main():
    context = ssl.create_default_context(cafile="/ca.pem")
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            return None
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect(), urllib.request.HTTPSHandler(context=context))
    while True:
        line = sys.stdin.buffer.readline(100000)
        if not line:
            return
        try:
            if len(line) >= 100000 or not line.endswith(b"\n"):
                raise ValueError("line limit")
            request = json.loads(line)
            route = request["route"]
            if not route.startswith(("/v1/", "/health/")) or any(x in route for x in ["\r", "\n", "#"]) or len(route) > 4096:
                raise ValueError("route denied")
            headers = {"Host": request["host"]}
            for key in ["Authorization", "Content-Type", "Idempotency-Key"]:
                if key in request["headers"]:
                    headers[key] = request["headers"][key]
            body = None if request["data"] is None else json.dumps(request["data"]).encode()
            if body is not None and len(body) > 65536:
                raise ValueError("body limit")
            req = urllib.request.Request("https://127.0.0.1:8443"+route, data=body, headers=headers, method=request["method"])
            try:
                response = opener.open(req, timeout=5)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                raw = response.read(131073)
                if len(raw) > 131072:
                    raise ValueError("response limit")
                result = {"code": response.status, "data": json.loads(raw)}
        except Exception as error:
            # Error descriptions can contain submitted text; expose class only.
            result = {"transport_error": type(error).__name__}
        print(json.dumps(result), flush=True)


if __name__ == "__main__":
    main()

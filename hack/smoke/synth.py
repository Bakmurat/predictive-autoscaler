"""Labelled SYNTHETIC request history for the install smoke test (hack/smoke-kind.sh): a counter
demo_requests_total{namespace="demo",service="web"} whose rate follows a daily sine (5-15 requests/s). It backfills
eight days into a VictoriaMetrics single node, then keeps writing the current value every 15 s. It also answers HTTP on
port 8000, and HTTPS on 6443 when a certificate is mounted at /tls, as destinations the components' network policies
must NOT reach. This is test data, not evidence."""

import math
import os
import ssl
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

VM = sys.argv[1].rstrip("/")
STEP_S = 30
DAYS = 8
SERIES = 'demo_requests_total{namespace="demo",service="web",origin="synthetic"}'


def rate(t):
    return 10.0 + 5.0 * math.sin(2 * math.pi * (t % 86400) / 86400)


def post(lines):
    req = urllib.request.Request(VM + "/api/v1/import/prometheus", data="\n".join(lines).encode(), method="POST")
    urllib.request.urlopen(req, timeout=30).read()


class Hello(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"synthetic\n")

    def log_message(self, *a):
        pass


def main():
    for _ in range(60):
        try:
            urllib.request.urlopen(VM + "/health", timeout=5).read()
            break
        except Exception:
            time.sleep(2)
    now = int(time.time())
    t, counter, batch = now - DAYS * 86400, 0.0, []
    while t <= now:
        counter += rate(t) * STEP_S
        batch.append(f"{SERIES} {counter:.1f} {t * 1000}")
        if len(batch) >= 5000:
            post(batch)
            batch = []
        t += STEP_S
    post(batch)
    print(f"backfill done: {DAYS} days at {STEP_S}s", flush=True)
    threading.Thread(target=lambda: HTTPServer(("", 8000), Hello).serve_forever(), daemon=True).start()
    if os.path.exists("/tls/tls.crt"):          # HTTPS on the API server's port, at an address no policy allows
        https = HTTPServer(("", 6443), Hello)
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain("/tls/tls.crt", "/tls/tls.key")
        https.socket = ctx.wrap_socket(https.socket, server_side=True)
        threading.Thread(target=https.serve_forever, daemon=True).start()
    last = t - STEP_S
    while True:
        now = time.time()
        counter += rate(now) * (now - last)
        last = now
        try:
            post([f"{SERIES} {counter:.1f} {int(now * 1000)}"])
        except Exception as e:
            print(f"write failed: {e}", flush=True)
        time.sleep(15)


if __name__ == "__main__":
    main()

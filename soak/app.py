"""Soak HTTP workload: counts actual requests to / and exposes them with its own namespace/service labels."""
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

NAMESPACE = os.environ["SOAK_NAMESPACE"]
SERVICE = os.environ["SOAK_SERVICE"]
counter = 0
lock = threading.Lock()


class App(BaseHTTPRequestHandler):
    def do_GET(self):
        global counter
        with lock:
            if self.path == "/":
                counter += 1
            value = counter
        self.send_response(200)
        self.end_headers()
        if self.path == "/metrics":
            self.wfile.write(
                f'demo_requests_total{{namespace="{NAMESPACE}",service="{SERVICE}",origin="soak"}} {value}\n'.encode())
        else:
            self.wfile.write(b"predictive-autoscaler soak workload\n")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("", 8000), App).serve_forever()

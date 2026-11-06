"""Tiny HTTP workload for the local quickstart; counts actual requests to /."""
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

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
                ('demo_requests_total{namespace="demo",service="web",origin="demo"} '
                 f'{value}\n').encode()
            )
        else:
            self.wfile.write(b"Predictive Autoscaler local demo\n")

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    ThreadingHTTPServer(("", 8000), App).serve_forever()

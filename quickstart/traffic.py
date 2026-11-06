"""Send actual demo requests and scrape each replica into the local metrics store."""
import socket
import threading
import time
import urllib.request


def scrape():
    while True:
        try:
            ips = {item[4][0] for item in socket.getaddrinfo(
                "web-metrics.demo.svc", 8000, family=socket.AF_INET,
                type=socket.SOCK_STREAM)}
            lines = []
            for ip in sorted(ips):
                with urllib.request.urlopen(f"http://{ip}:8000/metrics", timeout=3) as r:
                    metric = r.read(4096).decode().strip()
                metric = metric.replace('}', f',pod="{ip}"' + '}')
                lines.append(f"{metric} {int(time.time() * 1000)}")
            req = urllib.request.Request(
                "http://vm.monitoring.svc:8428/api/v1/import/prometheus",
                data=("\n".join(lines) + "\n").encode(), method="POST")
            with urllib.request.urlopen(req, timeout=5) as r:
                r.read()
            print(f"scraped {len(ips)} demo replicas", flush=True)
        except Exception as exc:
            print(f"scrape retry: {exc}", flush=True)
        time.sleep(15)


if __name__ == "__main__":
    threading.Thread(target=scrape, daemon=True).start()
    while True:
        start = time.monotonic()
        try:
            with urllib.request.urlopen("http://web.demo.svc:8000/", timeout=3) as r:
                r.read()
        except Exception as exc:
            print(f"request retry: {exc}", flush=True)
        time.sleep(max(0, 0.125 - (time.monotonic() - start)))

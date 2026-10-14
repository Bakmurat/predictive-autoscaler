"""Soak traffic: a 24-hour cycle with two short daily spikes per workload, plus a scraper that imports each replica's
request counter into the metrics store every 15 s. Generated load for mechanics checks only (soak/README.md)."""
import json
import math
import os
import socket
import threading
import time
import urllib.request

NAMESPACE = os.environ.get("SOAK_NAMESPACE", "pa-soak")
VM = os.environ.get("SOAK_VM", "http://vm.monitoring.svc:8428")
# per workload: low and high requests/s of the daily cycle; the peak is at PEAK_UTC_HOUR
TARGETS = json.loads(os.environ.get("SOAK_TARGETS", '{"active": [2, 14], "recommend": [2, 14], "conflict": [2, 10]}'))
PEAK_UTC_HOUR = float(os.environ.get("SOAK_PEAK_UTC_HOUR", "15"))
SPIKES = [(10.0, 1.8), (20.5, 1.6)]           # (UTC hour, multiplier) for 10 minutes
SPIKE_MINUTES = 10


def rate(low, high, t):
    """Requests/s at epoch t: a cosine day between low and high, with short multiplicative spikes."""
    hour = (t % 86400) / 3600.0
    day = low + (high - low) * (1 + math.cos(2 * math.pi * (hour - PEAK_UTC_HOUR) / 24)) / 2
    for at, mult in SPIKES:
        if 0 <= (hour - at) * 60 < SPIKE_MINUTES:
            day *= mult
    return day


def send(service, low, high):
    url = f"http://{service}.{NAMESPACE}.svc:8000/"
    debt = 0.0
    last = time.monotonic()
    while True:
        now = time.monotonic()
        debt += rate(low, high, time.time()) * (now - last)
        last = now
        while debt >= 1:
            debt -= 1
            try:
                with urllib.request.urlopen(url, timeout=3) as r:
                    r.read()
            except Exception as exc:
                print(f"{service}: request retry: {exc}", flush=True)
        time.sleep(0.02)


def scrape():
    while True:
        lines = []
        try:
            stamp = int(time.time() * 1000)
            for service in TARGETS:
                ips = {i[4][0] for i in socket.getaddrinfo(f"{service}-metrics.{NAMESPACE}.svc", 8000,
                                                            family=socket.AF_INET, type=socket.SOCK_STREAM)}
                for ip in sorted(ips):
                    with urllib.request.urlopen(f"http://{ip}:8000/metrics", timeout=3) as r:
                        metric = r.read(4096).decode().strip()
                    lines.append(metric.replace("}", f',pod="{ip}"' + "}") + f" {stamp}")
            req = urllib.request.Request(f"{VM}/api/v1/import/prometheus", data=("\n".join(lines) + "\n").encode(),
                                         method="POST")
            with urllib.request.urlopen(req, timeout=5) as r:
                r.read()
        except Exception as exc:
            print(f"scrape retry: {exc}", flush=True)
        time.sleep(15)


if __name__ == "__main__":
    threading.Thread(target=scrape, daemon=True).start()
    for svc, (lo, hi) in TARGETS.items():
        threading.Thread(target=send, args=(svc, lo, hi), daemon=True).start()
    while True:
        time.sleep(3600)

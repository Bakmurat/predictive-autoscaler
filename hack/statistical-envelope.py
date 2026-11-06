#!/usr/bin/env python3
"""A resource-envelope run of the statistical forecasting service, offline (in process, fake cluster and backend).

Drives N autoscalers through H hours of decisions (one /v2/predict per decision origin each, with matured windows,
calibration, trust and the journal) and reports peak RSS, CPU time, journal bytes and request latency as JSON. With
--check it compares them to the design's acceptance budgets (C2, section 3: working set <= 384 MiB, foreground p95 < 2 s,
the journal within its quota) and exits 1 on a breach.

This is an estimate of the service's own cost, not the container envelope. The release measurement runs the built image
(ml-engine/Dockerfile.statistical) on the aspire kind bed with the chart's statistical profile (requests 256Mi, limits
512Mi and 1 CPU), 8 autoscalers at a 1-minute cadence and the fit worker on, reading the pod's
container_memory_working_set_bytes, container_cpu_usage_seconds_total and the state volume's usage for at least 24 hours
(docs/helm.md, "Statistical forecaster profile").

Usage: hack/statistical-envelope.py [--pas 8] [--interval 60] [--hours 6] [--history-days 2] [--fit-mode off|worker]
                                    [--state-dir DIR] [--check] [--cgroup-peak]
In the image with fitting on, under the profile's limits: hack/statistical-container-envelope.sh (needs docker).
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import resource
import sys
import tempfile
import time
import urllib.parse
from pathlib import Path

_HERE = Path(__file__).resolve().parents[1] / "ml-engine"
sys.path.insert(0, str(_HERE) if _HERE.is_dir() else "/app")        # the checkout, or inside the image
os.environ["KUBECONFIG"] = "/dev/null"

from api.identity import CONTRACT  # noqa: E402
from forecasting import contracts as c  # noqa: E402
from forecasting.clock import ManualClock  # noqa: E402
from forecasting.service import ForecastService, ServiceConfig  # noqa: E402

DAY = 86400
BUDGET = {"peak_rss_mib": 384, "p95_seconds": 2.0, "cgroup_memory_peak_mib": 384}


def query_for(i):
    return f'sum(rate(istio_requests_total{{destination_workload="web-{i}",destination_workload_namespace="shop"}}[5m]))'


def pa_object(i, interval, lead, history_days):
    q = query_for(i)
    return {"metadata": {"uid": f"pa-{i}", "generation": 1},
            "spec": {"targetDeployment": {"name": f"web-{i}", "namespace": "shop"},
                     "prediction": {"sampleIntervalSeconds": interval, "historyDays": history_days,
                                    "horizonMinutes": 60}},
            "status": {"metricSource": {"query": q, "sha256": hashlib.sha256(q.encode()).hexdigest(),
                                        "observedGeneration": 1, "targetUID": f"dep-{i}", "contract": CONTRACT},
                       "prediction": {"lead": {"seconds": lead, "startupDelaySeconds": 300,
                                               "actuationAllowanceSeconds": 30, "method": "configured",
                                               "sampleCount": 0, "observedGeneration": 1}}}}


class Reader:
    def __init__(self, objects):
        self.objects = objects

    def get(self, path):
        name = path.rsplit("/", 1)[1]
        if "predictiveautoscalers" in path:
            return self.objects[name]
        return {"metadata": {"uid": "dep-" + name.split("-")[1]}}


def backend(url, **kw):
    q = dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(url).query))
    seed = int(hashlib.sha256(q["query"].encode()).hexdigest()[:4], 16)
    step = int(q["step"].rstrip("s"))
    vals = [[t, str(round(30 + 15 * math.sin(2 * math.pi * (t % DAY) / DAY + seed) + ((t * 2654435761 + seed) % 997)
                          / 400, 4))] for t in range(int(q["start"]), int(q["end"]) + 1, step)]
    return 200, json.dumps({"status": "success", "data": {"resultType": "matrix",
                                                          "result": [{"values": vals}]}}).encode()


def peak_rss_mib() -> float:
    """This process's resident high-water mark. VmHWM (Linux) belongs to the current program image; ru_maxrss would
    also carry a parent's peak across fork and exec (a harness started from a TensorFlow test run)."""
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1]) / 1024
    except OSError:
        pass
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024


def cgroup_peak_mib():
    """The container's memory high-water mark (cgroup v2, every process: the service AND its fit workers)."""
    for path in ("/sys/fs/cgroup/memory.peak", "/sys/fs/cgroup/memory/memory.max_usage_in_bytes"):
        try:
            with open(path) as fh:
                return round(int(fh.read().strip()) / (1 << 20), 1)
        except (OSError, ValueError):
            continue
    return None


def wait_for_fits(svc, limit_s=60.0) -> None:
    """Real fits have a whole decision interval (and a 30 s deadline): give the in-flight ones that time, so the
    compressed simulation does not outrun them."""
    end = time.monotonic() + limit_s
    while svc.worker is not None and svc.worker.busy() and time.monotonic() < end:
        time.sleep(0.05)


def run(pas=8, interval=60, hours=6.0, history_days=2, fit_mode="off", state_dir=None, cgroup=False, fits=None):
    lead = max(2 * interval, -(-(300 + interval + 30 + interval) // interval) * interval)
    objects = {f"web-{i}-pa": pa_object(i, interval, lead, history_days) for i in range(pas)}
    start = 1_793_923_200 - 1_793_923_200 % DAY + 3 * DAY
    clock = ManualClock(start * 1000)
    tmp = None
    if state_dir is None:
        tmp = tempfile.TemporaryDirectory()
        state_dir = tmp.name
    svc = ForecastService(ServiceConfig(prometheus_url="http://prom", state_dir=state_dir, fit_mode=fit_mode,
                                        max_autoscalers=max(8, pas)), reader=Reader(objects), fetch=backend,
                          clock=clock)
    if fit_mode == "worker" and fits is not None:              # tests: another (e.g. failing) fit recipe
        from forecasting.worker import FitWorker
        svc.worker.close()
        svc.worker = FitWorker(svc.artifacts, fits=fits)
    latencies, failures = [], 0
    fit_failures, statistical = {}, 0
    cpu0 = time.process_time()
    try:
        for k in range(int(hours * 3600 // interval)):
            origin = start + k * interval
            wait_for_fits(svc)
            clock.set(origin * 1000 + 5000)
            for name, pa in objects.items():
                settings = c.resolve_settings(pa).fingerprint()
                ms = pa["status"]["metricSource"]
                body = {"autoscaler_namespace": "shop", "autoscaler_name": name, "autoscaler_uid": pa["metadata"]["uid"],
                        "autoscaler_generation": 1, "target_uid": ms["targetUID"], "metric_query_sha256": ms["sha256"],
                        "contract": CONTRACT, "application": pa["spec"]["targetDeployment"]["name"],
                        "namespace": "shop", "forecast_contract": c.FORECAST_CONTRACT,
                        "decision_origin": c.format_ts(origin), "settings_sha256": settings}
                t0 = time.perf_counter()
                try:
                    svc.predict(body)
                except Exception:
                    failures += 1
                latencies.append(time.perf_counter() - t0)
        journal = svc.store.usage_bytes() if svc.store else None
        wait_for_fits(svc)
        fit_failures = {k: v for k, v in (svc.worker.failures if svc.worker else {}).items() if v}
        statistical = sum(1 for i in (svc.store.issuances() if svc.store else [])
                          if (i.payload()["candidates"].get("statistical") or {}).get("artifact_sha256"))
    finally:
        svc.close()
        if tmp is not None:
            tmp.cleanup()
    latencies.sort()
    q = lambda p: latencies[min(len(latencies) - 1, int(p * len(latencies)))] if latencies else None  # noqa: E731
    return {"autoscalers": pas, "interval_seconds": interval, "hours": hours, "requests": len(latencies),
            "failures": failures, "peak_rss_mib": round(peak_rss_mib(), 1),
            "cpu_seconds": round(time.process_time() - cpu0, 3), "journal_bytes": journal,
            "cgroup_memory_peak_mib": cgroup_peak_mib() if cgroup else None,
            "fit_mode": fit_mode, "fit_failures": fit_failures, "statistical_issuances": statistical,
            "p50_seconds": q(0.50), "p95_seconds": q(0.95), "max_seconds": latencies[-1] if latencies else None}


def breaches(out, cgroup=False):
    """What fails the run: a budget exceeded; a failed request; with fitting on, any failed fit or no issuance that
    used a fitted artifact (the seasonal-naive fallback must not hide a broken fit path); a requested cgroup
    measurement that is unavailable."""
    found = [k for k, limit in BUDGET.items() if out[k] is not None and out[k] > limit]
    if out["failures"]:
        found.append("failures")
    if out["fit_mode"] == "worker":
        if out["fit_failures"]:
            found.append("fit_failures")
        if out["statistical_issuances"] < 1:
            found.append("no_statistical_issuances")
    if cgroup and out["cgroup_memory_peak_mib"] is None:
        found.append("cgroup_unavailable")
    return found


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--pas", type=int, default=8)
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--hours", type=float, default=6.0)
    ap.add_argument("--history-days", type=int, default=2)
    ap.add_argument("--fit-mode", choices=("off", "worker"), default="off")
    ap.add_argument("--state-dir")
    ap.add_argument("--check", action="store_true")
    ap.add_argument("--cgroup-peak", action="store_true", help="report the container cgroup's memory peak (run "
                                                                 "inside the container: it includes the fit workers)")
    a = ap.parse_args(argv)
    out = run(a.pas, a.interval, a.hours, a.history_days, a.fit_mode, a.state_dir, a.cgroup_peak)
    out["breaches"] = breaches(out, cgroup=a.cgroup_peak)
    print(json.dumps(out, indent=2))
    return 1 if a.check and out["breaches"] else 0


if __name__ == "__main__":
    sys.exit(main())

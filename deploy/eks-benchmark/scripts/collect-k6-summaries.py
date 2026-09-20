#!/usr/bin/env python3
"""Hourly load summaries for the benchmark generators, derived from Prometheus (protocol v4 §3).

For each app and UTC hour: planned requests (pattern rate x minutes the generator was scheduled),
delivered requests as counted by k6 itself (k6_http_reqs_total, pushed by remote write), dropped
iterations (k6_dropped_iterations_total), failed requests (k6_http_reqs_total{expected_response="false"}),
and requests observed by the destination sidecar (istio_requests_total{reporter="destination"}).
Gate (protocol v4 §3): delivered within ±5 % of planned; dropped ≤ 0.05 % of planned; failed ≤ 0.1 % of
delivered; destination-observed within ±5 % of delivered. A missing series fails the hour.

Usage: collect-k6-summaries.py [--hour H] [--date YYYY-MM-DD] [--prom http://localhost:9090] [--json out]
Default: the last completed UTC hour; --port-forward opens a temporary port-forward to Prometheus.
"""
import argparse, json, subprocess, sys, time, datetime, urllib.request, urllib.parse

PATTERN = {0: 250, 1: 200, 2: 150, 3: 150, 4: 200, 5: 300, 6: 750, 7: 1250, 8: 2000, 9: 3000, 10: 3750, 11: 4250,
           12: 4500, 13: 5000, 14: 5500, 15: 6000, 16: 5000, 17: 4250, 18: 3000, 19: 2400, 20: 1000, 21: 600, 22: 400, 23: 300}
APPS = ("nginx-test", "nginx-reactive", "myapptwo")


def q(prom, query, at):
    url = prom + "/api/v1/query?" + urllib.parse.urlencode({"query": query, "time": at})
    d = json.load(urllib.request.urlopen(url, timeout=60))
    if d.get("status") != "success":
        raise RuntimeError(d)
    r = d["data"]["result"]
    return float(r[0]["value"][1]) if r else None


def main():
    ap = argparse.ArgumentParser(); ap.add_argument("--hour", type=int); ap.add_argument("--date"); ap.add_argument("--prom", default="http://localhost:9090")
    ap.add_argument("--port-forward", action="store_true"); ap.add_argument("--context", default="predictive-bench"); ap.add_argument("--json")
    ap.add_argument("--generator-start", help="ISO time the long-running generators started (partial first hour)")
    a = ap.parse_args()
    now = datetime.datetime.now(datetime.timezone.utc)
    end = now.replace(minute=0, second=0, microsecond=0)
    if a.hour is not None:
        day = datetime.date.fromisoformat(a.date) if a.date else now.date()
        end = datetime.datetime(day.year, day.month, day.day, a.hour, tzinfo=datetime.timezone.utc) + datetime.timedelta(hours=1)
        if end > now:
            print(f"hour {a.hour:02d}Z is not complete yet"); sys.exit(2)
    start = end - datetime.timedelta(hours=1); hour = start.hour
    pf = None
    if a.port_forward:
        port = a.prom.rsplit(":", 1)[-1]
        pf = subprocess.Popen(["kubectl", "--context", a.context, "-n", "monitoring", "port-forward", "svc/kps-kube-prometheus-stack-prometheus", f"{port}:9090"],
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(20):
            try:
                urllib.request.urlopen(a.prom + "/-/ready", timeout=2); break
            except Exception:
                time.sleep(1)
    gen_start = datetime.datetime.fromisoformat(a.generator_start.replace("Z", "+00:00")) if a.generator_start else None
    rows, verdict = [], True
    try:
        for app in APPS:
            sched_s = 3600.0
            if gen_start and gen_start > start:
                sched_s = max(0.0, (end - gen_start).total_seconds())
            planned = PATTERN[hour] * sched_s / 60.0
            at = end.timestamp()
            # every counter is read over the SAME window (the seconds the generator was scheduled in this
            # hour), so a partial first hour compares like with like
            w = f"{int(sched_s)}s"
            delivered = q(a.prom, f'sum(increase(k6_http_reqs_total{{testid="{app}"}}[{w}]))', at)
            dropped = q(a.prom, f'sum(increase(k6_dropped_iterations_total{{testid="{app}"}}[{w}]))', at) or 0.0
            failed = q(a.prom, f'sum(increase(k6_http_reqs_total{{testid="{app}",expected_response="false"}}[{w}]))', at) or 0.0
            observed = q(a.prom, f'sum(increase(istio_requests_total{{reporter="destination",destination_workload="{app}",destination_workload_namespace="demo"}}[{w}]))', at)
            p95_s = q(a.prom, f'max(k6_http_req_duration_p95{{testid="{app}"}})', at)   # k6 remote-write exports durations in seconds
            p95 = round(p95_s * 1000, 3) if p95_s is not None else None
            gate = {"k6_series_present": delivered is not None, "destination_series_present": observed is not None,
                    "delivered_within_5pct": delivered is not None and planned > 0 and abs(delivered / planned - 1) <= 0.05,
                    "dropped_le_0.05pct": planned > 0 and dropped / planned <= 0.0005,
                    "failed_le_0.1pct": (delivered or 0) > 0 and failed / delivered <= 0.001,
                    "observed_within_5pct_of_delivered": delivered and observed is not None and abs(observed / delivered - 1) <= 0.05}
            ok = all(gate.values()); verdict = verdict and ok
            rows.append({"target": app, "hour_start": start.strftime("%Y-%m-%dT%H:%M:%SZ"), "scheduled_seconds": sched_s, "planned_rpm": PATTERN[hour],
                         "planned_requests": round(planned), "k6_delivered": None if delivered is None else round(delivered),
                         "delivered_pct": None if delivered is None or not planned else round(100 * delivered / planned, 2),
                         "dropped": round(dropped), "dropped_pct": round(100 * dropped / planned, 3) if planned else None,
                         "failed": round(failed), "failed_pct": round(100 * failed / delivered, 3) if delivered else None,
                         "destination_observed": None if observed is None else round(observed), "p95_ms": p95, "gate": gate, "pass": ok})
    finally:
        if pf:
            pf.terminate()
    for r in rows:
        print(f"{r['target']} {r['hour_start']}: planned={r['planned_requests']} k6={r['k6_delivered']} ({r['delivered_pct']}%) dropped={r['dropped']} ({r['dropped_pct']}%) "
              f"failed={r['failed']} ({r['failed_pct']}%) destination={r['destination_observed']} p95={r['p95_ms']}ms -> "
              f"{'PASS' if r['pass'] else 'FAIL ' + ','.join(k for k, v in r['gate'].items() if not v)}")
    print(f"hour {start.strftime('%Y-%m-%dT%HZ')} gate: {'PASS' if verdict else 'FAIL'}")
    if a.json:
        json.dump({"hour_start": start.isoformat(), "collected_at": now.isoformat(), "rows": rows, "gate_pass": verdict}, open(a.json, "w"), indent=2)
    sys.exit(0 if verdict else 1)


if __name__ == "__main__":
    main()

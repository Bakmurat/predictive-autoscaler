#!/usr/bin/env python3
"""Infrastructure-event detector for the prodcluster campaign (evaluation protocol P8, draft v2; Codex r27/r30).

Evaluated after the stop and before any score is looked at. Declared sources only (VictoriaMetrics, 30-day retention):
kube-state-metrics node conditions, pod status reasons, PodScheduled; k6 heartbeats and generator pod lifecycle; the
validity mask; optionally the hourly load-gate rows. Output: IE intervals (classes a-e), unknown intervals (a source was
missing), load-gate hours that are not verified-clean, and a ten-minute target-slot classification
(invalid / unknown / unverified / verified_clean) with the declared 60-minute capacity-recovery washout.

Limits (declared): preemption and the scheduler's Unschedulable reason are not exported by this cluster's
kube-state-metrics, so (b) covers kubelet/node reasons only and (c) uses PodScheduled=False for more than 120 s.
A load-gate FAIL is listed for attribution, never an IE by itself (it may be an arm's own failure).
"""
import argparse, datetime, glob, json, os, sys, urllib.parse, urllib.request

SLOT = 600
WASHOUT = 3600                     # declared capacity-recovery assumption after an IE ends
SCHED_ALERT = 120                  # PodScheduled=False longer than this is a scheduling blockage
NAMESPACES = "demo|ml-engine"
APPS = ["nginx-test", "nginx-reactive", "myapptwo", "nginx-seasonal", "nginx-ensemble", "nginx-ensemble-q95"]
POD_REASONS = "Evicted|NodeLost|Shutdown|NodeAffinity|UnexpectedAdmissionError"


def iso(t):
    return datetime.datetime.fromtimestamp(t, datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(s):
    return int(datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=datetime.timezone.utc).timestamp())


class VM:
    def __init__(self, base):
        self.base = base.rstrip("/")

    def range(self, query, start, end, step):
        """[(labels, [t, ...] where the expression has a value)]; a failed or partial query raises."""
        url = self.base + "/api/v1/query_range?" + urllib.parse.urlencode(
            {"query": query, "start": start, "end": end, "step": step})
        body = json.load(urllib.request.urlopen(url, timeout=120))
        if body.get("status") != "success" or body.get("isPartial"):
            raise RuntimeError(f"query failed or partial: {query}")
        return [(r["metric"], [int(float(t)) for t, _ in r["values"]]) for r in body["data"]["result"]]


def runs(ts, step):
    """Contiguous runs [(first, last)] of sorted timestamps spaced by at most step."""
    out = []
    for t in sorted(ts):
        if out and t - out[-1][1] <= step:
            out[-1][1] = t
        else:
            out.append([t, t])
    return [tuple(r) for r in out]


def bracket(first, last, step):
    """Outward bracketing: the last healthy observation lies at most one step before the first evidence, the first
    healthy one at most one step after the last."""
    return first - step, last + step


def detect(vm, start, stop, step=30, mask=None):
    events, unknown = [], []

    def add(cls, kind, subject, a, b, evidence):
        events.append({"class": cls, "kind": kind, "subject": subject, "start": a, "end": b, "evidence": evidence})

    # sources present? (missing detector data never means "no incident")
    for name, q in (("kube-state-metrics", 'max(up{job="kube-state-metrics"})'),):
        present = set()
        for _, ts in vm.range(q + " == 1", start, stop, step):
            present |= set(ts)
        grid = range(start - start % step, stop, step)
        missing = [t for t in grid if t not in present]
        for a, b in runs(missing, step):
            unknown.append({"source": name, "start": a, "end": b + step})
    # (a) node not ready / pressure
    q = ('kube_node_status_condition{condition="Ready",status=~"false|unknown"} == 1 or '
         'kube_node_status_condition{condition=~"DiskPressure|MemoryPressure|PIDPressure",status="true"} == 1')
    for m, ts in vm.range(q, start, stop, step):
        for f, l in runs(ts, step):
            a, b = bracket(f, l, step)
            add("a", f"{m.get('condition')}={m.get('status')}", m.get("node"), a, b, q)
    # (b) benchmark pod evicted / lost with a kubelet or node reason
    q = f'kube_pod_status_reason{{namespace=~"{NAMESPACES}",reason=~"{POD_REASONS}"}} == 1'
    for m, ts in vm.range(q, start, stop, step):
        for f, l in runs(ts, step):
            a, b = bracket(f, l, step)
            add("b", m.get("reason"), f"{m.get('namespace')}/{m.get('pod')}", a, b, q)
    # (c) scheduling blockage: PodScheduled=False for more than SCHED_ALERT seconds
    q = f'kube_pod_status_scheduled{{namespace=~"{NAMESPACES}",condition="false"}} == 1'
    for m, ts in vm.range(q, start, stop, step):
        for f, l in runs(ts, step):
            if l - f + step > SCHED_ALERT:
                a, b = bracket(f, l, step)
                add("c", "PodScheduled=False>120s", f"{m.get('namespace')}/{m.get('pod')}", a, b, q)
    # (d) generator disturbance: k6 container restarts, heartbeat gaps (outside declared mask intervals)
    q = 'increase(kube_pod_container_status_restarts_total{namespace="demo",container="k6"}[2m]) > 0'
    for m, ts in vm.range(q, start, stop, step):
        for f, l in runs(ts, step):
            a, b = bracket(f - 120, l, step)
            add("d", "k6 container restart", m.get("pod"), a, b, q)
    for app in APPS:
        q = f'absent_over_time(k6_vus{{testid="{app}"}}[30s])'
        for _, ts in vm.range(q, start, stop, step):
            for f, l in runs(ts, step):
                a, b = bracket(f - 30, l, step)
                add("d", "k6 heartbeat gap", app, a, b, q)
    # (e) metrics-plane gaps and declared disturbances recorded in the validity mask
    for iv in (mask or {}).get("intervals", []):
        a, b = parse(iv["start"]), parse(iv["end"])
        if b > start and a < stop:
            add("e", "validity-mask interval", "all arms", a, b, iv.get("reason", ""))
    # declared mask intervals explain generator disturbances inside them
    declared = [(e["start"], e["end"]) for e in events if e["class"] == "e"]
    events = [e for e in events if not (e["class"] == "d" and any(a <= e["start"] and e["end"] <= b for a, b in declared))]
    return events, unknown


def gate_hours(directory):
    """Latest load-gate projections: {hour_start: {app: status}}."""
    out = {}
    for f in sorted(glob.glob(os.path.join(directory, "load-2*.json"))):
        for r in json.load(open(f)):
            out.setdefault(parse(r["hour_start"]), {})[r["app"]] = r["status"]
    return out


def classify(events, unknown, start, stop, gates=None):
    """Ten-minute target slots in [start, stop): invalid (inside an IE, rounded outward to the grid, or its washout),
    unknown (a source missing), unverified (load-gate hour not PASS on every arm), else verified_clean."""
    lo = lambda t: t - t % SLOT
    hi = lambda t: t if t % SLOT == 0 else t - t % SLOT + SLOT
    bad = [(lo(e["start"]), hi(e["end"]) + WASHOUT) for e in events]
    unk = [(lo(u["start"]), hi(u["end"])) for u in unknown]
    slots = {}
    for t in range(hi(start), stop, SLOT):
        if any(a <= t < b for a, b in bad):
            c = "invalid"
        elif any(a <= t < b for a, b in unk):
            c = "unknown"
        elif gates is not None:
            g = gates.get(t - t % 3600, {})
            c = "verified_clean" if len(g) == len(APPS) and all(v == "PASS" for v in g.values()) else "unverified"
        else:
            c = "verified_clean"
        slots[t] = c
    counts = {k: sum(1 for v in slots.values() if v == k) for k in ("invalid", "unknown", "unverified", "verified_clean")}
    total = len(slots)
    return slots, dict(counts, total=total,
                       compromised=bool(total) and (counts["invalid"] + counts["unknown"]) > 0.2 * total)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--prom", required=True)
    ap.add_argument("--start", required=True, help="first scored instant (UTC, ...Z)")
    ap.add_argument("--stop", required=True)
    ap.add_argument("--mask")
    ap.add_argument("--load-gate-dir")
    ap.add_argument("--json", required=True)
    a = ap.parse_args()
    start, stop = parse(a.start), parse(a.stop)
    mask = json.load(open(a.mask)) if a.mask else None
    events, unknown = detect(VM(a.prom), start, stop, mask=mask)
    gates = gate_hours(a.load_gate_dir) if a.load_gate_dir else None
    slots, summary = classify(events, unknown, start, stop, gates)
    fails = sorted({(iso(h), app) for h, g in (gates or {}).items() for app, s in g.items() if s == "FAIL"})
    out = {"detector": "infra_events.py v1", "start": a.start, "stop": a.stop, "summary": summary,
           "events": [dict(e, start=iso(e["start"]), end=iso(e["end"])) for e in events],
           "unknown": [dict(u, start=iso(u["start"]), end=iso(u["end"])) for u in unknown],
           "load_gate_fail_hours_for_attribution": fails,
           "slots": {iso(t): c for t, c in sorted(slots.items())}}
    json.dump(out, open(a.json, "w"), indent=1)
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())

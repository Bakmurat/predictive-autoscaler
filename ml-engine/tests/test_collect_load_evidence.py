"""deploy/prodcluster/collect_load_evidence.py v6: server-side gate, inventory since a declared start, verified snapshots.

Fixture timing follows prodcluster measurements (2026-10-05): k6 pushes every 10 s (k6_vus at x.475 s, a changed
counter stamped with its latest event); Envoy targets scraped every 30 s with `up` and every series sharing the scrape
timestamp; kube-state-metrics every 20 s; a terminating arm pod pushes bench_final_istio_* and a receipt (snapshot v2)
at its capture second. Cases from Codex Task 03 r15-r20.
"""
import datetime
import hashlib
import importlib.util
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location(
    "collect_load_evidence", os.path.join(HERE, "..", "..", "deploy", "prodcluster", "collect_load_evidence.py"))
cle = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cle)
import challenge_profile  # noqa: E402

T0 = 1_790_600_400.0
T1 = T0 + 3600
END = T1 + cle.GRACE
HOUR = datetime.datetime.fromtimestamp(T0, datetime.timezone.utc)
NOW = END + 60
T_INV = T0 - 7200
K6_START = T0 - 7200 + 0.3
APPROVED = {"A1'": {"approved_by": "user", "at": "test"}}


def grid(start, stop, step, offset):
    out, t = [], start + offset
    while t <= stop:
        out.append(round(t, 3)); t += step
    return out


# ---------- pure functions ----------

def test_k6_bounds_general_zero_and_unbounded_tail():
    assert cle.k6_bounds([(T0 - 100, 5.0), (T1 + 700, 9.0)], 0.0, T0, T1, cle.LAG) == (0.0, 4.0)
    assert cle.k6_bounds([(T0 - 100, 5.0)], 0.0, T0, T1, cle.LAG) == (0.0, 0.0)
    assert cle.k6_bounds([], 3.0, T0, T1, cle.LAG) == (0.0, 0.0)
    for late in (T1 + 100, T0 + 10):
        with pytest.raises(cle.Incomplete):
            cle.k6_bounds([(late, 7.0)], 0.0, T0, T1, cle.LAG)
    with pytest.raises(cle.Incomplete):
        cle.k6_bounds([(T0 + 100, 2.0)], 10.0, T0, T1, cle.LAG)


def test_r18_ms_flooring_an_event_just_inside_the_hour_is_never_zero():
    with pytest.raises(cle.Incomplete):
        cle.k6_bounds([(T0, 1.0)], 0.0, T0, T1, cle.LAG)
    assert cle.k6_bounds([(T0 - 0.001, 1.0)], 0.0, T0, T1, cle.LAG) == (0.0, 0.0)


def test_envoy_birth_interval_and_edges():
    sc = grid(T0 - 180, END, 30, 0.659)
    lo, hi = cle.target_series_bounds([(t, t - (T0 - 3600)) for t in sc], sc, T0, T1, T0 - 3600, None)
    assert 3570 <= lo <= 3600 <= hi <= 3630
    born = grid(T0 + 1210, END, 30, 0.0)
    assert cle.target_series_bounds([(t, 1.0) for t in born], born, T0, T1, T0 + 1200, None) == (1.0, 1.0)
    assert cle.target_series_bounds([(t, 1.0) for t in born], born, T0, T1, T0 - 5, None) == (0.0, 1.0)
    late = grid(T1 + 30, END, 30, 0.0)
    assert cle.target_series_bounds([(t, 9.0) for t in late], late, T0, T1, T1 + 20, None) == (0.0, 0.0)


def test_envoy_snapshot_closes_a_pod_and_capture_second_is_an_interval():
    sc = grid(T0 - 180, T0 + 1800, 30, 0.659)
    vals = [(t, t - (T0 - 3600)) for t in sc]
    F = vals[-1][1] + 7.0
    lo, hi = cle.target_series_bounds(vals, sc, T0, T1, T0 - 3600, (T0 + 1815.0, F))
    assert 0 < lo and hi - lo < 40
    with pytest.raises(cle.Incomplete):
        cle.target_series_bounds(vals, sc, T0, T1, T0 - 3600, None)
    with pytest.raises(cle.Incomplete):
        cle.target_series_bounds(vals, sc, T0, T1, T0 - 3600, (T0 + 1815.0, F - 20))
    late = sc + [T0 + 1830.659]
    with pytest.raises(cle.Incomplete):
        cle.target_series_bounds(vals + [(T0 + 1830.659, F + 3)], late, T0, T1, T0 - 3600, (T0 + 1815.0, F))
    pre = [t for t in sc if t < T0]
    assert cle.target_series_bounds([(t, 10.0) for t in pre], pre, T0, T1, T0 - 3600, (T0 - 5.0, 12.0)) == (0.0, 0.0)
    # captured in the second [t0-1, t0)? only c + 1 <= t0 counts as before the hour; c = t0 - 0.5 does not
    lo, hi = cle.target_series_bounds([(t, 10.0) for t in pre], pre, T0, T1, T0 - 3600, (T0 - 0.5, 12.0))
    assert (lo, hi) == (0.0, 2.0)
    # captured in [t1-0.5, t1+0.5): N(<t1) is only bracketed by the last scrape before t1 and F
    near = grid(T0 - 180, T1 - 30, 30, 0.659)
    lo, hi = cle.target_series_bounds([(t, 100.0) for t in near], near, T0, T1, T0 - 3600, (T1 - 0.5, 110.0))
    assert (lo, hi) == (0.0, 10.0)


def snapshot(pod, uid="u-1", version="2", n_extra=0, digest=None, conflict=False, uptime=900.0, c=T0 + 1815.0, values=(721.0, 700.0, 721.0)):
    payload = [({"__name__": "bench_final_istio_requests_total", "reporter": "destination", "response_code": "200",
                 "pod": pod, "pod_uid": uid, "namespace": "demo", "snapshot_version": version}, values[0]),
               ({"__name__": "bench_final_istio_request_duration_milliseconds_bucket", "reporter": "destination", "le": "5",
                 "pod": pod, "pod_uid": uid, "namespace": "demo", "snapshot_version": version}, values[1]),
               ({"__name__": "bench_final_istio_request_duration_milliseconds_bucket", "reporter": "destination", "le": "+Inf",
                 "pod": pod, "pod_uid": uid, "namespace": "demo", "snapshot_version": version}, values[2]),
               ({"__name__": "bench_final_istio_request_duration_milliseconds_sum", "reporter": "destination",
                 "pod": pod, "pod_uid": uid, "namespace": "demo", "snapshot_version": version}, 1234.5)]
    canon = sorted([f'istio_request_duration_milliseconds_bucket{{le="5",reporter="destination"}} {int(values[1])}',
                    f'istio_request_duration_milliseconds_bucket{{le="+Inf",reporter="destination"}} {int(values[2])}',
                    f'istio_requests_total{{reporter="destination",response_code="200"}} {int(values[0])}'])
    d = digest or hashlib.sha256("".join(x + "\n" for x in canon).encode()).hexdigest()
    lab = {"series": str(4 + n_extra), "canonical_series": "3", "canonical_sha256": d, "proxy_uptime_s": str(uptime),
           "hot_restart_epoch": "0", "pod": pod, "pod_uid": uid, "namespace": "demo", "snapshot_version": version}
    receipts = [(lab, c)] + ([(dict(lab, polls="9"), c + 3)] if conflict else [])
    return receipts, payload


def test_check_snapshot_identity_digest_epoch_and_version():
    r, p = snapshot("x")
    assert cle.check_snapshot(r, p, T0 + 1815.0 - 900) == (T0 + 1815.0, "u-1")
    bad = [snapshot("x", conflict=True), snapshot("x", n_extra=1), snapshot("x", digest="0" * 64), snapshot("x", version="1")]
    r2, p2 = snapshot("x"); p2[0][0]["pod_uid"] = "other"; bad.append((r2, p2))                 # payload of another UID
    r3, p3 = snapshot("x"); p3.append((dict(p3[0][0]), 721.0)); r3[0][0]["series"] = "5"; bad.append((r3, p3))  # duplicate
    for receipts, payload in bad:
        with pytest.raises(cle.Incomplete):
            cle.check_snapshot(receipts, payload, T0 + 1815.0 - 900)
    assert cle.check_snapshot(*snapshot("x"), T0 + 1815.0 - 900 - 12)[1] == "u-1"   # Envoy up 12 s after its container
    for started in (T0 + 1815.0 - 900 + 5, T0 + 1815.0 - 900 - 40):         # Envoy older than its container / far later
        with pytest.raises(cle.Incomplete):
            cle.check_snapshot(*snapshot("x"), started)
    with pytest.raises(cle.Incomplete):
        cle.check_snapshot(*snapshot("x"), None)                           # no KSM proxy start


def test_gate():
    P = 10000.0
    assert cle.gate(P, (P, P), (0, 0), (0, 0))[0] == "PASS"
    assert cle.gate(P, (0.9 * P, 0.97 * P), (0, 0), (0, 0))[0] == "INCOMPLETE"
    assert cle.gate(P, (0.8 * P, 0.9 * P), (0, 0), (0, 0))[0] == "FAIL"
    assert cle.gate(P, (P, 1.08 * P), (0, 0), (0, 0))[0] == "INCOMPLETE"
    assert cle.gate(P, (P, P), (0, 11), (0, 0))[0] == "INCOMPLETE"
    assert cle.gate(P, (P, P), (0, 0), (6, 6))[0] == "FAIL"


# ---------- fake VictoriaMetrics ----------

class FakeVM:
    def __init__(self, **o):
        self.o = o
        self.rate = challenge_profile.planned_requests(int(T0)) / 3600.0 * o.get("observed_share", 1.0)
        gap = o.get("hb_gap")
        self.hb = [t for t in grid(T0 - 180, END, 10, 0.475) if not (gap and gap[0] < t < gap[1])]
        self.ksm = grid(T_INV, END, 20, 0.486)

    def app_of(self, sel):
        for app in sorted(cle.APPS, key=len, reverse=True):
            for pat in (f'"{app}"', f'"k6-{app}"', f'"{app}-[', f'"k6-{app}-', f'"{app}-aaaa'):
                if pat in sel:
                    return app
        return None

    def pods(self, app):
        o = self.o
        out = {f"{app}-aaaa1111-p1": dict(start=T0 - 3600, ksm_end=END, frm=T0 - 3600, to=END, share=1.0, snap=None)}
        if app != "nginx-test":
            return out
        if o.get("scaled_down"):
            out[f"{app}-aaaa1111-p1"]["share"] = 0.7
            snap = None if o["scaled_down"] == "no_snapshot" else T0 + 1815.0
            out[f"{app}-aaaa1111-p3"] = dict(start=T0 - 3600, ksm_end=T0 + 1840, frm=T0 - 3600, to=T0 + 1800, share=0.3, snap=snap)
        if o.get("born"):
            out[f"{app}-aaaa1111-p1"]["share"] = 0.9
            out[f"{app}-aaaa1111-p2"] = dict(start=T0 + 1200, ksm_end=END, frm=T0 + 1210, to=END, share=0.1, snap=None)
        if o.get("pre_hour"):
            snap = T0 - 5.0 if o["pre_hour"] == "snapshot" else None
            last = {"old": T0 - 600}.get(o["pre_hour"], T0 - 20)
            out[f"{app}-aaaa1111-p0"] = dict(start=T0 - 7000, ksm_end=last + 30, frm=T0 - 7000, to=last, share=0.0, snap=snap)
        if o.get("alive_without_target"):
            out[f"{app}-aaaa1111-p9"] = dict(start=T0 - 3600, ksm_end=END, frm=None, to=None, share=0.0, snap=None)
        return out

    def value(self, p, t):
        d = self.pods(p.rsplit("-", 2)[0])[p]
        if p.endswith("p2"):
            return d["share"] * self.rate * (t - d["start"])
        if d["share"] == 0.0:
            return 5.0
        if p.endswith("p3"):
            return d["share"] * self.rate * (min(t, T0 + 1805) - (T0 - 3600))
        if self.o.get("scaled_down") and t >= T0 + 1805:
            return 0.7 * self.rate * (T0 + 1805 - (T0 - 3600)) + self.rate * (t - (T0 + 1805))
        return d["share"] * self.rate * (t - (T0 - 3600))

    def scrapes(self, d):
        return [] if d["frm"] is None else [t for t in grid(T_INV, END, 30, 0.659) if d["frm"] <= t <= d["to"]]

    def snap_parts(self, app, p):
        d = self.pods(app)[p]
        o, c = self.o, d["snap"]
        k = 0.5 if o.get("snap_low") else 1.0
        F = [self.value(p, c) * k, 0.9 * self.value(p, c) * k, self.value(p, c) * k]
        F = [float(int(x)) for x in F]
        receipts, payload = snapshot(p, uid="u-" + p, c=c, values=F, uptime=c - (d["start"] + 2),
                                     conflict=o.get("snap_conflict", False), digest="0" * 64 if o.get("snap_digest") else None,
                                     version="1" if o.get("snap_v1") else "2")
        if o.get("snap_partial"):
            payload = payload[:-1]
        if o.get("snap_uid"):
            payload[0][0]["pod_uid"] = "someone-else"
        return receipts, payload

    def raw(self, sel, t_end, window):
        o, app = self.o, self.app_of(sel)
        if o.get("partial"):
            raise cle.Incomplete("query failed or partial")
        lo = t_end - window
        name = sel.split("{")[0]
        ksm = [t for t in self.ksm if t > lo]
        k6pod = f"k6-{app}-abcd1234-xyz12"
        pod_q = sel.split('pod="')[1].split('"')[0] if 'pod="' in sel else None
        if name == "kube_replicaset_owner":
            return [({"replicaset": f"k6-{app}-abcd1234"}, [(t, 1.0) for t in ksm], [])]
        if name == "kube_pod_owner":
            return [({"pod": f"k6-{a}-abcd1234-xyz12", "owner_name": f"k6-{a}-abcd1234"}, [(t, 1.0) for t in ksm], []) for a in cle.APPS]
        if name == "kube_pod_start_time" and "k6-" in sel:
            return [({"pod": k6pod}, [(t, K6_START - 5) for t in ksm], [])]
        if name == "kube_pod_container_state_started" and 'container="k6"' in sel:
            st = T0 + 1800 if o.get("late_container") else K6_START
            return [({"pod": k6pod, "container": "k6"}, [(t, st) for t in ksm], [])]
        if name == "kube_pod_container_status_restarts_total" and 'container="k6"' in sel:
            return [({"pod": k6pod, "container": "k6"}, [(t, 1.0 if (o.get("restarted") and t > T0 + 900) else 0.0) for t in ksm], [])]
        arm_pods = self.pods(app)
        if pod_q:
            arm_pods = {pod_q: arm_pods[pod_q]}
        if name in ("kube_pod_container_status_restarts_total", "kube_pod_container_state_started", "kube_pod_start_time", "kube_pod_info"):
            if (o.get("no_ksm") and name in ("kube_pod_info", "kube_pod_start_time")) or (o.get("no_restart_series") and "restarts" in name):
                return []
            gap = (T0 + 600, T0 + 900) if (o.get("restart_gap") and "restarts" in name) else None
            out = []
            for p, d in arm_pods.items():
                if name == "kube_pod_container_status_restarts_total":
                    val = lambda t: 1.0 if (o.get("proxy_restart") and t > T0 + 900) else 0.0
                elif name == "kube_pod_container_state_started":
                    val = lambda t, d=d: d["start"] + 2
                elif name == "kube_pod_start_time":
                    val = lambda t, d=d: d["start"]
                else:
                    val = lambda t: 1.0
                pts = [(t, val(t)) for t in ksm if d["start"] <= t <= d["ksm_end"] and not (gap and gap[0] < t < gap[1])]
                if pts:
                    out.append(({"pod": p, "container": "istio-proxy"}, pts, []))
            return out
        if name == "up":
            out = []
            for i, (p, d) in enumerate(arm_pods.items()):
                sc = [t for t in self.scrapes(d) if t > lo]
                if sc:
                    out.append(({"job": cle.ENVOY_JOB, "instance": f"10.0.0.{i}:15090", "pod": p}, [(t, 1.0) for t in sc], []))
            return out
        if name == "bench_final_snapshot_receipt":
            out = []
            for p, d in arm_pods.items():
                if d["snap"] is not None:
                    for lab, c in self.snap_parts(app, p)[0]:
                        out.append((lab, [(c, c)], []))
            return out
        if sel.startswith('{__name__=~"bench_final_istio_.*"'):
            return [(m, [(self.pods(app)[pod_q]["snap"], v)], []) for m, v in self.snap_parts(app, pod_q)[1]]
        if name in ("bench_final_istio_requests_total", "bench_final_istio_request_duration_milliseconds_bucket"):
            want = name[len("bench_final_"):]
            c = self.pods(app)[pod_q]["snap"]
            return [(m, [(c, v)], []) for m, v in self.snap_parts(app, pod_q)[1]
                    if m["__name__"] == name and (want != "istio_request_duration_milliseconds_bucket" or m.get("le"))]
        if name in ("istio_requests_total", "istio_request_duration_milliseconds_bucket"):
            les = ["5", "+Inf"] if name.endswith("bucket") else [None]
            out = []
            for i, (p, d) in enumerate(arm_pods.items()):
                if d["frm"] is None or d["share"] == 0.0:
                    continue
                for le in les:
                    lab = {"reporter": "destination", "response_code": "200", "pod": p, "namespace": "demo", "container": "istio-proxy"}
                    if le:
                        lab = {"reporter": "destination", "le": le, "pod": p, "namespace": "demo", "container": "istio-proxy"}
                    frac = 0.9 if le == "5" else 1.0
                    job = "other/job" if (o.get("foreign") and p.endswith("p1") and app == "nginx-test") else cle.ENVOY_JOB
                    sc = [t for t in self.scrapes(d) if t > lo]
                    pts = [(t, float(int(frac * self.value(p, t)))) for t in sc]
                    out.append((dict(lab, job=job, instance=f"10.0.0.{i}:15090"), pts, []))
                    if o.get("dup_series") and p.endswith("p1") and app == "nginx-test":
                        out.append((dict(lab, job=job, instance=f"10.0.0.{i}:15090", prometheus="x"), pts, []))
            return out
        if name == "k6_vus":
            return [({"testid": app}, [(t, 30.0) for t in self.hb if t > lo], [])]
        if name == "k6_http_reqs_total" and 'expected_response="false"' in sel:
            if app == "nginx-test" and o.get("failure_in_hour"):
                return [({"testid": app, "expected_response": "false"}, [(T0 + 1000.9, 3.0)] + ([(T1 + 700.9, 4.0)] if o["failure_in_hour"] == "bounded" else []), [])]
            return []
        if name == "k6_http_reqs_total":
            r = challenge_profile.planned_requests(int(T0)) / 3600.0
            return [({"testid": app, "expected_response": "true"}, [(h + 0.9, r * (h - K6_START)) for h in self.hb if h + 0.9 > lo], [])]
        if name == "k6_dropped_iterations_total":
            if app == "nginx-test" and o.get("drop_late"):
                return [({"testid": app}, [(T1 + 100.9, 190.0)], [])]
            return []
        raise AssertionError(sel)

    def instant(self, query, t):
        app = self.app_of(query)
        if query.startswith("count by (pod) (count_over_time("):
            inner = query[len("count by (pod) (count_over_time("):]
            name = inner.split("{")[0]
            pods = []
            for p, d in self.pods(app).items():
                if name == "kube_pod_info" or (name == "up" and d["frm"] is not None) or \
                        (name == "bench_final_snapshot_receipt" and d["snap"] is not None) or \
                        (name == "istio_requests_total" and d["frm"] is not None and d["share"] > 0):
                    pods.append(({"pod": p}, 1.0))
            return pods
        if query.startswith("max by (pod) (max_over_time(up"):
            return [({"pod": p}, 1.0 if d["to"] is not None and d["to"] > T1 else 0.0)
                    for p, d in self.pods(app).items() if d["frm"] is not None and d["to"] >= T0 - 3600]
        if "k6_http_reqs_total" in query and "expected_response" not in query:
            r = challenge_profile.planned_requests(int(T0)) / 3600.0
            return [({"testid": app, "expected_response": "true"}, r * (t - 10 - K6_START))]
        return []


def run(vm, app="nginx-test", now=NOW, ledger=None, approvals=APPROVED):
    ledger = ledger if ledger is not None else {"accepted": {}, "terminated": {}}
    return [r for r in cle.collect(vm, HOUR, T_INV, ledger, approvals, now=now) if r["app"] == app][0]


def test_healthy_hour_passes_and_qualifies_only_with_the_recorded_approval():
    r = run(FakeVM())
    assert r["status"] == "PASS" and r["qualification"]["qualifies"], r
    assert r["failed"] == [0, 0] and r["dropped"] == [0, 0] and r["collector"].endswith("v6")
    assert r["observed"][0] <= r["planned"] <= r["observed"][1] and r["p95_server_ms_interior"] is not None
    q = run(FakeVM(), approvals={})["qualification"]
    assert not q["qualifies"] and q["pending_user_approval"] == ["A1'"]


def test_scale_down_with_a_valid_snapshot_passes_and_is_cached():
    ledger = {"accepted": {}, "terminated": {}}
    r = run(FakeVM(scaled_down="snapshot"), ledger=ledger)
    assert r["status"] == "PASS" and r["final_snapshots_in_hour"] == ["nginx-test-aaaa1111-p3"], r
    assert ledger["accepted"]["nginx-test-aaaa1111-p3"]["pod_uid"] == "u-nginx-test-aaaa1111-p3"


@pytest.mark.parametrize("over,msg", [
    ({"scaled_down": "no_snapshot"}, "has no final snapshot"),
    ({"scaled_down": "snapshot", "snap_conflict": True}, "receipts for one pod"),
    ({"scaled_down": "snapshot", "snap_partial": True}, "payload incomplete"),
    ({"scaled_down": "snapshot", "snap_uid": True}, "another pod UID"),
    ({"scaled_down": "snapshot", "snap_digest": True}, "digest mismatch"),
    ({"scaled_down": "snapshot", "snap_v1": True}, "wrong version"),
    ({"scaled_down": "snapshot", "snap_low": True}, "inconsistent"),
    ({"pre_hour": "none"}, "has no final snapshot"),
    ({"pre_hour": "old"}, "has no final snapshot"),
    ({"alive_without_target": True}, "has no final snapshot"),
    ({"no_ksm": True}, "no kube-state-metrics pod record"),
    ({"no_restart_series": True}, "restart counter"),
    ({"proxy_restart": True}, "restart counter"),
    ({"restart_gap": True}, "restart counter not observed"),
    ({"foreign": True}, "without its own Envoy target"),
    ({"dup_series": True}, "duplicate"),
    ({"hb_gap": (T1 + 300, T1 + 330)}, "lost flush"),
    ({"drop_late": True}, "tail yet"),
    ({"failure_in_hour": "unbounded"}, "tail yet"),
    ({"late_container": True}, "started inside the hour"),
    ({"restarted": True}, "changed in the hour"),
])
def test_incomplete_paths(over, msg):
    r = run(FakeVM(**over))
    assert r["status"] == "INCOMPLETE" and msg in r["reason"] and not r["qualification"]["qualifies"], (over, r)


def test_pods_closed_before_the_hour():
    assert run(FakeVM(pre_hour="snapshot"))["status"] == "PASS"
    ledger = {"accepted": {}, "terminated": {"nginx-test-aaaa1111-p0": {"terminated_before": T0 - 500, "record": "manual"}}}
    r = run(FakeVM(pre_hour="old"), ledger=ledger)
    assert r["status"] == "PASS" and r["pods_closed_before_hour"] == 1, r
    ledger["terminated"]["nginx-test-aaaa1111-p0"]["terminated_before"] = T0 + 100      # inside the hour: unknown tail
    r = run(FakeVM(pre_hour="old"), ledger=ledger)
    assert r["status"] == "INCOMPLETE" and "manual termination record inside the hour" in r["reason"], r


def test_born_pod_k6_counters_and_inventory_start():
    assert run(FakeVM(born=True))["status"] == "PASS"
    r = run(FakeVM(failure_in_hour="bounded"))
    assert r["failed"] == [0, 4] and r["status"] == "PASS", r
    late = [x for x in cle.collect(FakeVM(), HOUR, T0 + 1, {"accepted": {}, "terminated": {}}, APPROVED, now=NOW) if x["app"] == "nginx-test"][0]
    assert late["status"] == "INCOMPLETE" and "inventory start" in late["reason"]


def test_observed_short_of_plan_fails_and_partial_or_early_is_incomplete():
    assert run(FakeVM(observed_share=0.8))["status"] == "FAIL"
    assert run(FakeVM(partial=True))["status"] == "INCOMPLETE"
    assert run(FakeVM(), now=END - 30)["status"] == "INCOMPLETE"


def test_e1_and_e2_are_not_confused():
    r = run(FakeVM(), app="nginx-ensemble")
    assert r["status"] == "PASS" and r["generator_pod"] == "k6-nginx-ensemble-abcd1234-xyz12" and r["pods_relevant"] == ["nginx-ensemble-aaaa1111-p1"]

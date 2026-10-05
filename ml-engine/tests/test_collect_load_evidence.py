"""deploy/prodcluster/collect_load_evidence.py v5: server-side gate with final snapshots, conditional k6 G2/G3.

Fixture timing follows prodcluster measurements (2026-10-05): k6 pushes every 10 s (k6_vus at x.475 s, a changed
counter stamped with its latest event); Envoy targets scraped every 30 s with `up` and every series sharing the scrape
timestamp; kube-state-metrics every 20 s; a terminating arm pod pushes bench_final_istio_* and a receipt at its capture
time. Cases from Codex Task 03 r15-r19: lost flushes, late sparse updates, base decreases, partial responses, foreign
targets, pods ending in or near the hour with or without (valid) snapshots, birth intervals, ms boundaries.
"""
import datetime
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
K6_START = T0 - 7200 + 0.3
SNAP_SERIES = 3      # requests_total + two buckets in the fixture payload


def grid(start, stop, step, offset):
    out, t = [], start + offset
    while t <= stop:
        out.append(round(t, 3)); t += step
    return out


# ---------- pure functions ----------

def test_k6_bounds_general_zero_and_unbounded_tail():
    s = [(T0 - 100, 5.0), (T1 + 700, 9.0)]
    assert cle.k6_bounds(s, 0.0, T0, T1, cle.LAG) == (0.0, 4.0)
    assert cle.k6_bounds([(T0 - 100, 5.0)], 0.0, T0, T1, cle.LAG) == (0.0, 0.0)
    assert cle.k6_bounds([], 3.0, T0, T1, cle.LAG) == (0.0, 0.0)
    for late in (T1 + 100, T0 + 10):
        with pytest.raises(cle.Incomplete):
            cle.k6_bounds([(late, 7.0)], 0.0, T0, T1, cle.LAG)
    with pytest.raises(cle.Incomplete):
        cle.k6_bounds([(T0 + 100, 2.0)], 10.0, T0, T1, cle.LAG)                 # below the historical base


def test_r18_ms_flooring_an_event_just_inside_the_hour_is_never_zero():
    # an event at t0 + 0.5 ms exports L == t0 (floored): it is inside [t0, t1) and must not yield an exact zero
    with pytest.raises(cle.Incomplete):
        cle.k6_bounds([(T0, 1.0)], 0.0, T0, T1, cle.LAG)
    assert cle.k6_bounds([(T0 - 0.001, 1.0)], 0.0, T0, T1, cle.LAG) == (0.0, 0.0)  # floored to t0 - 1 ms: before


def test_envoy_birth_interval_and_edges():
    sc = grid(T0 - 180, END, 30, 0.659)
    vals = [(t, t - (T0 - 3600)) for t in sc]
    lo, hi = cle.target_series_bounds(vals, sc, T0, T1, T0 - 3600, None)
    assert 3570 <= lo <= 3600 <= hi <= 3630
    born = grid(T0 + 1210, END, 30, 0.0)
    assert cle.target_series_bounds([(t, 1.0) for t in born], born, T0, T1, T0 + 1200, None) == (1.0, 1.0)
    # first scraped after t0 with an earlier start: N(<t0) in [0, first value]
    assert cle.target_series_bounds([(t, 1.0) for t in born], born, T0, T1, T0 - 5, None) == (0.0, 1.0)


def test_envoy_snapshot_closes_a_pod_that_ends_in_the_hour():
    sc = grid(T0 - 180, T0 + 1800, 30, 0.659)
    vals = [(t, t - (T0 - 3600)) for t in sc]
    F = vals[-1][1] + 7.0
    lo, hi = cle.target_series_bounds(vals, sc, T0, T1, T0 - 3600, (T0 + 1815.0, F))
    assert hi - lo < 40 and lo > 0
    with pytest.raises(cle.Incomplete):
        cle.target_series_bounds(vals, sc, T0, T1, T0 - 3600, None)                   # vanished, no snapshot
    with pytest.raises(cle.Incomplete):
        cle.target_series_bounds(vals, sc, T0, T1, T0 - 3600, (T0 + 1815.0, F - 20))  # snapshot below a scrape
    late = sc + [T0 + 1830.659]
    with pytest.raises(cle.Incomplete):                                               # scrape after capture differs
        cle.target_series_bounds(vals + [(T0 + 1830.659, F + 3)], late, T0, T1, T0 - 3600, (T0 + 1815.0, F))
    pre = [t for t in sc if t < T0]
    assert cle.target_series_bounds([(t, 10.0) for t in pre], pre, T0, T1, T0 - 3600, (T0 - 5.0, 12.0)) == (0.0, 0.0)


def test_check_snapshot_rules():
    lab = {"series": "3", "proxy_uptime_s": "900", "hot_restart_epoch": "0"}
    c = T0 + 1815.0
    assert cle.check_snapshot([(lab, c)], {cle.ms(c): 3}, c - 800) == c
    for receipts, counts, first in (([(lab, c), (lab, c + 1)], {cle.ms(c): 3}, c - 800),
                                    ([(lab, c)], {cle.ms(c): 2}, c - 800),
                                    ([(dict(lab, hot_restart_epoch="1"), c)], {cle.ms(c): 3}, c - 800),
                                    ([(lab, c)], {cle.ms(c): 3}, c - 1000)):          # Envoy younger than its target
        with pytest.raises(cle.Incomplete):
            cle.check_snapshot(receipts, counts, first)


def test_gate():
    P = 10000.0
    assert cle.gate(P, (P, P), (0, 0), (0, 0))[0] == "PASS"
    assert cle.gate(P, (0.9 * P, 0.97 * P), (0, 0), (0, 0))[0] == "INCOMPLETE"
    assert cle.gate(P, (0.8 * P, 0.9 * P), (0, 0), (0, 0))[0] == "FAIL"
    assert cle.gate(P, (P, 1.08 * P), (0, 0), (0, 0))[0] == "INCOMPLETE"            # the whole interval must fit
    assert cle.gate(P, (P, P), (0, 11), (0, 0))[0] == "INCOMPLETE"
    assert cle.gate(P, (P, P), (0, 0), (6, 6))[0] == "FAIL"


# ---------- fake VictoriaMetrics ----------

class FakeVM:
    def __init__(self, **o):
        self.o = o
        self.rate = challenge_profile.planned_requests(int(T0)) / 3600.0 * o.get("observed_share", 1.0)
        gap = o.get("hb_gap")
        self.hb = [t for t in grid(T0 - 180, END, 10, 0.475) if not (gap and gap[0] < t < gap[1])]
        self.ksm = grid(T0 - 3600, END, 20, 0.486)

    def app_of(self, sel):
        for app in sorted(cle.APPS, key=len, reverse=True):
            for pat in (f'"{app}"', f'"k6-{app}"', f'"{app}-[', f'"k6-{app}-', f'"{app}-aaaa'):
                if pat in sel:
                    return app
        return None

    def pods(self, app):
        """{pod: dict(start, ksm_end, scrape_from, scrape_to, share, snapshot)}"""
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
            last = {"old": T0 - 600, "recent": T0 - 100}.get(o["pre_hour"], T0 - 20)
            ksm_end = T0 - 120 if o["pre_hour"] == "recent" else last + 30      # "recent": left Kubernetes, scraped < 3 min ago
            out[f"{app}-aaaa1111-p0"] = dict(start=T0 - 7000, ksm_end=ksm_end, frm=T0 - 7000, to=last, share=0.0, snap=snap)
        if o.get("alive_without_target"):
            out[f"{app}-aaaa1111-p9"] = dict(start=T0 - 3600, ksm_end=END, frm=None, to=None, share=0.0, snap=None)
        return out

    def value(self, p, t):
        d = self.pods(p.rsplit("-", 2)[0])[p]
        if p.endswith("p2"):
            return d["share"] * self.rate * (t - d["start"])
        if d["share"] == 0.0:
            return 5.0
        if p.endswith("p3"):                    # p3 served 30 % until its drain at T0+1805
            return d["share"] * self.rate * (min(t, T0 + 1805) - (T0 - 3600))
        return d["share"] * self.rate * (t - (T0 - 3600)) if t < T0 + 1805 or not self.o.get("scaled_down") else \
            0.7 * self.rate * (T0 + 1805 - (T0 - 3600)) + self.rate * (t - (T0 + 1805))

    def scrapes(self, d):
        return [] if d["frm"] is None else [t for t in grid(T0 - 3600, END, 30, 0.659) if d["frm"] <= t <= d["to"]]

    def raw(self, sel, t_end, window):
        o, app = self.o, self.app_of(sel)
        if o.get("partial"):
            raise cle.Incomplete("query failed or partial")
        lo = t_end - window
        name = sel.split("{")[0]
        ksm = [t for t in self.ksm if t > lo]
        k6pod = f"k6-{app}-abcd1234-xyz12"
        if name == "kube_replicaset_owner":
            return [({"replicaset": f"k6-{app}-abcd1234"}, [(t, 1.0) for t in ksm], [])]
        if name == "kube_pod_owner":
            return [({"pod": f"k6-{a}-abcd1234-xyz12", "owner_name": f"k6-{a}-abcd1234"}, [(t, 1.0) for t in ksm], []) for a in cle.APPS]
        if name == "kube_pod_start_time" and "k6-" in sel:
            return [({"pod": k6pod}, [(t, K6_START - 5) for t in ksm], [])]
        if name == "kube_pod_container_state_started":
            st = T0 + 1800 if o.get("late_container") else K6_START
            return [({"pod": k6pod, "container": "k6"}, [(t, st) for t in ksm], [])]
        if name == "kube_pod_container_status_restarts_total" and 'container="k6"' in sel:
            return [({"pod": k6pod, "container": "k6"}, [(t, 1.0 if (o.get("restarted") and t > T0 + 900) else 0.0) for t in ksm], [])]
        if name == "kube_pod_container_status_restarts_total":
            return [({"pod": p, "container": "istio-proxy"}, [(t, 1.0 if (o.get("proxy_restart") and t > T0 + 900) else 0.0)
                     for t in ksm if d["start"] <= t <= d["ksm_end"]], []) for p, d in self.pods(app).items()]
        if name in ("kube_pod_start_time", "kube_pod_info"):
            out = []
            for p, d in self.pods(app).items():
                pts = [(t, d["start"] if name == "kube_pod_start_time" else 1.0) for t in ksm if d["start"] <= t <= d["ksm_end"]]
                if pts:
                    out.append(({"pod": p}, pts, [d["ksm_end"] + 20] if name == "kube_pod_info" and d["ksm_end"] < END else []))
            return out
        if name == "up":
            out = []
            for i, (p, d) in enumerate(self.pods(app).items()):
                sc = [t for t in self.scrapes(d) if t > lo]
                if sc:
                    out.append(({"job": cle.ENVOY_JOB, "instance": f"10.0.0.{i}:15090", "pod": p}, [(t, 1.0) for t in sc], []))
            return out
        if name == "bench_final_snapshot_receipt":
            out = []
            for p, d in self.pods(app).items():
                if d["snap"] is not None:
                    c = d["snap"]
                    lab = {"pod": p, "series": str(SNAP_SERIES), "proxy_uptime_s": "9000", "hot_restart_epoch": "0"}
                    out.append((lab, [(c, c)], []))
                    if o.get("snap_conflict"):
                        out.append((dict(lab, sha256="other"), [(c + 3, c + 3)], []))
            return out
        if sel.startswith('{__name__=~"bench_final_istio_.*"'):
            p = sel.split('pod="')[1].split('"')[0]
            c = self.pods(app)[p]["snap"]
            n = SNAP_SERIES - (1 if o.get("snap_partial") else 0)
            return [({"__name__": f"bench_final_x{k}", "pod": p}, [(c, 1.0)], []) for k in range(n)]
        if name in ("istio_requests_total", "istio_request_duration_milliseconds_bucket",
                    "bench_final_istio_requests_total", "bench_final_istio_request_duration_milliseconds_bucket"):
            final = name.startswith("bench_final_")
            les = ["5", "+Inf"] if name.endswith("bucket") else [None]
            out = []
            for i, (p, d) in enumerate(self.pods(app).items()):
                if final and ('pod="' + p + '"') not in sel:
                    continue
                if d["share"] == 0.0 and not final and d["frm"] is None:
                    continue
                for le in les:
                    frac = 0.9 if le == "5" else 1.0
                    lab = {"reporter": "destination", "destination_workload": app, "response_code": "200", "pod": p,
                           "namespace": "demo"}
                    if le:
                        lab["le"] = le
                    if final:
                        F = frac * self.value(p, d["snap"]) * (0.5 if o.get("snap_low") else 1.0)
                        out.append((dict(lab, pod_uid="u-" + p, snapshot_version="1"), [(d["snap"], F)], []))
                    else:
                        sc = [t for t in self.scrapes(d) if t > lo]
                        job = "other/job" if (o.get("foreign") and p.endswith("p1") and app == "nginx-test") else cle.ENVOY_JOB
                        out.append((dict(lab, job=job, instance=f"10.0.0.{i}:15090", container="istio-proxy"),
                                    [(t, frac * self.value(p, t)) for t in sc], []))
            return out
        if name == "k6_vus":
            return [({"testid": app}, [(t, 30.0) for t in self.hb if t > lo], [])]
        if name == "k6_http_reqs_total" and 'expected_response="false"' in sel:
            if app == "nginx-test" and o.get("failure_in_hour"):
                return [({"testid": app, "expected_response": "false"}, [(T0 + 1000.9, 3.0)] + ([(T1 + 700.9, 4.0)] if o["failure_in_hour"] == "bounded" else []), [])]
            if app == "nginx-test" and o.get("failure_old"):
                return [({"testid": app, "expected_response": "false"}, [(T0 - 100.9, 25.0)], [])]
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
        if self.o.get("failure_old") and 'expected_response="false"' in query and '"nginx-test"' in query:
            return [({"testid": "nginx-test", "expected_response": "false"}, 25.0)]
        if "k6_http_reqs_total" in query and "expected_response" not in query:
            r = challenge_profile.planned_requests(int(T0)) / 3600.0
            return [({"testid": self.app_of(query), "expected_response": "true"}, r * (t - 10 - K6_START))]
        return []


def row(vm, app="nginx-test", now=NOW):
    return [r for r in cle.collect(vm, HOUR, now=now) if r["app"] == app][0]


def test_healthy_hour_passes_with_conditions_stated():
    r = row(FakeVM())
    assert r["status"] == "PASS", r
    assert r["failed"] == [0, 0] and r["dropped"] == [0, 0] and r["collector"].endswith("v5")
    assert r["observed"][0] <= r["planned"] <= r["observed"][1] and r["p95_server_ms"] is not None
    assert any("A1'" in c for c in r["conditions"]) and isinstance(r["diagnostic_delivered_conditional_on_2s_lag"], list)


def test_scale_down_with_a_valid_final_snapshot_passes():
    r = row(FakeVM(scaled_down="snapshot"))
    assert r["status"] == "PASS" and r["final_snapshots"] == ["nginx-test-aaaa1111-p3"], r


@pytest.mark.parametrize("over,msg", [
    ({"scaled_down": "no_snapshot"}, "no accepted final snapshot"),
    ({"scaled_down": "snapshot", "snap_conflict": True}, "receipts for one pod"),
    ({"scaled_down": "snapshot", "snap_partial": True}, "series count"),
    ({"scaled_down": "snapshot", "snap_low": True}, "inconsistent"),
    ({"pre_hour": "none"}, "no accepted final snapshot"),
    ({"pre_hour": "recent"}, "no accepted final snapshot"),
    ({"alive_without_target": True}, "without an Envoy target"),
    ({"foreign": True}, "without its own Envoy target"),
    ({"proxy_restart": True}, "istio-proxy restarted"),
    ({"hb_gap": (T1 + 300, T1 + 330)}, "lost flush"),
    ({"drop_late": True}, "tail yet"),
    ({"failure_in_hour": "unbounded"}, "tail yet"),
    ({"late_container": True}, "started inside the hour"),
    ({"restarted": True}, "changed in the hour"),
])
def test_incomplete_paths(over, msg):
    r = row(FakeVM(**over))
    assert r["status"] == "INCOMPLETE" and msg in r["reason"], (over, r)


def test_pre_hour_pod_with_snapshot_contributes_nothing_and_old_pods_fall_under_a2():
    assert row(FakeVM(pre_hour="snapshot"))["status"] == "PASS"
    r = row(FakeVM(pre_hour="old"))
    assert r["status"] == "PASS" and r["excluded_under_A2"] == ["nginx-test-aaaa1111-p0"], r


def test_born_pod_and_k6_counters():
    assert row(FakeVM(born=True))["status"] == "PASS"
    assert row(FakeVM(failure_old=True))["failed"] == [0, 0]
    r = row(FakeVM(failure_in_hour="bounded"))
    assert r["failed"] == [0, 4] and r["status"] == "PASS", r


def test_observed_short_of_plan_fails_and_partial_or_early_is_incomplete():
    assert row(FakeVM(observed_share=0.8))["status"] == "FAIL"
    assert row(FakeVM(partial=True))["status"] == "INCOMPLETE"
    assert row(FakeVM(), now=END - 30)["status"] == "INCOMPLETE"


def test_e1_and_e2_are_not_confused():
    r = row(FakeVM(), app="nginx-ensemble")
    assert r["status"] == "PASS" and r["generator_pod"] == "k6-nginx-ensemble-abcd1234-xyz12" and r["pods_alive"] == ["nginx-ensemble-aaaa1111-p1"]

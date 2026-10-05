"""deploy/prodcluster/collect_load_evidence.py v3: export-instant bounds, lifecycle, heartbeat grid, partial results.

Fixture timing follows prodcluster measurements (2026-10-05): k6 pushes every 10 s, k6_vus at x.475 s, a changed
counter about 0.9 s later, unchanged counters not at all; Envoy targets scraped every 30 s with `up` and every series
sharing the scrape timestamp; k6_dropped_iterations_total's first sample is the count so far. Cases from Codex Task 03
r15 (v1 false PASSes) and r16 (late boundary update, missing heartbeat minute, historical-base decrease, partial
response, birth interval) are included.
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
HOUR = datetime.datetime.fromtimestamp(T0, datetime.timezone.utc)
NOW = T1 + 900
K6_START = T0 - 7200 + 0.3
WIN0 = T0 - cle.SHORT


def hb_times(gap_at=None):
    out, t = [], WIN0 + 0.475
    while t <= T1 + cle.GRACE:
        if not (gap_at and gap_at[0] < t < gap_at[1]):
            out.append(t)
        t += 10
    return out


def scrape_times(start=WIN0, stop=T1 + cle.GRACE):
    out, t = [], start + 0.659
    while t <= stop:
        out.append(t); t += 30
    return out


# ---------- pure functions ----------

def test_flush_values_attribution_and_decrease():
    hb = [T0 - 10.0, T0, T0 + 10.0]
    assert cle.flush_values([(T0 + 0.9, 5.0)], hb, 2.0) == {T0 - 10.0: 2.0, T0: 5.0, T0 + 10.0: 5.0}
    # stamped with its last event before the flush (traffic paused): -1.27 s at 04:57:47Z live -> the same flush
    assert cle.flush_values([(T0 - 1.27, 5.0)], hb, 2.0)[T0] == 5.0
    assert cle.flush_values([(T0 + 5.0, 5.0)], hb, 0.0)[T0 + 10.0] == 5.0   # a sparse event mid-interval: next flush
    with pytest.raises(cle.Incomplete):
        cle.flush_values([(T0 + 3.0, 5.0), (T0 + 6.0, 6.0)], hb, 0.0)       # two samples in one flush
    with pytest.raises(cle.Incomplete):
        cle.flush_values([(T0 + 0.9, 32.0)], hb, 100.0)         # historical base 100, then 32: decrease


def test_k6_edge_uses_only_completed_flushes_below_and_the_next_flush_above():
    pts = {T0 - 20.0: 1.0, T0 - 10.0: 2.0, T0 - 0.5: 3.0, T0 + 9.5: 4.0}
    assert cle.k6_edge(pts, T0) == (2.0, 4.0)      # the flush at T0-0.5 may straddle T0: not a lower bound


def test_increment_bounds_and_edges():
    pts = {T0 - 10: 100.0, T0 + 10: 110.0, T1 - 10: 500.0, T1 + 10: 510.0}
    assert cle.increment_bounds(pts, T0, T1) == (390.0, 410.0)
    with pytest.raises(cle.Incomplete):
        cle.increment_bounds({T0 + 10: 1.0, T1 + 10: 2.0}, T0, T1)   # start edge not bracketed


def test_envoy_values_absent_at_a_scrape_is_zero_and_strays_rejected():
    scr = [T0 - 20.0, T0 + 10.0]
    assert cle.envoy_values([(T0 + 10.0, 5.0)], scr) == {T0 - 20.0: 0.0, T0 + 10.0: 5.0}
    with pytest.raises(cle.Incomplete):
        cle.envoy_values([(T0 + 3.0, 5.0)], scr)


def test_minute_grid():
    pts = [(K6_START + 60 * k, 6.0) for k in range(1, 100)]
    assert cle.minute_grid_complete(pts, K6_START, K6_START + 60 * 99)
    assert not cle.minute_grid_complete(pts[:50] + pts[51:], K6_START, K6_START + 60 * 99)   # one minute missing
    assert not cle.minute_grid_complete(pts + [pts[10]], K6_START, K6_START + 60 * 99)       # duplicate
    assert not cle.minute_grid_complete([(t, 2.0) for t, _ in pts], K6_START, K6_START + 60 * 99)


def test_gate_on_bounds():
    P = 10000.0
    assert cle.gate_on_bounds(P, (P, P), (0, 0), (0, 0), (P, P))[0] == "PASS"
    assert cle.gate_on_bounds(P, (P, P), (0, 0), (0, 190), (P, P))[0] == "INCOMPLETE"
    assert cle.gate_on_bounds(P, (P, P), (0, 0), (190, 190), (P, P))[0] == "FAIL"


# ---------- fake VictoriaMetrics ----------

class FakeVM:
    def __init__(self, **o):
        self.o = o
        self.hb = hb_times(o.get("hb_gap"))

    def app_of(self, sel):
        for app in sorted(cle.APPS, key=len, reverse=True):
            if f'"{app}"' in sel or f'"k6-{app}"' in sel or f'"{app}-[' in sel or f'"k6-{app}-' in sel:
                return app
        return None

    def pod(self, app):
        return f"k6-{app}-abcd1234-xyz12"

    def per_s(self):
        return challenge_profile.planned_requests(int(T0)) / 3600.0

    def raw(self, sel, t_end, window):
        o, app = self.o, self.app_of(sel)
        if o.get("partial"):
            raise cle.Incomplete("query failed or partial")
        name = sel.split("{")[0]
        ksm = [T0 - 600 + 20 * k for k in range(int((T1 + cle.GRACE - (T0 - 600)) // 20) + 1)]
        if name == "kube_replicaset_owner":
            return [({"replicaset": f"k6-{app}-abcd1234"}, [(t, 1.0) for t in ksm], 0)]
        if name == "kube_pod_owner":
            return [({"pod": self.pod(a), "owner_name": f"k6-{a}-abcd1234"}, [(t, 1.0) for t in ksm], 0) for a in cle.APPS]
        if name == "kube_pod_start_time" and "k6-" in sel:
            return [({"pod": self.pod(app)}, [(t, K6_START - 5) for t in ksm], 0)]
        if name == "kube_pod_container_state_started":
            start = T0 + 1800 if o.get("late_container") else K6_START
            return [({"pod": self.pod(app), "container": "k6"}, [(t, start) for t in ksm], 0)]
        if name == "kube_pod_container_status_restarts_total":
            if o.get("no_restart_counter"):
                return []
            return [({"pod": self.pod(app), "container": "k6"}, [(t, 1.0 if (o.get("restarted") and t > T0 + 900) else 0.0) for t in ksm], 0)]
        if name == "kube_pod_start_time":
            pods = [({"pod": f"{app}-aaaa1111-p1"}, [(T1, T0 - 3600)], 0)]
            if o.get("born_pod"):
                pods.append(({"pod": f"{app}-aaaa1111-p2"}, [(T1, T0 + 1200.0)], 0))
            return pods
        if name == "k6_vus":
            return [({"testid": app}, [(t, 30.0) for t in self.hb], 0)]
        if name == "k6_http_reqs_total" and 'expected_response="false"' in sel:
            if app == "nginx-test" and o.get("stalled_failure"):
                return [({"testid": app, "expected_response": "false", "status": "503"}, [(self.hb[60] + 0.9, 32.0)], 0)]
            return []
        if name == "k6_http_reqs_total":
            pts = [(h + 0.9, self.per_s() * (h - K6_START)) for h in self.hb]
            return [({"testid": app, "expected_response": "true", "status": "200"}, pts, 0)]
        if name == "k6_dropped_iterations_total":
            if app == "nginx-test" and o.get("drop_late"):
                after = [h for h in self.hb if h > T1][0]
                return [({"testid": app}, [(after + 0.9, 190.0)], 0)]
            if app == "nginx-test" and o.get("drop_inside"):
                return [({"testid": app}, [(self.hb[40] + 0.9, 190.0)], 0)]
            return []
        if name == "up":
            out = [({"job": cle.ENVOY_JOB, "instance": "10.0.0.1:15090", "pod": f"{a}-aaaa1111-p1"}, [(t, 1.0) for t in scrape_times()], 0) for a in cle.APPS]
            if o.get("born_pod"):
                out.append(({"job": cle.ENVOY_JOB, "instance": "10.0.0.2:15090", "pod": "nginx-test-aaaa1111-p2"},
                            [(t, 1.0) for t in scrape_times(start=T0 + 1210)], 0))
            if o.get("vanished_pod"):
                out.append(({"job": cle.ENVOY_JOB, "instance": "10.0.0.3:15090", "pod": "nginx-test-aaaa1111-p3"},
                            [(t, 1.0) for t in scrape_times(stop=T0 + 1800)], 0))
            return out
        if name in ("istio_requests_total", "istio_request_duration_milliseconds_bucket"):
            les = ["5", "+Inf"] if name.endswith("bucket") else [None]
            if o.get("no_buckets") and name.endswith("bucket"):
                return []
            out = []
            for le in les:
                frac = 0.9 if le == "5" else 1.0
                lab = {"job": cle.ENVOY_JOB, "instance": "10.0.0.1:15090", "pod": f"{app}-aaaa1111-p1"}
                if le:
                    lab["le"] = le
                share = 0.9 if (o.get("born_pod") or o.get("vanished_pod")) and app == "nginx-test" else 1.0
                out.append((lab, [(t, frac * share * self.per_s() * (t - (T0 - 3600))) for t in scrape_times()], 1 if le == "+Inf" else 0))
                if o.get("born_pod") and app == "nginx-test":
                    lb = dict(lab, instance="10.0.0.2:15090", pod="nginx-test-aaaa1111-p2")
                    out.append((lb, [(t, frac * 0.1 * self.per_s() * (t - (T0 + 1200))) for t in scrape_times(start=T0 + 1210)], 0))
                if o.get("vanished_pod") and app == "nginx-test":
                    lv = dict(lab, instance="10.0.0.3:15090", pod="nginx-test-aaaa1111-p3")
                    out.append((lv, [(t, frac * 0.1 * self.per_s() * (t - (T0 - 3600))) for t in scrape_times(stop=T0 + 1800)], 0))
            if o.get("envoy_wrong_instance") and app == "nginx-test":
                out = [(dict(m, instance="10.9.9.9:15090"), s, n) for m, s, n in out]
            return out
        raise AssertionError(sel)

    def instant(self, query, t):
        if self.o.get("bad_base") and 'expected_response="false"' in query and '"nginx-test"' in query:
            return [({"testid": "nginx-test", "expected_response": "false", "status": "503"}, 100.0)]
        if self.o.get("stalled_failure") and 'expected_response="false"' in query and '"nginx-test"' in query:
            return [({"testid": "nginx-test", "expected_response": "false", "status": "503"}, 31.0)]
        if "k6_http_reqs_total" in query and "expected_response" not in query:
            app = self.app_of(query)
            return [({"testid": app, "expected_response": "true", "status": "200"}, self.per_s() * (self.hb[0] - 10 - K6_START))]
        return []

    def minute_counts(self, sel, start, end):
        pts = [(start + 60 * k, 6.0) for k in range(1, int((end - start) // 60) + 1)]
        if self.o.get("grid_missing_minute"):
            pts = pts[:5] + pts[6:]
        return pts


def row(vm, app="nginx-test", now=NOW):
    return [r for r in cle.collect(vm, HOUR, now=now) if r["app"] == app][0]


def test_healthy_hour_passes():
    r = row(FakeVM())
    assert r["status"] == "PASS", r
    assert r["dropped"] == [0, 0] and r["failed"] == [0, 0] and r["heartbeat_grid_complete"]
    assert r["delivered"][1] - r["delivered"][0] < 0.01 * r["delivered"][0]
    assert r["p95_server_ms"] is not None and r["observed_upper_capped_by_delivered"] is False


def test_r16_first_drop_update_after_a_30s_heartbeat_gap_is_not_hidden():
    # heartbeats at T1-9.525 and T1+20.475: a 30-s gap (allowed); the drop is first pushed with the flush after T1
    r = row(FakeVM(drop_late=True, hb_gap=(T1 - 6, T1 + 14)))
    assert r.get("dropped", [None, None])[1] == 190 and r["status"] in ("INCOMPLETE", "FAIL"), r


def test_r16_missing_heartbeat_minute_blocks_zero_inference():
    r = row(FakeVM(grid_missing_minute=True, drop_inside=True))
    assert r["status"] == "INCOMPLETE" and "grid" in r["reason"]


def test_r16_historical_base_above_current_sample_is_incomplete():
    r = row(FakeVM(bad_base=True, stalled_failure=True))
    assert r["status"] == "INCOMPLETE" and "decrease" in r["reason"]


def test_r16_partial_response_is_incomplete():
    assert row(FakeVM(partial=True))["status"] == "INCOMPLETE"


def test_r16_envoy_series_bound_to_its_own_target():
    assert row(FakeVM(envoy_wrong_instance=True))["status"] == "INCOMPLETE"


def test_born_pod_counts_from_zero_and_vanished_pod_caps_by_delivered():
    r = row(FakeVM(born_pod=True))
    assert r["status"] == "PASS", r
    v = row(FakeVM(vanished_pod=True))
    assert v["observed_upper_capped_by_delivered"] is True and v["status"] in ("PASS", "INCOMPLETE")


def test_stalled_sparse_series_uses_its_last_value_not_zero():
    assert row(FakeVM(stalled_failure=True))["failed"] == [1, 1]


def test_series_born_inside_the_hour_counts_from_zero():
    r = row(FakeVM(drop_inside=True))
    assert r["dropped"] == [190, 190] and r["status"] == "FAIL"


def test_lifecycle_cases():
    assert row(FakeVM(no_restart_counter=True))["status"] == "INCOMPLETE"
    assert row(FakeVM(restarted=True))["status"] == "INCOMPLETE"
    assert row(FakeVM(late_container=True))["status"] == "INCOMPLETE"


def test_missing_latency_buckets_and_early_collection():
    assert row(FakeVM(no_buckets=True))["status"] == "INCOMPLETE"
    assert row(FakeVM(), now=T1 + 30)["status"] == "INCOMPLETE"


def test_e1_and_e2_generators_are_not_confused():
    r = row(FakeVM(), app="nginx-ensemble")
    assert r["status"] == "PASS" and r["generator_pod"] == "k6-nginx-ensemble-abcd1234-xyz12"

"""deploy/prodcluster/collect_load_evidence.py v2: bounds, lifecycle, coverage and the cases Codex Task 03 r15 found
that made v1 PASS on incomplete evidence. Facts behind the fixtures (prodcluster, 2026-10-05): k6 pushes a counter
only in intervals in which it changed; k6_dropped_iterations_total's first sample is the count so far (190); k6_vus is
pushed every 10 s; Envoy exposes every series on each 20-s scrape; vmagent writes NaN staleness markers.
"""
import datetime
import importlib.util
import math
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location(
    "collect_load_evidence", os.path.join(HERE, "..", "..", "deploy", "prodcluster", "collect_load_evidence.py"))
cle = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cle)
import challenge_profile  # noqa: E402  (path inserted by the module)

T0 = 1_790_600_400.0          # a whole UTC hour
T1 = T0 + 3600
HOUR = datetime.datetime.fromtimestamp(T0, datetime.timezone.utc)
NOW = T1 + 600
POD_START = T0 - 7200


def grid(a, b, step):
    t, out = a, []
    while t <= b:
        out.append(t); t += step
    return out


# ---------- pure functions ----------

def test_finite_drops_staleness_markers_and_counts_them():
    assert cle.finite([(1.0, 2.0), (2.0, float("nan"))]) == ([(1.0, 2.0)], 1)


def test_edge_bounds_and_counter_bounds():
    s = [(T0 - 5, 100.0), (T0 + 5, 110.0), (T1 - 5, 500.0), (T1 + 5, 510.0)]
    assert cle.edge_bounds(s, T0, 15, None) == (100.0, 110.0)
    assert cle.counter_bounds(s, T0, T1, 15) == (390.0, 410.0)
    with pytest.raises(cle.Incomplete):
        cle.counter_bounds([(T0 + 100, 190.0)], T0, T1, 15)                    # no base, absence not proven
    assert cle.counter_bounds([(T0 + 100, 190.0)], T0, T1, 15, base0=0.0) == (190.0, 190.0)
    with pytest.raises(cle.Incomplete):
        cle.counter_bounds([(T0 - 5, 100.0), (T0 + 50, 20.0)], T0, T1, 15)     # reset


def test_histogram_quantile():
    b = {"10": 50.0, "20": 90.0, "+Inf": 100.0}
    assert cle.histogram_quantile(0.5, b) == pytest.approx(10.0)
    assert cle.histogram_quantile(0.7, b) == pytest.approx(15.0)
    assert cle.histogram_quantile(0.95, b) == pytest.approx(20.0)
    assert cle.histogram_quantile(0.95, {"10": 5.0}) is None


def test_gate_on_bounds_pass_fail_incomplete():
    P = 10000.0
    assert cle.gate_on_bounds(P, (P, P), (0, 0), (0, 0), (P, P))[0] == "PASS"
    assert cle.gate_on_bounds(P, (P, P), (0, 0), (0, 190), (P, P))[0] == "INCOMPLETE"   # ambiguous drop near an edge
    assert cle.gate_on_bounds(P, (P, P), (0, 0), (190, 190), (P, P))[0] == "FAIL"
    assert cle.gate_on_bounds(P, (8000, 8000), (0, 0), (0, 0), (8000, 8000))[0] == "FAIL"


# ---------- fake VictoriaMetrics ----------

class FakeVM:
    """A healthy hour for every arm; tests switch individual pieces off."""

    def __init__(self, **o):
        self.o = o

    def app_of(self, sel):
        for app in sorted(cle.APPS, key=len, reverse=True):
            if f'"{app}"' in sel or f'"k6-{app}"' in sel or f'"{app}-[' in sel or f'"k6-{app}-' in sel:
                return app
        return None

    def gen_pod(self, app):
        return f"k6-{app}-abcd1234-xyz12"

    def raw(self, sel, t_end, window):
        o, app = self.o, self.app_of(sel)
        if o.get("error"):
            raise OSError("connection refused")
        name = sel.split("{")[0]
        ksm = grid(T0 - 600, T1 + 120, 20)
        if name == "kube_replicaset_owner":
            return [({"replicaset": f"k6-{app}-abcd1234"}, [(t, 1.0) for t in ksm], 0)]
        if name == "kube_pod_owner":
            out = []
            for a in cle.APPS:
                pts = [(t, 1.0) for t in ksm]
                if a == o.get("late_pod_app") and a == "nginx-test":
                    pts = [(t, 1.0) for t in ksm if t > T0 + 1800]
                out.append(({"pod": self.gen_pod(a), "owner_name": f"k6-{a}-abcd1234"}, pts, 0))
            return out
        if name == "kube_pod_start_time" and "k6-" in sel:
            start = T0 + 1800 if o.get("late_pod_app") == app else POD_START
            return [({"pod": self.gen_pod(app)}, [(t, start) for t in ksm], 0)]
        if name == "kube_pod_start_time":
            return [({"pod": f"{app}-p1"}, [(t, T0 - 3600) for t in grid(T0 - 120, T1 + 120, 20)], 0)]
        if name == "kube_pod_container_status_restarts_total":
            if o.get("no_restart_counter"):
                return []
            return [({"pod": self.gen_pod(app), "container": "k6"}, [(t, 1.0 if (o.get("restarted") and t > T0 + 900) else 0.0) for t in ksm], 0)]
        if name == "k6_vus":
            ts = grid(T0 - 120, T1 + 120, 10)
            if o.get("heartbeat_gap"):
                ts = [t for t in ts if not (T0 + 1000 <= t <= T0 + 1100)]
            return [({}, [(t, 30.0) for t in ts], 0)]
        planned = challenge_profile.planned_requests(int(T0))
        per_s = planned / 3600.0
        if name == "k6_http_reqs_total" and 'expected_response="false"' in sel:
            if app == "nginx-test" and o.get("stalled_failure"):
                return [({"testid": app, "expected_response": "false", "status": "503"}, [(T0 + 600, 32.0)], 0)]
            return []
        if name == "k6_http_reqs_total":
            pts = [(t, per_s * (t - POD_START)) for t in grid(T0 - 120, T1 + 120, 10)]
            if o.get("reset") and app == "nginx-test":
                pts = [(t, v if t < T0 + 1200 else v - per_s * 4000) for t, v in pts]
            return [({"testid": app, "expected_response": "true", "status": "200"}, pts, 0)]
        if name == "k6_dropped_iterations_total":
            if app == "nginx-test" and o.get("drop_inside"):
                return [({"testid": app}, [(T0 + 100, 190.0), (T0 + 110, 190.0)], 0)]
            if app == "nginx-test" and o.get("drop_after_edge"):
                return [({"testid": app}, [(T1 + 5, 190.0)], 0)]
            return []
        if name == "up":
            ok = not o.get("not_scraped")
            return [({"pod": f"{app}-p1"}, [(t, 1.0) for t in grid(T0 - 120, T1 + 120, 20)] if ok else [], 0)]
        if name == "istio_requests_total":
            pts = [(t, per_s * (t - (T0 - 3600))) for t in grid(T0 - 120, T1 + 120, 20)]
            if o.get("envoy_no_base") and app == "nginx-test":
                pts = [(t, v) for t, v in pts if t > T0]
            return [({"pod": f"{app}-p1", "response_code": "200"}, pts, 0)]
        if name == "istio_request_duration_milliseconds_bucket":
            if o.get("no_buckets"):
                return []
            ts = grid(T0 - 120, T1 + 120, 20)
            return [({"pod": f"{app}-p1", "le": "5"}, [(t, 0.9 * per_s * (t - (T0 - 3600))) for t in ts], 0),
                    ({"pod": f"{app}-p1", "le": "+Inf"}, [(t, per_s * (t - (T0 - 3600))) for t in ts], 1)]
        raise AssertionError(sel)

    def instant(self, query, t):
        if self.o.get("stalled_failure") and 'expected_response="false"' in query and '"nginx-test"' in query:
            return [({"testid": "nginx-test", "expected_response": "false", "status": "503"}, 31.0)]
        return []

    def per_minute_counts(self, sel, a, b):
        return [6.0] * max(0, int((b - a) // 60))


def row(vm, app="nginx-test", now=NOW):
    return [r for r in cle.collect(vm, HOUR, now=now) if r["app"] == app][0]


def test_healthy_hour_passes_with_tight_bounds():
    r = row(FakeVM())
    assert r["status"] == "PASS", r
    assert r["dropped"] == [0, 0] and r["failed"] == [0, 0]
    assert r["delivered"][1] - r["delivered"][0] < 0.01 * r["delivered"][0]
    assert r["p95_server_ms"] is not None and r["staleness_markers_in_latency_series"] == 1


def test_coverage_is_enforced_even_when_a_dropped_series_exists():
    assert row(FakeVM(drop_inside=True, heartbeat_gap=True))["status"] == "INCOMPLETE"


def test_missing_restart_evidence_is_incomplete():
    assert row(FakeVM(no_restart_counter=True))["status"] == "INCOMPLETE"


def test_missing_latency_buckets_are_incomplete():
    assert row(FakeVM(no_buckets=True))["status"] == "INCOMPLETE"


def test_stalled_sparse_series_uses_its_last_value_not_zero():
    r = row(FakeVM(stalled_failure=True))
    assert r["failed"] == [1, 1]          # 32 - 31, not 32


def test_series_born_inside_the_hour_counts_from_zero():
    r = row(FakeVM(drop_inside=True))
    assert r["dropped"] == [190, 190] and r["status"] == "FAIL"


def test_a_drop_pushed_just_after_the_hour_makes_it_ambiguous():
    r = row(FakeVM(drop_after_edge=True))
    assert r["dropped"] == [0, 190] and r["status"] == "INCOMPLETE"


def test_e1_and_e2_generators_are_not_confused():
    assert row(FakeVM(), app="nginx-ensemble")["status"] == "PASS"
    assert row(FakeVM(), app="nginx-ensemble")["generator_pod"] == "k6-nginx-ensemble-abcd1234-xyz12"


def test_collection_before_the_grace_period_is_incomplete():
    assert row(FakeVM(), now=T1 + 30)["status"] == "INCOMPLETE"


def test_generator_started_inside_the_hour_is_incomplete():
    assert row(FakeVM(late_pod_app="nginx-test"))["status"] == "INCOMPLETE"


def test_generator_restart_is_incomplete():
    assert row(FakeVM(restarted=True))["status"] == "INCOMPLETE"


def test_counter_reset_is_incomplete():
    assert row(FakeVM(reset=True))["status"] == "INCOMPLETE"


def test_envoy_series_without_base_and_unproven_absence_is_incomplete():
    assert row(FakeVM(envoy_no_base=True, not_scraped=True))["status"] == "INCOMPLETE"
    assert row(FakeVM(envoy_no_base=True))["status"] in ("PASS", "INCOMPLETE")    # scraped before: base 0 is proven


def test_query_error_is_incomplete():
    r = row(FakeVM(error=True))
    assert r["status"] == "INCOMPLETE" and "connection refused" in r["reason"]

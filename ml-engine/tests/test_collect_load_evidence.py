"""deploy/prodcluster/collect_load_evidence.py: counter increments, coverage, server-side p95 and the gate.

The fixtures follow the 2026-10-05 positive control on prodcluster: k6 emits k6_dropped_iterations_total only after
the first drop, and its first sample was 190 (no zero sample precedes it); the last push before k6 exits is lost.
"""
import importlib.util
import os

import pytest

HERE = os.path.dirname(os.path.abspath(__file__))
SPEC = importlib.util.spec_from_file_location(
    "collect_load_evidence", os.path.join(HERE, "..", "..", "deploy", "prodcluster", "collect_load_evidence.py"))
cle = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(cle)

T0, T1 = 1_000_000.0, 1_003_600.0


def test_series_starting_inside_the_hour_counts_its_first_sample():
    inc, resets = cle.series_increment([(T0 + 100, 190.0), (T0 + 110, 400.0)], T0, T1)
    assert (inc, resets) == (400.0, 0)        # increase() could miss the first 190; this rule does not


def test_base_is_last_value_before_the_hour_and_later_samples_are_ignored():
    s = [(T0 - 10, 100.0), (T0 + 10, 150.0), (T1, 200.0), (T1 + 10, 999.0)]
    assert cle.series_increment(s, T0, T1) == (100.0, 0)


def test_a_decrease_is_a_restart_and_adds_the_post_reset_value():
    s = [(T0 - 10, 100.0), (T0 + 10, 150.0), (T0 + 20, 20.0), (T0 + 30, 50.0)]
    assert cle.series_increment(s, T0, T1) == (100.0, 1)   # +50, reset +20, +30


def test_coverage_requires_pushes_at_every_interval_and_both_edges():
    full = [(T0 - 5 + 10 * i, float(i)) for i in range(362)]
    assert cle.coverage(full, T0, T1)[0]
    gap = [p for p in full if not (T0 + 1000 <= p[0] <= T0 + 1100)]
    assert not cle.coverage(gap, T0, T1)[0]
    late = [p for p in full if p[0] >= T0 + 60]
    assert not cle.coverage(late, T0, T1)[0]
    assert not cle.coverage([], T0, T1)[0]


def test_histogram_quantile_matches_the_prometheus_interpolation():
    b = {"10": 50.0, "20": 90.0, "+Inf": 100.0}
    assert cle.histogram_quantile(0.5, b) == pytest.approx(10.0)
    assert cle.histogram_quantile(0.7, b) == pytest.approx(15.0)
    assert cle.histogram_quantile(0.95, b) == pytest.approx(20.0)    # inside +Inf: the highest finite bound
    assert cle.histogram_quantile(0.95, {"10": 5.0}) is None          # no +Inf bucket
    assert cle.histogram_quantile(0.95, {"+Inf": 0.0}) is None        # no traffic


def test_gate_and_incomplete():
    assert cle.evaluate(1000, 1000, 0, None, 1000)["status"] == "INCOMPLETE"
    assert cle.evaluate(1000, 1000, 0, 0, 1000)["status"] == "PASS"
    assert cle.evaluate(1000, 1000, 2, 0, 1000)["status"] == "FAIL"     # failed 0.2 % of delivered
    assert cle.evaluate(1000, 940, 0, 0, 940)["status"] == "FAIL"       # delivered 6 % short of plan
    assert cle.evaluate(1000, 1000, 0, 1, 1000)["status"] == "FAIL"     # dropped 0.1 % of plan


def _fake(reqs_samples, restarts=0.0, pods=("k6-nginx-test-a",), dropped=None):
    def raw(prom, selector, t1, window):
        if selector.startswith("k6_http_reqs_total"):
            return [({"expected_response": "true"}, reqs_samples)]
        if selector.startswith("kube_pod_container_status_restarts_total"):
            return [({}, [(T0 - 10, 0.0), (T1, restarts)])]
        if selector.startswith("kube_pod_info"):
            return [({"pod": p}, [(T1, 1.0)]) for p in pods]
        if selector.startswith("k6_dropped_iterations_total"):
            return [] if dropped is None else [({}, dropped)]
        if selector.startswith("istio_requests_total"):
            return [({}, [(T0 - 10, 0.0), (T1, reqs_samples[-1][1] - reqs_samples[0][1])])]
        if selector.startswith("istio_request_duration_milliseconds_bucket"):
            return [({"le": "5"}, [(T0 - 10, 0.0), (T1, 900.0)]), ({"le": "+Inf"}, [(T0 - 10, 0.0), (T1, 1000.0)])]
        raise AssertionError(selector)
    return raw


def _steady(per_push):
    return [(T0 - 5 + 10 * i, per_push * i) for i in range(362)]


def test_absent_dropped_series_is_zero_only_with_full_coverage(monkeypatch):
    import challenge_profile
    planned = challenge_profile.planned_requests(int(T0))
    per_push = planned / 360.0
    monkeypatch.setattr(cle, "raw", _fake(_steady(per_push)))
    rows = cle.collect("x", __import__("datetime").datetime.fromtimestamp(T0, __import__("datetime").timezone.utc))
    r = rows[0]
    assert r["dropped"] == 0 and "sparse-emission" in r["dropped_basis"] and r["status"] == "PASS"
    assert r["p95_server_ms"] is not None
    # a push gap makes absence unprovable -> INCOMPLETE, never PASS
    gappy = [p for p in _steady(per_push) if not (T0 + 1000 <= p[0] <= T0 + 1100)]
    monkeypatch.setattr(cle, "raw", _fake(gappy))
    r = cle.collect("x", __import__("datetime").datetime.fromtimestamp(T0, __import__("datetime").timezone.utc))[0]
    assert r["dropped"] is None and r["status"] == "INCOMPLETE"
    # a generator restart in the hour also blocks the inference
    monkeypatch.setattr(cle, "raw", _fake(_steady(per_push), restarts=1.0))
    assert cle.collect("x", __import__("datetime").datetime.fromtimestamp(T0, __import__("datetime").timezone.utc))[0]["status"] == "INCOMPLETE"


def test_present_dropped_series_is_counted_from_its_first_sample(monkeypatch):
    import challenge_profile
    per_push = challenge_profile.planned_requests(int(T0)) / 360.0
    monkeypatch.setattr(cle, "raw", _fake(_steady(per_push), dropped=[(T0 + 100, 190.0), (T0 + 110, 380.0)]))
    r = cle.collect("x", __import__("datetime").datetime.fromtimestamp(T0, __import__("datetime").timezone.utc))[0]
    assert r["dropped"] == 380 and r["dropped_basis"] == "series present" and r["status"] == "FAIL"


def test_query_error_is_incomplete(monkeypatch):
    def boom(*a, **k):
        raise OSError("connection refused")
    monkeypatch.setattr(cle, "raw", boom)
    r = cle.collect("x", __import__("datetime").datetime.fromtimestamp(T0, __import__("datetime").timezone.utc))[0]
    assert r["status"] == "INCOMPLETE" and "connection refused" in r["error"]


def test_staleness_markers_are_not_values():
    # vmagent writes NaN staleness markers when a scraped pod disappears (the arms re-rolled 2026-10-05 04:58Z);
    # they end a series, they are not counter values
    s = [(T0 - 10, 100.0), (T0 + 10, 150.0), (T0 + 20, float("nan")), (T0 + 30, float("nan"))]
    assert cle.finite(s) == [(T0 - 10, 100.0), (T0 + 10, 150.0)]
    assert cle.series_increment(cle.finite(s), T0, T1) == (50.0, 0)

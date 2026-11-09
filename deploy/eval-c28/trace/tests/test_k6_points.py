"""Metric-point bucketing of k6 counter samples (contract v3): values summed, anchored to the declared start."""

import json

import pytest

import k6_points as K

START = "2026-11-09T04:20:00Z"
T0 = K.parse_time(START)


def pt(metric, t, value=1):
    from datetime import datetime, timezone
    iso = datetime.fromtimestamp(T0 + t, tz=timezone.utc).isoformat().replace("+00:00", "Z")
    return json.dumps({"type": "Point", "metric": metric, "data": {"time": iso, "value": value, "tags": {}}})


def test_counter_values_are_summed_not_counted():
    lines = [pt("dropped_iterations", 10, 3), pt("dropped_iterations", 70, 5), pt("http_reqs", 1), pt("http_reqs", 2)]
    r = K.bucket(lines, T0)
    assert r["buckets"]["dropped_iterations"] == {"0": 3.0, "1": 5.0}
    assert r["buckets"]["http_reqs"] == {"0": 2.0}
    assert r["totals"]["dropped_iterations"] == 8.0 and r["label"] == "metric-point buckets"


def test_delayed_response_lands_in_the_next_minute_bucket():
    # A request dispatched at 59.8 s whose response takes 0.5 s is emitted at 60.3 s: the metric-point bucket is
    # minute 1, although dispatch was in minute 0. This is why these buckets are not dispatch counts.
    dispatched_at, duration = 59.8, 0.5
    r = K.bucket([pt("http_reqs", dispatched_at + duration)], T0)
    assert r["buckets"]["http_reqs"] == {"1": 1.0}
    assert "NOT dispatch" in r["time_basis"]


def test_buckets_anchor_to_the_declared_start_not_the_first_point():
    lines = [pt("http_reqs", 30), pt("http_reqs", 95)]
    assert K.bucket(lines, T0)["buckets"]["http_reqs"] == {"0": 1.0, "1": 1.0}
    # The anchor changes the buckets, so it must come from the run's start receipt, not from the data.
    assert K.bucket(lines, T0 + 30)["buckets"]["http_reqs"] == {"0": 1.0, "1": 1.0}
    assert K.bucket(lines, T0 + 40)["buckets"]["http_reqs"] == {"0": 1.0}
    assert K.bucket(lines, T0 + 40)["before_start"]["http_reqs"] == 1.0


def test_non_points_and_other_metrics_are_ignored_and_bad_values_refused():
    lines = [json.dumps({"type": "Metric", "metric": "http_reqs", "data": {}}), pt("vus", 1, 7), pt("http_reqs", 1)]
    assert K.bucket(lines, T0)["totals"]["http_reqs"] == 1.0
    for bad in (-1, float("nan"), True, "3"):
        with pytest.raises(K.PointsError):
            K.bucket([pt("dropped_iterations", 1, bad)], T0)
    with pytest.raises(K.PointsError):
        K.parse_time("2026-11-09T04:20:00")


def test_cli_requires_start_and_never_overwrites(tmp_path):
    p = tmp_path / "points.json"
    p.write_text("\n".join([pt("http_reqs", 1), pt("dropped_iterations", 61, 2)]) + "\n")
    out = tmp_path / "b.json"
    assert K.main(["--points", str(p), "--scenario-start", START, "--out", str(out)]) == 0
    r = json.loads(out.read_text())
    assert r["buckets"]["dropped_iterations"] == {"1": 2.0} and r["scenario_start"] == START
    assert K.main(["--points", str(p), "--scenario-start", START, "--out", str(out)]) == 2

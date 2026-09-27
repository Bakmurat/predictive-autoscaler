"""Hourly load gate: planned requests come from the workload profile (repeating-v2 then challenge-v1)."""
import datetime
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "collect_k6_summaries", ROOT / "deploy" / "eks-benchmark" / "scripts" / "collect-k6-summaries.py")
gate = importlib.util.module_from_spec(spec)
spec.loader.exec_module(gate)
cp = gate.challenge_profile
UTC = datetime.timezone.utc


def hour(y, m, d, h):
    start = datetime.datetime(y, m, d, h, tzinfo=UTC)
    return start, start + datetime.timedelta(hours=1)


def test_before_the_regime_planned_equals_the_fixed_hourly_table():
    for h in range(24):
        start, end = hour(2026, 9, 27, h)
        assert gate.planned_requests(start, end, 3600.0) == gate.PATTERN[h] * 60


def test_partial_first_hour_before_the_regime_matches_the_old_formula():
    start, end = hour(2026, 9, 27, 14)
    assert gate.planned_requests(start, end, 1234.0) == pytest.approx(gate.PATTERN[14] * 1234.0 / 60.0)


def test_challenge_hours_sum_the_ten_minute_slots():
    start, end = hour(2026, 9, 28, 15)
    expected = sum(cp.rate_at(int(start.timestamp()) + i * 600) * 10 for i in range(6))
    assert gate.planned_requests(start, end, 3600.0) == pytest.approx(expected)
    assert expected != gate.PATTERN[15] * 60          # the regime really changes the plan
    assert gate.planned_requests(start, end, 3600.0) == pytest.approx(cp.planned_requests(int(start.timestamp())))


def test_partial_hour_inside_the_regime_counts_only_scheduled_seconds():
    start, end = hour(2026, 9, 28, 9)
    s0 = int(start.timestamp())
    # generator started 25 minutes into the hour: 5 min of slot 2, then slots 3, 4, 5
    expected = cp.rate_at(s0 + 1200) * 5 + sum(cp.rate_at(s0 + i * 600) * 10 for i in (3, 4, 5))
    assert gate.planned_requests(start, end, 35 * 60.0) == pytest.approx(expected)


def test_the_hour_before_the_boundary_is_still_repeating_v2():
    start, end = hour(2026, 9, 27, 23)
    assert gate.planned_requests(start, end, 3600.0) == gate.PATTERN[23] * 60

"""Workload programs, k6 plans, fault timetable and the frozen session order (manifest section 5)."""

from datetime import datetime, timezone

import pytest

from c28 import frozen as F
from c28 import schedule as S


def test_the_ordinary_day_follows_the_frozen_shape():
    base = S.minute_means(S.ordinary_points())
    assert base[0] == pytest.approx(2) and base[5 * 60 + 30] == pytest.approx(2)
    assert base[9 * 60 + 30] == pytest.approx(8) and base[18 * 60] == pytest.approx(9, abs=0.02)
    assert base[int(7.5 * 60)] == pytest.approx(5.0167, abs=1e-3)                   # the 06:00-09:00 ramp


def test_the_scored_day_has_every_challenge_override():
    base = S.minute_means(S.scored_points())
    m = lambda h, mi=0: base[h * 60 + mi]  # noqa: E731
    assert m(0, 30) == pytest.approx(2)
    assert m(0, 59) == pytest.approx(2) and m(1, 0) == pytest.approx(8) and m(1, 29) == pytest.approx(8)
    assert m(1, 30) == pytest.approx(2)                                            # 30-minute unannounced step
    assert m(9, 0) == pytest.approx(2) and m(11, 59) == pytest.approx(2)           # holiday departure
    assert m(12, 19) == pytest.approx(3) and m(12, 20) == pytest.approx(7)         # metric-outage window load
    assert m(14, 19) == pytest.approx(3) and m(14, 20) == pytest.approx(7)
    assert m(16, 14) == pytest.approx(3) and m(16, 15) == pytest.approx(8)         # quota window load
    assert m(20, 14) == pytest.approx(3) and m(20, 15) == pytest.approx(8)         # slow-start window load
    assert max(base) <= F.MAX_DEMAND_U


def test_programs_are_deterministic_per_seed_and_bounded():
    a, b = S.program("scored", 201), S.program("scored", 201)
    assert a == b and S.program("scored", 202) != a
    base = S.minute_means(S.scored_points())
    assert all(abs(x / y - 1) <= F.PERTURBATION + 1e-12 for x, y in zip(a, base))
    with pytest.raises(ValueError):
        S.program("compressed", 1)


def test_the_k6_plan_is_open_loop_and_records_the_scheduled_arrivals():
    p = S.k6_plan("scored", 201, 35.0, "c28-pa-n-s20")
    assert p["label"] == "confirmation" and p["retries"] == 0 and p["timeout"] == "2s" and p["time_unit"] == "1m"
    assert len(p["stages"]) == 2 * 1440 and len(p["scheduled_per_minute"]) == 1440
    assert sum(int(s["duration"][:-2]) for s in p["stages"]) == 86_400_000
    assert p["pre_allocated_vus"] >= max(s["target"] for s in p["stages"]) / 60 * 2
    assert p == S.k6_plan("scored", 201, 35.0, "c28-pa-n-s20") and p["digest"] == S.digest(p)
    total = sum(p["scheduled_per_minute"])
    assert total == pytest.approx(sum(round(u * 35 * 60) for u in S.program("scored", 201)), rel=1e-3)
    fast = S.k6_plan("scored", 201, 35.0, "c28-pa-n-s20", accelerate=24)
    assert fast["label"] == "accelerated-smoke" and sum(int(s["duration"][:-2]) for s in fast["stages"]) == 3_600_000
    assert [s["target"] for s in fast["stages"][::2]] == [s["target"] for s in p["stages"][::2]]  # same arrival rates
    with pytest.raises(ValueError):
        S.k6_plan("scored", 201, 35.0, "t", accelerate=7)


def test_the_fault_timetable_is_absolute_and_starts_at_midnight():
    f = S.fault_timetable(datetime(2026, 11, 20, tzinfo=timezone.utc))
    assert f["metrics_route"]["outages"] == [{"start": "2026-11-20T12:15:00Z", "end": "2026-11-20T12:30:00Z"}]
    assert f["metric_outage_applicability"] == {"H-CPU": "not_tested", "H-RPS": "required", "PA-R": "required",
                                                "PA-N": "required", "PA-E": "required"}       # section 12
    # The demand step of 12:20 stays in every arm's program, H-CPU's included (the full workload is kept).
    for arm_target in ("c28-h-cpu-s20", "c28-pa-n-s20"):
        plan = S.k6_plan("scored", 201, 35.0, arm_target)
        assert plan["scheduled_per_minute"][12 * 60 + 21] > 1.5 * plan["scheduled_per_minute"][12 * 60 + 18]
    assert f["forecast_route"]["outages"] == [{"start": "2026-11-20T14:15:00Z", "end": "2026-11-20T14:35:00Z"}]
    assert f["slow_start"]["slow_starts"] == [{"start": "2026-11-20T20:00:00Z", "end": "2026-11-20T20:45:00Z",
                                               "extra_startup_seconds": 120}]
    assert [q["at"] for q in f["quota"]] == ["2026-11-20T16:10:00Z", "2026-11-20T16:40:00Z"]
    with pytest.raises(ValueError):
        S.fault_timetable(datetime(2026, 11, 20, 1, tzinfo=timezone.utc))


def test_the_session_order_is_frozen_and_balanced():
    o = S.session_order()
    assert o == S.session_order() and len(o) == 2 * 2 * 5
    for p in F.PROFILES:
        s1 = [x["arm"] for x in o if x["profile"] == p and x["session"] == 1]
        s2 = [x["arm"] for x in o if x["profile"] == p and x["session"] == 2]
        assert sorted(s1) == sorted(F.ARMS) and s2 == list(reversed(s1))
    assert {tuple(x["seeds"]) for x in o} == {(201, 202, 203), (204, 205)}

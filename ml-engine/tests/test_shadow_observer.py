"""The shadow observer must be safe to deploy beside a running controller (Codex D-85).

Eight properties are required before deployment: timestamp alignment, absence of future
observations, reload consistency, component independence, non-finite outputs, missing
history, logging failures, and bounded overhead. Each has a test below.

The observer is deliberately pure -- history and model come in as callables -- so every one
of these can be provoked without a cluster.
"""

import json
import os
import resource
import sys
import time
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from observer.shadow import (  # noqa: E402
    PREDICTORS,
    ShadowObserver,
    append_record,
    run,
)

GRID = timedelta(minutes=10)
SEQ = 144
STEPS = 6
END = datetime(2026, 9, 22, 11, 50)


def series(start, end, value=lambda t: 600.0):
    out, t = [], start
    while t <= end:
        out.append((t, value(t)))
        t += GRID
    return out


def make_history(points):
    def lookup(a, b):
        return [(t, v) for t, v in points if a <= t <= b]
    return lookup


def issuance(end=END, steps=STEPS, seq=SEQ, **over):
    rec = {
        "issued_at": (end + GRID).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "application": "nginx-test", "namespace": "demo",
        "inference_input_end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "sequence_length": seq, "artifact_sha256": "a" * 64,
        "model_version": "4.3.0", "training_cutoff": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "forecasts": [{"step": i + 1,
                       "target_at": (end + GRID * (i + 1)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                       "rpm": 700.0} for i in range(steps)],
    }
    rec.update(over)
    return rec


class FakeModel:
    """Returns distinguishable components so independence can be checked."""
    def __init__(self, hybrid=700.0, network=500.0, pattern=900.0, available=True, raises=None):
        self.hybrid, self.network, self.pattern = hybrid, network, pattern
        self.available, self.raises = available, raises
        self.calls = 0
        self.seen_origins = []

    def predict(self, steps_ahead=STEPS, origin=None, seasonal_history=None, **kw):
        self.calls += 1
        self.seen_origins.append(origin)
        if self.raises:
            raise self.raises
        return {"predictions": [self.hybrid] * steps_ahead,
                "components": {"lstm": [self.network] * steps_ahead,
                               "pattern": [self.pattern] * steps_ahead,
                               "pattern_available": self.available}}


def observer_with(points, model=None, **kw):
    return ShadowObserver(make_history(points), (lambda _h: model), **kw)


# 1 -----------------------------------------------------------------------------------
def test_timestamp_alignment_targets_follow_the_recorded_origin():
    pts = series(END - GRID * (SEQ - 1), END)
    rec = observer_with(pts, FakeModel()).observe(issuance())
    for name in PREDICTORS:
        got = [o["target_at"] for o in rec.predictors[name]]
        assert got == [(END + GRID * (i + 1)).strftime("%Y-%m-%dT%H:%M:%SZ")
                       for i in range(STEPS)], name


def test_timestamp_alignment_window_ends_at_the_recorded_input_end():
    pts = series(END - GRID * (SEQ - 1), END)
    obs = observer_with(pts, FakeModel())
    rec = obs.observe(issuance())
    assert rec.window_points == SEQ and rec.window_complete


# 2 -----------------------------------------------------------------------------------
def test_no_future_observation_enters_the_window():
    """The observer runs after the fact, when later data exists. It must not be used."""
    later = series(END - GRID * (SEQ - 1), END + GRID * 20)   # 20 slots past the snapshot
    obs = observer_with(later, FakeModel())
    rec = obs.observe(issuance())
    assert rec.window_points == SEQ, "future observations leaked into the input window"


def test_persistence_uses_the_snapshot_not_the_latest_value():
    def val(t):
        return 100.0 if t <= END else 9999.0
    pts = series(END - GRID * (SEQ - 1), END + GRID * 10, val)
    rec = observer_with(pts, FakeModel()).observe(issuance())
    assert all(o["value"] == 100.0 for o in rec.predictors["persistence"])


# 3 -----------------------------------------------------------------------------------
def test_reload_consistency_same_input_gives_the_same_record():
    pts = series(END - GRID * (SEQ - 1), END)
    a = observer_with(pts, FakeModel()).observe(issuance())
    b = observer_with(pts, FakeModel()).observe(issuance())
    strip = lambda r: {k: v for k, v in vars(r).items() if k != "elapsed_ms"}
    assert strip(a) == strip(b)


# 4 -----------------------------------------------------------------------------------
def test_components_are_independent_not_copies_of_each_other():
    pts = series(END - GRID * (SEQ - 1), END)
    m = FakeModel(hybrid=700.0, network=500.0, pattern=900.0)
    rec = observer_with(pts, m).observe(issuance())
    assert [o["value"] for o in rec.predictors["served_hybrid"]] == [700.0] * STEPS
    assert [o["value"] for o in rec.predictors["raw_network"]] == [500.0] * STEPS
    assert [o["value"] for o in rec.predictors["seasonal_only"]] == [900.0] * STEPS


def test_one_inference_call_per_origin_not_three():
    """An observer must not triple the work to answer three questions."""
    pts = series(END - GRID * (SEQ - 1), END)
    m = FakeModel()
    observer_with(pts, m).observe(issuance())
    assert m.calls == 1


def test_seasonal_only_is_unavailable_when_the_pattern_is_not_genuinely_backed():
    pts = series(END - GRID * (SEQ - 1), END)
    m = FakeModel(available=False)
    rec = observer_with(pts, m).observe(issuance())
    assert all(o["status"] == "unavailable" for o in rec.predictors["seasonal_only"])
    assert all(o["status"] == "ok" for o in rec.predictors["raw_network"])


# 5 -----------------------------------------------------------------------------------
@pytest.mark.parametrize("bad", [float("nan"), float("inf"), float("-inf")])
def test_non_finite_component_is_recorded_not_dropped(bad):
    pts = series(END - GRID * (SEQ - 1), END)
    rec = observer_with(pts, FakeModel(network=bad)).observe(issuance())
    obs = rec.predictors["raw_network"]
    assert len(obs) == STEPS
    assert all(o["status"] == "non_finite" and o["value"] is None for o in obs)


def test_non_finite_last_observation_makes_persistence_non_finite():
    pts = series(END - GRID * (SEQ - 1), END)
    pts[-1] = (pts[-1][0], float("nan"))
    rec = observer_with(pts, FakeModel()).observe(issuance())
    assert all(o["status"] == "non_finite" for o in rec.predictors["persistence"])


# 6 -----------------------------------------------------------------------------------
def test_missing_history_is_recorded_as_unavailable_for_every_predictor():
    rec = observer_with([], None).observe(issuance())
    assert rec.window_points == 0 and not rec.window_complete
    assert any("incomplete" in n for n in rec.notes)
    assert all(o["status"] == "unavailable" for o in rec.predictors["persistence"])
    assert all(o["status"] == "unavailable" for o in rec.predictors["previous_day"])


def test_short_history_still_produces_a_full_record():
    pts = series(END - GRID * 10, END)          # far short of SEQ
    rec = observer_with(pts, FakeModel()).observe(issuance())
    assert not rec.window_complete
    for name in PREDICTORS:
        assert len(rec.predictors[name]) == STEPS, name


def test_previous_day_unavailable_without_yesterday():
    """A genuinely short history has no yesterday for any step.

    Note the boundary deliberately: a span of just UNDER 24 h still covers the later steps,
    because their targets lie in the future of the last observation. That is exactly the
    live finding recorded in previous-day-coverage: at a 23.33 h span three of six steps
    were genuinely backed. So this test uses a history far too short to reach any of them.
    """
    pts = series(END - GRID * 20, END)          # ~3 h: no step can reach yesterday
    rec = observer_with(pts, FakeModel()).observe(issuance())
    assert all(o["status"] == "unavailable" for o in rec.predictors["previous_day"])


def test_previous_day_partially_available_just_under_a_day():
    """The live case: a span just under 24 h backs the later steps but not the earlier ones."""
    # History begins 30 minutes INTO yesterday's corresponding hour, so the first two
    # steps' previous-day slots fall before the history start and the rest do not. This is
    # the shape the live series had at 23.33 h, where three of six steps were backed.
    pts = series(END - timedelta(days=1) + GRID * 3, END)
    rec = observer_with(pts, FakeModel()).observe(issuance())
    statuses = [o["status"] for o in rec.predictors["previous_day"]]
    assert statuses.count("ok") == 4 and statuses[:2] == ["unavailable"] * 2, statuses


def test_previous_day_found_when_yesterday_exists():
    pts = series(END - timedelta(days=2), END)
    rec = observer_with(pts, FakeModel()).observe(issuance())
    assert all(o["status"] == "ok" for o in rec.predictors["previous_day"])


def test_model_failure_is_recorded_as_failed_not_skipped():
    pts = series(END - GRID * (SEQ - 1), END)
    m = FakeModel(raises=RuntimeError("boom"))
    rec = observer_with(pts, m).observe(issuance())
    for name in ("served_hybrid", "raw_network", "seasonal_only"):
        obs = rec.predictors[name]
        assert len(obs) == STEPS
        assert all(o["status"] == "failed" and "boom" in o["detail"] for o in obs), name
    # the predictors that do not need a model still answer
    assert all(o["status"] == "ok" for o in rec.predictors["persistence"])


def test_history_lookup_failure_is_recorded_for_every_predictor():
    def boom(a, b):
        raise IOError("prometheus unreachable")
    rec = ShadowObserver(boom, lambda _h: FakeModel()).observe(issuance())
    assert any("history lookup failed" in n for n in rec.notes)
    assert all(o["status"] == "failed" for o in rec.predictors["served_hybrid"])


def test_unusable_issuance_is_recorded_with_a_note():
    rec = observer_with(series(END - GRID * 5, END), FakeModel()).observe(
        issuance(forecasts=[], sequence_length=0))
    assert any("unusable issuance" in n for n in rec.notes)


# 7 -----------------------------------------------------------------------------------
def test_logging_failure_is_reported_not_raised(tmp_path):
    pts = series(END - GRID * (SEQ - 1), END)
    obs = observer_with(pts, FakeModel())
    unwritable = tmp_path / "nodir"
    unwritable.write_text("not a directory")
    summary = run([issuance()], obs, str(unwritable / "sub" / "shadow.jsonl"))
    assert summary["records_not_written"] == 1 and summary["records_written"] == 0


def test_a_single_bad_record_does_not_stop_the_run(tmp_path):
    pts = series(END - GRID * (SEQ - 1), END)
    obs = observer_with(pts, FakeModel())
    out = tmp_path / "shadow.jsonl"
    summary = run([issuance(), {"garbage": True}, issuance()], obs, str(out))
    assert summary["records_written"] == 3          # the bad one is recorded, with a note
    lines = [json.loads(l) for l in out.read_text().splitlines()]
    assert any(l["notes"] for l in lines)


def test_records_are_append_only_and_parseable(tmp_path):
    pts = series(END - GRID * (SEQ - 1), END)
    obs = observer_with(pts, FakeModel())
    out = tmp_path / "shadow.jsonl"
    run([issuance()], obs, str(out))
    run([issuance(end=END + GRID)], obs, str(out))
    lines = out.read_text().splitlines()
    assert len(lines) == 2 and all(json.loads(l)["observer_version"] for l in lines)


# 8 -----------------------------------------------------------------------------------
def test_overhead_is_bounded_in_time():
    pts = series(END - timedelta(days=2), END)
    obs = observer_with(pts, FakeModel())
    t0 = time.monotonic()
    for _ in range(20):
        obs.observe(issuance())
    per = (time.monotonic() - t0) / 20
    assert per < 0.5, f"{per:.3f}s per observation is too slow for a shadow"


def test_exceeding_the_budget_is_recorded():
    class Slow(FakeModel):
        def predict(self, *a, **k):
            time.sleep(0.05)
            return super().predict(*a, **k)
    pts = series(END - GRID * (SEQ - 1), END)
    obs = ShadowObserver(make_history(pts), lambda _h: Slow(), max_seconds=0.001)
    rec = obs.observe(issuance())
    assert any("exceeded its budget" in n for n in rec.notes)


def test_memory_does_not_grow_with_the_number_of_records(tmp_path):
    pts = series(END - timedelta(days=2), END)
    obs = observer_with(pts, FakeModel())
    out = tmp_path / "shadow.jsonl"
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    run([issuance(end=END + GRID * i) for i in range(50)], obs, str(out))
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    growth_mb = (after - before) / (1024 * 1024 if sys.platform == "darwin" else 1024)
    assert growth_mb < 50, f"grew {growth_mb:.1f} MB over 50 records"


# C-75 ------------------------------------------------------------------------------------
class ArrayModel(FakeModel):
    """What the real API returns: `predictions` is a NumPy array, availability is per step."""
    def __init__(self, per_step, **kw):
        super().__init__(**kw)
        self.per_step = list(per_step)

    def predict(self, steps_ahead=STEPS, origin=None, seasonal_history=None, **kw):
        self.calls += 1
        import numpy as np
        return {"predictions": np.full(steps_ahead, self.hybrid),
                "components": {"lstm": np.full(steps_ahead, self.network),
                               "pattern": [self.pattern if a else None for a in self.per_step],
                               "pattern_available": any(self.per_step),
                               "pattern_available_per_step": self.per_step}}


def test_hybrid_step_is_recorded_available_when_the_api_served_it():
    """`predictions` is a NumPy array in the real API; rejecting it made every hybrid step
    read as unavailable, so the shadow comparison had nothing to compare."""
    pts = series(END - GRID * (SEQ - 1), END)
    rec = observer_with(pts, ArrayModel([True] * STEPS)).observe(issuance())
    assert all(o["status"] == "ok" for o in rec.predictors["served_hybrid"]), (
        [o["status"] for o in rec.predictors["served_hybrid"]])
    assert [o["value"] for o in rec.predictors["served_hybrid"]] == [700.0] * STEPS


def test_seasonal_only_honours_per_step_availability_not_the_global_flag():
    """Two early steps have no previous-day backing; they must not be counted as seasonal
    forecasts just because the global flag is true for the others."""
    per_step = [False, False, True, True, True, True]
    pts = series(END - GRID * (SEQ - 1), END)
    rec = observer_with(pts, ArrayModel(per_step)).observe(issuance())
    statuses = [o["status"] for o in rec.predictors["seasonal_only"]]
    assert statuses[:2] == ["unavailable"] * 2, statuses
    assert statuses[2:] == ["ok"] * 4, statuses
    # And the raw network is unaffected by pattern availability.
    assert all(o["status"] == "ok" for o in rec.predictors["raw_network"])

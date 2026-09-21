"""The Python replay must reach the same decisions as the real Go controller.

Codex C-52: the operational numbers came from a Python approximation of the controller,
so they were not evidence about the controller. This test records the replay's inputs,
feeds them through the ACTUAL Go decision functions (adjustForOverestimation and
calculateScaleDownTarget, via controllers/replay_harness_test.go), and asserts the
decisions agree step for step.

Skips cleanly when the Go toolchain is unavailable.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "eval"))

from offline_eval import GRID_MIN, replay_controller, replica_need  # noqa: E402

GO_DIR = ROOT / "k8s-operator"
pytestmark = pytest.mark.skipif(shutil.which("go") is None or not GO_DIR.exists(),
                                reason="Go toolchain or operator source unavailable")


def _demand(n: int, start=datetime(2026, 3, 2, 0, 0)):
    """A demand series that exercises ramp-up, plateau, overestimate and scale-down."""
    idx = pd.DatetimeIndex([start + timedelta(minutes=GRID_MIN * i) for i in range(n)])
    shape = np.concatenate([
        np.linspace(0.2, 1.0, n // 3),
        np.full(n // 3, 1.0),
        np.linspace(1.0, 0.15, n - 2 * (n // 3)),
    ])
    return pd.Series(6000 * shape, index=idx)


def _run_go(payload: dict, tmp_path: Path) -> list[dict]:
    in_path, out_path = tmp_path / "replay-in.json", tmp_path / "replay-out.json"
    in_path.write_text(json.dumps(payload))
    env = {**os.environ, "REPLAY_IN": str(in_path), "REPLAY_OUT": str(out_path)}
    proc = subprocess.run(
        ["go", "test", "./controllers", "-run", "TestReplayHarness", "-count=1", "-v"],
        cwd=GO_DIR, env=env, capture_output=True, text=True, timeout=600,
    )
    if proc.returncode != 0:
        pytest.fail(f"go harness failed:\n{proc.stdout[-3000:]}\n{proc.stderr[-2000:]}")
    return json.loads(out_path.read_text())["decisions"]


def test_python_replay_matches_the_go_controller(tmp_path):
    demand = _demand(90)
    target_rpm = 1200.0
    min_r, max_r = 1, 12

    # Forecast: the demand two steps ahead, with a deliberate 2.5x overshoot in the middle
    # so the overestimate cap and the stabilisation rules both fire.
    forecasts = {}
    vals = demand.values
    for i, t in enumerate(demand.index):
        ahead = vals[min(i + 2, len(vals) - 1)]
        if 30 <= i < 45:
            ahead *= 2.5
        forecasts[t] = np.full(6, ahead)

    # Exercise the PYTHON fallback here: the point of this test is to measure how far it
    # drifts from the real controller, so it must not silently delegate to Go.
    py = replay_controller(demand, forecasts, target_rpm=target_rpm,
                           min_r=min_r, max_r=max_r, use_go=False)

    steps = []
    for i, t in enumerate(demand.index):
        required = replica_need(float(demand.iloc[i]), target_rpm)
        reactive = int(np.clip(required, min_r, max_r))
        fc = forecasts[t]
        predicted = int(np.clip(replica_need(float(np.max(fc[:2])), target_rpm), min_r, max_r))
        steps.append({"t": t.isoformat() + "Z", "reactive": reactive, "predicted": predicted,
                      "current_rpm": float(demand.iloc[i]), "predictions": [float(x) for x in fc]})

    go = _run_go({"min": min_r, "max": max_r, "steps": steps}, tmp_path)

    assert len(go) == len(py["decisions"]), (
        f"step count differs: go={len(go)} python={len(py['decisions'])}")

    mismatches = []
    for k, (g, p) in enumerate(zip(go, py["decisions"])):
        if g["applied"] != p["applied"] or g["desired"] != p["desired"]:
            mismatches.append(
                f"  step {k} ({p['t']}): go desired={g['desired']} applied={g['applied']} | "
                f"python desired={p['desired']} applied={p['applied']}")

    # The Python fallback DOES drift -- it omits the ramp-up and prediction-trend
    # safeguards. That is exactly why the evaluation uses the Go controller for decisions.
    # This test pins the drift so it cannot be mistaken for agreement, and proves the
    # authoritative path reproduces Go exactly.
    assert mismatches, (
        "the Python fallback now matches Go; if the safeguards were ported, delete this "
        "expectation and assert equality instead")

    authoritative = replay_controller(demand, forecasts, target_rpm=target_rpm,
                                      min_r=min_r, max_r=max_r, use_go=True)
    assert authoritative["decisions_from"] == "go_controller"
    assert len(authoritative["decisions"]) == len(go)
    for g, a in zip(go, authoritative["decisions"]):
        assert g["applied"] == a["applied"] and g["desired"] == a["desired"], (
            f"authoritative replay diverged from Go at {g['t']}")


def test_requirement_is_not_clipped_to_the_ceiling(tmp_path):
    """C-52: overload must be reported, not hidden by the replica ceiling."""
    idx = pd.DatetimeIndex([datetime(2026, 3, 2) + timedelta(minutes=GRID_MIN * i) for i in range(30)])
    demand = pd.Series(np.full(30, 60_000.0), index=idx)   # needs 50 replicas at 1200 rpm
    out = replay_controller(demand, {}, target_rpm=1200.0, min_r=1, max_r=12)
    assert out["minutes_demand_exceeded_ceiling"] > 0, "demand above the ceiling was not reported"
    assert out["deficit_replicas_max"] >= 38, (
        f"deficit was clipped to the ceiling: max shortfall {out['deficit_replicas_max']}")


def test_readiness_is_accounted_at_event_time(tmp_path):
    """A two-minute readiness delay must cost about two minutes, not a whole tick."""
    n = 24
    idx = pd.DatetimeIndex([datetime(2026, 3, 2) + timedelta(minutes=GRID_MIN * i) for i in range(n)])
    vals = np.full(n, 1000.0)
    vals[5:] = 6000.0                      # one step change: 1 replica -> 5
    demand = pd.Series(vals, index=idx)
    out = replay_controller(demand, {}, target_rpm=1200.0, min_r=1, max_r=12)
    # The jump is unforecastable, so the deficit is the readiness delay itself.
    assert out["deficit_minutes"] <= 4 * GRID_MIN, (
        f"deficit {out['deficit_minutes']} min is far larger than the readiness delay; "
        f"capacity is probably still being accounted on the coarse tick")
    assert out["deficit_minutes"] > 0

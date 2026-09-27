"""Live capacity accounting: shortage/surplus replica-minutes from Ready replicas vs own traffic."""
import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location(
    "capacity_report", ROOT / "deploy" / "eks-benchmark" / "scoring" / "capacity_report.py")
cr = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cr)

START, STEP = 1790000000 - 1790000000 % 30, 30


def series(values):
    return {START + i * STEP: v for i, v in enumerate(values)}


def test_shortage_and_surplus_in_replica_minutes():
    # required: 1000 rpm / 600 -> 2 pods; ready 1, 2, 3 for two samples each
    ready = series([1, 1, 2, 2, 3, 3])
    rpm = series([1000] * 6)
    r = cr.summarize(ready, rpm, START, START + 6 * STEP, STEP, 600, 12)
    assert r["shortage_replica_minutes"] == 1.0      # 2 samples x 1 pod x 0.5 min
    assert r["surplus_replica_minutes"] == 1.0
    assert r["short_minutes"] == 1.0
    assert r["ready_changes"] == 2 and r["mean_ready_replicas"] == 2.0 and r["coverage"] == 1.0


def test_exact_multiple_of_capacity_is_not_rounded_up():
    r = cr.summarize(series([10]), series([6000.0]), START, START + STEP, STEP, 600, 12)
    assert r["shortage_replica_minutes"] == 0 and r["surplus_replica_minutes"] == 0


def test_missing_samples_are_counted_not_filled():
    ready = series([2, 2, 2, 2])
    rpm = {START: 1000, START + STEP: 1000}           # traffic missing for two samples
    r = cr.summarize(ready, rpm, START, START + 4 * STEP, STEP, 600, 12)
    assert r["samples"] == 2 and r["expected_samples"] == 4 and r["coverage"] == 0.5


def test_ceiling_minutes_and_zero_traffic_needs_one_pod():
    r = cr.summarize(series([12, 12, 1]), series([9000, 9000, 0]), START, START + 3 * STEP, STEP, 600, 12)
    assert r["minutes_at_ceiling"] == 1.0
    assert r["shortage_replica_minutes"] == pytest.approx(3 * 0.5 * 2)   # 15 needed, 12 ready, twice

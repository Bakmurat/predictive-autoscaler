"""Every repetition of the divergence experiment must survive into its record.

Codex C-95 / D-120: `eval/reproduce_divergence.py` keyed repetitions by `str(model_seed)`.
Unseeded repetitions all carry the seed `None`, so five repetitions per arm collapsed into
one JSON entry and four outcomes were lost -- including two of the three non-finite clipping
runs the console log recorded. These tests pin the three properties that failure violated:

  1. a unique id per repetition, and all of them kept;
  2. a recorded failure type per repetition;
  3. spread denominators over SUCCESSFUL repetitions only.

The fourth test checks the repaired artifact against the surviving primary record (the log),
so the claim "no repetition was lost" is verified against evidence, not against itself.
"""

import json
import os
import re
import sys

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "eval"))

from reproduce_divergence import (FAILURE_NON_FINITE, PERMITTED_CLAIM,  # noqa: E402
                                  summarise_arm)
from repair_divergence_record import parse_log  # noqa: E402

LOG = os.path.join(ROOT, "eval", "divergence.log")
REPAIRED = os.path.join(ROOT, "eval", "divergence-repaired.json")
DEFECTIVE = os.path.join(ROOT, "eval", "divergence.json")


def reps(*outcomes):
    """outcomes: floats are successful MAEs, strings are failure types."""
    out = []
    for n, o in enumerate(outcomes, start=1):
        r = {"rep_id": f"rep{n:02d}", "model_seed": None}
        if isinstance(o, str):
            r.update({"failure_type": o, "failure_detail": o})
        else:
            r.update({"failure_type": None, "mae": float(o)})
        out.append(r)
    return out


def test_every_repetition_is_kept_with_a_unique_id():
    s = summarise_arm(reps(100.0, 200.0, FAILURE_NON_FINITE, 150.0, FAILURE_NON_FINITE))
    assert s["runs"] == 5
    assert len(s["repetitions"]) == 5, "a repetition was dropped"
    ids = [r["rep_id"] for r in s["repetitions"]]
    assert len(set(ids)) == 5, f"repetition ids collide: {ids}"


def test_failure_type_is_recorded_per_repetition_not_just_counted():
    s = summarise_arm(reps(100.0, FAILURE_NON_FINITE, 150.0))
    assert s["failed_runs"] == 1
    assert s["failures_by_type"] == {FAILURE_NON_FINITE: ["rep02"]}
    assert s["repetitions"][1]["failure_type"] == FAILURE_NON_FINITE
    assert s["repetitions"][0]["failure_type"] is None


def test_spread_denominator_counts_successful_repetitions_only():
    s = summarise_arm(reps(100.0, FAILURE_NON_FINITE, FAILURE_NON_FINITE, 200.0))
    assert s["successful_runs"] == 2 and s["failed_runs"] == 2
    assert s["mae_min"] == 100.0 and s["mae_max"] == 200.0
    assert s["max_over_min"] == 2.0
    assert s["spread_basis"] == {"over": "successful repetitions only",
                                 "n_successful": 2, "n_total": 4}


def test_a_failure_is_not_a_threshold_exceedance():
    """A run with no finite MAE cannot be compared against a threshold, and a run that merely
    exceeded the threshold did not fail. The old record conflated the two under 'diverged'."""
    s = summarise_arm(reps(100.0, FAILURE_NON_FINITE, 9999.0), threshold=1000.0)
    assert s["failed_runs"] == 1 and s["failed_rep_ids"] == ["rep02"]
    assert s["threshold_exceeded_runs"] == 1 and s["threshold_exceeded_rep_ids"] == ["rep03"]


def test_all_successful_returns_a_full_denominator():
    s = summarise_arm(reps(10.0, 20.0))
    assert s["failed_runs"] == 0 and s["spread_basis"]["n_successful"] == 2


def test_no_successful_repetition_yields_no_spread_rather_than_a_crash():
    s = summarise_arm(reps(FAILURE_NON_FINITE, FAILURE_NON_FINITE))
    assert s["mae_min"] is None and s["max_over_min"] is None
    assert s["spread_basis"]["n_successful"] == 0


@pytest.mark.skipif(not (os.path.exists(LOG) and os.path.exists(REPAIRED)),
                    reason="the 2026-09-21 divergence run's artifacts are not present")
def test_repaired_artifact_holds_every_repetition_the_log_recorded():
    parsed = parse_log(open(LOG).read())
    repaired = json.load(open(REPAIRED))
    by_scenario = {r["scenario"]: r for r in repaired["results"]}
    assert repaired["permitted_claim"] == PERMITTED_CLAIM
    total_log = total_json = 0
    for sc in parsed:
        arms = by_scenario[sc["scenario"]]["arms"]
        for arm, log_reps in sc["arms"].items():
            kept = arms[arm]["repetitions"]
            assert len(kept) == len(log_reps), (
                f"{sc['scenario']}/{arm}: log has {len(log_reps)} repetitions, "
                f"the record kept {len(kept)}")
            assert len({r['rep_id'] for r in kept}) == len(kept)
            total_log += len(log_reps)
            total_json += len(kept)
    assert total_json == total_log == 40

    # The claim the repaired data supports, checked against the data itself.
    failures = {(r["scenario"], arm): a["failed_runs"]
                for r in repaired["results"] for arm, a in r["arms"].items()}
    assert sum(failures.values()) == 3
    assert all(arm == "b_clipnorm" for (_sc, arm), n in failures.items() if n)


@pytest.mark.skipif(not os.path.exists(DEFECTIVE),
                    reason="the defective artifact is not present")
def test_the_defective_record_is_still_demonstrably_defective():
    """Keeps the evidence of the bug next to its fix: the old file really did keep one
    repetition per arm under the key 'None' while the log recorded five."""
    old = json.load(open(DEFECTIVE))
    for r in old["results"]:
        for arm, a in r["arms"].items():
            assert list(a["seeds"]) == ["None"]
            assert a["runs"] == 5 and len(a["seeds"]) == 1

"""The prodcluster validity mask (deploy/prodcluster/validity-mask.json) obeys the same rules as the EKS one.

A new campaign starts on prodcluster (2026-10-05): its mask is versioned separately, carries no EKS intervals,
and its history start is either still pending (null, before the generators run) or a ten-minute grid instant.
"""
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data import gapfill  # noqa: E402
from tests import test_validity_mask_file as eks  # noqa: E402

MASK = os.path.join(os.path.dirname(__file__), "..", "..", "deploy", "prodcluster", "validity-mask.json")


def load():
    with open(MASK) as fh:
        return json.load(fh)


def test_prodcluster_mask_shape():
    m = load()
    assert m["version"] >= 1 and m["grid_seconds"] == eks.GRID
    assert 'destination_workload="nginx-test"' in m["series"] and 'destination_workload_namespace="demo"' in m["series"]
    start = m["benchmark_history_start"]
    assert start is None or gapfill._ts(start) % eks.GRID == 0, start
    for iv in m["intervals"]:
        assert iv["start"] >= "2026-10-05", ("no EKS interval may leak into the prodcluster mask", iv)


def test_prodcluster_intervals_ordered_and_gate_safe():
    m = load()
    prev = None
    for iv in m["intervals"]:
        a, b = gapfill._ts(iv["start"]), gapfill._ts(iv["end"])
        assert a < b and (prev is None or a >= prev) and iv["reason"].strip(), iv
        prev = b
        if eks.GATE_ONLY.search(iv["reason"]):
            assert not eks.grid_samples_inside(a, b), iv


def test_prodcluster_regimes_start_at_history_start():
    m = load()
    assert m["regimes"], "regimes must name the traffic profile"
    if m["benchmark_history_start"] is not None:
        assert m["regimes"][0]["start"] == m["benchmark_history_start"]
    assert m["regimes"][-1]["profile"] == "challenge-v1" and m["regimes"][-1]["end"] is None

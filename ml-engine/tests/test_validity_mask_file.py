"""The committed validity mask (deploy/eks-benchmark/validity-mask.json) obeys the rules the trainer relies on.

Why: masked samples are never interpolated and the trainer keeps the longest contiguous run, so an interval
that swallows a ten-minute grid sample cuts the training history at that point for the next 24 h. On
2026-09-29 mask v4 did exactly that (18Z training cutoff fell to 13:10Z). An interval recorded only for the
hourly load gates ("no ten-minute grid sample lies inside" / "is valid ... excluded from the interval") must
therefore contain no grid sample; endpoints are inclusive (data/gapfill.apply_mask uses a <= t <= b).
"""
import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from data import gapfill  # noqa: E402

MASK = os.path.join(os.path.dirname(__file__), "..", "..", "deploy", "eks-benchmark", "validity-mask.json")
GRID = 600
GATE_ONLY = re.compile(r"no ten-minute grid sample lies inside|grid sample .* is valid .* excluded from the interval|"
                       r"grid sample closed before .* and is valid", re.I)


def load():
    with open(MASK) as fh:
        return json.load(fh)


def grid_samples_inside(start_s, end_s):
    first = -(-start_s // GRID) * GRID
    return [t for t in range(first, end_s + 1, GRID)]


def test_mask_is_well_formed_and_monotone():
    m = load()
    assert m["grid_seconds"] == GRID and m["version"] >= 5
    prev_end = None
    for iv in m["intervals"]:
        a, b = gapfill._ts(iv["start"]), gapfill._ts(iv["end"])
        assert a < b, iv
        assert prev_end is None or a >= prev_end, ("intervals must be ordered and non-overlapping", iv)
        prev_end = b
        assert iv["reason"].strip(), iv


def test_load_gate_only_intervals_contain_no_grid_sample():
    m = load()
    offenders = []
    for iv in m["intervals"]:
        if GATE_ONLY.search(iv["reason"]):
            inside = grid_samples_inside(gapfill._ts(iv["start"]), gapfill._ts(iv["end"]))
            if inside:
                offenders.append((iv["start"], iv["end"], [gapfill._iso(t) for t in inside]))
    assert not offenders, offenders


def test_apply_mask_drops_endpoint_samples_inclusively():
    # the property the rule above protects against
    pts = [(t, 1.0) for t in range(0, 10 * GRID, GRID)]
    kept, info = gapfill.apply_mask(pts, {"version": 99, "intervals": [{"start": gapfill._iso(3 * GRID), "end": gapfill._iso(4 * GRID)}]})
    assert [t for t, _ in kept] == [0, GRID, 2 * GRID] + [t * GRID for t in range(5, 10)]
    kept2, _ = gapfill.apply_mask(pts, {"version": 99, "intervals": [{"start": gapfill._iso(3 * GRID + 30), "end": gapfill._iso(4 * GRID - 30)}]})
    assert len(kept2) == 10                                      # a between-samples interval drops nothing


def test_v4_style_interval_would_have_been_caught():
    bad = {"start": "2026-09-29T13:20:00Z", "end": "2026-09-29T13:22:00Z"}
    assert grid_samples_inside(gapfill._ts(bad["start"]), gapfill._ts(bad["end"])) == [gapfill._ts("2026-09-29T13:20:00Z")]

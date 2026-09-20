"""Validity mask and bounded interior-gap interpolation for ten-minute request-rate series.

Predeclared rules (benchmark protocol v4, Codex Task 03 round 4):
* A validity mask lists intervals whose samples describe changed workload behaviour (node rolls,
  network failures, replica resets), not missing telemetry. Samples inside an interval are dropped and
  are never interpolated. Samples before ``benchmark_history_start`` are dropped in the benchmark role
  and kept only in the diagnostic role. Raw data is never modified; the mask is recorded in provenance.
* Only bounded INTERIOR telemetry gaps are filled: at most ``max_missing_slots`` (3) consecutive
  missing grid slots, i.e. real endpoints at most ``max_endpoint_gap_s`` (2,400 s = 40 min) apart,
  both endpoints genuinely observed and not later than the training cutoff. Never extrapolate.
* Filled slots may not exceed ``max_fill_fraction`` (5 %) of the selected window.
* No fill may cross a train/validation/test boundary, and no fill may use an observation from a later
  partition than the slot it fills (leakage). Filled slots are flagged so imputed target labels can be
  excluded from validation, evaluation and reported accuracy.
* Scoring observations are never filled (the scorer reads Prometheus point samples directly).
* For inference, the latest input must be a genuinely observed, fresh sample; interior gaps inside
  the input window follow the same rule and are reported.

Algorithm version: ``linear-v1`` (linear interpolation between the two real endpoints).
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Dict, List, Optional, Sequence, Tuple

ALGORITHM = "linear-v1"
GRID_S = 600
MAX_MISSING_SLOTS = 3
MAX_ENDPOINT_GAP_S = (MAX_MISSING_SLOTS + 1) * GRID_S  # 2,400 s = 40 min
MAX_FILL_FRACTION = 0.05


def _ts(value) -> int:
    """ISO-8601 string or datetime -> epoch seconds (UTC)."""
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp())


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def load_mask(path: Optional[str]) -> Dict:
    """Load the validity mask; a missing path yields an empty mask (nothing excluded)."""
    if not path:
        return {"version": 0, "intervals": [], "benchmark_history_start": None, "source": None}
    with open(path) as fh:
        m = json.load(fh)
    m["source"] = path
    m.setdefault("intervals", [])
    return m


def apply_mask(points: Sequence[Tuple[int, float]], mask: Dict, role: str = "benchmark") -> Tuple[List[Tuple[int, float]], Dict]:
    """Drop samples inside mask intervals (always) and before benchmark_history_start (benchmark role).

    points: iterable of (epoch_seconds, value), any order. Returns (kept sorted, info).
    """
    intervals = [(_ts(i["start"]), _ts(i["end"]), i.get("reason", "")) for i in mask.get("intervals", [])]
    start = mask.get("benchmark_history_start")
    start_s = _ts(start) if (start and role == "benchmark") else None
    kept, dropped_interval, dropped_before = [], 0, 0
    per_interval = [0] * len(intervals)
    for t, v in sorted(points):
        if start_s is not None and t < start_s:
            dropped_before += 1
            continue
        hit = False
        for k, (a, b, _) in enumerate(intervals):
            if a <= t <= b:
                per_interval[k] += 1
                hit = True
                break
        if hit:
            dropped_interval += 1
            continue
        kept.append((t, v))
    info = {"mask_version": mask.get("version", 0), "mask_source": mask.get("source"), "role": role,
            "benchmark_history_start": start if role == "benchmark" else None,
            "dropped_before_history_start": dropped_before, "dropped_in_intervals": dropped_interval,
            "intervals": [{"start": _iso(a), "end": _iso(b), "reason": r, "dropped": n} for (a, b, r), n in zip(intervals, per_interval)]}
    return kept, info


def mask_intervals(mask: Optional[Dict]) -> List[Tuple[int, int]]:
    """Epoch (start, end) pairs of a validity mask's intervals."""
    return [(_ts(i["start"]), _ts(i["end"])) for i in (mask or {}).get("intervals", [])]


def fill_interior_gaps(points: Sequence[Tuple[int, float]], cutoff: Optional[int] = None,
                       boundaries: Sequence[int] = (), grid_s: int = GRID_S,
                       max_missing: int = MAX_MISSING_SLOTS, max_fill_fraction: float = MAX_FILL_FRACTION,
                       forbidden: Sequence[Tuple[int, int]] = ()):
    """Fill bounded interior gaps of a grid series by linear interpolation.

    points: (epoch, value) on the grid, sorted or not. cutoff: latest epoch an endpoint may have (the
    training cutoff); endpoints later than it disqualify a gap. boundaries: epoch timestamps of
    partition boundaries (a slot t belongs to the partition of the largest boundary <= t); a gap whose
    two endpoints fall in different partitions is never filled. forbidden: (start, end) epoch intervals
    (the validity mask) — a gap that overlaps one is changed behaviour, not missing telemetry, and is
    never filled (it stays a hard break in the series).

    Returns (series, imputed_flags, record) where series is the longest contiguous run after filling,
    imputed_flags marks filled slots, and record documents every decision for provenance.
    The 5 % cap is applied to the selected run: if exceeded, fills are removed from the end of the run
    (latest gaps first) until the cap holds, and the run is re-selected.
    """
    pts = sorted(points)
    by_t = {t: v for t, v in pts}
    gaps_considered, filled = [], {}
    for (t0, v0), (t1, v1) in zip(pts, pts[1:]):
        if t1 - t0 <= grid_s:
            continue
        missing = (t1 - t0) // grid_s - 1
        rec = {"start": _iso(t0 + grid_s), "end": _iso(t1 - grid_s), "missing_slots": int(missing),
               "endpoints": [[_iso(t0), v0], [_iso(t1), v1]], "endpoint_gap_s": int(t1 - t0), "filled": False}
        if (t1 - t0) % grid_s != 0:
            rec["reason"] = "endpoints off grid"
        elif missing > max_missing:
            rec["reason"] = f"gap longer than {max_missing} slots"
        elif cutoff is not None and t1 > cutoff:
            rec["reason"] = "right endpoint after the training cutoff (would extrapolate)"
        elif boundaries and _partition(t0, boundaries) != _partition(t1, boundaries):
            rec["reason"] = "gap crosses a partition boundary"
        elif any(a <= t1 - grid_s and b >= t0 + grid_s for a, b in forbidden):
            rec["reason"] = "gap overlaps a validity-mask interval (changed behaviour, not missing telemetry)"
        else:
            vals = []
            for k in range(1, missing + 1):
                t = t0 + k * grid_s
                v = v0 + (v1 - v0) * (k / (missing + 1))
                filled[t] = v
                vals.append([_iso(t), round(v, 3)])
            rec.update({"filled": True, "filled_values": vals, "algorithm": ALGORITHM})
        gaps_considered.append(rec)
    # assemble, select the longest contiguous run, enforce the cap
    merged = sorted(list(by_t.items()) + list(filled.items()))
    run = _longest_run(merged, grid_s)
    n_filled_in_run = sum(1 for t, _ in run if t in filled)
    cap_removed = []
    while run and n_filled_in_run > max_fill_fraction * len(run):
        # remove the latest filled gap inside the run, then re-select
        latest = max(t for t, _ in run if t in filled)
        gap = next(g for g in gaps_considered if g["filled"] and any(_ts(x[0]) == latest for x in g["filled_values"]))
        for x in gap["filled_values"]:
            filled.pop(_ts(x[0]), None)
        gap["filled"] = False
        gap["reason"] = f"removed: filled slots would exceed {max_fill_fraction:.0%} of the selected window"
        cap_removed.append(gap["start"])
        merged = sorted(list(by_t.items()) + list(filled.items()))
        run = _longest_run(merged, grid_s)
        n_filled_in_run = sum(1 for t, _ in run if t in filled)
    flags = [t in filled for t, _ in run]
    record = {"algorithm": ALGORITHM, "grid_seconds": grid_s, "max_missing_slots": max_missing,
              "max_endpoint_gap_s": (max_missing + 1) * grid_s, "max_fill_fraction": max_fill_fraction,
              "cutoff": _iso(cutoff) if cutoff is not None else None,
              "boundaries": [_iso(b) for b in boundaries],
              "gaps_considered": gaps_considered, "gaps_filled": sum(1 for g in gaps_considered if g["filled"]),
              "slots_filled_in_window": int(n_filled_in_run), "window_points": len(run),
              "fill_fraction": round(n_filled_in_run / len(run), 4) if run else 0.0,
              "cap_removed_gaps": cap_removed,
              "window_start": _iso(run[0][0]) if run else None, "window_end": _iso(run[-1][0]) if run else None}
    return run, flags, record


def _partition(t: int, boundaries: Sequence[int]) -> int:
    return sum(1 for b in boundaries if t >= b)


def _longest_run(series: List[Tuple[int, float]], grid_s: int) -> List[Tuple[int, float]]:
    best, cur = [], []
    for p in series:
        if cur and p[0] - cur[-1][0] != grid_s:
            if len(cur) > len(best):
                best = cur
            cur = []
        cur.append(p)
    return cur if len(cur) > len(best) else best


def check_inference_window(points: Sequence[Tuple[int, float]], now: int, sequence_length: int,
                           grid_s: int = GRID_S, max_age_s: int = 2 * GRID_S,
                           forbidden: Sequence[Tuple[int, int]] = ()):
    """Validate and, if needed, fill the inference input window.

    The latest sample must be genuinely observed and at most max_age_s old (fresh). Interior gaps inside
    the last ``sequence_length`` slots follow fill_interior_gaps with the latest sample as the cutoff
    (so nothing after it is used and nothing is extrapolated). Returns (window, record) or raises
    ValueError with the reason.
    """
    pts = sorted(points)
    if not pts:
        raise ValueError("no input observations")
    last_t = pts[-1][0]
    if now - last_t > max_age_s:
        raise ValueError(f"latest observation {_iso(last_t)} is stale ({now - last_t}s old, max {max_age_s}s)")
    run, flags, rec = fill_interior_gaps(pts, cutoff=last_t, grid_s=grid_s, forbidden=forbidden)
    if not run or run[-1][0] != last_t:
        raise ValueError("latest observation is not part of the contiguous input window")
    if len(run) < sequence_length:
        raise ValueError(f"input window has {len(run)} contiguous slots, need {sequence_length} (gaps not fillable under the rule)")
    window = run[-sequence_length:]
    wflags = flags[-sequence_length:]
    rec = dict(rec, latest_observed=_iso(last_t), latest_is_imputed=bool(wflags[-1]),
               imputed_in_window=int(sum(wflags)), window_slots=sequence_length)
    if wflags[-1]:
        raise ValueError("latest input slot is imputed; refusing to forecast from an imputed endpoint")
    return window, rec

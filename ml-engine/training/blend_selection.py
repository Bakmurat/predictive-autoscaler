"""Validation-selected blend weight for the hybrid (network + previous-day pattern) forecaster.

History:
- 2026-09-28 (rule `argmin`): the trainer never attached a seasonal history, so its evaluation scored the raw
  network alone, whose held-out MAE swung between 232 and 2,318 rpm while the pattern alone scored tens of rpm.
  The selection then scored the SERVED blend for a short list of pattern weights and kept the lowest MAE.
- 2026-09-30 (rule `margin-lb-pooled-hysteresis-1`, this module): on the live trainings of 2026-09-28/29 the argmin
  rule moved between pattern_only, pattern_0.95 and deployed_ramp on held-out partitions of ~50 strongly
  autocorrelated origins that overlap in time. The model lab's block bootstrap showed the 12Z and 18Z choices were
  close to coin tosses (flip probability ~0.44), and the network's gains were level shrinkage and a chance bias
  cancellation, not forecasting (07-jupyter-model-lab/LAB-RESULTS-20260930.md section 2). This module is a port of
  the lab's reference `stable_blend_selection.py`; its decision function reproduces the lab's 13 test vectors to 1e-9
  (tests/test_blend_selection.py).

Rule `margin-lb-pooled-hysteresis-1` (all numbers are parameters, defaults in DEFAULTS):

  0. Baseline = pattern_only (weight 1): the network has to earn its share; it is never the default.
  1. Evidence = the per-origin mean absolute served-blend errors of every candidate on the CURRENT held-out partition
     plus the stored ones of the previous `pool_partitions - 1` trainings (most recent first). Pooled MAE per candidate
     = mean over all pooled origins.
  2. Gain of candidate c over a reference r: g = (MAE_r - MAE_c) / MAE_r on the pooled evidence.
  3. Its uncertainty: a paired moving-block bootstrap. Each partition is resampled separately: ceil(K / L) blocks of L
     contiguous origins with uniformly drawn starts in 0..K-L, concatenated and cut to K; the same indices serve every
     candidate. `resamples` replicates; the lower bound is the replicate gain at 0-based position
     floor(q * resamples) of the ascending sort (q = lower_bound_quantile). Random numbers come from SplitMix64 seeded
     with `seed` (pure Python integers, so the draws are exact and portable).
  4. c BEATS r when g >= min_gain AND lower bound > lb_threshold.
  5. Hysteresis. If the previous training chose a network candidate p that is still scored and whose pooled gain over
     pattern_only is >= hold_gain, p is the incumbent: it is kept unless another network candidate beats p (step 4 with
     r = p); then the best such challenger is chosen. Otherwise the choice is the network candidate with the highest
     gain over pattern_only among those that beat pattern_only, else pattern_only. Ties keep the earlier candidate.

Serving choices of the port (the lab left them to the implementation):
- Per-origin errors come from `LSTMForecastModel.evaluate(..., return_origin_errors=True)`, the same code path that
  computes the candidates' MAEs, so the evidence cannot drift from the scores.
- Chaining: the record stores `stability.origin_errors` (current partition) and `stability.pooled_history` (the
  previous partitions it pooled). The next training reads the previous sidecar and pools
  [previous origin_errors] + previous pooled_history[:pool_partitions - 2]; `previous` is the previous chosen
  candidate. Both are taken ONLY from a sidecar written under this rule (`rule_id` match), so a choice made by the
  argmin rule never gets hysteresis protection (`history_from_sidecar`).
"""
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd

CANDIDATES = (("deployed_ramp", None), ("pattern_0.85", 0.85), ("pattern_0.95", 0.95), ("pattern_only", 1.0))
BASELINE = "pattern_only"
RULE_ID = "margin-lb-pooled-hysteresis-1"
RULE = ("network candidates must beat pattern_only by >= min_gain of its pooled MAE with a block-bootstrap lower "
        "bound > lb_threshold over the current and previous held-out partitions; an incumbent network blend is kept "
        "while its pooled gain >= hold_gain unless a challenger beats it the same way; default pattern_only")
DEFAULTS = {"min_gain": 0.05, "lower_bound_quantile": 0.10, "lb_threshold": 0.0, "block_origins": 12,
            "resamples": 2000, "seed": 20260930, "pool_partitions": 3, "hold_gain": 0.0}

_MASK64 = (1 << 64) - 1


class SplitMix64:
    """SplitMix64 (Steele, Lea & Flood 2014; the seeding generator of xoshiro). Pure integers: portable, exact."""

    def __init__(self, seed: int):
        self.state = int(seed) & _MASK64

    def next(self) -> int:
        self.state = (self.state + 0x9E3779B97F4A7C15) & _MASK64
        z = self.state
        z = ((z ^ (z >> 30)) * 0xBF58476D1CE4E5B9) & _MASK64
        z = ((z ^ (z >> 27)) * 0x94D049BB133111EB) & _MASK64
        return z ^ (z >> 31)

    def below(self, n: int) -> int:
        """An integer in [0, n) (plain modulo; the bias is below 2**-50 for n < 2**14)."""
        return self.next() % int(n)


def _params(params):
    p = dict(DEFAULTS)
    if params:
        unknown = set(params) - set(DEFAULTS)
        if unknown:
            raise ValueError(f"unknown parameter(s): {sorted(unknown)}")
        p.update(params)
    if not (0.0 < p["lower_bound_quantile"] < 1.0):
        raise ValueError("lower_bound_quantile must lie in (0, 1)")
    for k in ("block_origins", "resamples", "pool_partitions"):
        if int(p[k]) < 1:
            raise ValueError(f"{k} must be >= 1")
    return p


def origin_errors(abs_errors) -> list:
    """Per-origin mean absolute error from a (K, steps) array of absolute cell errors (or pass through a 1-D list)."""
    a = np.asarray(abs_errors, dtype=float)
    return (a.mean(axis=1) if a.ndim == 2 else a).tolist()


def block_indices(K: int, L: int, rng: SplitMix64) -> list:
    """One moving-block resample of K origins with blocks of L (L > K -> one block of K, i.e. the identity)."""
    L = min(int(L), K)
    n_blocks = -(-K // L)
    idx = []
    for _ in range(n_blocks):
        s = rng.below(K - L + 1)
        idx.extend(range(s, s + L))
    return idx[:K]


def _pooled_mae(parts, name, idx=None):
    tot, n = 0.0, 0
    for j, part in enumerate(parts):
        e = part[name]
        sel = e if idx is None else [e[i] for i in idx[j]]
        tot += math.fsum(sel)
        n += len(sel)
    return tot / n


def gain_stats(parts, cand, ref, params):
    """Point gain of `cand` over `ref` on the pooled evidence and its block-bootstrap lower bound."""
    p = _params(params)
    m_ref, m_c = _pooled_mae(parts, ref), _pooled_mae(parts, cand)
    g = (m_ref - m_c) / m_ref if m_ref > 0 else 0.0
    rng = SplitMix64(p["seed"])
    reps = []
    for _ in range(int(p["resamples"])):
        idx = [block_indices(len(part[ref]), p["block_origins"], rng) for part in parts]
        r, c = _pooled_mae(parts, ref, idx), _pooled_mae(parts, cand, idx)
        reps.append((r - c) / r if r > 0 else 0.0)
    reps.sort()
    lb = reps[int(math.floor(p["lower_bound_quantile"] * len(reps)))]
    return {"gain": g, "lower_bound": lb, "mae_ref": m_ref, "mae_cand": m_c,
            "beats": bool(g >= p["min_gain"] and lb > p["lb_threshold"])}


def decide(partitions, previous=None, params=None, candidates=CANDIDATES) -> dict:
    """The rule on per-origin errors. `partitions`: most recent first; each maps candidate name -> per-origin mean
    absolute errors (a list of K floats, or a (K, steps) array of absolute cell errors). Only candidates with finite
    errors in every pooled partition take part; pattern_only must be among them."""
    p = _params(params)
    parts = [{k: origin_errors(v) for k, v in part.items()} for part in partitions[:int(p["pool_partitions"])]]
    if not parts:
        raise ValueError("no partition given")
    order = [name for name, _ in candidates]
    if not all(BASELINE in part for part in parts):
        raise ValueError("pattern_only must be scored on every pooled partition")
    usable = [c for c in order if all(c in part and len(part[c]) == len(part[BASELINE]) and len(part[c]) > 0
                                      and all(math.isfinite(x) for x in part[c]) for part in parts)]
    if BASELINE not in usable:
        raise ValueError("pattern_only must be scored on every pooled partition")
    network = [c for c in usable if c != BASELINE]
    vs_base = {c: gain_stats(parts, c, BASELINE, p) for c in network}
    path, chosen, vs_incumbent = None, None, {}
    incumbent = previous if (previous in network and vs_base[previous]["gain"] >= p["hold_gain"]) else None
    if incumbent is not None:
        vs_incumbent = {c: gain_stats(parts, c, incumbent, p) for c in network if c != incumbent}
        challengers = [c for c in network if c != incumbent and vs_incumbent[c]["beats"]]
        if challengers:
            chosen = max(challengers, key=lambda c: (vs_incumbent[c]["gain"], -order.index(c)))
            path = "challenger beat the incumbent"
        else:
            chosen, path = incumbent, "incumbent held (hysteresis)"
    else:
        winners = [c for c in network if vs_base[c]["beats"]]
        if winners:
            chosen = max(winners, key=lambda c: (vs_base[c]["gain"], -order.index(c)))
            path = "network candidate beat pattern_only"
        else:
            chosen = BASELINE
            path = ("previous network blend fell below hold_gain; " if previous in network else "") + \
                "no network candidate beat pattern_only"
    return {"chosen": chosen, "weight": dict(candidates)[chosen], "decision_path": path, "previous": previous,
            "incumbent": incumbent, "pooled_partitions": len(parts),
            "pooled_origins": sum(len(part[BASELINE]) for part in parts),
            "pooled_mae": {c: _pooled_mae(parts, c) for c in usable},
            "vs_pattern_only": vs_base, "vs_incumbent": vs_incumbent, "params": p, "rule_id": RULE_ID}


def _naive_utc(frame_or_series):
    out = frame_or_series.copy()
    idx = pd.DatetimeIndex(out.index)
    if idx.tz is not None:
        idx = idx.tz_convert(None)
    out.index = idx
    return out


def attach_history(model, series: pd.Series):
    """Give the model the timestamp-indexed observations its previous-day lookup needs (naive UTC)."""
    model.seasonal_history = _naive_utc(pd.Series(series.to_numpy(dtype=float), index=series.index))
    return model.seasonal_history


def history_from_sidecar(meta, params=None):
    """(previous, history_partitions) for the next training from the previous training's provenance sidecar (a dict,
    or a path to the .meta.json). Only a record written under this rule counts; anything else -> (None, [])."""
    if isinstance(meta, (str, Path)):
        try:
            meta = json.loads(Path(meta).read_text())
        except (OSError, ValueError):
            return None, []
    rec = (meta or {}).get("blend_selection") or {}
    st = rec.get("stability") or {}
    if rec.get("rule_id") != RULE_ID or st.get("rule_id") != RULE_ID or not isinstance(st.get("origin_errors"), dict):
        return None, []
    depth = int(_params(params)["pool_partitions"]) - 1
    history = [st["origin_errors"]] + list(st.get("pooled_history") or [])
    return rec.get("chosen"), history[:max(0, depth)]


def select_blend_weight(model, test_data: pd.DataFrame, imputed=None, target_column: str = "value",
                        candidates=CANDIDATES, previous=None, history_partitions=None, params=None) -> dict:
    """Score every candidate weight on `test_data`, decide with the stable rule, set the chosen weight on the model
    (`pattern_weight_override`, honoured by predict()) and return the record. `previous` and `history_partitions`
    come from the previous training's sidecar (history_from_sidecar)."""
    history = getattr(model, "seasonal_history", None)
    if history is None or len(history) == 0:
        model.pattern_weight_override = None
        return {"chosen": "deployed_ramp", "weight": None, "mae": None, "candidates": [],
                "reason": "no seasonal history attached; selection skipped", "rule": RULE, "rule_id": RULE_ID}
    td = _naive_utc(test_data)
    rows, current = [], {}
    for name, weight in candidates:
        model.pattern_weight_override = weight
        ev = model.evaluate(td, target_column=target_column, imputed=imputed, return_origin_errors=True)
        rows.append({"candidate": name, "weight": weight, "mae": ev.get("mae"), "rmse": ev.get("rmse"),
                     "bias": ev.get("bias"), "scored": ev.get("scored"),
                     "network_only_mae": (ev.get("network_only") or {}).get("mae"),
                     "pattern_steps_genuine": ev.get("pattern_steps_genuine"),
                     "pattern_steps_total": ev.get("pattern_steps_total")})
        if ev.get("scored") == "served_blend" and isinstance(ev.get("mae"), (int, float)) and math.isfinite(ev["mae"]):
            current[name] = ev["origin_abs_errors"]
    if BASELINE not in current:
        model.pattern_weight_override = None
        return {"chosen": "deployed_ramp", "weight": None, "mae": None, "candidates": rows,
                "reason": "served blend could not be scored on the held-out partition; deployed ramp kept",
                "rule": RULE, "rule_id": RULE_ID}
    p = _params(params)
    pooled_history = list(history_partitions or [])[:int(p["pool_partitions"]) - 1]
    dec = decide([current] + pooled_history, previous=previous, params=p, candidates=candidates)
    model.pattern_weight_override = dec["weight"]
    chosen_row = next(r for r in rows if r["candidate"] == dec["chosen"])
    dec["origin_errors"] = current
    dec["pooled_history"] = pooled_history
    return {"chosen": dec["chosen"], "weight": dec["weight"], "mae": chosen_row["mae"], "candidates": rows,
            "reason": None, "rule": RULE, "rule_id": RULE_ID, "stability": dec}

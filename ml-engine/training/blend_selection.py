"""Validation-selected blend weight for the hybrid (network + previous-day pattern) forecaster.

Why (2026-09-28): the trainer never attached a seasonal history to the model, so its evaluation scored
the raw network alone, and the network's held-out MAE on the benchmark series swung between 232 and
2,318 rpm across trainings while the pattern component alone scores tens of rpm. Served at the deployed
ramp (9-30 % network weight) that network added hundreds of rpm to the served forecast.

The selection scores the SERVED blend on the held-out partition for a short list of candidate pattern
weights and keeps the one with the lowest MAE; the network keeps weight only when it demonstrably helps.
Ties keep the earlier candidate, so the deployed ramp is never replaced without a strict improvement.
The chosen weight is stored on the model (`pattern_weight_override`), which `predict()` already honours,
so no serving change is needed; the provenance records every candidate's score.
"""
import math

import pandas as pd

CANDIDATES = (("deployed_ramp", None), ("pattern_0.85", 0.85), ("pattern_0.95", 0.95), ("pattern_only", 1.0))
RULE = "lowest served-blend MAE on the held-out partition; ties keep the earlier candidate (deployed ramp first)"


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


def select_blend_weight(model, test_data: pd.DataFrame, imputed=None, target_column: str = "value",
                        candidates=CANDIDATES) -> dict:
    """Evaluate every candidate weight on `test_data`, set the best on the model, return the record."""
    history = getattr(model, "seasonal_history", None)
    if history is None or len(history) == 0:
        model.pattern_weight_override = None
        return {"chosen": "deployed_ramp", "weight": None, "mae": None, "candidates": [],
                "reason": "no seasonal history attached; selection skipped", "rule": RULE}
    td = _naive_utc(test_data)
    rows = []
    for name, weight in candidates:
        model.pattern_weight_override = weight
        ev = model.evaluate(td, target_column=target_column, imputed=imputed)
        mae = ev.get("mae")
        rows.append({"candidate": name, "weight": weight, "mae": mae, "rmse": ev.get("rmse"),
                     "bias": ev.get("bias"), "scored": ev.get("scored"),
                     "network_only_mae": (ev.get("network_only") or {}).get("mae"),
                     "pattern_steps_genuine": ev.get("pattern_steps_genuine"),
                     "pattern_steps_total": ev.get("pattern_steps_total")})
    usable = [r for r in rows if r["scored"] == "served_blend" and r["mae"] is not None
              and isinstance(r["mae"], (int, float)) and math.isfinite(r["mae"])]
    if not usable:
        model.pattern_weight_override = None
        return {"chosen": "deployed_ramp", "weight": None, "mae": None, "candidates": rows,
                "reason": "served blend could not be scored on the held-out partition; deployed ramp kept",
                "rule": RULE}
    best = min(usable, key=lambda r: r["mae"])          # min keeps the first of equal values
    model.pattern_weight_override = best["weight"]
    return {"chosen": best["candidate"], "weight": best["weight"], "mae": best["mae"], "candidates": rows,
            "reason": None, "rule": RULE}

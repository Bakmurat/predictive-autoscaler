#!/usr/bin/env python3
"""Baseline measurement of the pattern/percentile/confidence path (Codex D-92).

Taken BEFORE any accuracy change so that each later change is attributable. It records, per
origin, everything Codex asked to preserve: matched inputs, raw component forecasts, weights,
percentiles, confidence, matured targets, signed errors and per-step availability. Divergences
and unavailable forecasts are COUNTED, never filtered away.

This harness deliberately exercises only the components that need no trained model: the
previous-day pattern lookup, the effective percentile, the blend weights and the confidence
calculation. No model exists on the benchmark yet, so the network component is recorded as
unavailable rather than simulated -- that is the honest state, and it is what the cold-start
repair (D-88) is about.

Usage:
    eval/.venv/bin/python eval/accuracy_baseline.py --series eval/data/series-fresh.json \
        --out eval/baseline-<utc>.json [--window 144] [--history-start 2026-09-20T18:30:00Z]
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO / "ml-engine"))

from models.lstm_model import LSTMForecastModel, weighted_percentile  # noqa: E402

STEP_MINUTES = 10
STEPS_AHEAD = 6


def load_series(path: Path, history_start: str | None) -> pd.Series:
    raw = json.loads(path.read_text())
    values = raw["data"]["result"][0]["values"]
    idx, vals = [], []
    for ts, v in values:
        idx.append(datetime.fromtimestamp(int(ts), tz=timezone.utc).replace(tzinfo=None))
        vals.append(float(v))
    s = pd.Series(vals, index=pd.DatetimeIndex(idx)).sort_index()
    if history_start:
        start = datetime.fromisoformat(history_start.replace("Z", "+00:00")).replace(tzinfo=None)
        s = s[s.index >= start]
    return s


def effective_percentile(last_window: np.ndarray, mape_for_floor: float = 0.0) -> dict:
    """Reproduce the deployed percentile rule exactly (lstm_model.py:492-508)."""
    if last_window is not None and len(last_window) >= 12:
        prev_mean = float(np.mean(last_window[-12:-6]))
        last_mean = float(np.mean(last_window[-6:]))
        direction_pct = 70 if last_mean < prev_mean else 75
        direction = "falling" if last_mean < prev_mean else "rising_or_flat"
    else:
        prev_mean = last_mean = float("nan")
        direction_pct = 75
        direction = "insufficient"
    mape_pct = max(50, min(75, 75 - max(0, mape_for_floor - 10) * (25.0 / 20.0)))
    if direction_pct >= 75:
        eff = max(50, min(75, max(direction_pct, mape_pct)))
    else:
        eff = max(50, min(75, min(direction_pct, mape_pct)))
    return {
        "direction": direction,
        "direction_pct": direction_pct,
        "mape_pct": mape_pct,
        "effective_pct": eff,
        "prev_mean": prev_mean,
        "last_mean": last_mean,
    }


def pattern_per_step(series: pd.Series, origin: pd.Timestamp, effective_pct: float) -> list[dict]:
    """Per-step previous-day lookup WITHOUT the span guard and WITHOUT neighbour substitution.

    This is the measurement instrument, not the deployed code: it reports what each step could
    have had, so the deployed behaviour can be compared against it.
    """
    tolerance = pd.Timedelta(minutes=5)
    out = []
    for step in range(STEPS_AHEAD):
        target = origin + pd.Timedelta(minutes=STEP_MINUTES * (step + 1))
        day_values, day_weights, matched = [], [], []
        for d in range(1, 8):
            want = target - pd.Timedelta(days=d)
            if want < series.index[0] - tolerance:
                break
            pos = series.index.get_indexer([want], method="nearest")[0]
            if pos < 0:
                continue
            if abs(series.index[pos] - want) <= tolerance:
                day_values.append(float(series.iloc[pos]))
                day_weights.append(0.3 ** (d - 1))
                matched.append(series.index[pos].isoformat())
        rec = {
            "step": step + 1,
            "target_at": target.isoformat(),
            "support": len(day_values),
            "matched_source_timestamps": matched,
            "available": bool(day_values),
        }
        rec["value"] = (
            float(weighted_percentile(day_values, day_weights, effective_pct))
            if day_values
            else None
        )
        out.append(rec)
    return out


def blend_weights(steps_ahead: int = STEPS_AHEAD) -> list[float]:
    """The deployed schedule (lstm_model.py:379): pattern weight per step."""
    return [min(0.95, 0.7 + (s / max(steps_ahead, 1)) * 0.25) for s in range(steps_ahead)]


def confidence_from(agreement: float | None, steps_ahead: int = STEPS_AHEAD) -> float:
    horizon_penalty = max(0.4, 1.0 - (steps_ahead / 288))
    agreement_term = agreement if agreement is not None else 0.5
    return max(0.3, min(0.9, 0.5 * agreement_term + 0.5 * horizon_penalty))


def deployed_pattern(series: pd.Series, origin: pd.Timestamp, window: pd.Series,
                     effective_pct: float, api_gate: bool) -> dict:
    """What the DEPLOYED code produces, including both gates (D-88).

    api_gate mirrors ml-engine/api/main.py:542-547 (`len(pts) > len(window)`): the seasonal
    history is withheld entirely unless the full series is longer than the inference window.
    """
    if not api_gate:
        return {"source": "withheld_by_api_gate", "values": None, "steps_served": 0}
    model = LSTMForecastModel.__new__(LSTMForecastModel)
    arr, source = model._pattern_forecast(
        origin=origin.to_pydatetime(), steps_ahead=STEPS_AHEAD,
        seasonal_history=series, effective_pct=effective_pct)
    if arr is None:
        return {"source": source, "values": None, "steps_served": 0}
    return {"source": source, "values": [float(x) for x in arr], "steps_served": int(len(arr))}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--series", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--window", type=int, default=144)
    ap.add_argument("--history-start", default=None)
    ap.add_argument("--origins", type=int, default=40)
    ap.add_argument("--api-gate", choices=["legacy", "fixed"], default="fixed",
                    help="legacy reproduces the pre-D-88 `len(pts) > len(window)` gate in "
                         "api/main.py; fixed passes whatever timestamped history exists")
    args = ap.parse_args()

    series = load_series(Path(args.series), args.history_start)
    if len(series) < args.window + STEPS_AHEAD + 1:
        print(f"series too short: {len(series)} points, need "
              f"{args.window + STEPS_AHEAD + 1}", file=sys.stderr)
        return 2

    # Origins: the most recent N positions for which every target has matured.
    last_usable = len(series) - STEPS_AHEAD - 1
    first = max(args.window - 1, last_usable - args.origins + 1)
    origin_positions = list(range(first, last_usable + 1))

    records, counts = [], {
        "origins": 0,
        "deployed_pattern_unavailable": 0,
        "deployed_pattern_available": 0,
        "api_gate_withheld": 0,
        "span_guard_refused": 0,
        "steps_possible": 0,
        "steps_served_by_deployed": 0,
        "steps_discarded": 0,
        "neighbour_substituted_steps": 0,
        "network_unavailable": 0,
    }

    for pos in origin_positions:
        origin = series.index[pos]
        window = series.iloc[pos - args.window + 1: pos + 1]
        # The API hands the model the full masked series; its gate compares that against the
        # inference window. Reproduce both the gated and ungated view.
        full = series.iloc[: pos + 1]
        api_gate = (len(full) > len(window)) if args.api_gate == "legacy" else bool(len(full))

        pct = effective_percentile(window.values)
        possible = pattern_per_step(full, origin, pct["effective_pct"])
        deployed = deployed_pattern(full, origin, window, pct["effective_pct"], api_gate)

        # Span guard, reproduced for attribution.
        span_s = (full.index[-1] - full.index[0]).total_seconds()
        span_refused = span_s < 24 * 3600

        n_possible = sum(1 for p in possible if p["available"])
        n_served = deployed["steps_served"]
        # Neighbour substitution: deployed returned a value for a step that had no support.
        subs = 0
        if deployed["values"] is not None:
            subs = sum(1 for i, p in enumerate(possible) if not p["available"])

        # Matured targets and signed errors for the pattern that WAS possible.
        signed = []
        for p in possible:
            tpos = pos + p["step"]
            actual = float(series.iloc[tpos]) if tpos < len(series) else None
            err = None if (actual is None or p["value"] is None) else p["value"] - actual
            signed.append({"step": p["step"], "actual": actual, "signed_error": err})

        counts["origins"] += 1
        counts["steps_possible"] += n_possible
        counts["steps_served_by_deployed"] += n_served
        counts["steps_discarded"] += max(0, n_possible - n_served)
        counts["neighbour_substituted_steps"] += subs
        counts["network_unavailable"] += 1  # no trained model exists on the benchmark
        if not api_gate:
            counts["api_gate_withheld"] += 1
        if span_refused:
            counts["span_guard_refused"] += 1
        if deployed["values"] is None:
            counts["deployed_pattern_unavailable"] += 1
        else:
            counts["deployed_pattern_available"] += 1

        records.append({
            "origin": origin.isoformat(),
            "window_points": int(len(window)),
            "window_span_hours": round(
                (window.index[-1] - window.index[0]).total_seconds() / 3600, 3),
            "full_history_points": int(len(full)),
            "full_span_hours": round(span_s / 3600, 3),
            "api_gate_passed": bool(api_gate),
            "span_guard_refused": bool(span_refused),
            "percentile": pct,
            "blend_weights_pattern": blend_weights(),
            "network": {"available": False, "reason": "no trained model on the benchmark"},
            "confidence_deployed": confidence_from(None),
            "pattern_possible_per_step": possible,
            "pattern_deployed": deployed,
            "matured": signed,
        })

    out = {
        "kind": "accuracy_baseline_pre_change",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "series": args.series,
        "history_start": args.history_start,
        "series_points": int(len(series)),
        "series_span_hours": round(
            (series.index[-1] - series.index[0]).total_seconds() / 3600, 3),
        "window": args.window,
        "api_gate_modelled": args.api_gate,
        "counts": counts,
        "records": records,
    }
    Path(args.out).write_text(json.dumps(out, indent=2))
    print(json.dumps(counts, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

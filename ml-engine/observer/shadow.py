#!/usr/bin/env python3
"""Shadow observer: compare forecast components at the origins the controller actually used.

Codex D-85. This is an OBSERVER, deliberately not a controller change:

  * it never runs inside the reconcile loop and adds no inference call per reconcile;
  * it consumes the immutable input snapshot the controller already recorded, rather than
    querying "now" and hoping the window matches;
  * it records unavailable and failed predictions instead of skipping them, because a
    predictor that cannot answer is a result, not a gap;
  * it writes its own append-only log and touches nothing the controller owns.

The question it exists to answer is the amended one (D-86): **served hybrid versus
seasonal-only**. Raw network, previous-day and persistence are recorded alongside so the
separate question -- is the bare network any good -- can be asked later without a second run.

The input snapshot
------------------
Every controller issuance records `inference_input_end` and `sequence_length`. Those two
fields define the exact window the model saw: `sequence_length` ten-minute slots ending at
`inference_input_end`. Reconstructing from them is what makes this a shadow of the real
decision rather than a fresh, differently-aligned experiment. An observation later than
`inference_input_end` is never admitted -- see `_window_from_snapshot`.
"""

from __future__ import annotations

import json
import logging
import math
import os
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Sequence

logger = logging.getLogger(__name__)

GRID = timedelta(minutes=10)
PREDICTORS = ("served_hybrid", "raw_network", "seasonal_only", "previous_day", "persistence")


def _parse(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        t = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    return t.astimezone(timezone.utc).replace(tzinfo=None)


def _iso(t: datetime) -> str:
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def _finite(v) -> bool:
    try:
        return math.isfinite(float(v))
    except (TypeError, ValueError):
        return False


@dataclass
class StepObservation:
    """One predictor's answer for one horizon step, including the ways it can fail."""
    step: int
    target_at: str
    value: Optional[float] = None
    status: str = "ok"                 # ok | unavailable | failed | non_finite
    detail: str = ""

    @staticmethod
    def unavailable(step: int, target_at: str, why: str) -> "StepObservation":
        return StepObservation(step, target_at, None, "unavailable", why)

    @staticmethod
    def failed(step: int, target_at: str, why: str) -> "StepObservation":
        return StepObservation(step, target_at, None, "failed", why[:200])


@dataclass
class ShadowRecord:
    """One origin, every predictor, recorded whether or not it answered."""
    issued_at: str
    application: str
    namespace: str
    inference_input_end: str
    sequence_length: int
    artifact_sha256: str = ""
    model_version: str = ""
    training_cutoff: str = ""
    window_points: int = 0
    window_complete: bool = False
    predictors: Dict[str, List[dict]] = field(default_factory=dict)
    observer_version: str = "1"
    elapsed_ms: float = 0.0
    notes: List[str] = field(default_factory=list)


class ShadowObserver:
    """Replays the components at recorded origins. Pure: no cluster writes, no serving path."""

    def __init__(self, history_lookup, model_loader=None, max_seconds: float = 5.0):
        """
        history_lookup(start, end) -> ordered [(datetime, float)] for a closed interval.
        model_loader(artifact_sha256) -> a fitted model, or None when it cannot be had.
        max_seconds bounds the work per record; exceeding it is recorded, never silent.
        """
        self._history = history_lookup
        self._load_model = model_loader
        self._max_seconds = max_seconds

    # -- input snapshot ---------------------------------------------------------------
    def _window_from_snapshot(self, end: datetime, sequence_length: int):
        """The exact window the controller's model saw: `sequence_length` slots ending at `end`.

        Nothing after `end` may enter. This is the guard that keeps the shadow honest: the
        observer runs long after the fact, when later observations exist and would otherwise
        be trivially available.
        """
        start = end - GRID * (sequence_length - 1)
        pts = list(self._history(start, end))
        pts = [(t, v) for t, v in pts if t <= end]           # never the future
        pts.sort(key=lambda p: p[0])
        return pts, start

    # -- predictors -------------------------------------------------------------------
    @staticmethod
    def _persistence(window, steps, targets):
        if not window:
            return [StepObservation.unavailable(i + 1, targets[i], "empty window")
                    for i in range(steps)]
            
        last = window[-1][1]
        if not _finite(last):
            return [StepObservation(i + 1, targets[i], None, "non_finite", "last observation")
                    for i in range(steps)]
        return [StepObservation(i + 1, targets[i], float(last)) for i in range(steps)]

    def _previous_day(self, steps, targets, tolerance_min=5):
        out = []
        for i in range(steps):
            tgt = _parse(targets[i])
            want = tgt - timedelta(days=1)
            tol = timedelta(minutes=tolerance_min)
            pts = list(self._history(want - tol, want + tol))
            pts = [(t, v) for t, v in pts if abs((t - want).total_seconds()) <= tol.total_seconds()]
            if not pts:
                out.append(StepObservation.unavailable(i + 1, targets[i], "no same-time observation yesterday"))
                continue
            nearest = min(pts, key=lambda p: abs((p[0] - want).total_seconds()))
            if not _finite(nearest[1]):
                out.append(StepObservation(i + 1, targets[i], None, "non_finite", "previous-day value"))
            else:
                out.append(StepObservation(i + 1, targets[i], float(nearest[1])))
        return out

    def _from_model(self, model, window, steps, targets, origin, seasonal):
        """served_hybrid, raw_network and seasonal_only from ONE inference call.

        The model already returns its components, so the three are read from a single
        prediction: an observer must not triple the work to answer three questions.
        """
        blank = {k: [StepObservation.unavailable(i + 1, targets[i], "no model")
                     for i in range(steps)] for k in ("served_hybrid", "raw_network", "seasonal_only")}
        if model is None:
            return blank
        try:
            out = model.predict(steps_ahead=steps, origin=origin, seasonal_history=seasonal)
        except Exception as e:                                    # recorded, never skipped
            why = f"{type(e).__name__}: {e}"
            return {k: [StepObservation.failed(i + 1, targets[i], why) for i in range(steps)]
                    for k in ("served_hybrid", "raw_network", "seasonal_only")}
        comp = out.get("components", {}) or {}
        res = {}
        for key, src in (("served_hybrid", out.get("predictions")),
                         ("raw_network", comp.get("lstm")),
                         ("seasonal_only", comp.get("pattern"))):
            if key == "seasonal_only" and not comp.get("pattern_available", False):
                res[key] = [StepObservation.unavailable(i + 1, targets[i], "no genuine previous-day backing")
                            for i in range(steps)]
                continue
            if not isinstance(src, (list, tuple)) or len(src) < steps:
                res[key] = [StepObservation.unavailable(i + 1, targets[i], "component absent from the prediction")
                            for i in range(steps)]
                continue
            res[key] = [StepObservation(i + 1, targets[i], float(src[i])) if _finite(src[i])
                        else StepObservation(i + 1, targets[i], None, "non_finite", key)
                        for i in range(steps)]
        return res

    # -- one issuance -----------------------------------------------------------------
    def observe(self, issuance: dict) -> ShadowRecord:
        t0 = time.monotonic()
        forecasts = issuance.get("forecasts") or []
        steps = len(forecasts)
        targets = [f.get("target_at", "") for f in forecasts]
        end = _parse(issuance.get("inference_input_end"))
        seq = int(issuance.get("sequence_length") or 0)
        rec = ShadowRecord(
            issued_at=issuance.get("issued_at", ""),
            application=issuance.get("application", ""),
            namespace=issuance.get("namespace", ""),
            inference_input_end=issuance.get("inference_input_end", ""),
            sequence_length=seq,
            artifact_sha256=issuance.get("artifact_sha256", ""),
            model_version=str(issuance.get("model_version", "")),
            training_cutoff=issuance.get("training_cutoff", ""),
        )
        if steps == 0 or end is None or seq <= 0:
            rec.notes.append("unusable issuance record: missing forecasts, input end or sequence length")
            rec.predictors = {k: [] for k in PREDICTORS}
            rec.elapsed_ms = (time.monotonic() - t0) * 1000
            return rec
        # Every exit below stores dicts, exactly like the normal path: a record the scorer
        # cannot parse is worse than no record.

        try:
            window, start = self._window_from_snapshot(end, seq)
        except Exception as e:
            rec.notes.append(f"history lookup failed: {type(e).__name__}: {e}")
            rec.predictors = {k: [asdict(StepObservation.failed(i + 1, targets[i], "history lookup failed"))
                                  for i in range(steps)] for k in PREDICTORS}
            rec.elapsed_ms = (time.monotonic() - t0) * 1000
            return rec

        rec.window_points = len(window)
        rec.window_complete = len(window) >= seq
        if not rec.window_complete:
            rec.notes.append(f"input window incomplete: {len(window)} of {seq} slots")

        seasonal = None
        try:
            hist_start = end - timedelta(days=8)
            seasonal = list(self._history(hist_start, end))
            seasonal = [(t, v) for t, v in seasonal if t <= end]
        except Exception as e:
            rec.notes.append(f"seasonal history unavailable: {type(e).__name__}: {e}")

        model = None
        if self._load_model is not None:
            try:
                model = self._load_model(rec.artifact_sha256)
            except Exception as e:
                rec.notes.append(f"model load failed: {type(e).__name__}: {e}")

        preds = self._from_model(model, window, steps, targets, end, seasonal)
        preds["persistence"] = self._persistence(window, steps, targets)
        preds["previous_day"] = self._previous_day(steps, targets)
        rec.predictors = {k: [asdict(o) for o in v] for k, v in preds.items()}

        rec.elapsed_ms = (time.monotonic() - t0) * 1000
        if rec.elapsed_ms > self._max_seconds * 1000:
            rec.notes.append(f"observation exceeded its budget: {rec.elapsed_ms:.0f} ms > "
                             f"{self._max_seconds * 1000:.0f} ms")
        return rec


def append_record(path: str, rec: ShadowRecord) -> bool:
    """Append one record. A logging failure is reported, never raised into the caller."""
    try:
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "a") as fh:
            fh.write(json.dumps(asdict(rec), separators=(",", ":")) + "\n")
        return True
    except Exception as e:
        logger.error("shadow observer: could not append to %s: %s", path, e)
        return False


def run(issuances: Sequence[dict], observer: ShadowObserver, out_path: str) -> dict:
    """Observe every issuance. Returns a summary; never raises on a single bad record."""
    written = failed_write = 0
    per_status: Dict[str, Dict[str, int]] = {k: {} for k in PREDICTORS}
    for item in issuances:
        try:
            rec = observer.observe(item)
        except Exception as e:                                   # defensive: keep going
            logger.error("shadow observer: record failed entirely: %s", e)
            continue
        for k, obs in rec.predictors.items():
            for o in obs:
                per_status.setdefault(k, {})
                per_status[k][o["status"]] = per_status[k].get(o["status"], 0) + 1
        if append_record(out_path, rec):
            written += 1
        else:
            failed_write += 1
    return {"records_written": written, "records_not_written": failed_write,
            "status_counts": per_status}

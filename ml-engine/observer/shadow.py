#!/usr/bin/env python3
"""Shadow observer: compare forecast components at the origins the controller actually used.

Codex D-85. This is an OBSERVER, deliberately not a controller change:

  * it never runs inside the reconcile loop and adds no inference call per reconcile;
  * it reconstructs the controller's recorded window geometry from caller-supplied history,
    rather than querying a window ending "now";
  * it records unavailable and failed predictions instead of skipping them, because a
    predictor that cannot answer is a result, not a gap;
  * it writes its own append-only log and touches nothing the controller owns.

Version 2 preserves recorded served RPM and requires explicit historical state for the
seasonal component; see observer/README.md for the version 1 invalidation and input contract.

The question it exists to answer is the amended one (D-86): **served hybrid versus
seasonal-only**. Raw network, previous-day and persistence are recorded alongside so the
separate question -- is the bare network any good -- can be asked later without a second run.

The input snapshot
------------------
Every controller issuance records `inference_input_end` and `sequence_length`. These define
window geometry, not immutable input values or historical preprocessing. The caller must
supply the original observations and prove historical equivalence separately before using
components for attribution. An observation later than `inference_input_end` is never admitted
to the reconstruction -- see `_window_from_snapshot` and the README's replay limitations.
"""

from __future__ import annotations

import json
import logging
import math
import copy

import numpy as np
import pandas as pd
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
        t = ts if isinstance(ts, datetime) else datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if t.tzinfo is None:
            t = t.replace(tzinfo=timezone.utc)
    except (ValueError, TypeError, AttributeError):
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
    observer_version: str = "2"
    reconstruction: dict = field(default_factory=dict)
    elapsed_ms: float = 0.0
    notes: List[str] = field(default_factory=list)


class ShadowObserver:
    """Replays the components at recorded origins. Pure: no cluster writes, no serving path."""

    def __init__(self, history_lookup, model_loader=None, max_seconds: float = 5.0,
                 replay_state_lookup=None):
        """
        history_lookup(start, end) -> masked, genuinely observed [(datetime, float)] history.
        This adapter never imputes. A missing input slot makes reconstruction unavailable;
        use the separate exact-replay path for originally gap-filled inputs.
        model_loader(artifact_sha256) -> a fitted model, or None when it cannot be had.
        replay_state_lookup(issuance) -> {mape_for_floor, source} from that origin's
        reconstructed chronology. Absent state makes the seasonal component unavailable.
        max_seconds is an elapsed-time warning, not an interrupting execution timeout.
        """
        self._history = history_lookup
        self._load_model = model_loader
        self._max_seconds = max_seconds
        self._replay_state = replay_state_lookup

    # -- input snapshot ---------------------------------------------------------------
    def _window_from_snapshot(self, end: datetime, sequence_length: int):
        """Reconstruct `sequence_length` slots ending at the recorded `end`.

        Nothing after `end` may enter. This is the guard that keeps the shadow honest: the
        observer runs long after the fact, when later observations exist and would otherwise
        be trivially available.
        """
        start = end - GRID * (sequence_length - 1)
        pts = list(self._history(start, end))
        pts = [(_parse(t), v) for t, v in pts]
        if any(t is None for t, _ in pts):
            raise ValueError("invalid history timestamp")
        pts = [(t, v) for t, v in pts if start <= t <= end]
        pts.sort(key=lambda p: p[0])
        return pts, start

    # -- predictors -------------------------------------------------------------------
    @staticmethod
    def _persistence(window, steps, targets, origin):
        endpoint = [v for t, v in window if t == origin]
        if len(endpoint) != 1:
            return [StepObservation.unavailable(i + 1, targets[i],
                    "no unambiguous observation at recorded origin")
                    for i in range(steps)]

        last = endpoint[0]
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
            pts = [(_parse(t), v) for t, v in pts]
            pts = [(t, v) for t, v in pts if t is not None and
                   abs((t - want).total_seconds()) <= tol.total_seconds()]
            if not pts:
                out.append(StepObservation.unavailable(i + 1, targets[i], "no same-time observation yesterday"))
                continue
            nearest = min(pts, key=lambda p: abs((p[0] - want).total_seconds()))
            if not _finite(nearest[1]):
                out.append(StepObservation(i + 1, targets[i], None, "non_finite", "previous-day value"))
            else:
                out.append(StepObservation(i + 1, targets[i], float(nearest[1])))
        return out

    def _from_model(self, model, window, steps, targets, origin, seasonal, feedback):
        """Call the actual model on a request-local wrapper with the recorded input.

        Feature assembly and component algorithms remain in model.predict(), as in serving.
        Recorded served RPM is handled separately and never replaced by a reconstruction.
        """
        keys = ("raw_network", "seasonal_only")
        blank = {k: [StepObservation.unavailable(i + 1, targets[i], "no model")
                     for i in range(steps)] for k in keys}
        if model is None:
            return blank
        try:
            if model.sequence_length != len(window):
                raise ValueError("artifact sequence length differs from recorded snapshot")
            request_model = copy.copy(model)
            values = np.asarray([v for _, v in window], dtype=float).reshape(-1, 1)
            request_model.last_sequence = model.scaler.transform(values).flatten()
            timestamps = [t for t, _ in window]
            request_model.input_timestamps = timestamps
            # With no feedback, only the independent raw-network output is consumed.
            # The state-dependent seasonal/final outputs are never presented as historical.
            request_model.mape_for_floor = feedback if feedback is not None else 0.0
            out = request_model.predict(steps_ahead=steps, origin=origin,
                                        input_timestamps=timestamps,
                                        seasonal_history=seasonal)
            if (_parse(out.get("origin")) != origin or
                    [_parse(t) for t in out.get("target_timestamps", [])] !=
                    [_parse(t) for t in targets]):
                raise ValueError("model output origin/targets differ from recorded issuance")
        except Exception as e:
            why = f"{type(e).__name__}: {e}"
            return {k: [StepObservation.failed(i + 1, targets[i], why)
                        for i in range(steps)] for k in keys}
        comp = out.get("components", {}) or {}
        per_step_avail = comp.get("pattern_available_per_step")
        if not isinstance(per_step_avail, (list, tuple)) or len(per_step_avail) < steps:
            per_step_avail = [bool(comp.get("pattern_available", False))] * steps
        res = {}
        for key, src in (("raw_network", comp.get("lstm")),
                         ("seasonal_only", comp.get("pattern"))):
            if key == "seasonal_only" and feedback is None:
                res[key] = [StepObservation.unavailable(i + 1, targets[i],
                            "contemporaneous feedback state not provided") for i in range(steps)]
                continue
            try:
                arr = list(np.asarray(src, dtype=object).ravel()) if src is not None else None
            except Exception:
                arr = None
            if arr is None or len(arr) != steps:
                res[key] = [StepObservation.unavailable(i + 1, targets[i],
                            "component absent or wrong length") for i in range(steps)]
                continue
            obs = []
            for i in range(steps):
                if key == "seasonal_only" and not per_step_avail[i]:
                    obs.append(StepObservation.unavailable(i + 1, targets[i],
                               "no genuine previous-day backing"))
                elif _finite(arr[i]):
                    obs.append(StepObservation(i + 1, targets[i], float(arr[i])))
                else:
                    obs.append(StepObservation(i + 1, targets[i], None, "non_finite", key))
            res[key] = obs
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

        # These values are what the controller actually recorded, even when replay fails.
        served = []
        for i, f in enumerate(forecasts):
            value = f.get("rpm")
            if value is None:
                served.append(StepObservation.unavailable(i + 1, targets[i], "recorded RPM absent"))
            elif _finite(value):
                served.append(StepObservation(i + 1, targets[i], float(value)))
            else:
                served.append(StepObservation(i + 1, targets[i], None, "non_finite", "recorded RPM"))
        rec.predictors["served_hybrid"] = [asdict(o) for o in served]
        expected_targets = [end + GRID * (i + 1) for i in range(steps)]
        if (steps > 6 or end.minute % 10 or end.second or end.microsecond or
                [_parse(t) for t in targets] != expected_targets or
                [f.get("step") for f in forecasts] != list(range(1, steps + 1))):
            rec.notes.append("invalid issuance target grid")
            for observation in rec.predictors["served_hybrid"]:
                observation.update(status="failed", detail="invalid issuance target grid")
            for key in PREDICTORS:
                if key != "served_hybrid":
                    rec.predictors[key] = [asdict(StepObservation.failed(i + 1, targets[i],
                                           "invalid issuance target grid")) for i in range(steps)]
            rec.elapsed_ms = (time.monotonic() - t0) * 1000
            return rec

        try:
            window, start = self._window_from_snapshot(end, seq)
        except Exception as e:
            rec.notes.append(f"history lookup failed: {type(e).__name__}: {e}")
            for key in PREDICTORS:
                if key != "served_hybrid":
                    rec.predictors[key] = [asdict(StepObservation.failed(i + 1, targets[i],
                                           "history lookup failed")) for i in range(steps)]
            rec.elapsed_ms = (time.monotonic() - t0) * 1000
            return rec

        rec.window_points = len(window)
        expected_window = [start + GRID * i for i in range(seq)]
        rec.window_complete = ([t for t, _ in window] == expected_window and
                               all(_finite(v) for _, v in window))
        if not rec.window_complete:
            rec.notes.append(f"input window incomplete or invalid: {len(window)} of {seq} slots")

        # Never allow model.predict's saved-history fallback to supply a different snapshot.
        seasonal = pd.Series(dtype=float, index=pd.DatetimeIndex([]))
        try:
            hist = [(_parse(t), v) for t, v in self._history(end - timedelta(days=8), end)]
            if any(t is None for t, _ in hist):
                raise ValueError("invalid seasonal timestamp")
            hist = [(t, v) for t, v in hist if end - timedelta(days=8) <= t <= end]
            if len({t for t, _ in hist}) != len(hist) or not all(_finite(v) for _, v in hist):
                raise ValueError("duplicate or non-finite seasonal history")
            seasonal = pd.Series([v for _, v in hist],
                                 index=pd.DatetimeIndex([t for t, _ in hist])).sort_index()
        except Exception as e:
            rec.notes.append(f"seasonal history unavailable: {type(e).__name__}: {e}")

        feedback = None
        rec.reconstruction = {"history_contract": "caller-supplied masked observed history; no imputation",
                              "feedback_source": None}
        if self._replay_state is not None:
            try:
                state = self._replay_state(issuance)
                if (not isinstance(state, dict) or not _finite(state.get("mape_for_floor")) or
                        float(state["mape_for_floor"]) < 0 or
                        not isinstance(state.get("source"), str) or not state["source"].strip()):
                    raise ValueError("feedback requires finite nonnegative MAPE and source")
                feedback = float(state["mape_for_floor"])
                rec.reconstruction.update(feedback_source=state["source"], mape_for_floor=feedback)
            except Exception as e:
                rec.notes.append(f"feedback state unavailable: {type(e).__name__}: {e}")

        model = None
        if self._load_model is not None and rec.window_complete:
            try:
                model = self._load_model(rec.artifact_sha256)
            except Exception as e:
                rec.notes.append(f"model load failed: {type(e).__name__}: {e}")
        if rec.window_complete:
            preds = self._from_model(model, window, steps, targets, end, seasonal, feedback)
        else:
            preds = {k: [StepObservation.unavailable(i + 1, targets[i], "invalid input snapshot")
                         for i in range(steps)] for k in ("raw_network", "seasonal_only")}
        preds["served_hybrid"] = served
        preds["persistence"] = self._persistence(window, steps, targets, end)
        try:
            preds["previous_day"] = self._previous_day(steps, targets)
        except Exception as e:
            preds["previous_day"] = [StepObservation.failed(i + 1, targets[i],
                                     f"history lookup failed: {e}") for i in range(steps)]
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

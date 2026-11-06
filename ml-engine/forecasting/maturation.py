"""When an issued window becomes a terminal outcome, and what that outcome is (shared by the service and the replay).

A window W_t = {t+Δ, ..., t+L} can close once window_end + settlement has passed. It is scored only when every target
grid point has been received by now: Y_t is the maximum of those sampled values (RPS), never interpolated. A missing
point is retried until window_end + settlement + grace; after that the window closes 'unscorable' and stays so,
whatever arrives later (late values are journal corrections or later observations, never a backfilled online outcome).
Received-by-now is strict: a value whose receipt time is after `now` does not exist yet, even if its event time is
past.

The outcome payload holds Y, the issued base and upper bound, coverage (null when no bound was issued: a cold-start
window can seed scores but cannot claim coverage), the signed residual Y - b and |Y - b|, a digest of the target values
and the completion time. Calibration (C2.4) adds the pinball loss and the adaptive-level update on top of this record.
"""

from __future__ import annotations

from typing import Mapping, Optional, Sequence, Tuple

from forecasting.contracts import canonical_json, sha256_hex

OUTCOME_SCHEMA = "forecast-outcome/v1"


def window_outcome(*, targets: Sequence[int], base_rps: float, upper_rps: Optional[float],
                   received: Mapping[int, Tuple[float, int]], now_ms: int, settlement_s: int,
                   grace_s: int) -> Optional[Tuple[str, dict]]:
    """None while the window is pending, else (status, payload) with status 'scored' or 'unscorable'.

    `received` maps event time -> (value RPS, first receipt ms)."""
    window_end = targets[-1]
    close_ms = (window_end + settlement_s) * 1000
    if now_ms < close_ms:
        return None
    values, missing, completed = [], [], 0
    for t in targets:
        got = received.get(t)
        if got is None or got[1] > now_ms:
            missing.append(t)
        else:
            values.append(got[0])
            completed = max(completed, got[1])
    if missing:
        if now_ms < close_ms + grace_s * 1000:
            return None
        return "unscorable", {"schema": OUTCOME_SCHEMA, "reason": "missing_truth", "missing_targets": len(missing),
                              "first_missing": missing[0], "closed_at_ms": now_ms}
    y = max(values)
    return "scored", {"schema": OUTCOME_SCHEMA, "y_window_max_rps": y, "base_window_max_rps": base_rps,
                      "upper_window_max_rps": upper_rps,
                      "covered": None if upper_rps is None else y <= upper_rps,
                      "residual_rps": y - base_rps, "abs_error_rps": abs(y - base_rps),
                      "values_sha256": sha256_hex(canonical_json(values)), "completed_at_ms": completed}

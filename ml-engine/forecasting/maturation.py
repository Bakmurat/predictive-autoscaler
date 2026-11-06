"""When an issued window becomes a terminal outcome, and what that outcome is (shared by the service and the replay).

A window W_t = {t+Δ, ..., t+L} can close once window_end + settlement has passed. It is scored only when every target
grid point was received by the FIXED truth deadline window_end + settlement + grace (and by now): Y_t is the maximum of
those sampled values (RPS), never interpolated. Processing later never extends the deadline: a value received after it
does not count, whenever (and after however many restarts) the window is evaluated, so the result depends only on the
receipts, not on when the service happened to look. Until the deadline a missing point is waited for; at or after it
the window closes 'unscorable' and stays so (late values remain journal observations, never a backfilled outcome).
Received-by-now is strict as well: a value whose receipt time is after `now` does not exist yet.

Truth inside a declared exclusion (an incident window of the dataset or installation) is explicitly unavailable: the
window closes 'unscorable' with reason 'excluded' as soon as it can close. It stays in the availability denominator
and never reaches scoring or calibration.

The outcome payload holds Y, the issued base and upper bound, coverage (null when no bound was issued: a cold-start
window can seed scores but cannot claim coverage), the signed residual Y - b and |Y - b|, a digest of the target values
and the completion time. Calibration (C2.4) adds the pinball loss and the adaptive-level update on top of this record.
"""

from __future__ import annotations

from typing import Callable, Mapping, Optional, Sequence, Tuple

from forecasting.contracts import canonical_json, sha256_hex

OUTCOME_SCHEMA = "forecast-outcome/v1"


def truth_deadline_ms(window_end: int, settlement_s: int, grace_s: int) -> int:
    return (window_end + settlement_s + grace_s) * 1000


def check_window(targets: Sequence[int]) -> None:
    """The exact window: one or more grid times, strictly increasing at a constant step (no hole, no repeat)."""
    if not targets or any(not isinstance(t, int) or isinstance(t, bool) for t in targets):
        raise ValueError("a window has integer target times")
    steps = {b - a for a, b in zip(targets, targets[1:])}
    if len(steps) > 1 or (steps and min(steps) <= 0):
        raise ValueError("window targets must be evenly spaced and increasing")


def window_outcome(*, targets: Sequence[int], base_rps: float, upper_rps: Optional[float],
                   received: Mapping[int, Tuple[float, int]], now_ms: int, settlement_s: int, grace_s: int,
                   excluded: Optional[Callable[[int], Optional[str]]] = None) -> Optional[Tuple[str, dict]]:
    """None while the window is pending, else (status, payload) with status 'scored' or 'unscorable'.

    `received` maps event time -> (value RPS, first receipt ms); `excluded(t)` names the exclusion covering t, if any."""
    check_window(targets)
    window_end = targets[-1]
    close_ms = (window_end + settlement_s) * 1000
    deadline_ms = truth_deadline_ms(window_end, settlement_s, grace_s)
    if now_ms < close_ms:
        return None
    if excluded is not None:
        hit = [(t, why) for t in targets if (why := excluded(t))]
        if hit:
            return "unscorable", {"schema": OUTCOME_SCHEMA, "reason": "excluded", "excluded_targets": len(hit),
                                  "first_excluded": hit[0][0], "exclusion": hit[0][1], "deadline_ms": deadline_ms}
    cutoff = min(now_ms, deadline_ms)          # nothing received after the fixed deadline is ever admitted
    values, missing, completed = [], [], 0
    for t in targets:
        got = received.get(t)
        if got is None or got[1] > cutoff:
            missing.append(t)
        else:
            values.append(got[0])
            completed = max(completed, got[1])
    if missing:
        if now_ms < deadline_ms:
            return None
        return "unscorable", {"schema": OUTCOME_SCHEMA, "reason": "missing_truth", "missing_targets": len(missing),
                              "first_missing": missing[0], "deadline_ms": deadline_ms}
    y = max(values)
    return "scored", {"schema": OUTCOME_SCHEMA, "y_window_max_rps": y, "targets": len(targets),
                      "base_window_max_rps": base_rps,
                      "upper_window_max_rps": upper_rps,
                      "covered": None if upper_rps is None else y <= upper_rps,
                      "residual_rps": y - base_rps, "abs_error_rps": abs(y - base_rps),
                      "values_sha256": sha256_hex(canonical_json(values)), "completed_at_ms": completed}

"""Deterministic workload programs, k6 arrival plans and fault timetables (EVAL-C28-MANIFEST section 5).

Every program is a function of (kind, seed, capacity c) only, so the arms of one paired block see identical arrivals.
Demand is written u = offered_RPS / c. A day is 1,440 one-minute targets: the piecewise-linear daily shape (or the
scored day's overrides) averaged over each minute, times a seeded +-5 % minute perturbation. All step changes fall on
minute boundaries. No schedule or fault timetable is ever given to a forecaster.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Tuple

from . import frozen as F

DAY_S = 86_400
MINUTES = 1_440


def _interp(points: List[Tuple[float, float]], t: float) -> float:
    """Piecewise-linear value at t (seconds). Two points at one time are a step: the later point applies from t on."""
    for (t0, u0), (t1, u1) in zip(points, points[1:]):
        if t0 <= t < t1:
            return u0 + (u1 - u0) * (t - t0) / (t1 - t0) if t1 > t0 else u1
    return points[-1][1]


def ordinary_points() -> List[Tuple[float, float]]:
    return [(h * 3600.0, float(u)) for h, u in F.DAILY_SHAPE]


def scored_points() -> List[Tuple[float, float]]:
    """The scored day: the ordinary cycle with the fixed challenge overrides of section 5."""
    h = 3600.0
    o = ordinary_points()
    ou = lambda t: _interp(o, t)  # noqa: E731
    pts = [
        (0, ou(0)), (1 * h, ou(1 * h)),
        (1 * h, 8), (1.5 * h, 8), (1.5 * h, ou(1.5 * h)),          # 01:00 unannounced step 2c -> 8c for 30 min
        (6 * h, ou(6 * h)), (9 * h, ou(9 * h)),                      # 06:00-09:00 ordinary ramp 2c -> 8c
        (9 * h, 2), (12 * h, 2),                                     # 09:00-12:00 holiday departure: 2c, not 8c
        (12 * h, 3), (12 * h + 20 * 60, 3), (12 * h + 20 * 60, 7), (14 * h, 7),   # metric outage window
        (14 * h, 3), (14 * h + 20 * 60, 3), (14 * h + 20 * 60, 7), (16 * h, 7),   # forecast outage window
        (16 * h, 3), (16 * h + 15 * 60, 3), (16 * h + 15 * 60, 8), (18 * h, 8),   # quota-blocked window
        (18 * h, ou(18 * h)), (20 * h, ou(20 * h)),                  # 18:00-20:00 ordinary 9c -> 5c
        (20 * h, 3), (20 * h + 15 * 60, 3), (20 * h + 15 * 60, 8), (22 * h, 8),   # slow-start window
        (22 * h, ou(22 * h)), (24 * h, ou(24 * h)),                  # 22:00-24:00 ordinary decline
    ]
    return [(float(t), float(u)) for t, u in pts]


def minute_means(points: List[Tuple[float, float]]) -> List[float]:
    """Mean u over each minute (exact for piecewise-linear segments; steps fall on minute boundaries)."""
    out = []
    for k in range(MINUTES):
        a, b = 60.0 * k, 60.0 * (k + 1)
        # Simpson on the minute, sampling just inside its ends so a step at a boundary belongs to the right minute.
        eps = 1e-6
        out.append((_interp(points, a + eps) + 4 * _interp(points, (a + b) / 2) + _interp(points, b - eps)) / 6)
    return out


def perturbation(seed: int) -> List[float]:
    rng = random.Random(f"c28-perturbation-{seed}")
    return [1.0 + rng.uniform(-F.PERTURBATION, F.PERTURBATION) for _ in range(MINUTES)]


def program(kind: str, seed: int) -> List[float]:
    """Per-minute demand in units of c: kind 'ordinary' (warm-up, qualification) or 'scored' (challenge day)."""
    if kind not in ("ordinary", "scored"):
        raise ValueError(f"unknown program {kind!r}")
    base = minute_means(scored_points() if kind == "scored" else ordinary_points())
    m = perturbation(seed)
    return [u * f for u, f in zip(base, m)]


def jitter_seed(seed: int) -> int:
    """The k6 arrival-jitter seed: independent of the perturbation stream, still a function of the block seed."""
    return int(hashlib.sha256(f"c28-jitter-{seed}".encode()).hexdigest()[:8], 16)


def k6_plan(kind: str, seed: int, capacity_rps: float, target: str, accelerate: int = 1) -> Dict:
    """The k6 ramping-arrival-rate plan (timeUnit 1m): each program minute is a short ramp to its target, then flat.

    accelerate > 1 plays the program faster for the kind smoke only (each program minute lasts 60/accelerate s at the
    same arrival rates); it is labelled accelerated-smoke and is never a day or a confirmation run."""
    if not (capacity_rps > 0) or accelerate < 1 or 60_000 % accelerate:
        raise ValueError("capacity must be positive and accelerate must divide 60,000 ms")
    us = program(kind, seed)
    per_min = [max(0, round(u * capacity_rps * 60)) for u in us]   # iterations per real minute
    step_ms = 60_000 // accelerate
    ramp_ms = 1_000 if step_ms > 1_000 else step_ms
    stages, scheduled, prev = [], [], per_min[0]
    for r in per_min:
        stages.append({"target": r, "duration": f"{ramp_ms}ms"})
        if step_ms > ramp_ms:
            stages.append({"target": r, "duration": f"{step_ms - ramp_ms}ms"})
        # Expected scheduled arrivals in this program minute: the ramp averages the two targets.
        scheduled.append(((prev + r) / 2 * ramp_ms + r * (step_ms - ramp_ms)) / 60_000)
        prev = r
    max_rps = max(per_min) / 60
    pre = int(math.ceil(max_rps * F.REQUEST_TIMEOUT_S * 1.5)) + 10
    plan = {"schema": "c28-k6-plan/v1", "kind": kind, "seed": seed, "capacity_rps": capacity_rps, "target": target,
            "accelerate": accelerate, "label": "confirmation" if accelerate == 1 else "accelerated-smoke",
            "time_unit": "1m", "start_rate": per_min[0], "stages": stages,
            "scheduled_per_minute": scheduled, "pre_allocated_vus": pre, "max_vus": 2 * pre,
            "timeout": f"{F.REQUEST_TIMEOUT_S}s", "jitter_seed": jitter_seed(seed), "retries": 0}
    plan["digest"] = digest(plan)
    return plan


def digest(obj) -> str:
    body = {k: v for k, v in obj.items() if k != "digest"} if isinstance(obj, dict) else obj
    return hashlib.sha256(json.dumps(body, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _ts(t: datetime) -> str:
    return t.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def fault_timetable(block_start: datetime) -> Dict:
    """Absolute fault windows of a scored day starting at block_start (a UTC midnight)."""
    if block_start.tzinfo is None or block_start.utcoffset() != timedelta(0) or \
            (block_start.hour, block_start.minute, block_start.second, block_start.microsecond) != (0, 0, 0, 0):
        raise ValueError("a scored block starts at a UTC midnight")
    at = lambda h, m=0: block_start + timedelta(hours=h, minutes=m)  # noqa: E731
    return {
        "schema": "c28-faults/v1", "block_start": _ts(block_start),
        # Each proxy reads its own file (tcpproxy MODE): outages refuse and cut the evaluation-owned route.
        "metrics_route": {"outages": [{"start": _ts(at(12, 15)), "end": _ts(at(12, 30))}]},
        # Section 12: the route outage is the request-metric arms' (H-RPS, PA-R, PA-N, PA-E); H-CPU keeps normal CPU
        # telemetry and the full program, its CPU-metric outage N/A (not tested). Recorded so every block says so.
        "metric_outage_applicability": dict(F.METRIC_OUTAGE_APPLICABILITY),
        "forecast_route": {"outages": [{"start": _ts(at(14, 15)), "end": _ts(at(14, 35))}]},
        # The server reads this at process start: pods started in the window become ready 120 s later.
        "slow_start": {"slow_starts": [{"start": _ts(at(20)), "end": _ts(at(20, 45)), "extra_startup_seconds": 120}]},
        # Applied by the injector to the PriorityClass-scoped ResourceQuota (the only live mutation).
        "quota": [{"at": _ts(at(16, 10)), "action": "deny_new_pods"}, {"at": _ts(at(16, 40)), "action": "restore"}],
    }


def session_order(profiles=tuple(F.PROFILES), arms=F.ARMS) -> List[Dict]:
    """The arm-session order, frozen before collection (seed 20261106): two sessions per arm and profile (seeds
    201-203, then 204-205). Session 2 runs the arms in the reverse of session 1's shuffled order, so early and late
    calendar positions balance; the profiles alternate."""
    rng = random.Random(F.ORDER_SEED)
    halves = [list(F.CONFIRMATION_SEEDS[:3]), list(F.CONFIRMATION_SEEDS[3:])]
    order = []
    shuffled = {p: rng.sample(list(arms), len(arms)) for p in profiles}
    for session, seeds in enumerate(halves, start=1):
        for p in profiles:
            seq = shuffled[p] if session == 1 else list(reversed(shuffled[p]))
            for arm in seq:
                order.append({"profile": p, "arm": arm, "session": session, "seeds": seeds})
    return order

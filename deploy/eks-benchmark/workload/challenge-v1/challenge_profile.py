"""Reference implementation of the challenge-v1 offered-load schedule (Python mirror of load.js).

The offered request rate is a pure function of wall-clock time, the sealed seed and the parameters
in profile.json, so every generator offers identical traffic and any checker can recompute the
planned load. Before `regime_start` it is exactly the repeating-v2 hourly profile.

multiplier(slot k) = clip(1 + noise_k + drift_k + level_k, lo, hi), per ten-minute slot, where
  noise_k : AR(1), phi, stationary sd sigma, output clipped to +/-clip (the recursion is not clipped)
  drift_k : triangle wave, period P hours, amplitude A (0 -> +A -> 0 -> -A -> 0)
  level_k : piecewise-constant level shifts; at slot k, if at least `min_dwell_slots` have passed
            since the last shift and u < p, the level becomes uniform(-max_abs, +max_abs)
Randomness comes from a counter-based mulberry32 variant (seed, slot, stream), identical in JS.
"""
import json
import math
import os
from datetime import datetime, timezone
from functools import lru_cache

HERE = os.path.dirname(os.path.abspath(__file__))
PROFILE = json.load(open(os.path.join(HERE, "profile.json")))
SLOT = PROFILE["slot_seconds"]
START = int(datetime.fromisoformat(PROFILE["regime_start"].replace("Z", "+00:00")).timestamp())
M32 = 0xFFFFFFFF


def _imul(a, b):
    return (a * b) & M32


def u01(seed, k, j):
    """Uniform in [0, 1) for (seed, slot k, stream j); bit-identical to load.js u01()."""
    a = (seed + _imul(k & M32, 0x9E3779B1) + _imul(j, 0x85EBCA6B)) & M32
    a = (a + 0x6D2B79F5) & M32
    t = _imul(a ^ (a >> 15), a | 1)
    t = (t ^ ((t + _imul(t ^ (t >> 7), t | 61)) & M32)) & M32
    return ((t ^ (t >> 14)) & M32) / 4294967296.0


def normal(seed, k):
    u1, u2 = u01(seed, k, 0), u01(seed, k, 1)
    return math.sqrt(-2.0 * math.log(1.0 - u1)) * math.cos(2.0 * math.pi * u2)


def drift(k):
    amp, period = PROFILE["drift"]["amplitude"], PROFILE["drift"]["period_hours"]
    x = ((k * SLOT / 3600.0) % period) / period
    if x < 0.25:
        tri = x / 0.25
    elif x < 0.75:
        tri = 1.0 - (x - 0.25) / 0.25
    else:
        tri = -1.0 + (x - 0.75) / 0.25
    return amp * tri


@lru_cache(maxsize=4)
def multipliers(n_slots):
    """Multipliers for slots 0 .. n_slots-1 after regime_start, with the components that made them."""
    seed = PROFILE["seed"]
    nz, sh = PROFILE["noise"], PROFILE["shifts"]
    lo, hi = PROFILE["multiplier_bounds"]
    a = nz["sigma"] * math.sqrt(1.0 - nz["phi"] ** 2)
    n, level, last = 0.0, 0.0, -10 ** 9
    out = []
    for k in range(n_slots):
        n = nz["phi"] * n + a * normal(seed, k)
        nc = max(-nz["clip"], min(nz["clip"], n))
        if k - last >= sh["min_dwell_slots"] and u01(seed, k, 2) < sh["probability_per_slot"]:
            level = (2.0 * u01(seed, k, 3) - 1.0) * sh["max_abs_level"]
            last = k
        d = drift(k)
        out.append((max(lo, min(hi, 1.0 + nc + d + level)), nc, d, level))
    return tuple(out)


def rate_at(t):
    """Offered requests per minute during the slot containing epoch second t (rounded)."""
    base = PROFILE["base_pattern_utc_rpm"][datetime.fromtimestamp(t, tz=timezone.utc).hour]
    if t < START:
        return math.floor(base + 0.5)
    k = (int(t) - START) // SLOT
    return math.floor(base * multipliers(k + 1)[k][0] + 0.5)


def planned_requests(hour_start):
    """Planned requests over [hour_start, hour_start + 3600) (for the hourly load gates)."""
    return sum(rate_at(hour_start + i * SLOT) * SLOT / 60.0 for i in range(3600 // SLOT))

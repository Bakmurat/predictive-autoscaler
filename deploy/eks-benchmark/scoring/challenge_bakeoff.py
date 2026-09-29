#!/usr/bin/env python3
"""Offline bake-off of forecasting methods on challenge-v1-style traffic (fresh seeds, never the sealed one).

Why: the 2026-09-26 model lab compared 53 forecasters on the real trace and on burst / level-shift
stress cases, but never on the challenge-v1 process now driving the benchmark (bounded AR(1) noise,
slow drift, level shifts). This script generates that exact process with other seeds, runs each method
under the same causal rolling-origin protocol on the ten-minute grid, and scores the replicas the
operator would have set with its own rule. It is descriptive evidence for choosing a candidate; it is
not a benchmark result and claims nothing about the live arms.

Methods (each returns six ten-minute steps from an origin; only the +10 and +20 steps enter the
20-minute lead window the operator uses):
  e1            the deployed seasonal ensemble (models/seasonal_ensemble.py, raw = 0.5 HW + 0.5 profile-AR)
  hw            its Holt-Winters component alone
  profile_ar    its seven-day profile + AR(3) component alone
  profile7      seven-day same-slot mean, no residual model
  yesterday     same slot one day earlier
  persistence   last observed value
  profile_ratio profile7 scaled by an EWMA of the last hour's actual / profile ratio (level-adaptive)
  theta         standard Theta (SES with drift on the deseasonalised window, refit every origin)
  median3       per-step median of hw, profile_ar, theta   (the lab's best worst case)
  mean3         per-step mean of the same three
Replay arms without a forecaster: reactive_only (desired = ceil(current / per-pod rpm)) and oracle
(perfect knowledge of the next two slots).

Controller replay (mirrors k8s-operator/controllers/predictiveautoscaler_controller.go on the grid):
  predictive pods = ceil((max(step+10, step+20) + margin) / per_pod_rpm)
  desired = clamp(max(predictive pods, ceil(current / per_pod_rpm)), min, max)
  scale-up takes effect for the next slot; scale-down only after desired has been below the Ready count
  for one full slot (the 5-minute stabilisation hold, rounded up to the grid); when a slot's requirement
  exceeds the Ready count the reactive path catches up after `reactive_lag_min` minutes of shortage.
  shortage = (required - ready) x lag minutes; surplus = (ready - required) x 10 minutes.
Margin policies: none, q90 (each method's own trailing-24 h 90th-percentile lead-window error, the
live E1 rule), fixed10 (10 % of the lead value).

Usage:
  challenge_bakeoff.py [--seeds 5] [--first-seed 1] [--challenge-days 14] [--warm-days 7]
                       [--per-pod-rpm 600] [--max-replicas 12] [--json out.json] [--methods a,b]
  challenge_bakeoff.py --real series.json --app nginx-test   # replay the harness on a real series
"""
import argparse
import datetime
import json
import math
import os
import sys
from collections import OrderedDict

import numpy as np

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, "..", "..", ".."))
sys.path.insert(0, os.path.join(ROOT, "ml-engine"))
sys.path.insert(0, os.path.join(ROOT, "deploy", "eks-benchmark", "workload", "challenge-v1"))
from models import seasonal_ensemble as se  # noqa: E402
import challenge_profile as cp  # noqa: E402

SLOT = se.SLOT_SECONDS
SEASON = se.SEASON
STEPS = se.STEPS
LEAD_STEPS = 2                      # +10 and +20 minutes lie inside the 20-minute lead window
MEASUREMENT_NOISE = 0.003           # relative jitter of the sampled 1-minute rate
DEFAULT_T0 = int(datetime.datetime(2026, 9, 21, tzinfo=datetime.timezone.utc).timestamp())


# ------------------------------------------------------------------------------------------
# Traffic: the challenge-v1 process with an arbitrary seed
# ------------------------------------------------------------------------------------------

def multipliers_for(seed, n_slots, profile=None):
    """Same recursion as challenge_profile.multipliers, for any seed (bit-identical for the sealed one)."""
    p = profile or cp.PROFILE
    nz, sh = p["noise"], p["shifts"]
    lo, hi = p["multiplier_bounds"]
    a = nz["sigma"] * math.sqrt(1.0 - nz["phi"] ** 2)
    n, level, last = 0.0, 0.0, -10 ** 9
    out = []
    for k in range(n_slots):
        n = nz["phi"] * n + a * cp.normal(seed, k)
        nc = max(-nz["clip"], min(nz["clip"], n))
        if k - last >= sh["min_dwell_slots"] and cp.u01(seed, k, 2) < sh["probability_per_slot"]:
            level = (2.0 * cp.u01(seed, k, 3) - 1.0) * sh["max_abs_level"]
            last = k
        out.append(max(lo, min(hi, 1.0 + nc + cp.drift(k) + level)))
    return out


def merged_profile(override=None):
    """A copy of the sealed profile with nested overrides applied (stress variants of the process)."""
    p = json.loads(json.dumps(cp.PROFILE))
    for key, val in (override or {}).items():
        if isinstance(val, dict) and isinstance(p.get(key), dict):
            p[key].update(val)
        else:
            p[key] = val
    return p


def offered_rates(seed, warm_days, challenge_days, t0=DEFAULT_T0, profile=None, events=None):
    """Offered rpm per slot: repeating-v2 for warm_days, then the challenge process for challenge_days.

    `events` overlays the lab's stress cases on the challenge process (slot indices count from t0):
      {"bursts": [(day, minute_of_day, width_slots, factor), ...],   # e.g. (3, 11*60+40, 6, 2.5)
       "shift": (day, minute_of_day, factor)}                          # e.g. (7, 12*60, 1.55)
    `day` counts from the first challenge day. Returns (rates, windows) where windows holds the
    scoring windows as half-open ranges of OBSERVATION slots: the sampled series at slot j carries
    rates[j - 1] (`sampled_series`), so an event on offered slots [at, at + width) is observed on
    [at + 1, at + width + 1) and the windows are placed there ("burst_inside" = the observed burst,
    "burst_after" = the 2 h after it, "shift_24h" = the first 24 h of the observed shift). `replay`
    and the window MAE key their per-slot sums on the same observation index (k + 1, k + 2)."""
    profile = profile or cp.PROFILE
    base = profile["base_pattern_utc_rpm"]
    n_warm, n_ch = warm_days * SEASON, challenge_days * SEASON
    mult = multipliers_for(seed, n_ch, profile)
    rates = []
    for k in range(n_warm + n_ch):
        t = t0 + k * SLOT
        hour = datetime.datetime.fromtimestamp(t, tz=datetime.timezone.utc).hour
        m = 1.0 if k < n_warm else mult[k - n_warm]
        rates.append(base[hour] * m)
    windows = {"burst_inside": [], "burst_after": [], "shift_24h": []}
    for day, minute, width, factor in (events or {}).get("bursts", []):
        at = n_warm + day * SEASON + minute // (SLOT // 60)
        for k in range(at, min(len(rates), at + width)):
            rates[k] *= factor
        windows["burst_inside"].append((at + 1, at + width + 1))
        windows["burst_after"].append((at + width + 1, at + width + 13))
    shift = (events or {}).get("shift")
    if shift:
        day, minute, factor = shift
        at = n_warm + day * SEASON + minute // (SLOT // 60)
        for k in range(at, len(rates)):
            rates[k] *= factor
        windows["shift_24h"].append((at + 1, at + SEASON + 1))
    return [math.floor(r + 0.5) for r in rates], windows


def sampled_series(rates, seed, t0=DEFAULT_T0, noise=MEASUREMENT_NOISE):
    """What the operator and the API observe: the 1-minute rate ending exactly at the slot boundary,
    which still carries the previous slot's offered rate (the live sampling artefact)."""
    rng = np.random.default_rng(seed)
    pts = []
    for k, _ in enumerate(rates):
        level = rates[k - 1] if k > 0 else rates[0]
        pts.append((t0 + k * SLOT, float(level) * (1.0 + noise * rng.standard_normal())))
    return pts


# ------------------------------------------------------------------------------------------
# Forecasters
# ------------------------------------------------------------------------------------------

def _finite(v):
    return v is not None and np.isfinite(v)


def theta_forecast(yf, origin, window=7 * SEASON, steps=STEPS):
    """Standard Theta: SES with drift on the deseasonalised window (Hyndman & Billah 2003 form),
    multiplicative seasonal indices from the window's slot-of-day means; refit at every origin."""
    lo = max(0, origin - window + 1)
    z = yf[lo:origin + 1]
    if len(z) < 2 * SEASON or not np.all(np.isfinite(z)):
        return [float("nan")] * steps
    slots = (np.arange(lo, origin + 1)) % SEASON
    mean_all = float(np.mean(z))
    if mean_all <= 0:
        return [float("nan")] * steps
    S = np.ones(SEASON)
    for s in range(SEASON):
        sel = z[slots == s]
        if sel.size:
            S[s] = max(1e-6, float(np.mean(sel)) / mean_all)
    d = z / S[slots]
    n = d.size
    x = np.arange(n, dtype=float)
    b = float(np.polyfit(x, d, 1)[0])            # drift of the theta-0 line
    # SES with alpha chosen to minimise one-step squared error (golden-section search)
    def sse(alpha):
        level = d[0]
        err = 0.0
        for v in d[1:]:
            err += (v - level) ** 2
            level = alpha * v + (1 - alpha) * level
        return err
    lo_a, hi_a = 0.01, 0.99
    g = (math.sqrt(5) - 1) / 2
    a1, a2 = hi_a - g * (hi_a - lo_a), lo_a + g * (hi_a - lo_a)
    f1, f2 = sse(a1), sse(a2)
    for _ in range(30):
        if f1 < f2:
            hi_a, a2, f2 = a2, a1, f1
            a1 = hi_a - g * (hi_a - lo_a)
            f1 = sse(a1)
        else:
            lo_a, a1, f1 = a1, a2, f2
            a2 = lo_a + g * (hi_a - lo_a)
            f2 = sse(a2)
    alpha = (lo_a + hi_a) / 2
    level = d[0]
    for v in d[1:]:
        level = alpha * v + (1 - alpha) * level
    out = []
    for h in range(1, steps + 1):
        drift_term = 0.5 * b * (h - 1 + 1.0 / alpha - (1 - alpha) ** n / alpha)
        out.append(max(0.0, (level + drift_term) * S[(origin + h) % SEASON]))
    return out


def profile_ratio_forecast(y, yf, origin, steps=STEPS, halflife_slots=3.0, support=6, clip=(0.6, 1.6)):
    """Seven-day profile scaled by an EWMA of the last hour's actual / profile ratio."""
    w, num, den = math.log(2) / halflife_slots, 0.0, 0.0
    for j in range(support):
        t = origin - j
        if t < 0:
            break
        p = se.profile_at(y, t)
        if _finite(p) and p > 0 and _finite(yf[t]):
            wt = math.exp(-w * j)
            num += wt * (yf[t] / p)
            den += wt
    ratio = min(clip[1], max(clip[0], num / den)) if den > 0 else 1.0
    out = []
    for s in range(1, steps + 1):
        p = se.profile_at(y, origin + s)
        out.append(p * ratio if _finite(p) else float("nan"))
    return out


def bias_corrected(grid, origin, cache_key, raw, support=6, halflife_slots=3.0, clip=(0.85, 1.20)):
    """E1 scaled by an EWMA of actual / E1's own +10-minute forecast over the last hour (a feedback
    correction for the slow level drift the fixed-weight ensemble follows late)."""
    if not all(np.isfinite(raw)):
        return [float("nan")] * STEPS
    yf = grid.filled()
    w, num, den = math.log(2) / halflife_slots, 0.0, 0.0
    for j in range(support):
        t = origin - j
        if t - 1 < 0:
            break
        try:
            prev = se.components_at(grid, t - 1, cache_key)["raw"][0]
        except (se.ForecastUnavailable, ValueError):
            continue
        if np.isfinite(prev) and prev > 0 and np.isfinite(yf[t]):
            wt = math.exp(-w * j)
            num += wt * (yf[t] / prev)
            den += wt
    ratio = min(clip[1], max(clip[0], num / den)) if den > 0 else 1.0
    return [max(0.0, v * ratio) for v in raw]


def failure_plan(grid, o0, n_origins, kind, windows, seed):
    """Which component is unavailable at which origins (the lab's single-component failure test).
    kind: "hw_burst_generation"  -> the Holt-Winters component of every generation whose refit boundary
                                    lies inside or within 2 h after a burst is unavailable (a fit
                                    contaminated by the burst is refused)
          "theta_random_generation" -> Theta is unavailable for one random six-hour generation per day
          "hw_random_generation"    -> Holt-Winters unavailable for one random generation per day
    Returns {component: set(origin indices)}."""
    plan = {"hw": set(), "theta": set(), "profile_ar": set()}
    if not kind:
        return plan
    gen_slots = REFIT_SECONDS_SLOTS = se.REFIT_SECONDS // SLOT
    origins = range(o0, o0 + n_origins)
    def boundary_of(k):
        return (grid.ts(k) // se.REFIT_SECONDS) * se.REFIT_SECONDS
    if kind == "hw_burst_generation":
        bad = set()
        for (a, b) in windows.get("burst_inside", []) + windows.get("burst_after", []):
            for k in range(a, b):
                bad.add(boundary_of(k))
        plan["hw"] = {k for k in origins if boundary_of(k) in bad}
    elif kind in ("theta_random_generation", "hw_random_generation"):
        rng = np.random.default_rng(seed + 7)
        comp = "theta" if kind.startswith("theta") else "hw"
        days = n_origins // SEASON + 1
        bad = set()
        for d in range(days):
            k = o0 + d * SEASON + int(rng.integers(0, SEASON))
            if k < o0 + n_origins:
                bad.add(boundary_of(k))
        plan[comp] = {k for k in origins if boundary_of(k) in bad}
    else:
        raise ValueError(f"unknown failure kind {kind}")
    return plan


def forecasts_at(grid, origin, cache_key, failures=None):
    """All methods' six-step forecasts from one origin, as {name: [6 floats]} (NaN = unavailable).
    `failures` = {component: set(origins)} marks a component unavailable at those origins; e1 then
    refuses (its live rule: a partial forecast is refused), `median3` refuses unless all three members
    are finite, and `median3_finite` serves the median of the finite members (the mean of two)."""
    y, yf = grid.y, grid.filled()
    nan6 = [float("nan")] * STEPS
    out = OrderedDict()
    failures = failures or {}
    try:
        comp = se.components_at(grid, origin, cache_key)
        out["e1"], out["hw"], out["profile_ar"] = comp["raw"], comp["hw"], comp["profile_ar"]
    except (se.ForecastUnavailable, ValueError):
        out["e1"], out["hw"], out["profile_ar"] = nan6, nan6, nan6
    if origin in failures.get("hw", ()):
        out["hw"], out["e1"] = nan6, nan6
    if origin in failures.get("profile_ar", ()):
        out["profile_ar"], out["e1"] = nan6, nan6
    out["profile7"] = [se.profile_at(y, origin + s) for s in range(1, STEPS + 1)]
    out["yesterday"] = [yf[origin + s - SEASON] if origin + s - SEASON >= 0 else float("nan")
                        for s in range(1, STEPS + 1)]
    out["persistence"] = [float(yf[origin])] * STEPS
    out["profile_ratio"] = profile_ratio_forecast(y, yf, origin)
    out["e1_bc"] = bias_corrected(grid, origin, cache_key, out["e1"])
    out["theta"] = nan6 if origin in failures.get("theta", ()) else theta_forecast(yf, origin)
    trio = np.array([out["hw"], out["profile_ar"], out["theta"]], dtype=float)
    finite_rows = [r for r in trio if np.all(np.isfinite(r))]
    if len(finite_rows) == 3:
        out["median3"] = [float(v) for v in np.median(trio, axis=0)]
        out["mean3"] = [float(v) for v in np.mean(trio, axis=0)]
        out["median3_finite"] = out["median3"]
    else:
        out["median3"], out["mean3"] = nan6, nan6
        out["median3_finite"] = ([float(v) for v in np.median(np.array(finite_rows), axis=0)]
                                 if len(finite_rows) >= 2 else nan6)
    return out


# ------------------------------------------------------------------------------------------
# Margin, scoring, replay
# ------------------------------------------------------------------------------------------

def q90_margins(y, leads, origins, quantile=se.MARGIN_QUANTILE, window=se.MARGIN_WINDOW_SLOTS, mode="absolute"):
    """The live rule for every method: q90 of matured lead-window errors over the trailing 24 h,
    at least MARGIN_MIN_SAMPLES samples, clipped to [0, 0.8 x lead]. `leads[k]` = max(step+10, step+20)
    forecast issued at origin k (NaN when unavailable). Equals seasonal_ensemble.margin_at for e1 at the
    default quantile and window; other values are policy variants under test."""
    margins = {}
    for k in origins:
        samples = []
        for j in range(max(0, k - window + 1), k - 1):
            if j + 2 >= len(y) or not (np.isfinite(y[j]) and np.isfinite(y[j + 1]) and np.isfinite(y[j + 2])):
                continue
            lj = leads.get(j)
            if lj is None or not np.isfinite(lj) or (mode == "relative" and lj <= 0):
                continue
            samples.append(max(y[j + 1], y[j + 2]) / lj - 1.0 if mode == "relative" else max(y[j + 1], y[j + 2]) - lj)
        lead = leads.get(k)
        if len(samples) < se.MARGIN_MIN_SAMPLES or lead is None or not np.isfinite(lead):
            margins[k] = 0.0
        elif mode == "relative":
            margins[k] = float(lead * np.clip(np.quantile(samples, quantile), 0.0, se.MARGIN_CLIP_FRACTION))
        else:
            q = float(np.quantile(samples, quantile))
            margins[k] = float(np.clip(q, 0.0, se.MARGIN_CLIP_FRACTION * lead))
    return margins


def pods(rpm, per_pod):
    return max(1, math.ceil(rpm / per_pod - 1e-9)) if np.isfinite(rpm) and rpm > 0 else 1


def replay(y, origins, lead_of, per_pod, min_r, max_r, reactive_lag_min=2.0, slot_min=SLOT / 60.0,
           windows=None):
    """Controller replay over consecutive origins. lead_of(k) -> lead rpm incl. margin, or NaN.
    Returns shortage / surplus replica-minutes, short minutes, mean Ready, scale changes, and the same
    sums inside each named scoring window (slot index ranges on the requirement slot k + 1)."""
    ready = min(max_r, max(min_r, pods(y[origins[0]], per_pod)))
    prev_desired = ready
    short = surplus = short_min = 0.0
    total_ready, changes, n = 0.0, 0, 0
    per_slot = {}
    for k in origins:
        if k + 1 >= len(y) or not np.isfinite(y[k + 1]):
            break
        lead = lead_of(k)
        pred = pods(lead, per_pod) if np.isfinite(lead) else 0
        reactive = pods(y[k], per_pod)
        desired = min(max_r, max(min_r, max(pred, reactive)))
        if desired >= ready:
            nxt = desired
        elif prev_desired < ready:          # below the Ready count for a full slot: the hold has elapsed
            nxt = desired
        else:
            nxt = ready
        prev_desired = desired
        if nxt != ready:
            changes += 1
        ready = nxt
        required = min(max_r, pods(y[k + 1], per_pod))
        if ready >= required:
            s_add, sp_add = 0.0, (ready - required) * slot_min
        else:
            gap = required - ready
            s_add, sp_add = gap * reactive_lag_min, 0.0
            short_min += reactive_lag_min
            ready = required                 # the reactive path caught up for the rest of the slot
            changes += 1
        short += s_add
        surplus += sp_add
        per_slot[k + 1] = (s_add, sp_add)
        total_ready += ready
        n += 1
    out = {"shortage_replica_min": round(short, 1), "surplus_replica_min": round(surplus, 1),
           "short_min": round(short_min, 1), "mean_ready": round(total_ready / n, 3) if n else None,
           "changes": changes, "slots": n}
    if windows:
        out["windows"] = {}
        for name, ranges in windows.items():
            ws = sum(per_slot.get(j, (0.0, 0.0))[0] for a, b in ranges for j in range(a, b))
            wp = sum(per_slot.get(j, (0.0, 0.0))[1] for a, b in ranges for j in range(a, b))
            out["windows"][name] = {"shortage_replica_min": round(ws, 1), "surplus_replica_min": round(wp, 1),
                                    "slots": sum(b - a for a, b in ranges)}
    return out


def evaluate_series(points, first_origin_ts, cache_key, methods=None, per_pod=600.0, min_r=1, max_r=12,
                    windows=None, failures=None, seed=0):
    grid = se.Grid.from_points(points)
    y = grid.y
    o0 = grid.index(first_origin_ts)
    origins = list(range(o0, len(y) - LEAD_STEPS))
    windows = {k: v for k, v in (windows or {}).items() if v} or None
    fplan = failure_plan(grid, o0, len(origins), failures, windows or {}, seed) if failures else None
    # Forecasts start one margin window earlier so every method's q90 window is as full at the first
    # scored origin as the live arm's was at the regime switch; only `origins` are scored and replayed.
    warm = list(range(max(0, o0 - se.MARGIN_WINDOW_SLOTS), o0))
    F = {}                                  # method -> {origin: [6]}
    for k in warm + origins:
        for name, f in forecasts_at(grid, k, cache_key, fplan).items():
            if methods and name not in methods:
                continue
            F.setdefault(name, {})[k] = f
    results = OrderedDict()
    for name, fk in F.items():
        leads = {k: (max(f[0], f[1]) if np.isfinite(f[0]) and np.isfinite(f[1]) else float("nan"))
                 for k, f in fk.items()}
        sc = {k: f for k, f in fk.items() if k >= o0}
        e10 = [abs(f[0] - y[k + 1]) for k, f in sc.items() if np.isfinite(f[0])]
        e20 = [abs(f[1] - y[k + 2]) for k, f in sc.items() if np.isfinite(f[1])]
        p10 = [abs(f[0] - y[k + 1]) / y[k + 1] for k, f in sc.items() if np.isfinite(f[0]) and y[k + 1] > 0]
        under = [max(y[k + 1], y[k + 2]) > leads[k] for k in sc if np.isfinite(leads[k])]
        avail = sum(1 for k in sc if np.isfinite(leads[k])) / max(1, len(sc))
        margins = q90_margins(y, leads, origins)
        m80 = q90_margins(y, leads, origins, quantile=0.80)
        m95 = q90_margins(y, leads, origins, quantile=0.95)
        m90_12h = q90_margins(y, leads, origins, window=se.MARGIN_WINDOW_SLOTS // 2)
        r90 = q90_margins(y, leads, origins, mode="relative")
        r80 = q90_margins(y, leads, origins, quantile=0.80, mode="relative")
        pol = OrderedDict()
        R = lambda f: replay(y, origins, f, per_pod, min_r, max_r, windows=windows)
        pol["none"] = R(lambda k: leads.get(k, float("nan")))
        pol["q80"] = R(lambda k: leads.get(k, float("nan")) + m80.get(k, 0.0))
        pol["q90"] = R(lambda k: leads.get(k, float("nan")) + margins.get(k, 0.0))
        pol["q95"] = R(lambda k: leads.get(k, float("nan")) + m95.get(k, 0.0))
        pol["q90_12h"] = R(lambda k: leads.get(k, float("nan")) + m90_12h.get(k, 0.0))
        pol["rq80"] = R(lambda k: leads.get(k, float("nan")) + r80.get(k, 0.0))
        pol["rq90"] = R(lambda k: leads.get(k, float("nan")) + r90.get(k, 0.0))
        pol["fixed10"] = R(lambda k: leads.get(k, float("nan")) * 1.10)
        wmae = {}
        for wname, ranges in (windows or {}).items():
            errs = [abs(fk[k][1] - y[k + 2]) for a, b in ranges for k in range(a - 2, b - 2)
                    if k in fk and np.isfinite(fk[k][1]) and k + 2 < len(y)]
            wmae[wname] = round(float(np.mean(errs)), 1) if errs else None
        results[name] = {"mae10": round(float(np.mean(e10)), 1) if e10 else None,
                         "mae20": round(float(np.mean(e20)), 1) if e20 else None,
                         "mape10_pct": round(100 * float(np.mean(p10)), 2) if p10 else None,
                         "under_forecast_rate": round(float(np.mean(under)), 3) if under else None,
                         "availability": round(avail, 3),
                         "median_q90_margin": round(float(np.median([margins[k] for k in origins])), 1),
                         "window_mae20": wmae, "policies": pol}
    results["reactive_only"] = {"policies": {"none": replay(y, origins, lambda k: float("nan"), per_pod, min_r, max_r,
                                                            windows=windows)}}
    results["oracle"] = {"policies": {"none": replay(
        y, origins, lambda k: max(y[k + 1], y[k + 2]) if k + 2 < len(y) else float("nan"), per_pod, min_r, max_r,
        windows=windows)}}
    return results, origins


# ------------------------------------------------------------------------------------------
# Runner and report
# ------------------------------------------------------------------------------------------

def run_seed(seed, warm_days, challenge_days, methods, per_pod, max_r, profile=None, events=None,
             failures=None):
    rates, windows = offered_rates(seed, warm_days, challenge_days, profile=profile, events=events)
    points = sampled_series(rates, seed)
    first_origin = DEFAULT_T0 + warm_days * SEASON * SLOT
    return evaluate_series(points, first_origin, f"bakeoff-{seed}", methods, per_pod, 1, max_r,
                           windows=windows, failures=failures, seed=seed)[0]


def aggregate(per_seed):
    """Mean over seeds of every numeric field, plus the worst seed for q90 shortage."""
    names = list(next(iter(per_seed.values())).keys())
    agg = OrderedDict()
    for name in names:
        rows = [r[name] for r in per_seed.values() if name in r]
        entry = {}
        for key in ("mae10", "mae20", "mape10_pct", "under_forecast_rate", "availability", "median_q90_margin"):
            vals = [r[key] for r in rows if r.get(key) is not None]
            entry[key] = round(float(np.mean(vals)), 3) if vals else None
        entry["policies"] = {}
        for pol in rows[0]["policies"]:
            keys = ("shortage_replica_min", "surplus_replica_min", "short_min", "mean_ready", "changes")
            m = {k: round(float(np.mean([r["policies"][pol][k] for r in rows])), 1) for k in keys}
            m["worst_shortage"] = round(max(r["policies"][pol]["shortage_replica_min"] for r in rows), 1)
            if "windows" in rows[0]["policies"][pol]:
                m["windows"] = {}
                for w in rows[0]["policies"][pol]["windows"]:
                    m["windows"][w] = {k: round(float(np.mean([r["policies"][pol]["windows"][w][k] for r in rows])), 1)
                                       for k in ("shortage_replica_min", "surplus_replica_min")}
            entry["policies"][pol] = m
        if rows[0].get("window_mae20"):
            entry["window_mae20"] = {w: round(float(np.mean([r["window_mae20"][w] for r in rows if r["window_mae20"].get(w) is not None])), 1)
                                     for w in rows[0]["window_mae20"] if any(r["window_mae20"].get(w) is not None for r in rows)}
        agg[name] = entry
    return agg


def print_windows(agg):
    """Event-window view: q90 shortage / surplus inside each window, and MAE at +20 there."""
    wins = [w for r in agg.values() for p in r["policies"].values() for w in p.get("windows", {})]
    wins = list(OrderedDict.fromkeys(wins))
    if not wins:
        return
    print("event windows (q90 policy; shortage/surplus replica-minutes inside the window; MAE at +20 min inside it):")
    print(f"{'method':14s} | " + " | ".join(f"{w:>26s}" for w in wins))
    for name, r in agg.items():
        p = r["policies"].get("q90") or r["policies"].get("none")
        cells = []
        for w in wins:
            ww = p.get("windows", {}).get(w)
            mae = (r.get("window_mae20") or {}).get(w)
            cells.append(f"{ww['shortage_replica_min']:7.1f}/{ww['surplus_replica_min']:8.1f} {('mae ' + str(mae)) if mae is not None else '':>10s}" if ww else " " * 26)
        print(f"{name:14s} | " + " | ".join(cells))


def print_report(agg, seeds, days):
    print(f"challenge-v1 process, {len(seeds)} fresh seed(s) {seeds}, {days} challenge day(s) each, "
          f"per-slot replay with the operator's rule (mean over seeds; worst = worst seed's shortage)")
    pols = [p for p in next(iter(agg.values()))["policies"]]
    for r in agg.values():
        for p in r["policies"]:
            if p not in pols:
                pols.append(p)
    head = " | ".join(f"{p + ' short/surp':>16s}" for p in pols)
    print(f"{'method':14s} {'mae10':>7s} {'mae20':>7s} {'mape10':>7s} {'under':>6s} {'q90wrst':>7s} {'margin':>6s} | {head}")
    for name, r in agg.items():
        p = r["policies"]
        def ss(pol):
            return f"{p[pol]['shortage_replica_min']:7.1f}/{p[pol]['surplus_replica_min']:8.1f}" if pol in p else " " * 16
        acc = (f"{r['mae10']:7.1f} {r['mae20']:7.1f} {r['mape10_pct']:6.2f}% {r['under_forecast_rate']:6.3f}"
               if r.get("mae10") is not None else " " * 29)
        # a single series (the --real path) has no seed aggregate: its worst shortage is its shortage
        q = (f"{p['q90'].get('worst_shortage', p['q90']['shortage_replica_min']):7.1f} {r['median_q90_margin']:6.1f}"
             if "q90" in p else " " * 14)
        print(f"{name:14s} {acc} {q} | " + " | ".join(ss(pol) for pol in pols))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", type=int, default=5)
    ap.add_argument("--first-seed", type=int, default=1)
    ap.add_argument("--challenge-days", type=int, default=14)
    ap.add_argument("--warm-days", type=int, default=7)
    ap.add_argument("--per-pod-rpm", type=float, default=600.0)
    ap.add_argument("--max-replicas", type=int, default=12)
    ap.add_argument("--methods", default="")
    ap.add_argument("--override", default="", help="JSON merged over profile.json for a stress variant, e.g. "
                    "'{\"noise\": {\"sigma\": 0.08, \"clip\": 0.2}, \"shifts\": {\"max_abs_level\": 0.25}}'")
    ap.add_argument("--events", default="", help="JSON stress events on the challenge process: "
                    "'{\"bursts\": [[3, 700, 6, 2.5]], \"shift\": [7, 720, 1.55]}' (day, minute of day, width slots, factor)")
    ap.add_argument("--failures", default="", help="single-component failure test: hw_burst_generation | "
                    "theta_random_generation | hw_random_generation")
    ap.add_argument("--json")
    ap.add_argument("--real", help="JSON {app: [[ts, rpm], ...]} from Prometheus; replay the harness on it")
    ap.add_argument("--app", default="nginx-test")
    ap.add_argument("--real-first-origin", help="ISO time of the first origin for --real (default: last 2 days)")
    a = ap.parse_args()
    methods = [m for m in a.methods.split(",") if m] or None
    if a.real:
        pts = [(int(t), float(v)) for t, v in json.load(open(a.real))[a.app]]
        last = pts[-1][0]
        first = (int(datetime.datetime.fromisoformat(a.real_first_origin.replace("Z", "+00:00")).timestamp())
                 if a.real_first_origin else last - 2 * 86400)
        first -= first % SLOT
        res, origins = evaluate_series(pts, first, "real", methods, a.per_pod_rpm, 1, a.max_replicas)
        print(f"real series {a.app}: {len(pts)} points, origins {len(origins)} from "
              f"{datetime.datetime.fromtimestamp(first, datetime.timezone.utc).isoformat()}")
        print_report(res, ["real"], round(len(origins) / SEASON, 2))
        if a.json:
            json.dump({"real": res}, open(a.json, "w"), indent=1)
        return 0
    seeds = list(range(a.first_seed, a.first_seed + a.seeds))
    assert cp.PROFILE["seed"] not in seeds, "never evaluate on the sealed seed"
    profile = merged_profile(json.loads(a.override)) if a.override else None
    events = json.loads(a.events) if a.events else None
    if events:
        events = {"bursts": [tuple(b) for b in events.get("bursts", [])],
                  "shift": tuple(events["shift"]) if events.get("shift") else None}
    per_seed = OrderedDict()
    for s in seeds:
        per_seed[s] = run_seed(s, a.warm_days, a.challenge_days, methods, a.per_pod_rpm, a.max_replicas, profile,
                               events, a.failures or None)
        print(f"seed {s} done", file=sys.stderr)
    agg = aggregate(per_seed)
    if a.override:
        print(f"process override: {a.override}")
    if a.events:
        print(f"events: {a.events}")
    if a.failures:
        print(f"component failures: {a.failures}")
    print_report(agg, seeds, a.challenge_days)
    print_windows(agg)
    if a.json:
        json.dump({"seeds": seeds, "warm_days": a.warm_days, "challenge_days": a.challenge_days,
                   "per_pod_rpm": a.per_pod_rpm, "max_replicas": a.max_replicas,
                   "sealed_seed_excluded": cp.PROFILE["seed"], "override": json.loads(a.override) if a.override else None,
                   "events": events, "failures": a.failures or None,
                   "aggregate": agg, "per_seed": per_seed},
                  open(a.json, "w"), indent=1)
    return 0


if __name__ == "__main__":
    sys.exit(main())

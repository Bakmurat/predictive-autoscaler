"""Acceptance protocol B of the trust repair: the operating characteristics of the coverage rules
(forecasting.trust, policy budgeted-coverage-tests/v1), measured. This is the reproducible QUALIFICATION JOB of the
protocol (T5): it is expensive and is not part of the unit-test run; tests/test_trust_monitor.py guards the same
characteristics cheaply (exact arithmetic and a small fixed-seed simulation).

Declared before any validation result exists:

  - Targets. False alarms: P(any false coverage demotion of a target within 7 days) <= 5 %, all coverage rules and
    all enabled families together, under the nominal model. Detection: >= 95 % probability that a sustained 40 %
    miscoverage is detected within 64 newly matured gate outcomes.
  - Nominal model. (N0) independent gate misses at the nominal rate: exact arithmetic (the union bound the
    thresholds are derived from, trust.rolling_limit / day_limit / consecutive_bound_ppm). (N1) the repaired
    calibration running on the declared independent noise models of evaluation.calibration_acceptance (additive,
    proportional, mixed): the bound's real miss sequence, with the adaptive level, the scale refits and the service's
    timing. The AR(1) variants of those models are the declared serially dependent models: reported, not part of the
    nominal model, and a missed target there is reported as missed.
  - What one path is. One signal path per seed; the product's seasonal-naive forecast and calibration issue a bound
    for every cohort window (calibration_acceptance.simulate); the candidate becomes Trusted at the first evaluation
    where every gate holds (48 h after its first bound, 64 bounds); a FALSE ALARM is a demotion by the coverage gate
    within the 7 days after that. The estimate for a target with F enabled families is F times the per-candidate
    probability (Bonferroni: valid under any dependence between the families).
  - The monitor. fast_monitor evaluates the rules at every evaluation where their state can change (a folded gate
    outcome, an hour boundary of the 7-day window, a completed day, the end of the shadow period), with the limits
    of trust.coverage_limits. real_monitor drives the product's own functions (trust.note_issuance, note_outcome,
    evaluate with its hysteresis) at EVERY request of the grid, overlapping windows included, with a restart
    (the state through JSON, as a checkpoint) at chosen requests; `equivalence` checks that both give the same
    first trust, the same demotions with the same rules and the same re-promotions on the same paths.
  - Detection. Gate misses that are independent at the nominal rate up to a change point and at 40 % (also 50 %,
    and 100 %: an abrupt all-miss step) after it; detection = a coverage rule failing at one of the first 64
    outcomes after the change, CONDITIONAL ON AN ELIGIBLE STATE AT THE CHANGE (protocol amendment 1): a path on which
    a rule failed at any outcome of the pre-change stretch is not eligible there (it is alarmed, or demoted with its
    re-promotion dwell possibly still running); it is excluded from the denominator and reported separately, never
    counted as a detection. The serially dependent variant thresholds a latent Gaussian AR(1) series whose
    coefficient is AR_RHO ** (lead / 60 s), the dependence the declared AR(1) noise leaves between consecutive cohort
    windows. Wall time from the change to the demotion being visible: outcomes x lead + settlement + one request
    interval.
  - Seeds. DEVELOPMENT_SEEDS chose the thresholds (procedure in `select`). VALIDATION_SEEDS are frozen: run once
    after the thresholds are frozen, and nothing is tuned afterwards.
  - Uncertainty. Probabilities over independent paths with 95 % Wilson intervals. A target is MET when the whole
    interval is on the right side, NOT MET when the estimate is on the wrong side, INCONCLUSIVE otherwise.

  python -m evaluation.trust_qualification select --out select.json                 # development seeds only
  python -m evaluation.trust_qualification false-alarms --seeds validation --out false-alarms.json
  python -m evaluation.trust_qualification detection --seeds validation --out detection.json
  python -m evaluation.trust_qualification equivalence --seeds validation --paths 12 --out equivalence.json
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import math
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence, Tuple

import numpy as np

from evaluation import calibration_acceptance as acceptance
from forecasting import calibration as cal
from forecasting import trust as T
from forecasting.contracts import TrustSettings

DAY, HOUR = 86400, 3600
SETTLEMENT, GRACE = 120, 600
NOMINAL_BPS = 1000
FALSE_ALARM_TARGET, DETECTION_TARGET, DETECTION_OUTCOMES = 0.05, 0.95, 64
PATH_DAYS = {180: 12, 300: 12, 1200: 14}                 # first bound + 48 h shadow + 7 days, with margin
# (grid seconds, lead seconds, declared): the two evaluation profiles carry the criteria; the default 300 s grid with
# a 1200 s lead is reported beside them.
CONFIGS = ((60, 180, True), (60, 300, True), (300, 1200, False))
NOMINAL_MODELS = ("additive", "proportional", "mixed")
DEPENDENT_MODELS = ("additive_ar1", "proportional_ar1", "mixed_ar1")
FAMILIES = (1, 2)

DEVELOPMENT_SEEDS = tuple(range(200_000, 201_000))
VALIDATION_SEEDS = tuple(range(950_000, 951_000))        # frozen; run once after the thresholds are frozen
# Paths per cell of the false-alarm run (the first so many seeds of the set): the nominal models on the declared
# configurations get every seed; the dependent models and the reported configuration fewer (they are reported, and the
# reported configuration needs the slower exact kernel).
PATHS_DEPENDENT, PATHS_REPORTED = 500, 300
DETECTION_DEVELOPMENT_SEED, DETECTION_VALIDATION_SEED = 20_261_110, 95_000_001
DETECTION_TRIALS = 400_000

# The threshold sets `select` compares (development seeds): n of the rolling window and the three budgets, in ppm per
# target and 7 days. Only triples whose sum is within the 5 % target are compared, so the exact bound under
# independent nominal misses holds by construction for every lead, nominal rate and family count.
GRID_N = (32, 48, 64)
GRID_ROLLING_PPM = (20_000, 25_000, 30_000, 33_000, 35_000, 40_000)
GRID_CONSECUTIVE_PPM = (5_000, 7_000, 10_000)
GRID_DAY_PPM = (5_000, 8_000, 10_000)
BUDGET_PPM = 50_000


# --- gate outcomes of one path ----------------------------------------------------------------------------------------

@dataclass
class GateOutcomes:
    """The prospective gate outcomes of one path, in window-end order: window ends, 1 = miss, and the origin of the
    first issuance that carried a bound (where the shadow period starts)."""
    grid: int
    lead: int
    window_end: np.ndarray
    miss: np.ndarray
    first_bound_origin: int
    last_origin: int


def request_origin(t: int, grid: int, offset: int = acceptance.REQUEST_OFFSET) -> int:
    """The first request origin (a multiple of the grid) whose clock, origin + offset, has reached t."""
    return -(-(t - offset) // grid) * grid


def gate_outcomes(model: str, seed: int, grid: int, lead: int, days: Optional[int] = None) -> GateOutcomes:
    """One path of the declared model through the repaired calibration (calibration_acceptance.simulate)."""
    days = days or PATH_DAYS[lead]
    x = acceptance.signal(acceptance.MODEL[model], seed, days, grid)
    p = acceptance.policy(lead)
    kernel = "fast" if p.max_scores * lead < p.retention_seconds - DAY else "exact"
    path = acceptance.simulate(x, lead, kernel=kernel, timing=acceptance.Timing(grid=grid))
    bounded = np.isfinite(path.upper)
    scaled = np.flatnonzero(np.isfinite(path.sigma))
    if not bounded.any() or len(scaled) < p.min_windows:
        raise ValueError("the path is too short for a bound")
    # The first bounded issuance is the request at which the min_windows-th score is folded (any origin of the grid,
    # not only a cohort origin).
    first = request_origin(int(path.origin[scaled[p.min_windows - 1]]) + lead + SETTLEMENT, grid)
    return GateOutcomes(grid, lead, (path.origin[bounded] + lead).astype(np.int64),
                        (path.y[bounded] > path.upper[bounded]).astype(np.int64), first, int(path.origin[-1]))


# --- the rules at every evaluation where their state can change ---------------------------------------------------------

@dataclass
class MonitorResult:
    trusted_at: Optional[int]                     # request origin of the first promotion
    demotions: List[dict]                         # {"at": origin, "rules": [...], "repromoted_at": origin or None}
    looks: int

    @property
    def false_alarm(self) -> Optional[dict]:
        """The first coverage demotion within 7 days of the first promotion, if any."""
        if self.trusted_at is None or not self.demotions:
            return None
        d = self.demotions[0]
        return d if d["at"] <= self.trusted_at + 7 * DAY else None


def fast_monitor(out: GateOutcomes, t: TrustSettings, families: int, *, offset: int = acceptance.REQUEST_OFFSET,
                 until: Optional[int] = None) -> MonitorResult:
    """The coverage gate, the shadow gate and the hysteresis of forecasting.trust over one path (module docstring).
    Every other gate is taken as holding."""
    grid, lead = out.grid, out.lead
    lim = T.coverage_limits(t, lead, NOMINAL_BPS, families)
    w, m = out.window_end, out.miss
    n_out = len(w)
    fold = -(-(w + SETTLEMENT - offset) // grid) * grid                   # the request that folds each outcome
    end = int(fold[-1]) if until is None else until
    first_day, last_day = int(w[0] // DAY), int(end // DAY)
    day_fin = [request_origin((d + 1) * DAY + SETTLEMENT + GRACE, grid, offset) for d in range(first_day, last_day + 1)]
    shadow = request_origin(out.first_bound_origin + t.min_shadow_hours * HOUR, grid, offset)
    hours = np.arange(-(-int(fold[0]) // HOUR) * HOUR, end + 1, HOUR)
    events = np.unique(np.concatenate([fold, hours, np.array(day_fin + [shadow], dtype=np.int64)]))
    events = events[(events >= fold[0]) & (events <= end)]
    now = events + offset
    seen = np.searchsorted(fold, events, side="right")                    # outcomes folded by each event
    cs = np.concatenate([[0], np.cumsum(m)])
    rolling = cs[seen] - cs[np.maximum(seen - lim.recent_n, 0)]
    run = np.zeros(n_out + 1, dtype=np.int64)
    for i in range(n_out):
        run[i + 1] = run[i] + 1 if m[i] else 0
    fail = {"rolling": rolling >= lim.rolling_misses, "consecutive": run[seen] >= lim.consecutive}
    # The 7-day window: hour buckets by window end, from the first whole hour at or after now - 7 d (trust._hours).
    lo_hour = -(-(now - 7 * DAY) // HOUR)
    idx_lo = np.minimum(np.searchsorted(w // HOUR, lo_hour, side="left"), seen)
    bounded7 = seen - idx_lo
    covered7 = bounded7 - (cs[seen] - cs[idx_lo])
    threshold = (1.0 - NOMINAL_BPS / T.BPS) - t.coverage_tolerance_bps / 10_000
    with np.errstate(divide="ignore", invalid="ignore"):
        fail["seven_day"] = (bounded7 == 0) | (covered7 / np.maximum(bounded7, 1) < threshold)
    # Completed UTC days inside the window (trust.completed_days), each tested once it is final.
    day_of = w // DAY
    fail["day"] = np.zeros(len(events), dtype=bool)
    lo_day, hi_day = -(-((lo_hour + 1) * HOUR) // DAY), (now - SETTLEMENT - GRACE) // DAY - 1
    for d in range(first_day, last_day + 1):
        sel = day_of == d
        bd, ms = int(sel.sum()), int(m[sel].sum())
        if bd >= t.worst_day_min_outcomes and ms >= lim.day_misses(bd):
            fail["day"] |= (lo_day <= d) & (d <= hi_day)
    cov_fail = np.zeros(len(events), dtype=bool)
    for rule in T.COVERAGE_RULES:
        cov_fail |= fail[rule]
    shadow_ok = (now - out.first_bound_origin >= t.min_shadow_hours * HOUR) & (bounded7 >= t.min_prospective_windows)
    ok = shadow_ok & ~cov_fail
    res = MonitorResult(None, [], int(n_out))
    idx = np.flatnonzero(ok)
    if idx.size == 0:
        return res
    i = int(idx[0])
    res.trusted_at = int(events[i])
    hold_s, hold_n = t.repromotion_hours * HOUR, t.repromotion_windows
    while True:
        bad = np.flatnonzero(~ok[i + 1:])
        if bad.size == 0:
            return res
        j = i + 1 + int(bad[0])                               # demoted at this evaluation
        d = {"at": int(events[j]), "rules": [r for r in T.COVERAGE_RULES if fail[r][j]], "repromoted_at": None}
        res.demotions.append(d)
        # Re-promotion: the gates hold at every evaluation for hold_s, and hold_n new gate outcomes since the demotion.
        k, since = j + 1, None
        while k < len(events):
            if not ok[k]:
                since = None
            else:
                since = int(events[k]) if since is None else since
                nxt = int(events[k + 1]) if k + 1 < len(events) else end + grid
                # Between two events the state is constant; a request every `grid` seconds sees it.
                earliest = max(since + hold_s, int(events[k]))
                if seen[k] - seen[j] >= hold_n and earliest < nxt and earliest <= end:
                    d["repromoted_at"] = -(-earliest // grid) * grid
                    break
            k += 1
        if d["repromoted_at"] is None:
            return res
        i = k


# --- the product's own functions at every request -------------------------------------------------------------------------

def real_monitor(out: GateOutcomes, t: TrustSettings, families: int, *, offset: int = acceptance.REQUEST_OFFSET,
                 until: Optional[int] = None, restart_every: int = 0) -> MonitorResult:
    """trust.note_issuance, note_outcome and evaluate (gates, hysteresis, selection) at EVERY request of the grid:
    every origin issues a window (overlapping windows included, each with its outcome when due; only cohort windows
    are gate outcomes), exactly as the service folds them. With restart_every > 0 the whole state goes through JSON
    every that many requests, as a checkpoint read after a restart."""
    grid, lead = out.grid, out.lead
    lim = T.coverage_limits(t, lead, NOMINAL_BPS, families)
    epoch, recipe, model = "e" * 64, "qualification", T.NAIVE
    ledger, hyst = T.new_ledger(epoch, model, recipe, grid), {}
    miss_at = {int(we): int(mi) for we, mi in zip(out.window_end, out.miss)}
    first_gate = int(out.window_end[0]) - lead
    start = first_gate - 2 * DAY                              # two days of unbounded issuances first (availability)
    end = (int(out.window_end[-1]) + SETTLEMENT) if until is None else until
    g_kw = dict(lead_seconds=lead, nominal_miscoverage=NOMINAL_BPS / T.BPS, replay_suite_passed=True,
                startup_budget_valid=True, outcome_deadline_seconds=SETTLEMENT + GRACE, families=families)
    res = MonitorResult(None, [], len(out.window_end))
    seq, pending, trusted, requests = 0, [], False, 0
    for o in range(-(-start // grid) * grid, end + 1, grid):
        now = o + offset
        while pending and pending[0][1] + SETTLEMENT <= now:
            iseq, wend, origin, bounded = pending.pop(0)
            gate = cal.in_cohort(origin, lead)
            missed = bool(miss_at.get(wend, 0)) if gate and bounded else False
            y = 2.0 if missed else 0.5
            seq += 1
            rec = cal.CalibrationRecord(seq, epoch, model, recipe, f"iss-{iseq}", origin, wend, 1.0,
                                        1.0 if bounded else None, "scored", y, cal.UNIT_SCALE)
            ledger = T.note_outcome(ledger, T.EvidenceRecord(rec, "prospective", now * 1000, (0.0, 0.0, 0.0)),
                                    lead_s=lead, nominal_miscoverage=NOMINAL_BPS / T.BPS, limits=lim)
        bounded_now = o >= out.first_bound_origin
        ci = T.CandidateInputs(model, ledger, bounded_now, 512 if bounded_now else 0, 128, 30 * DAY, DAY, True)
        verdict, hyst = T.evaluate([ci], hyst, T.Global(now_s=now, **g_kw), t)
        v = verdict.candidates[model]
        if v.trusted and not trusted:
            if res.trusted_at is None:
                res.trusted_at = o
            else:
                res.demotions[-1]["repromoted_at"] = o
        elif trusted and not v.trusted:
            if [k for k, held in v.gates.items() if k != "coverage" and not held]:
                raise AssertionError(f"a gate other than coverage failed at {o}: {v.gates}")
            rules = hyst[model]["last_demotion"]["coverage"]["failing"]
            res.demotions.append({"at": o, "rules": list(rules), "repromoted_at": None,
                                  "latched": hyst[model]["last_demotion"]["latched_alarm"] is not None})
        trusted = v.trusted
        seq += 1
        ledger = T.note_issuance(ledger, seq=seq, issuance_id=f"iss-{seq}", epoch=epoch, model=model, recipe=recipe,
                                 origin=o, window_end=o + lead, deadline_s=o + lead + SETTLEMENT + GRACE, lead_s=lead,
                                 mode="prospective", bounded=bounded_now)
        pending.append((seq, o + lead, o, bounded_now))
        requests += 1
        if restart_every and requests % restart_every == 0:
            ledger, hyst = json.loads(json.dumps(ledger)), json.loads(json.dumps(hyst))
    return res


# --- detection ------------------------------------------------------------------------------------------------------------

def _normal_quantile(p: float) -> float:
    """The standard normal quantile (Acklam's rational approximation, relative error below 1.2e-9)."""
    a = (-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02, 1.383577518672690e+02,
         -3.066479806614716e+01, 2.506628277459239e+00)
    b = (-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02, 6.680131188771972e+01,
         -1.328068155288572e+01)
    c = (-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00, -2.549732539343734e+00,
         4.374664141464968e+00, 2.938163982698783e+00)
    d = (7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00, 3.754408661907416e+00)
    if p < 0.02425:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
            ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p > 1 - 0.02425:
        return -_normal_quantile(1 - p)
    q = p - 0.5
    r = q * q
    return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
        (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)


def detection(lim: T.CoverageLimits, miss_rate: float, *, trials: int, seed: int, phi: float = 0.0,
              outcomes: int = DETECTION_OUTCOMES) -> Dict[str, object]:
    """P(a rolling or consecutive alarm state at one of the first `outcomes` gate outcomes after the miss rate
    changes from nominal to miss_rate | the path is ELIGIBLE at the change), and the distribution of the first such
    outcome. Eligible: no rule failed at any outcome of the pre-change stretch (4 n outcomes), so the candidate is
    trusted and not alarmed at the change. The other paths are excluded from the denominator and reported
    ("excluded"; "failing_before_change" is the share failing at the change itself): a rule that already fails is
    not a detection of the change. "unconditional" is the earlier reading (every path, the already failing ones
    counted as detected), kept for comparison only. Misses are the exceedances of a latent Gaussian AR(1) series
    with coefficient phi (0: independent)."""
    rng = np.random.default_rng(seed)
    nominal = lim.miss_bps / T.BPS
    pre = 4 * lim.recent_n
    cut0 = _normal_quantile(1 - nominal)
    cut1 = -math.inf if miss_rate >= 1.0 else _normal_quantile(1 - miss_rate)
    n = lim.recent_n
    ring = np.zeros((trials, n), dtype=np.int8)
    count = np.zeros(trials, dtype=np.int32)
    run = np.zeros(trials, dtype=np.int32)
    z = rng.standard_normal(trials)
    innov = math.sqrt(1.0 - phi * phi)
    first = np.full(trials, outcomes + 1, dtype=np.int32)
    by_rule = {"rolling": np.zeros(trials, dtype=bool), "consecutive": np.zeros(trials, dtype=bool)}
    already = np.zeros(trials, dtype=bool)
    alarmed_before = np.zeros(trials, dtype=bool)
    for i in range(pre + outcomes):
        z = phi * z + innov * rng.standard_normal(trials)
        miss = (z > (cut0 if i < pre else cut1)).astype(np.int8)
        col = i % n
        count += miss - ring[:, col]
        ring[:, col] = miss
        run = np.where(miss == 1, run + 1, 0)
        rolling, consecutive = count >= lim.rolling_misses, run >= lim.consecutive
        if i < pre:
            alarmed_before |= rolling | consecutive         # not eligible at the change: never a detection of it
        if i == pre - 1:
            already = rolling | consecutive
        if i >= pre:
            hit = (rolling | consecutive) & (first > outcomes)
            first[hit] = i - pre + 1
            by_rule["rolling"] |= rolling
            by_rule["consecutive"] |= consecutive
    eligible = ~alarmed_before
    count_eligible = int(eligible.sum())
    detected = int(((first <= outcomes) & eligible).sum())
    lo, hi = wilson(detected, count_eligible)
    got = np.sort(first[(first <= outcomes) & eligible])
    share = (lambda v: float(v[eligible].mean())) if count_eligible else (lambda v: None)
    return {"miss_rate": miss_rate, "phi": phi, "trials": trials, "eligible": count_eligible,
            "excluded": trials - count_eligible, "detected": detected,
            "probability": detected / count_eligible if count_eligible else None, "lo": lo, "hi": hi,
            "failing_before_change": float(already.mean()),
            "unconditional": float((first <= outcomes).mean()),
            "median_outcomes": int(got[len(got) // 2]) if len(got) else None,
            "p95_outcomes": int(got[min(len(got) - 1, int(0.95 * len(got)))]) if len(got) else None,
            "by_rolling": share(by_rule["rolling"]), "by_consecutive": share(by_rule["consecutive"])}


def wall_seconds(outcomes: int, lead: int, grid: int) -> int:
    """From the change to the demotion being visible, at worst: the outcomes' windows, the settlement and one
    request interval."""
    return outcomes * lead + SETTLEMENT + grid


# --- statistics -----------------------------------------------------------------------------------------------------------

def wilson(k: int, n: int, z: float = 1.959964) -> Tuple[float, float]:
    if n == 0:
        return 0.0, 1.0
    p = k / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    h = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return max(0.0, c - h), min(1.0, c + h)


def reading_at_most(lo: float, est: float, hi: float, target: float) -> str:
    if est > target:
        return "NOT MET"
    return "MET" if hi <= target else "INCONCLUSIVE"


def reading_at_least(lo: float, est: float, hi: float, target: float) -> str:
    if est < target:
        return "NOT MET"
    return "MET" if lo >= target else "INCONCLUSIVE"


def union_bound_ppm(t: TrustSettings, lead: int, families: int) -> Dict[str, float]:
    """The exact false-alarm budget of a TARGET per 7 days under independent nominal misses (N0), by rule, in ppm.
    The 7-day gate is not in it (it is measured)."""
    lim = T.coverage_limits(t, lead, NOMINAL_BPS, families)
    a, n, k = NOMINAL_BPS, lim.recent_n, lim.rolling_misses
    rolling = 0.0 if k > n else \
        lim.looks * (a / T.BPS) * T.binomial_tail(n - 1, a, k - 1) / T.BPS ** (n - 1) * T.PPM * families
    cons = T.consecutive_bound_ppm(lim.consecutive, a, lim.looks) * families
    per_day = lim.looks // 7
    day = 7 * T.binomial_tail(per_day, a, lim.day_misses(per_day)) / T.BPS ** per_day * T.PPM * families
    return {"rolling": rolling, "consecutive": cons, "day": day, "total": rolling + cons + day,
            "rolling_misses": k, "recent_n": n, "consecutive_misses": lim.consecutive, "looks": lim.looks,
            "day_outcomes": per_day, "day_misses": lim.day_misses(per_day)}


def false_alarms(model: str, grid: int, lead: int, seeds: Sequence[int], settings: Sequence[TrustSettings],
                 families: Sequence[int] = FAMILIES, progress=None) -> List[dict]:
    """For every trust setting and family count: the per-candidate false-alarm count over the seeds' paths."""
    rows = [{"model": model, "grid": grid, "lead": lead, "families": f, "settings": dataclasses.asdict(t),
             "paths": 0, "trusted": 0, "alarms": 0, "by_rule": {r: 0 for r in T.COVERAGE_RULES},
             "repromoted_48h": 0, "miss_rate_sum": 0.0, "hours_to_alarm": []}
            for t in settings for f in families]
    for n, seed in enumerate(seeds):
        out = gate_outcomes(model, seed, grid, lead)
        i = 0
        for t in settings:
            for f in families:
                r = fast_monitor(out, t, f)
                row = rows[i]
                i += 1
                row["paths"] += 1
                row["miss_rate_sum"] += float(out.miss.mean())
                if r.trusted_at is None:
                    continue
                row["trusted"] += 1
                fa = r.false_alarm
                if fa is not None:
                    row["alarms"] += 1
                    for rule in fa["rules"]:
                        row["by_rule"][rule] += 1
                    row["hours_to_alarm"].append(round((fa["at"] - r.trusted_at) / HOUR, 2))
                    if fa["repromoted_at"] is not None and fa["repromoted_at"] - fa["at"] <= 48 * HOUR:
                        row["repromoted_48h"] += 1
        if progress and (n + 1) % 100 == 0:
            progress(model, grid, lead, n + 1)
    for row in rows:
        lo, hi = wilson(row["alarms"], row["trusted"])
        p = row["alarms"] / row["trusted"] if row["trusted"] else None
        f = row["families"]
        row.update(candidate={"probability": p, "lo": lo, "hi": hi},
                   target={"probability": None if p is None else min(1.0, f * p), "lo": min(1.0, f * lo),
                           "hi": min(1.0, f * hi)},
                   mean_miss_rate=row.pop("miss_rate_sum") / max(1, row["paths"]))
        row["reading"] = "NOT MEASURED" if p is None else reading_at_most(row["target"]["lo"],
                                                                         row["target"]["probability"],
                                                                         row["target"]["hi"], FALSE_ALARM_TARGET)
    return rows


def _digest(doc: dict) -> dict:
    doc["sha256"] = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()
    return doc


def _seeds(name: str) -> Tuple[int, ...]:
    return DEVELOPMENT_SEEDS if name == "development" else VALIDATION_SEEDS


def _log(*a) -> None:
    print(*a, file=sys.stderr, flush=True)


# --- commands -------------------------------------------------------------------------------------------------------------

def select(paths: int = len(DEVELOPMENT_SEEDS), trials: int = 100_000) -> dict:
    """Feasibility first, on DEVELOPMENT seeds only. For every threshold set of the grid:
      (1) the exact union bound of the target's 7-day false-alarm probability under independent nominal misses, at
          each declared lead with two families;
      (2) the detection probability of a sustained 40 % miscoverage within 64 outcomes (independent misses), at each
          declared lead, with one and with two families;
      (3) the simulated false-alarm probability through the real calibration on the independent noise models.
    A set is FEASIBLE when (1) is at most 5 % at every declared lead, the lower end of (2) is at least 95 % everywhere
    and the upper end of (3), for a target with two families, is at most 5 % on every nominal model and declared
    lead. The frozen set is the feasible one with the largest worst-case detection probability (the minimum of (2)
    over the declared leads and family counts: the target with the least margin), then the largest rolling budget
    (the same limit then holds at shorter leads), then the largest day budget, the rest going to the consecutive
    rule. If none is feasible the result says so and lists the trade-off: nothing is chosen."""
    base = TrustSettings()
    declared = [(g, lead) for g, lead, d in CONFIGS if d]
    grid_sets = [dataclasses.replace(base, acute_recent_windows=n, rolling_alarm_ppm=r, consecutive_alarm_ppm=c,
                                     day_alarm_ppm=d)
                 for n in GRID_N for r in GRID_ROLLING_PPM for c in GRID_CONSECUTIVE_PPM for d in GRID_DAY_PPM
                 if r + c + d <= BUDGET_PPM]
    rows = []
    for t in grid_sets:
        row = {"n": t.acute_recent_windows, "rolling_ppm": t.rolling_alarm_ppm,
               "consecutive_ppm": t.consecutive_alarm_ppm, "day_ppm": t.day_alarm_ppm, "union": {}, "detection": {}}
        ok = True
        for _, lead in declared:
            ub = union_bound_ppm(t, lead, 2)
            row["union"][str(lead)] = ub
            ok &= ub["total"] <= FALSE_ALARM_TARGET * T.PPM
            for f in FAMILIES:
                lim = T.coverage_limits(t, lead, NOMINAL_BPS, f)
                key = (lim.recent_n, lim.rolling_misses, lim.consecutive)
                d = _DETECTION_CACHE.get(key)
                if d is None:
                    d = _DETECTION_CACHE[key] = detection(lim, 0.4, trials=trials, seed=DETECTION_DEVELOPMENT_SEED)
                row["detection"][f"{lead}/{f}"] = {k: d[k] for k in ("probability", "lo", "hi")}
                ok &= d["lo"] >= DETECTION_TARGET
        row["arithmetic_feasible"] = bool(ok)
        row["worst_detection"] = min(d["probability"] for d in row["detection"].values())
        rows.append((t, row))
    candidates = [t for t, row in rows if row["arithmetic_feasible"]]
    _log(f"select: {len(candidates)} of {len(rows)} threshold sets pass the exact budget and the detection target")
    sims: Dict[int, List[dict]] = {i: [] for i in range(len(candidates))}
    for grid, lead in declared:
        # Sets with the same limits at this lead are the same monitor: simulate each distinct one once.
        lims = [T.coverage_limits(t, lead, NOMINAL_BPS, 2) for t in candidates]
        distinct: Dict[T.CoverageLimits, int] = {}
        for i, lim in enumerate(lims):
            distinct.setdefault(lim, i)
        reps = sorted(distinct.values())
        _log(f"select: lead {lead}: {len(reps)} distinct monitors among {len(candidates)} sets")
        for model in NOMINAL_MODELS:
            got = false_alarms(model, grid, lead, DEVELOPMENT_SEEDS[:paths], [candidates[i] for i in reps],
                               families=(2,), progress=lambda *a: _log("select", *a))
            by_rep = dict(zip(reps, got))
            for i, lim in enumerate(lims):
                r = by_rep[distinct[lim]]
                sims[i].append({k: r[k] for k in ("model", "lead", "trusted", "alarms", "by_rule", "target",
                                                   "reading", "mean_miss_rate")})
    feasible = []
    for i, t in enumerate(candidates):
        row = next(r for tt, r in rows if tt is t)
        row["simulated"] = sims[i]
        row["feasible"] = all(s["target"]["hi"] <= FALSE_ALARM_TARGET for s in sims[i])
        if row["feasible"]:
            feasible.append(t)
    chosen = None
    if feasible:
        worst = {id(t): row["worst_detection"] for t, row in rows}
        best = min(feasible, key=lambda t: (-worst[id(t)], -t.rolling_alarm_ppm, -t.day_alarm_ppm,
                                            -t.consecutive_alarm_ppm, t.acute_recent_windows))
        chosen = {"acute_recent_windows": best.acute_recent_windows, "rolling_alarm_ppm": best.rolling_alarm_ppm,
                  "consecutive_alarm_ppm": best.consecutive_alarm_ppm, "day_alarm_ppm": best.day_alarm_ppm}
    return _digest({"schema": "trust-threshold-selection/v1", "seed_set": "development", "paths": paths,
                    "detection_trials": trials, "declared_configs": declared, "nominal_models": list(NOMINAL_MODELS),
                    "feasible": bool(feasible), "feasible_sets": len(feasible), "chosen": chosen,
                    "grid": [row for _, row in rows]})


_DETECTION_CACHE: Dict[tuple, dict] = {}


def run_false_alarms(seed_set: str, paths: Optional[int] = None, models: Optional[Sequence[str]] = None) -> dict:
    seeds = _seeds(seed_set)[:paths]
    t = TrustSettings()
    cells = []
    for grid, lead, declared in CONFIGS:
        for model in models or NOMINAL_MODELS + DEPENDENT_MODELS:
            use = seeds[:PATHS_REPORTED] if not declared else seeds if model in NOMINAL_MODELS else \
                seeds[:PATHS_DEPENDENT]
            for row in false_alarms(model, grid, lead, use, [t], progress=lambda *a: _log("false-alarms", *a)):
                row.update(declared=declared, nominal=model in NOMINAL_MODELS)
                cells.append(row)
    return _digest({"schema": "trust-false-alarms/v1", "seed_set": seed_set, "seeds": [seeds[0], seeds[-1]],
                    "paths_dependent": PATHS_DEPENDENT, "paths_reported": PATHS_REPORTED,
                    "trust_policy": t.policy, "settings": dataclasses.asdict(t), "target": FALSE_ALARM_TARGET,
                    "union_bound_ppm": {f"{lead}/{f}": union_bound_ppm(t, lead, f) for _, lead, _ in CONFIGS
                                        for f in FAMILIES}, "cells": cells})


def run_detection(seed_set: str, trials: int = DETECTION_TRIALS) -> dict:
    seed = DETECTION_DEVELOPMENT_SEED if seed_set == "development" else DETECTION_VALIDATION_SEED
    t = TrustSettings()
    cells = []
    for grid, lead, declared in CONFIGS:
        phi_dep = acceptance.AR_RHO ** (lead / 60.0)
        for f in FAMILIES:
            lim = T.coverage_limits(t, lead, NOMINAL_BPS, f)
            for label, rate, phi in (("40 %", 0.4, 0.0), ("50 %", 0.5, 0.0), ("all-miss step", 1.0, 0.0),
                                     ("40 % dependent", 0.4, phi_dep), ("50 % dependent", 0.5, phi_dep)):
                d = detection(lim, rate, trials=trials, seed=seed, phi=phi)
                d.update(case=label, grid=grid, lead=lead, families=f, declared=declared,
                         rolling_misses=lim.rolling_misses, recent_n=lim.recent_n, consecutive=lim.consecutive,
                         wall_seconds_median=None if d["median_outcomes"] is None else
                         wall_seconds(d["median_outcomes"], lead, grid),
                         wall_seconds_p95=None if d["p95_outcomes"] is None else
                         wall_seconds(d["p95_outcomes"], lead, grid),
                         wall_seconds_64=wall_seconds(DETECTION_OUTCOMES, lead, grid),
                         reading=reading_at_least(d["lo"], d["probability"], d["hi"], DETECTION_TARGET))
                cells.append(d)
                _log("detection", lead, f, label, round(d["probability"], 4))
    return _digest({"schema": "trust-detection/v1", "seed_set": seed_set, "seed": seed, "trials": trials,
                    "target": DETECTION_TARGET, "outcomes": DETECTION_OUTCOMES, "trust_policy": t.policy,
                    "settings": dataclasses.asdict(t), "cells": cells})


def run_equivalence(seed_set: str, paths: int = 12, restart_every: int = 997) -> dict:
    """The product's functions at every request (with restarts) against fast_monitor on the same paths, plus paths
    with injected misses so that demotions and re-promotions are exercised."""
    seeds = _seeds(seed_set)[:paths]
    t = TrustSettings()
    cells, same = [], True
    for grid, lead, _ in CONFIGS[:2]:
        for k, seed in enumerate(seeds):
            model = (NOMINAL_MODELS + DEPENDENT_MODELS)[k % 6]
            out = gate_outcomes(model, seed, grid, lead)
            if k % 2:                                         # inject a burst of misses a day after first trust
                at = np.searchsorted(out.window_end, out.first_bound_origin + 72 * HOUR)
                burst = np.random.default_rng(seed).random(40 + 40 * (k % 3)) < 0.45
                out.miss[at:at + len(burst)] |= burst.astype(np.int64)
            f = 1 + k % 2
            fast, real = fast_monitor(out, t, f), real_monitor(out, t, f, restart_every=restart_every)
            a = (fast.trusted_at, [(d["at"], d["rules"], d["repromoted_at"]) for d in fast.demotions])
            b = (real.trusted_at, [(d["at"], d["rules"], d["repromoted_at"]) for d in real.demotions])
            cells.append({"model": model, "seed": seed, "lead": lead, "families": f, "equal": a == b,
                          "trusted_at": fast.trusted_at, "demotions": fast.demotions, "real": real.demotions})
            same &= a == b
            _log("equivalence", lead, seed, model, "equal" if a == b else "DIFFERENT", len(fast.demotions))
    return _digest({"schema": "trust-monitor-equivalence/v1", "seed_set": seed_set, "paths": len(cells),
                    "restart_every": restart_every, "all_equal": bool(same), "cells": cells})


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("command", choices=("select", "false-alarms", "detection", "equivalence"))
    ap.add_argument("--seeds", default="development", choices=("development", "validation"))
    ap.add_argument("--paths", type=int)
    ap.add_argument("--trials", type=int)
    ap.add_argument("--models")
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    if a.command == "select":
        if a.seeds != "development":
            ap.error("thresholds are selected on development seeds only")
        doc = select(**{k: v for k, v in (("paths", a.paths), ("trials", a.trials)) if v})
    elif a.command == "false-alarms":
        doc = run_false_alarms(a.seeds, a.paths, a.models.split(",") if a.models else None)
    elif a.command == "detection":
        doc = run_detection(a.seeds, **({"trials": a.trials} if a.trials else {}))
    else:
        doc = run_equivalence(a.seeds, a.paths or 12)
    with open(a.out, "w") as fh:
        json.dump(doc, fh, indent=1, sort_keys=True)
    print(json.dumps({k: doc[k] for k in doc if k not in ("cells", "grid")}, indent=1, sort_keys=True))
    print(f"result sha256 {doc['sha256']} -> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

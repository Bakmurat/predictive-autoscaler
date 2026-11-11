"""Acceptance protocol B of the trust repair, amendment 2: the operating characteristics of the coverage rules
(forecasting.trust, policy budgeted-coverage-tests/v2) at ONE profile, measured through the complete monitor. This is
the reproducible QUALIFICATION JOB (protocol T5): it is expensive and is not part of the unit-test run;
tests/test_trust_monitor.py guards the same characteristics cheaply.

Everything below was frozen, with the implementation and the thresholds, in the commit that introduced it, BEFORE any
validation seed was read. The validation is run once.

  - Profile. Grid 60 s, lead 600 s, seasonal-naive only (one family), nominal miscoverage 10 %, default calibration,
    trust and outcome-timing settings: contracts.VALIDATED_PROFILES[0]. Nothing here says anything about another
    lead, grid, risk or family set.
  - Targets (unchanged). False alarms: P(any false coverage demotion of a target within 7 days) <= 5 %, the four
    coverage rules together (rolling, consecutive, completed day, seven-day). Detection: >= 95 % probability that a
    sustained 40 % miscoverage is detected within 64 newly matured gate outcomes.
  - Named models (the qualification is EMPIRICAL under these; it is not a guarantee for a class of miss processes).
      bernoulli           independent gate misses at 10 %, one outcome per lead;
      additive, proportional, mixed
                          the product's calibration (calibration_acceptance.simulate: seasonal-naive forecast, scale
                          refits, adaptive level, the service's timing) on the declared independent noise models;
      additive_ar1, proportional_ar1, mixed_ar1
                          the same on the declared AR(1) noise, rho = 0.7 per 60 s sample: the stress model.
  - One path. One seed gives one signal path (or one Bernoulli sequence). The candidate becomes Trusted at the first
    evaluation where every gate holds (48 h after its first bound, 64 bounds). fast_monitor evaluates the four rules,
    the shadow gate and the hysteresis at every evaluation where their state can change, with the limits of
    trust.coverage_limits; real_monitor drives the product's own functions at every request, with restarts, and
    `equivalence` (and the unit tests) check that both give the same promotions, demotions, rules and re-promotions.
  - False alarm of a path: a demotion by the coverage gate within the 7 days after the first promotion.
  - Detection on a path. The change falls at the first gate window that ends a day plus a seeded uniform part of a
    day after the first promotion (so anywhere in the diurnal cycle). A path is ELIGIBLE when the candidate has been
    Trusted without interruption since before the change is first folded (so no rule is failing then); the others
    are excluded from the denominator and counted. From the change on the misses are replaced by:
      independent   independent misses at 40 % (also 50 %, and 100 %: an abrupt all-miss step);
      clustered     (calibrated paths only) the path's own normalized scores (y - base) / sigma above a fixed
                    threshold: the miss sequence a too-low bound would give, WITH the model's own serial dependence.
                    The thresholds (CLUSTERED_SCORE) are the pooled 60th and 50th percentiles of the post-shadow
                    scores of the development paths, frozen here; the miss rate they realize on the validation paths
                    is reported. The adaptive calibration's reaction after the change is left out by the definition
                    of "sustained".
    Detected: a coverage demotion at or before the request that folds the 64th changed outcome. Delay: changed
    outcomes folded at the demotion, and wall time from the start of the first changed window to the demoting
    request (the lead, the settlement and the request grid included); 64 outcomes are 64 x 600 s + 120 s + 60 s.
  - Acceptance rules. Probabilities over independent paths with 95 % Wilson intervals. A cell is MET when the whole
    interval is on the right side, NOT MET when the estimate is on the wrong side, INCONCLUSIVE otherwise.
      criterion cells, false alarms: each of the seven sources;
      criterion cells, detection:    independent 40 % on each of the seven sources, clustered 40 % on each of the six
                                     calibrated sources.
    The profile is qualified only if every criterion cell is MET. Reported beside them, not criteria: the 50 % and
    all-miss cases, the delays, the rules behind each alarm, re-promotion within 48 h, the excluded paths.
  - Seeds. DEVELOPMENT_SEEDS were used by the feasibility work (R8) and to freeze CLUSTERED_SCORE. VALIDATION_SEEDS
    were never read before the freeze. Per path: the signal from calibration_acceptance.signal(model, seed); Bernoulli
    misses from default_rng([seed, 1001]); the change point and the post-change draws from default_rng([seed, 1002]).
  - `latent_detection` (below) is a SEPARATE model: exceedances of a latent Gaussian AR(1) series. It is not the miss
    process of the calibrated bound and is not claimed equivalent to it; it is a cheap guard for the unit tests.

  python -m evaluation.trust_qualification validate --seeds validation --sources bernoulli,additive,... --out a.json
  python -m evaluation.trust_qualification report a.json b.json --out validation.json     # readings and tables
  python -m evaluation.trust_qualification equivalence --seeds development --paths 12 --out equivalence.json
  python -m evaluation.trust_qualification clustered-thresholds --out thresholds.json    # development seeds only
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
from forecasting.contracts import VALIDATED_PROFILES, TrustSettings

DAY, HOUR = 86400, 3600
SETTLEMENT, GRACE = 120, 600
NOMINAL_BPS = 1000
FALSE_ALARM_TARGET, DETECTION_TARGET, DETECTION_OUTCOMES = 0.05, 0.95, 64
GRID, LEAD = 60, 600                                     # the profile (contracts.VALIDATED_PROFILES[0])
FAMILIES = 1                                             # seasonal-naive only
PATH_DAYS = 12                                           # first bound + 48 h shadow + 7 days + a change + 64 outcomes
NOMINAL_MODELS = ("additive", "proportional", "mixed")
DEPENDENT_MODELS = ("additive_ar1", "proportional_ar1", "mixed_ar1")
BERNOULLI = "bernoulli"
SOURCES = (BERNOULLI,) + NOMINAL_MODELS + DEPENDENT_MODELS
INDEPENDENT_CASES = (("independent 40 %", 0.4), ("independent 50 %", 0.5), ("all-miss step", 1.0))
CLUSTERED_CASES = (("clustered 40 %", 0), ("clustered 50 %", 1))
CRITERION_CASES = ("independent 40 %", "clustered 40 %")

DEVELOPMENT_SEEDS = tuple(range(310_000, 311_000))
VALIDATION_SEEDS = tuple(range(1_260_000, 1_264_000))    # frozen; never read before the freeze; run once
BERNOULLI_STREAM, CHANGE_STREAM = 1001, 1002
# The clustered alternative's score thresholds per calibrated source: (60th, 50th) pooled percentile of the normalized
# scores of the DEVELOPMENT paths after the shadow period (`clustered-thresholds`). Frozen before validation.
CLUSTERED_SCORE: Dict[str, Tuple[float, float]] = {
    "additive": (0.1464, 0.0),
    "proportional": (0.1401, 0.0003),
    "mixed": (0.1427, -0.0003),
    "additive_ar1": (0.1931, 0.0008),
    "proportional_ar1": (0.1868, 0.0005),
    "mixed_ar1": (0.1905, 0.0006),
}


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
    score: Optional[np.ndarray] = None            # normalized score (y - base) / sigma of each outcome (calibrated paths)


def request_origin(t: int, grid: int, offset: int = acceptance.REQUEST_OFFSET) -> int:
    """The first request origin (a multiple of the grid) whose clock, origin + offset, has reached t."""
    return -(-(t - offset) // grid) * grid


def gate_outcomes(model: str, seed: int, grid: int = GRID, lead: int = LEAD, days: int = PATH_DAYS) -> GateOutcomes:
    """One path of the declared model through the repaired calibration (calibration_acceptance.simulate)."""
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
                        (path.y[bounded] > path.upper[bounded]).astype(np.int64), first, int(path.origin[-1]),
                        (path.y[bounded] - path.base[bounded]) / path.sigma[bounded])


def bernoulli_outcomes(seed: int, grid: int = GRID, lead: int = LEAD, days: int = PATH_DAYS,
                       rate: float = NOMINAL_BPS / T.BPS) -> GateOutcomes:
    """Independent gate misses at `rate`: one outcome per lead from the first window, bounded from the start."""
    ends = np.arange(lead, days * DAY + 1, lead, dtype=np.int64)
    miss = (np.random.default_rng([seed, BERNOULLI_STREAM]).random(len(ends)) < rate).astype(np.int64)
    return GateOutcomes(grid, lead, ends, miss, 0, int(ends[-1]) - lead)


def source_outcomes(source: str, seed: int) -> GateOutcomes:
    return bernoulli_outcomes(seed) if source == BERNOULLI else gate_outcomes(source, seed)


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
    k7 = np.array([lim.seven_day_misses(int(b)) for b in np.unique(bounded7)])
    need = k7[np.searchsorted(np.unique(bounded7), bounded7)]
    with np.errstate(divide="ignore", invalid="ignore"):
        fail["seven_day"] = (bounded7 == 0) | ((covered7 / np.maximum(bounded7, 1) < threshold) &
                                               (bounded7 - covered7 >= need))
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
        d = {"at": int(events[j]), "rules": [r for r in T.COVERAGE_RULES if fail[r][j]], "repromoted_at": None,
             "seen": int(seen[j])}
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
                startup_budget_valid=True, outcome_deadline_seconds=SETTLEMENT + GRACE, families=families,
                configuration_validated=True)
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


# --- a separate latent model (unit-test guard only) ---------------------------------------------------------------------

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


def latent_detection(lim: T.CoverageLimits, miss_rate: float, *, trials: int, seed: int, phi: float = 0.0,
              outcomes: int = DETECTION_OUTCOMES) -> Dict[str, object]:
    """A SEPARATE MODEL, for the unit tests' cheap guard only (module docstring): not the calibrated bound's misses.
    P(a rolling or consecutive alarm state at one of the first `outcomes` gate outcomes after the miss rate
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



# --- the frozen thresholds of the profile -----------------------------------------------------------------------------------

def limits_report(t: TrustSettings = TrustSettings(), lead: int = LEAD, families: int = FAMILIES) -> Dict[str, object]:
    """The thresholds the profile's monitor uses and the budgets they were computed for (forecasting.trust)."""
    lim = T.coverage_limits(t, lead, NOMINAL_BPS, families)
    per_day = DAY // lead
    ratio = lambda m: (NOMINAL_BPS + t.coverage_tolerance_bps) * m // T.BPS + 1      # noqa: E731  misses that fail the ratio
    return {"policy": lim.policy, "families": families, "lead": lead, "looks": lim.looks, "miss_bps": lim.miss_bps,
            "dependence_bps": lim.dependence_bps, "recent_n": lim.recent_n, "rolling_misses": lim.rolling_misses,
            "consecutive": lim.consecutive, "day_outcomes": per_day, "day_misses": lim.day_misses(per_day),
            "seven_day": [{"outcomes": m, "count_condition": lim.seven_day_misses(m), "coverage_condition": ratio(m),
                           "misses_to_fail": max(lim.seven_day_misses(m), ratio(m))}
                          for m in (64, 144, 288, 432, 576, 720, 864, lim.looks)],
            "budgets_ppm": {"rolling": t.rolling_alarm_ppm, "consecutive": t.consecutive_alarm_ppm,
                            "day": t.day_alarm_ppm, "seven_day": t.seven_day_alarm_ppm}}


# --- validation: the complete monitor, path by path -------------------------------------------------------------------------

def fold_origin(window_end: int, grid: int, offset: int = acceptance.REQUEST_OFFSET) -> int:
    """The request origin that folds an outcome (fast_monitor's rule)."""
    return -(-(window_end + SETTLEMENT - offset) // grid) * grid


def trusted_throughout(res: MonitorResult, before: int) -> bool:
    """Promoted before `before` and Trusted at every request from the last (re-)promotion up to it."""
    if res.trusted_at is None or res.trusted_at >= before:
        return False
    return all(d["repromoted_at"] is not None and d["repromoted_at"] < before
               for d in res.demotions if d["at"] < before)


def change_point(out: GateOutcomes, res: MonitorResult, seed: int):
    """(index of the first changed outcome or None when 64 outcomes do not follow, the path's change generator)."""
    rng = np.random.default_rng([seed, CHANGE_STREAM])
    at = res.trusted_at + DAY + int(rng.integers(0, DAY))
    tau = int(np.searchsorted(out.window_end, at, side="left"))
    return (tau if tau + DETECTION_OUTCOMES <= len(out.window_end) else None), rng


def path_result(source: str, seed: int, t: TrustSettings = TrustSettings()) -> Dict[str, object]:
    """One path through the complete monitor: its false alarm (if any) and every detection case (module docstring)."""
    out = source_outcomes(source, seed)
    res = fast_monitor(out, t, FAMILIES)
    got: Dict[str, object] = {"trusted": res.trusted_at is not None, "miss_rate": float(out.miss.mean()),
                              "false_alarm": None, "detection": {}}
    cases = [c for c, _ in INDEPENDENT_CASES] + ([c for c, _ in CLUSTERED_CASES] if source != BERNOULLI else [])
    if res.trusted_at is None:
        got["detection"] = {c: {"state": "excluded"} for c in cases}
        return got
    fa = res.false_alarm
    if fa is not None:
        got["false_alarm"] = {"rules": fa["rules"], "hours": (fa["at"] - res.trusted_at) / HOUR,
                              "repromoted_48h": fa["repromoted_at"] is not None and
                              fa["repromoted_at"] - fa["at"] <= 48 * HOUR}
    tau, rng = change_point(out, res, seed)
    if tau is None:
        got["detection"] = {c: {"state": "short"} for c in cases}
        return got
    at_change = fold_origin(int(out.window_end[tau]), out.grid)
    if not trusted_throughout(res, at_change):
        got["detection"] = {c: {"state": "excluded"} for c in cases}
        return got
    n_post = len(out.miss) - tau
    draws = {rate: rng.random(n_post) for _, rate in INDEPENDENT_CASES[:2]}
    post = {c: (np.ones(n_post, dtype=bool) if rate >= 1.0 else draws[rate] < rate) for c, rate in INDEPENDENT_CASES}
    if source != BERNOULLI:
        for c, i in CLUSTERED_CASES:
            post[c] = out.score[tau:] > CLUSTERED_SCORE[source][i]
    stop = tau + DETECTION_OUTCOMES
    for c in cases:
        miss = out.miss.copy()
        miss[tau:] = post[c]
        changed = GateOutcomes(out.grid, out.lead, out.window_end[:stop], miss[:stop], out.first_bound_origin,
                               out.last_origin)
        hit = next((d for d in fast_monitor(changed, t, FAMILIES).demotions if d["at"] >= at_change), None)
        cell = {"state": "eligible", "detected": hit is not None, "post_miss_rate": float(post[c][:DETECTION_OUTCOMES].mean())}
        if hit is not None:
            cell.update(outcomes=hit["seen"] - tau, rules=hit["rules"],
                        wall_seconds=hit["at"] + acceptance.REQUEST_OFFSET - (int(out.window_end[tau]) - out.lead))
        got["detection"][c] = cell
    return got


def validate(sources: Sequence[str], seeds: Sequence[int], progress=None) -> dict:
    """Every source over the seeds: the raw counts of each cell (no reading here: `report` reads them)."""
    cells = []
    for source in sources:
        if source not in SOURCES:
            raise ValueError(f"unknown source {source!r}")
        cell = {"source": source, "paths": 0, "trusted": 0, "alarms": 0, "by_rule": {r: 0 for r in T.COVERAGE_RULES},
                "repromoted_48h": 0, "hours_to_alarm": [], "miss_rate_sum": 0.0, "detection": {}}
        for n, seed in enumerate(seeds):
            r = path_result(source, seed)
            cell["paths"] += 1
            cell["trusted"] += r["trusted"]
            cell["miss_rate_sum"] += r["miss_rate"]
            fa = r["false_alarm"]
            if fa is not None:
                cell["alarms"] += 1
                cell["repromoted_48h"] += fa["repromoted_48h"]
                cell["hours_to_alarm"].append(round(fa["hours"], 2))
                for rule in fa["rules"]:
                    cell["by_rule"][rule] += 1
            for case, d in r["detection"].items():
                c = cell["detection"].setdefault(case, {"eligible": 0, "excluded": 0, "short": 0, "detected": 0,
                                                        "outcomes": [0] * (DETECTION_OUTCOMES + 1), "wall_seconds": [],
                                                        "by_rule": {x: 0 for x in T.COVERAGE_RULES},
                                                        "post_miss_rate_sum": 0.0})
                if d["state"] != "eligible":
                    c[d["state"]] += 1
                    continue
                c["eligible"] += 1
                c["post_miss_rate_sum"] += d["post_miss_rate"]
                if d["detected"]:
                    c["detected"] += 1
                    c["outcomes"][d["outcomes"]] += 1
                    c["wall_seconds"].append(int(d["wall_seconds"]))
                    for rule in d["rules"]:
                        c["by_rule"][rule] += 1
            if progress and (n + 1) % 200 == 0:
                progress(source, n + 1)
        cells.append(cell)
    return _digest({"schema": "trust-validation-cells/v1", "trust_policy": TrustSettings().policy,
                    "profile": dataclasses.asdict(VALIDATED_PROFILES[0]), "seeds": [seeds[0], seeds[-1], len(seeds)],
                    "settings": dataclasses.asdict(TrustSettings()), "limits": limits_report(), "cells": cells})


def _quantile(hist: Sequence[int], q: float) -> Optional[int]:
    total, run = sum(hist), 0
    if not total:
        return None
    for value, count in enumerate(hist):
        run += count
        if run > q * total or run == total:
            return value
    return None


def wall_deadline_seconds() -> int:
    """From the start of the first changed window to the request that folds the 64th changed outcome, at worst."""
    return DETECTION_OUTCOMES * LEAD + SETTLEMENT + GRID


def report(docs: Sequence[dict]) -> dict:
    """The readings of a complete validation: every source exactly once, the same seeds, policy and limits."""
    cells = [c for d in docs for c in d["cells"]]
    names = [c["source"] for c in cells]
    if sorted(names) != sorted(SOURCES):
        raise ValueError(f"the validation needs every source exactly once, got {names}")
    for key in ("trust_policy", "profile", "seeds", "settings", "limits"):
        if any(d[key] != docs[0][key] for d in docs):
            raise ValueError(f"the parts disagree on {key}")
    rows, verdict = [], True
    for source in SOURCES:
        c = next(x for x in cells if x["source"] == source)
        lo, hi = wilson(c["alarms"], c["trusted"])
        p = c["alarms"] / c["trusted"] if c["trusted"] else None
        fa = {"probability": None if p is None else FAMILIES * p, "lo": FAMILIES * lo, "hi": min(1.0, FAMILIES * hi),
              "alarms": c["alarms"], "trusted": c["trusted"], "paths": c["paths"], "by_rule": c["by_rule"],
              "repromoted_48h": c["repromoted_48h"], "mean_miss_rate": c["miss_rate_sum"] / max(1, c["paths"])}
        fa["reading"] = "NOT MEASURED" if p is None else reading_at_most(fa["lo"], fa["probability"], fa["hi"],
                                                                         FALSE_ALARM_TARGET)
        verdict &= fa["reading"] == "MET"
        det = {}
        for case, d in c["detection"].items():
            dlo, dhi = wilson(d["detected"], d["eligible"])
            dp = d["detected"] / d["eligible"] if d["eligible"] else None
            wall = sorted(d["wall_seconds"])
            det[case] = {"probability": dp, "lo": dlo, "hi": dhi, "detected": d["detected"], "eligible": d["eligible"],
                         "excluded": d["excluded"], "short": d["short"], "criterion": case in CRITERION_CASES,
                         "post_change_miss_rate": d["post_miss_rate_sum"] / max(1, d["eligible"]),
                         "median_outcomes": _quantile(d["outcomes"], 0.5), "p95_outcomes": _quantile(d["outcomes"], 0.95),
                         "median_wall_seconds": wall[len(wall) // 2] if wall else None,
                         "p95_wall_seconds": wall[min(len(wall) - 1, int(0.95 * len(wall)))] if wall else None,
                         "by_rule": d["by_rule"],
                         "reading": "NOT MEASURED" if dp is None else reading_at_least(dlo, dp, dhi, DETECTION_TARGET)}
            if case in CRITERION_CASES:
                verdict &= det[case]["reading"] == "MET"
        rows.append({"source": source, "false_alarms": fa, "detection": det})
    d0 = docs[0]
    return _digest({"schema": "trust-validation/v1", "trust_policy": d0["trust_policy"], "profile": d0["profile"],
                    "seeds": d0["seeds"], "settings": d0["settings"], "limits": d0["limits"],
                    "targets": {"false_alarms": FALSE_ALARM_TARGET, "detection": DETECTION_TARGET,
                                "outcomes": DETECTION_OUTCOMES, "wall_deadline_seconds": wall_deadline_seconds()},
                    "parts": [d["sha256"] for d in docs], "rows": rows,
                    "qualified": bool(verdict)})


def _pct(p: Optional[float], lo: float, hi: float) -> str:
    return "n/a" if p is None else f"{100 * p:.2f} % [{100 * lo:.2f}, {100 * hi:.2f}]"


def _dur(seconds: Optional[int]) -> str:
    if seconds is None:
        return "n/a"
    return f"{seconds // 3600} h {seconds % 3600 // 60:02d} min"


def tables(doc: dict) -> str:
    """The validation as Markdown: false alarms per source, then every detection case."""
    out = ["| Source | Paths (trusted) | False coverage demotions in 7 days | P [95 %] | By rule | Re-promoted in 48 h | "
           "Reading (<= 5 %) |", "|" + "---|" * 7]
    for r in doc["rows"]:
        f = r["false_alarms"]
        rules = ", ".join(f"{k} {v}" for k, v in f["by_rule"].items() if v) or "none"
        out.append(f"| {r['source']} | {f['paths']} ({f['trusted']}) | {f['alarms']} | "
                   f"{_pct(f['probability'], f['lo'], f['hi'])} | {rules} | {f['repromoted_48h']} | **{f['reading']}** |")
    out += ["", "| Source | Post-change misses | Eligible (excluded) | Miss rate realized | Detected within 64 [95 %] | "
            "Median / p95 outcomes | Median / p95 wall time | Criterion | Reading (>= 95 %) |", "|" + "---|" * 9]
    for r in doc["rows"]:
        for case, d in r["detection"].items():
            out.append(f"| {r['source']} | {case} | {d['eligible']} ({d['excluded'] + d['short']}) | "
                       f"{100 * d['post_change_miss_rate']:.1f} % | {_pct(d['probability'], d['lo'], d['hi'])} | "
                       f"{d['median_outcomes']} / {d['p95_outcomes']} | {_dur(d['median_wall_seconds'])} / "
                       f"{_dur(d['p95_wall_seconds'])} | {'yes' if d['criterion'] else 'reported'} | "
                       f"{('**' + d['reading'] + '**') if d['criterion'] else d['reading']} |")
    return "\n".join(out)


def clustered_thresholds(seeds: Sequence[int] = DEVELOPMENT_SEEDS) -> Dict[str, List[float]]:
    """The pooled 60th and 50th percentiles of the post-shadow normalized scores, per calibrated source."""
    out = {}
    for source in NOMINAL_MODELS + DEPENDENT_MODELS:
        pooled = []
        for seed in seeds:
            o = gate_outcomes(source, seed)
            pooled.append(o.score[o.window_end >= o.first_bound_origin + 48 * HOUR])
        s = np.concatenate(pooled)
        out[source] = [round(float(np.quantile(s, 0.6)), 4), round(float(np.quantile(s, 0.5)), 4)]
    return out


def run_equivalence(seeds: Sequence[int], paths: int = 12, restart_every: int = 997) -> dict:
    """The product's functions at every request (with restarts) against fast_monitor on the same paths of the
    profile, half of them with injected misses so that demotions and re-promotions are exercised."""
    t = TrustSettings()
    cells, same = [], True
    for k, seed in enumerate(seeds[:paths]):
        source = SOURCES[k % len(SOURCES)]
        out = source_outcomes(source, seed)
        if k % 2:                                             # a burst of misses a day after first trust
            at = np.searchsorted(out.window_end, out.first_bound_origin + 72 * HOUR)
            burst = np.random.default_rng(seed).random(40 + 40 * (k % 3)) < 0.45
            out.miss[at:at + len(burst)] |= burst[:len(out.miss[at:at + len(burst)])].astype(np.int64)
        fast, real = fast_monitor(out, t, FAMILIES), real_monitor(out, t, FAMILIES, restart_every=restart_every)
        a = (fast.trusted_at, [(d["at"], d["rules"], d["repromoted_at"]) for d in fast.demotions])
        b = (real.trusted_at, [(d["at"], d["rules"], d["repromoted_at"]) for d in real.demotions])
        cells.append({"source": source, "seed": seed, "equal": a == b, "trusted_at": fast.trusted_at,
                      "demotions": fast.demotions, "real": real.demotions})
        same &= a == b
        _log("equivalence", seed, source, "equal" if a == b else "DIFFERENT", len(fast.demotions))
    return _digest({"schema": "trust-monitor-equivalence/v2", "paths": len(cells), "restart_every": restart_every,
                    "all_equal": bool(same), "cells": cells})


def _digest(doc: dict) -> dict:
    doc["sha256"] = hashlib.sha256(json.dumps(doc, sort_keys=True).encode()).hexdigest()
    return doc


def _seeds(name: str) -> Tuple[int, ...]:
    return DEVELOPMENT_SEEDS if name == "development" else VALIDATION_SEEDS


def _log(*a) -> None:
    print(*a, file=sys.stderr, flush=True)


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("command", choices=("validate", "report", "equivalence", "clustered-thresholds"))
    ap.add_argument("parts", nargs="*", help="report: the validate outputs to read together")
    ap.add_argument("--seeds", default="development", choices=("development", "validation"))
    ap.add_argument("--sources", default=",".join(SOURCES))
    ap.add_argument("--paths", type=int)
    ap.add_argument("--out", required=True)
    a = ap.parse_args(argv)
    if a.command == "validate":
        seeds = _seeds(a.seeds)[:a.paths]
        doc = validate(a.sources.split(","), seeds, progress=lambda *x: _log("validate", *x))
    elif a.command == "report":
        doc = report([json.load(open(p)) for p in a.parts])
        print(tables(doc))
    elif a.command == "equivalence":
        doc = run_equivalence(_seeds(a.seeds), a.paths or 12)
    else:
        doc = _digest({"schema": "trust-clustered-thresholds/v1", "seeds": [DEVELOPMENT_SEEDS[0], DEVELOPMENT_SEEDS[-1]],
                       "thresholds": clustered_thresholds()})
    with open(a.out, "w") as fh:
        json.dump(doc, fh, indent=1, sort_keys=True)
    print(json.dumps({k: doc[k] for k in doc if k not in ("cells", "rows")}, indent=1, sort_keys=True))
    print(f"result sha256 {doc['sha256']} -> {a.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())

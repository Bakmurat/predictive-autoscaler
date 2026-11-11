"""Trust for forecast contract v2: evidence ledgers, per-candidate gates with hysteresis, paired challenger promotion,
the fallback order with selection hysteresis and the ready-to-activate verdict. Pure and replayable: every function maps
(state, inputs, now) to a new state or a verdict; nothing reads a clock, the network or Kubernetes (design C2, section
7).

Evidence ledger (one per epoch, model AND recipe; records of another recipe are refused, so a replacement recipe starts
its own evidence). Issuances and matured records are folded in exactly once (journal sequence numbers) into hourly
buckets (by window end, or by origin for issuances), kept for the 7-day retention. PROSPECTIVE and REPLAY evidence are
counted apart in every count: replay may warm up calibration, but every gate, availability figure and error diagnostic
uses prospective evidence only (hindsight cannot authorize a write). The ledger also tracks each prospective issuance
whose outcome is still due, with its truth deadline: an issued window whose deadline has passed without an outcome is
OVERDUE and counts against availability, so a stalled maturation cannot look healthy.

Time windows are whole hours: a trailing window of S seconds at `now` covers the hour buckets from the first whole
hour at or after now - S to the hour holding now. With an hour-aligned now it covers exactly [now - S, now]; otherwise
up to one hour less (never more). Coverage, availability and pairs all use this rule.

Gates per candidate (all thresholds are TrustSettings parameters, fingerprinted with the settings):
  identity/storage  identity valid and the journal healthy                    else IdentityMismatch / StateUnavailable
  data              current input fresh, and the fit no older than max_fit_age (statistical)    else DataUnavailable
  history           the family's minimum history (naive: 1 day; E1-product: 7 days)            else HistoryInsufficient
  calibration       an upper bound is available now (>= min_windows scores, rank feasible)  else CalibrationInsufficient
  prospective       >= min_shadow_hours since the first prospective bound and >= min_prospective_windows prospective
                    gate bounds                                                                else ShadowInsufficient
  coverage          the four budgeted coverage rules below, all holding                       else CoverageLow
  availability      over the last day, >= min_availability of (a) the matured-or-overdue issued windows scorable
                    (scored / (scored + unscorable + overdue)) and (b) the expected forecast opportunities issued
                    (issued origins / grid origins since the first issuance); None (nothing to count) fails
                                                                                               else DataUnavailable
Coverage rules (policy budgeted-coverage-tests/v2). Each is a test on prospective gate outcomes (cohort windows with
an issued bound). Their thresholds are COMPUTED UNDER ONE STATED MODEL of the miss sequence, and their computed
false-alarm probabilities share one budget (rolling + consecutive + day + seven-day = 5 % per target and 7 days,
divided equally among the enabled families).

  The model (what the arithmetic assumes, nothing more). The gate misses X_1, X_2, ... are a stationary two-state
  Markov chain: P(X = 1) = a, the nominal miscoverage, and P(X_t = 1 | X_{t-1} = 1) = q, with
  q = max(miss_dependence_bps, a) (0.175 by default); so P(X_t = 1 | X_{t-1} = 0) = a (1 - q) / (1 - a). Initialization:
  every probability below is computed with the chain in its stationary distribution at the first outcome of the
  window (or with the outcome leaving the window covered, where stated). q = a is independence.
  What this is NOT: a guarantee for every miss process whose measured P(miss | previous miss) is at most q. A
  process with that lag-1 dependence need not be first-order Markov, and the boundaries are not shown conservative
  over a class of processes. The policy is QUALIFIED EMPIRICALLY under named models (independent misses; the misses
  of the calibrated bound on independent noise and on AR(1) noise with rho = 0.7), on an allowlist of configurations
  (contracts.VALIDATED_PROFILES; evaluation/trust_qualification.py). Outside the allowlist predictive eligibility
  is refused (reason ConfigurationNotValidated); the rules still run and are reported.

  rolling      the last acute_recent_windows (n) gate outcomes hold at least k misses. k is the smallest count such
               that, for every count from k on, N x P(the rule STARTS failing at a look) is within the rule's budget
               (rolling_alarm_ppm / families), N = 7 d / lead looks. The rule starts failing at a look only if the
               newest outcome is a miss, the outcome that left the window was covered, and the n - 1 between them
               hold exactly k - 1 misses; that probability is computed for the model in exact integer arithmetic.
               The expected number of such onsets in 7 days bounds the probability of any. It needs a full window,
               which a trusted candidate has: the settings require min_prospective_windows >= acute_recent_windows.
  consecutive  c gate outcomes in a row missed, with c the smallest count, not below acute_consecutive_misses (5),
               with (N a (1 - q) + a) q^(c - 1) within the budget (consecutive_alarm_ppm / families): the expected
               number of runs of c that start in 7 days, plus one already under way (exact integers).
  day          a COMPLETED UTC day (inside the 7-day window, past its outcome deadline: day end + settlement + grace,
               with at least worst_day_min_outcomes gate outcomes) holds at least k_m misses among its m outcomes,
               k_m the smallest count with P(S_m >= k_m) <= (day_alarm_ppm / families) / 7 under the model: one
               test per day, seven days in the window. The evidence of a completed day is final, so every later
               evaluation reads the same answer: the day is looked at once. A failed day keeps the rule failed while
               it is inside the 7-day window (until an hour before its start leaves it). The current, partial day is
               never tested.
  seven_day    prospective gate coverage over the trailing 7 days is below nominal - coverage_tolerance AND the m
               gate outcomes of that window hold at least k7_m misses, k7_m the smallest count with
               (N + 168) x P(S_m >= k7_m) within the budget (seven_day_alarm_ppm / families): a union over every new
               outcome and every hourly shift of the window in 7 days. With many outcomes the coverage condition is
               the binding one (the rule is then what it always was); with few (long leads, a young candidate) the
               count is, so that a nominal bound is not demoted by the noise of a small sample. THE PRICE: at long
               leads the rule no longer reacts to a moderate sustained undercoverage that a week's outcomes cannot
               tell from noise at this false-alarm budget. At a 600 s lead, one family: a full week needs 153
               of 1,008 outcomes missed where the coverage condition alone needed 152; a two-day window needs 59 of
               288 (20.5 %) where it needed 44; at a 3,600 s lead a full week needs 39 of 168 where it needed 26.
  The tail probabilities P(S_m >= k) of the day and seven-day rules are computed by the model's recursion in
  double precision (count_limits); the rolling and consecutive thresholds are exact. The thresholds of the
  allowlisted configurations are pinned by tests.
A statistical LOOK of the rolling and consecutive rules happens when a new gate outcome is folded into the ledger
(exactly once, by its journal sequence number), never at a poll: the ledger's monitor counts each ALARM (a rule going
from holding to failing at a look) and remembers the last one. An evaluation between two looks reads the same ledger
and so the same state; an alarm that came and went between two evaluations is still seen by the next one (the
hysteresis remembers how many alarms it has seen), so a demotion cannot be skipped, repeated or counted twice, and
both parts are in the checkpointed state (restart and replay give the same alarms). Missing outcomes are not misses:
they stay an availability matter.

Hysteresis (per candidate, bound to its epoch and recipe): a trusted candidate failing any gate is demoted at once (Degraded).
Once demoted, it is promoted again only after its gates have held for repromotion_hours and >= repromotion_windows new
prospective gate bounds have matured since the demotion; never in the same evaluation as the demotion.

Challenger promotion (paired, against the trusted daily naive; the paired evidence must belong to the candidates'
epoch, name the challenger and the naive, and carry both recipes): >= challenger_min_pairs paired
prospective gate bounds whose retained window ends SPAN >= challenger_min_hours, mean pinball loss <=
challenger_loss_ratio x the naive's, and the same-policy replay costs (shortage, replica-time, from
forecasting.policy_sim) each <= challenger_cost_ratio x the naive's. Costs count only when their binding matches: the
same epoch, both models and recipes of the paired evidence, the policy fingerprint the caller expects (capacity, bounds,
caps, headroom, startup delay, decision interval) and a window covering the retained pairs. They count only on
adequate evidence too (cost_evidence_ok): >= challenger_min_pairs decisions where both forecasts had a bound, which are
>= min_availability of the replayed decisions, and seconds without a truth measurement <= (1 - min_availability) of
the integrated time (the explicit missing-truth limit; 5% by default). A zero naive loss keeps the naive. Without
matching, adequate costs nothing is promoted.

Selection (fallback order lstm -> statistical -> seasonal_naive -> reactive-only) has its own hysteresis: a safety
failure (the selected candidate, or the naive it was compared with, not trusted) falls back at once; a challenger is
selected only after its promotion criteria have held continuously for repromotion_hours; once selected it is kept
until its paired loss is worse than challenger_exit_loss_ratio x the naive's or its costs fail. Marginal superiority
therefore cannot make the selection flap. The selection state is bound to the epoch: a new epoch starts it over. Never
an untrusted naive because a richer model failed.
PredictionReady additionally needs a VALIDATED CONFIGURATION (the caller says whether grid, lead, risk and family set
are on the allowlist the trust policy was qualified for; otherwise the reason is ConfigurationNotValidated whatever the
evidence) and the activation evidence this service can check (versioned replay suite, startup budget): it means "ready
to activate"; Recommend still makes no writes, and it says nothing about a workload's SLO.
"""

from __future__ import annotations

import functools
import math
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

import numpy as np

from forecasting.calibration import CalibrationRecord, in_cohort, pinball_rps
from forecasting.contracts import TRUST_POLICY_REVISION, TrustSettings
from forecasting.maturation import candidate

LEDGER_SCHEMA = "evidence-ledger/v4"
PPM = 1_000_000
BPS = 10_000
COVERAGE_RULES = ("rolling", "consecutive", "day", "seven_day")
HOUR = 3600
DAY = 86400
FALLBACK_ORDER = ("lstm", "statistical", "seasonal_naive")
NAIVE = "seasonal_naive"
MODES = ("prospective", "replay")
SELECTION = "_selection"
REASON_ORDER = ("IdentityMismatch", "StateUnavailable", "DataUnavailable", "HistoryInsufficient",
                "CalibrationInsufficient", "ShadowInsufficient", "CoverageLow", "ConfigurationNotValidated",
                "LeadInsufficient", "ReplayGateMissing", "Ready")
# The candidate history minimums of the cold-start ladder (design C2, section 4); an optional LSTM brings its own.
REQUIRED_HISTORY_SECONDS = {"seasonal_naive": DAY, "statistical": 7 * DAY}
# Prospective: issued (all, gate), scored/unscorable (all, gate), bounded/covered (all, gate), pinball, error sums.
# Replay: issued, scored, unscorable, gate bounded/covered. Nothing else is counted for replay.
_FLOATS = ("pin", "abs", "bias", "st0", "st1", "st2")
_FIELDS = ("iss", "g_iss", "sc", "un", "g_sc", "g_un", "a_bd", "a_cv", "g_bd", "g_cv", "n_step", *_FLOATS,
           "r_iss", "r_sc", "r_un", "r_bd", "r_cv")


class TrustError(Exception):
    pass


# --- coverage limits: thresholds under the stated transition model (module docstring) -----------------------------------

@functools.lru_cache(maxsize=4096)
def binomial_tail(n: int, miss_bps: int, k: int) -> int:
    """The numerator of P(Bin(n, miss_bps / 10^4) >= k) over the denominator 10^(4 n): an exact integer. (Independent
    misses, the model's q = a case: kept as the reference the model's thresholds are checked against.)"""
    if k <= 0:
        return BPS ** n
    a, b = miss_bps, BPS - miss_bps
    total, term = 0, None
    for j in range(n, k - 1, -1):                      # C(n, j) a^j b^(n-j), from j = n down
        term = a ** n if j == n else term * (j + 1) * b // ((n - j) * a)
        total += term
    return total


def model_dependence_bps(miss_bps: int, dependence_bps: int) -> int:
    """q of the model in basis points: the configured P(miss | previous miss), never below the miss rate itself (the
    model has no negative dependence)."""
    return max(miss_bps, dependence_bps)


def _transitions(miss_bps: int, dependence_bps: int) -> Tuple[int, int, int, int, int]:
    """(w00, w01, w10, w11, D): the chain's transition probabilities as integers over the common denominator D.
    From covered: miss with a (1 - q) / (1 - a); from a miss: miss with q."""
    a, q = miss_bps, model_dependence_bps(miss_bps, dependence_bps)
    d = BPS * (BPS - a)
    w01, w11 = a * (BPS - q), q * (BPS - a)
    w00, w10 = d - w01, d - w11
    g = math.gcd(math.gcd(w00, w01), math.gcd(math.gcd(w10, w11), d))
    return w00 // g, w01 // g, w10 // g, w11 // g, d // g


@functools.lru_cache(maxsize=1024)
def rolling_limit(n: int, miss_bps: int, dependence_bps: int, looks: int, budget_ppm: int, families: int) -> int:
    """The smallest k in 1..n such that, for every count from k on, looks x P(onset) <= budget_ppm / families, or
    n + 1 when there is none (the rule then never alarms). Onset at a look of a full window: the outcome that left
    was covered, the n - 1 between hold exactly k - 1 misses, the newest is a miss. Exact integers: numerators over
    BPS x D^n, the chain stationary at the outcome that left."""
    w00, w01, w10, w11, d = _transitions(miss_bps, dependence_bps)
    cov, mis = [0] * (n + 1), [0] * (n + 1)          # numerators by miss count: last outcome covered / missed
    cov[0] = BPS - miss_bps                          # the outcome that left the window: covered, stationary
    for _ in range(n - 1):
        new_cov = [w00 * cov[j] + w10 * mis[j] for j in range(n + 1)]
        new_mis = [0] + [w01 * cov[j] + w11 * mis[j] for j in range(n)]
        cov, mis = new_cov, new_mis
    onset = [w01 * cov[j] + w11 * mis[j] for j in range(n + 1)]     # onset[j]: j misses between, then a miss
    scale = budget_ppm * BPS * d ** n
    k = n + 1
    while k > 1 and looks * onset[k - 2] * PPM * families <= scale:
        k -= 1
    return k


@functools.lru_cache(maxsize=1024)
def consecutive_limit(least: int, miss_bps: int, dependence_bps: int, looks: int, budget_ppm: int,
                      families: int) -> int:
    """The smallest c >= least with (looks a (1 - q) + a) q^(c - 1) <= budget_ppm / families (per 7 days): the
    expected number of runs of c misses that start at a look of 7 days, plus one under way at their start. Exact."""
    a, q = miss_bps, model_dependence_bps(miss_bps, dependence_bps)
    c = max(1, least)
    while (looks * a * (BPS - q) + a * BPS) * q ** (c - 1) * PPM * families > budget_ppm * BPS ** (c + 1):
        c += 1
        if c > 4096:
            raise TrustError("no run length fits the consecutive rule's budget")
    return c


def consecutive_bound_ppm(c: int, miss_bps: int, dependence_bps: int, looks: int) -> float:
    """The model's bound, in ppm, on P(c consecutive misses somewhere in `looks` outcomes)."""
    a, q = miss_bps / BPS, model_dependence_bps(miss_bps, dependence_bps) / BPS
    return (looks * a * (1.0 - q) + a) * q ** (c - 1) * PPM


@functools.lru_cache(maxsize=64)
def count_limits(upto: int, miss_bps: int, dependence_bps: int, alpha_num: int, alpha_den: int) -> Tuple[int, ...]:
    """k[m] for m = 0..upto: the smallest count with P(S_m >= k) <= alpha_num / alpha_den, S_m the misses among m
    consecutive outcomes of the model started in its stationary distribution (m + 1: no count is that unlikely).
    Double precision; the recursion adds non-negative terms only and the tail is summed from its small end."""
    w00, w01, w10, w11, d = _transitions(miss_bps, dependence_bps)
    p00, p01, p10, p11 = w00 / d, w01 / d, w10 / d, w11 / d
    alpha = alpha_num / alpha_den
    cov, mis = np.zeros(upto + 2), np.zeros(upto + 2)
    cov[0], mis[1] = 1.0 - miss_bps / BPS, miss_bps / BPS
    out = [1] + [0] * upto
    for m in range(1, upto + 1):
        if m > 1:
            new_cov = p00 * cov + p10 * mis
            new_mis = np.zeros(upto + 2)
            new_mis[1:] = p01 * cov[:-1] + p11 * mis[:-1]
            cov, mis = new_cov, new_mis
        tail = np.cumsum((cov[:m + 2] + mis[:m + 2])[::-1])[::-1]       # tail[k] = P(S_m >= k); tail[m + 1] = 0
        out[m] = int(np.count_nonzero(tail > alpha))                    # the tail is non-increasing in k
    return tuple(out)


def _limit_at(table: Tuple[int, ...], outcomes: int) -> int:
    """The table's threshold for a count of outcomes; beyond the table every further outcome is taken as a possible
    miss (P(S_m >= k + m - M) <= P(S_M >= k)), which keeps the bound valid."""
    top = len(table) - 1
    return table[outcomes] if outcomes <= top else table[top] + (outcomes - top)


@dataclass(frozen=True)
class CoverageLimits:
    """The thresholds of the coverage rules for one candidate: derived from the trust settings, the lead (looks per
    7 days), the nominal miscoverage and the number of enabled families that share the target's budget."""
    policy: str
    recent_n: int
    rolling_misses: int                 # alarm at this many misses among the last recent_n gate outcomes
    consecutive: int                    # alarm at this many gate outcomes missed in a row (budget-derived)
    looks: int                          # gate outcomes per 7 days
    miss_bps: int
    dependence_bps: int                 # the model's P(miss | previous miss), basis points
    day_alarm_ppm: int
    seven_day_alarm_ppm: int
    families: int

    def day_misses(self, outcomes: int) -> int:
        """A completed day of this many gate outcomes fails at this many misses."""
        table = count_limits(self.looks // 7 + 2, self.miss_bps, self.dependence_bps, self.day_alarm_ppm,
                             PPM * self.families * 7)
        return _limit_at(table, outcomes)

    def seven_day_misses(self, outcomes: int) -> int:
        """The seven-day rule's count condition: at least this many misses among the window's gate outcomes."""
        table = count_limits(self.looks + 64, self.miss_bps, self.dependence_bps, self.seven_day_alarm_ppm,
                             PPM * self.families * (self.looks + 168))
        return _limit_at(table, outcomes)


def coverage_limits(t: TrustSettings, lead_seconds: int, miss_bps: int, families: int) -> CoverageLimits:
    if t.policy != TRUST_POLICY_REVISION:
        raise TrustError(f"unknown trust policy {t.policy!r}")
    if lead_seconds <= 0 or not 0 < miss_bps < BPS or families < 1:
        raise TrustError("coverage limits need a lead, a nominal miscoverage and at least one family")
    looks = max(1, 7 * DAY // lead_seconds)
    q = model_dependence_bps(miss_bps, t.miss_dependence_bps)
    return CoverageLimits(t.policy, t.acute_recent_windows,
                          rolling_limit(t.acute_recent_windows, miss_bps, q, looks, t.rolling_alarm_ppm, families),
                          consecutive_limit(t.acute_consecutive_misses, miss_bps, q, looks, t.consecutive_alarm_ppm,
                                            families), looks, miss_bps, q, t.day_alarm_ppm, t.seven_day_alarm_ppm,
                          families)


# --- evidence records and ledgers -----------------------------------------------------------------------------------

@dataclass(frozen=True)
class EvidenceRecord:
    """A calibration record plus what trust needs: the evidence mode and the matched per-step errors."""
    cal: CalibrationRecord
    mode: str
    committed_at_ms: int
    step_abs_errors: Optional[Tuple[float, float, float]]     # first, middle, last target; None unless scored

    @classmethod
    def from_journal(cls, issuance, outcome) -> "EvidenceRecord":
        rec = CalibrationRecord.from_journal(issuance, outcome)
        b, p = issuance.payload(), outcome.payload()
        mode = b.get("evidence_mode")
        if mode not in MODES:
            raise TrustError(f"the issuance does not declare its evidence mode ({mode!r})")
        steps = None
        if outcome.status == "scored":
            pts, ys = candidate(b, outcome.model)["point_rps"], p["values"]
            if len(pts) != len(ys):
                raise TrustError("the outcome's values do not match the issued trajectory")
            idx = (0, len(ys) // 2, len(ys) - 1)
            steps = tuple(abs(pts[i] - ys[i]) for i in idx)
        return cls(rec, mode, outcome.committed_at_ms, steps)


def new_ledger(epoch: str, model: str, recipe: str, interval_s: int) -> dict:
    return {"schema": LEDGER_SCHEMA, "epoch": epoch, "model": model, "recipe": recipe, "interval_s": interval_s,
            "buckets": {}, "pending": {}, "recent": [], "consecutive_misses": 0, "first_prospective_s": None,
            "first_issue_s": None, "last_prospective_outcome_s": None, "last_outcome_seq": 0, "last_issuance_seq": 0,
            "newest_s": None,
            "monitor": {"looks": 0, "alarms": {"rolling": 0, "consecutive": 0}, "last_alarm": None},
            "totals": {"issued": 0, "covered": 0, "missed": 0, "unscorable": 0, "prospective_gate_bounded": 0,
                       "replay_gate_bounded": 0}}


def _bucket(ledger: dict, t: int) -> dict:
    key = str(t // HOUR)
    b = ledger["buckets"].get(key)
    if b is None:
        b = ledger["buckets"][key] = {f: (0.0 if f in _FLOATS else 0) for f in _FIELDS}
    return b


def _prune(ledger: dict, now_s: int, retention_s: int) -> None:
    newest = max(now_s, ledger["newest_s"] or now_s)
    ledger["newest_s"] = newest
    floor = (newest - retention_s) // HOUR
    ledger["buckets"] = {k: v for k, v in ledger["buckets"].items() if int(k) > floor}
    # Pending issuances matter only to the last day's availability: older ones are dropped (bounded state).
    ledger["pending"] = {k: v for k, v in ledger["pending"].items() if int(k) > newest - DAY}


def _copy(ledger: dict) -> dict:
    mon = ledger["monitor"]
    return {**ledger, "buckets": {k: dict(v) for k, v in ledger["buckets"].items()}, "recent": list(ledger["recent"]),
            "pending": dict(ledger["pending"]), "totals": dict(ledger["totals"]),
            "monitor": {**mon, "alarms": dict(mon["alarms"])}}


def _same_policy(ledger: dict) -> None:
    """A ledger of another trust policy (another schema: no monitor, other rules) is never read as this one's."""
    if ledger.get("schema") != LEDGER_SCHEMA:
        raise TrustError(f"the evidence ledger has schema {ledger.get('schema')!r}, not {LEDGER_SCHEMA}: evidence "
                         "of another trust policy is not carried over")


def _same_identity(ledger: dict, epoch: str, model: str, recipe: str) -> None:
    _same_policy(ledger)
    if (epoch, model, recipe) != (ledger["epoch"], ledger["model"], ledger["recipe"]):
        raise TrustError("the evidence belongs to another epoch, model or recipe")


def note_issuance(ledger: dict, *, seq: int, issuance_id: str, epoch: str, model: str, recipe: str, origin: int,
                  window_end: int, deadline_s: int, lead_s: int, mode: str, bounded: bool,
                  retention_s: int = 7 * DAY) -> dict:
    """An issuance of this candidate at `origin` (exactly once by its journal sequence number). A prospective one is
    pending until its outcome arrives; past deadline_s (window end + settlement + grace) it is overdue."""
    _same_identity(ledger, epoch, model, recipe)
    if seq <= ledger["last_issuance_seq"]:
        return ledger
    if mode not in MODES:
        raise TrustError(f"unknown evidence mode {mode!r}")
    s = _copy(ledger)
    s["last_issuance_seq"] = seq
    b = _bucket(s, origin)
    if mode == "replay":
        b["r_iss"] += 1
    else:
        b["iss"] += 1
        b["g_iss"] += int(in_cohort(origin, lead_s))
        s["totals"]["issued"] += 1
        s["pending"][str(window_end)] = deadline_s     # one issuance per origin in an epoch: the window end names it
        if s["first_issue_s"] is None:
            s["first_issue_s"] = origin
        if bounded and s["first_prospective_s"] is None:
            s["first_prospective_s"] = origin
    _prune(s, origin, retention_s)
    return s


def rolling_state(recent: Sequence[int], consecutive_misses: int, limits: CoverageLimits) -> Dict[str, bool]:
    """Whether the rolling and the consecutive rule FAIL on this evidence (module docstring)."""
    return {"rolling": len(recent) - sum(recent) >= limits.rolling_misses,
            "consecutive": consecutive_misses >= limits.consecutive}


def note_outcome(ledger: dict, r: EvidenceRecord, *, lead_s: int, nominal_miscoverage: float, limits: CoverageLimits,
                 retention_s: int = 7 * DAY) -> dict:
    """A matured outcome of this candidate (exactly once by its journal sequence number). A prospective gate outcome
    with a bound is one statistical look of the rolling and consecutive coverage rules."""
    c = r.cal
    _same_identity(ledger, c.epoch, c.model, c.recipe)
    if c.seq <= ledger["last_outcome_seq"]:
        return ledger
    s = _copy(ledger)
    s["last_outcome_seq"] = c.seq
    s["pending"].pop(str(c.window_end), None)
    gate = in_cohort(c.decision_origin, lead_s)
    b = _bucket(s, c.window_end)
    scored = c.status == "scored"
    covered = scored and c.upper_rps is not None and c.y_rps <= c.upper_rps
    if r.mode == "replay":                      # counted apart, and only these counts
        b["r_sc" if scored else "r_un"] += 1
        if scored and gate and c.upper_rps is not None:
            b["r_bd"] += 1
            b["r_cv"] += int(covered)
            s["totals"]["replay_gate_bounded"] += 1
        _prune(s, c.window_end, retention_s)
        return s
    if not scored:
        b["un"] += 1
        b["g_un"] += int(gate)
        s["totals"]["unscorable"] += 1
        _prune(s, c.window_end, retention_s)
        return s
    y, base, u = c.y_rps, c.base_rps, c.upper_rps
    b["sc"] += 1
    b["g_sc"] += int(gate)
    b["abs"] += abs(y - base)
    b["bias"] += base - y
    if r.step_abs_errors is not None:
        b["n_step"] += 1
        for f, v in zip(("st0", "st1", "st2"), r.step_abs_errors):
            b[f] += v
    if u is not None:
        b["a_bd"] += 1
        b["a_cv"] += int(covered)
        s["totals"]["covered" if covered else "missed"] += 1
        if gate:
            b["g_bd"] += 1
            b["g_cv"] += int(covered)
            b["pin"] += pinball_rps(y, u, nominal_miscoverage)
            s["totals"]["prospective_gate_bounded"] += 1
            before = rolling_state(s["recent"], s["consecutive_misses"], limits)
            s["recent"] = (s["recent"] + [int(covered)])[-limits.recent_n:]
            s["consecutive_misses"] = 0 if covered else s["consecutive_misses"] + 1
            s["last_prospective_outcome_s"] = c.window_end
            after = rolling_state(s["recent"], s["consecutive_misses"], limits)
            mon = s["monitor"]
            mon["looks"] += 1
            for rule in ("rolling", "consecutive"):
                if after[rule] and not before[rule]:              # the rule starts failing at this look: one alarm
                    mon["alarms"][rule] += 1
                    mon["last_alarm"] = {"rule": rule, "window_end": c.window_end, "look": mon["looks"],
                                         "misses": len(s["recent"]) - sum(s["recent"]), "of": len(s["recent"]),
                                         "consecutive": s["consecutive_misses"]}
    _prune(s, c.window_end, retention_s)
    return s


def _hours(now_s: int, seconds: int) -> Tuple[int, int]:
    """The hour buckets of a trailing window (module docstring): from the first whole hour at or after now - seconds
    to the hour holding now."""
    return -(-(now_s - seconds) // HOUR), now_s // HOUR


def window_sums(ledger: dict, now_s: int, seconds: int) -> Dict[str, float]:
    """Bucket sums over the trailing `seconds` at now_s (see _hours)."""
    lo, hi = _hours(now_s, seconds)
    out = {f: 0 for f in _FIELDS}
    for k, v in ledger["buckets"].items():
        if lo <= int(k) <= hi:
            for f in _FIELDS:
                out[f] += v[f]
    return out


def overdue(ledger: dict, now_s: int, seconds: int) -> int:
    """Prospective issued windows in the trailing window whose truth deadline has passed without an outcome."""
    lo, hi = _hours(now_s, seconds)
    return sum(1 for end, deadline in ledger["pending"].items() if lo <= int(end) // HOUR <= hi and deadline <= now_s)


def expected_opportunities(ledger: dict, now_s: int, seconds: int) -> int:
    """Grid origins in the trailing window since the first prospective issuance: every one should have an issuance."""
    first, dt = ledger["first_issue_s"], ledger["interval_s"]
    if first is None:
        return 0
    lo, _ = _hours(now_s, seconds)
    start = max(lo * HOUR, first)
    start, end = -(-start // dt) * dt, now_s // dt * dt
    return (end - start) // dt + 1 if end >= start else 0


def availability(ledger: dict, now_s: int, seconds: int = DAY) -> Dict[str, Optional[float]]:
    """{"outcomes": scored / (scored + unscorable + overdue), "predictions": issued / expected origins}; None when
    there is nothing to count. Prospective evidence only."""
    w = window_sums(ledger, now_s, seconds)
    late = overdue(ledger, now_s, seconds)
    expected = expected_opportunities(ledger, now_s, seconds)
    return {"outcomes": _ratio(w["sc"], w["sc"] + w["un"] + late),
            "predictions": _ratio(min(w["iss"], expected), expected), "overdue": late}


def _ratio(num, den) -> Optional[float]:
    return num / den if den else None


def completed_days(ledger: dict, now_s: int, deadline_s: int, seconds: int = 7 * DAY) -> List[Tuple[int, int, int]]:
    """[(utc_day, gate_bounded, gate_covered)] for the UTC days that lie wholly inside the trailing window AND are
    complete: the outcome deadline of the day's last window (day end + deadline_s) has passed, so every window that
    ended in the day is terminal and the counts are final. The current day is never among them. A day counts as
    inside only while it starts AFTER the window's oldest hour bucket: that bucket is the one retention drops next
    (_prune), and a day must never be judged on counts that have started to expire."""
    lo, _ = _hours(now_s, seconds)
    first, last = -(-((lo + 1) * HOUR) // DAY), (now_s - deadline_s) // DAY - 1
    days: Dict[int, List[int]] = {}
    for k, v in ledger["buckets"].items():
        d = int(k) * HOUR // DAY
        if first <= d <= last:
            got = days.setdefault(d, [0, 0])
            got[0] += v["g_bd"]
            got[1] += v["g_cv"]
    return [(d, bd, cv) for d, (bd, cv) in sorted(days.items())]


def daily_gate_coverage(ledger: dict, now_s: int, seconds: int = 7 * DAY) -> List[Tuple[int, int, int]]:
    """[(utc_day, gate_bounded, gate_covered)] for the UTC days in the window (prospective)."""
    days: Dict[int, List[int]] = {}
    lo, hi = _hours(now_s, seconds)
    for k, v in ledger["buckets"].items():
        if lo <= int(k) <= hi:
            d = days.setdefault(int(k) * HOUR // DAY, [0, 0])
            d[0] += v["g_bd"]
            d[1] += v["g_cv"]
    return [(d, bd, cv) for d, (bd, cv) in sorted(days.items())]


# --- paired challenger evidence -------------------------------------------------------------------------------------

def new_pairs(epoch: str, challenger: str, challenger_recipe: str, baseline_recipe: str, baseline: str = NAIVE) -> dict:
    return {"epoch": epoch, "challenger": challenger, "baseline": baseline,
            "recipes": {challenger: challenger_recipe, baseline: baseline_recipe}, "pending": {}, "buckets": {},
            "last_seq": 0}


def note_pair(pairs: dict, r: EvidenceRecord, *, lead_s: int, nominal_miscoverage: float,
              retention_s: int = 7 * DAY) -> dict:
    """Pair prospective gate bounds of the challenger and the baseline at the same issuance (exactly once). A record of
    either model under another recipe is refused."""
    c = r.cal
    if c.epoch != pairs["epoch"] or c.model not in pairs["recipes"] or c.seq <= pairs["last_seq"]:
        return pairs
    if c.recipe != pairs["recipes"][c.model]:
        raise TrustError("the paired evidence belongs to another recipe")
    s = {**pairs, "pending": dict(pairs["pending"]), "buckets": {k: dict(v) for k, v in pairs["buckets"].items()},
         "last_seq": c.seq}
    if r.mode != "prospective" or c.status != "scored" or c.upper_rps is None or \
            not in_cohort(c.decision_origin, lead_s):
        return s
    loss = pinball_rps(c.y_rps, c.upper_rps, nominal_miscoverage)
    other = s["pending"].pop(str(c.window_end), None)          # one issuance per origin: the window end names it
    if other is None or other[0] == c.model or other[2] != c.issuance_id:    # pair only the same issuance
        s["pending"][str(c.window_end)] = [c.model, loss, c.issuance_id]
    else:
        chal, base = (loss, other[1]) if c.model == s["challenger"] else (other[1], loss)
        b = s["buckets"].setdefault(str(c.window_end // HOUR), {"n": 0, "chal": 0.0, "base": 0.0,
                                                                 "first": c.window_end, "last": c.window_end})
        b["n"] += 1
        b["chal"] += chal
        b["base"] += base
        b["first"], b["last"] = min(b["first"], c.window_end), max(b["last"], c.window_end)
    floor = (c.window_end - retention_s) // HOUR
    s["buckets"] = {k: v for k, v in s["buckets"].items() if int(k) > floor}
    # Both models' outcomes of one window mature together; a partner missing for a day never comes (bounded state).
    s["pending"] = {k: v for k, v in s["pending"].items() if int(k) > c.window_end - DAY}
    return s


def pair_sums(pairs: dict, now_s: int, seconds: int = 7 * DAY) -> Dict[str, Optional[float]]:
    """Retained pairs in the trailing window: count, loss sums and the SPAN of their window ends (expired pairs never
    contribute, so waiting cannot stand in for paired evidence)."""
    lo, hi = _hours(now_s, seconds)
    out = {"n": 0, "chal": 0.0, "base": 0.0, "first": None, "last": None}
    for k, v in pairs["buckets"].items():
        if lo <= int(k) <= hi:
            out["n"] += v["n"]
            out["chal"] += v["chal"]
            out["base"] += v["base"]
            out["first"] = v["first"] if out["first"] is None else min(out["first"], v["first"])
            out["last"] = v["last"] if out["last"] is None else max(out["last"], v["last"])
    out["span"] = out["last"] - out["first"] if out["n"] else 0
    return out


# --- evaluation -----------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class CandidateInputs:
    model: str
    ledger: dict                          # its recipe is the candidate's identity for trust and hysteresis
    bound_available: bool                 # forecasting.calibration.upper_bound gave a bound now
    calibration_windows: int              # scores in the buffer now
    min_calibration_windows: int
    history_seconds: int
    required_history_seconds: int
    data_fresh: bool
    fit_age_seconds: Optional[int] = None         # None for kernels without a fit (seasonal-naive)

    @property
    def recipe(self) -> str:
        return self.ledger["recipe"]


@dataclass(frozen=True)
class Global:
    now_s: int
    lead_seconds: int
    nominal_miscoverage: float
    identity_ok: bool = True
    store_healthy: bool = True
    replay_suite_passed: bool = False
    startup_budget_valid: bool = False
    policy_sha256: Optional[str] = None       # the policy the costs must have been simulated under
    outcome_deadline_seconds: int = 720       # settlement + outcome grace: when a UTC day's outcomes are final
    families: int = 2                         # enabled families: they share the target's false-alarm budget
    # The configuration (grid, lead, risk, family set) is on the allowlist the trust policy was qualified for
    # (contracts.validated_profile). False, the default: never PredictionReady, whatever the evidence.
    configuration_validated: bool = False

    @property
    def miss_bps(self) -> int:
        return round(self.nominal_miscoverage * BPS)


@dataclass(frozen=True)
class CandidateVerdict:
    model: str
    recipe: str
    gates: Mapping[str, bool]
    reason: str
    phase: str
    trusted: bool
    coverage_gate: Optional[float]
    coverage_all: Optional[float]
    availability: Optional[float]                 # outcomes
    prediction_availability: Optional[float]      # issued / expected opportunities
    overdue: int
    prospective_gate_bounds: int
    replay_gate_bounds: int
    eta_seconds: Optional[int]
    coverage_failing: Tuple[str, ...] = ()        # the coverage rules failing now ("alarm": one seen since the last look)


@dataclass(frozen=True)
class Verdict:
    selected: Optional[str]               # None: reactive-only
    phase: str
    reason: str
    prediction_ready: bool
    candidates: Mapping[str, CandidateVerdict]
    promoted: Mapping[str, bool] = field(default_factory=dict)


def new_hysteresis(recipe: str, epoch: str) -> dict:
    return {"epoch": epoch, "recipe": recipe, "trusted": False, "ever_trusted": False, "demoted_at_s": None, "bounds_at_demotion": 0,
            "gates_ok_since_s": None, "alarms_seen": 0, "demotions": 0, "last_demotion": None}


def new_selection(epoch: str) -> dict:
    return {"epoch": epoch, "selected": None, "recipe": None, "since_s": None, "switches": 0, "entry_since": {}}


def candidate_gates(ci: CandidateInputs, g: Global, t: TrustSettings) -> Tuple[Dict[str, bool], dict]:
    led, now = ci.ledger, g.now_s
    _same_policy(led)
    threshold = (1.0 - g.nominal_miscoverage) - t.coverage_tolerance_bps / 10_000
    w7 = window_sums(led, now, 7 * DAY)
    av = availability(led, now)
    cov_gate = _ratio(w7["g_cv"], w7["g_bd"])
    cov_all = _ratio(w7["a_cv"], w7["a_bd"])
    first = led["first_prospective_s"]
    shadow_ok = first is not None and now - first >= t.min_shadow_hours * HOUR and \
        w7["g_bd"] >= t.min_prospective_windows
    limits = coverage_limits(t, g.lead_seconds, g.miss_bps, g.families)
    recent = led["recent"]
    acute = rolling_state(recent, led["consecutive_misses"], limits)
    days = [[d, bd, cv, limits.day_misses(bd)] for d, bd, cv in completed_days(led, now, g.outcome_deadline_seconds)
            if bd >= t.worst_day_min_outcomes]
    bd7, cv7 = int(w7["g_bd"]), int(w7["g_cv"])
    k7 = limits.seven_day_misses(bd7)
    failing = {"rolling": acute["rolling"], "consecutive": acute["consecutive"],
               "day": any(bd - cv >= k for _, bd, cv, k in days),
               "seven_day": cov_gate is None or (cov_gate < threshold and bd7 - cv7 >= k7)}
    coverage = {"failing": [r for r in COVERAGE_RULES if failing[r]],
                "rolling": [len(recent) - sum(recent), len(recent), limits.rolling_misses],
                "consecutive": [led["consecutive_misses"], limits.consecutive],
                "days": days, "failed_days": [d for d, bd, cv, k in days if bd - cv >= k],
                "seven_day": [cv7, bd7, k7], "looks": led["monitor"]["looks"],
                "alarms": sum(led["monitor"]["alarms"].values())}
    fresh_fit = ci.fit_age_seconds is None or ci.fit_age_seconds <= t.max_fit_age_hours * HOUR
    floor = t.min_availability_bps / 10_000
    gates = {
        "identity": g.identity_ok, "state": g.store_healthy, "data": ci.data_fresh and fresh_fit,
        "history": ci.history_seconds >= ci.required_history_seconds,
        "calibration": ci.bound_available, "prospective": shadow_ok,
        "coverage": not coverage["failing"],
        "availability": all(v is not None and v >= floor for v in (av["outcomes"], av["predictions"])),
    }
    missing = max(ci.min_calibration_windows - ci.calibration_windows,
                  t.min_prospective_windows - int(w7["g_bd"]), 0)
    shadow_left = t.min_shadow_hours * HOUR - (now - first if first is not None else 0)
    # Advisory only: the windows still missing, never a promise. Low coverage with enough windows has no ETA.
    eta = max(missing * g.lead_seconds, shadow_left, 0)
    if not gates["coverage"] and w7["g_bd"] >= t.min_prospective_windows:
        eta = None
    return gates, {"coverage_gate": cov_gate, "coverage_all": cov_all, "availability": av["outcomes"],
                   "prediction_availability": av["predictions"], "overdue": av["overdue"],
                   "prospective": int(w7["g_bd"]), "replay": int(w7["r_bd"]), "eta": eta, "coverage": coverage}


_GATE_REASON = (("identity", "IdentityMismatch"), ("state", "StateUnavailable"), ("data", "DataUnavailable"),
                ("history", "HistoryInsufficient"), ("calibration", "CalibrationInsufficient"),
                ("prospective", "ShadowInsufficient"), ("coverage", "CoverageLow"),
                ("availability", "DataUnavailable"))


def unseen_alarm(hyst: Optional[dict], ledger: dict) -> bool:
    """A rolling or consecutive alarm was counted in the ledger since this hysteresis last looked (it may have come
    and gone between two evaluations; it still demotes, once)."""
    same = hyst is not None and (hyst.get("epoch"), hyst.get("recipe")) == (ledger["epoch"], ledger["recipe"])
    return sum(ledger["monitor"]["alarms"].values()) > (hyst.get("alarms_seen", 0) if same else 0)


def step(hyst: dict, gates_ok: bool, ledger: dict, now_s: int, t: TrustSettings, why: Optional[dict] = None) -> dict:
    """The hysteresis transition for one evaluation (a state of another epoch or recipe starts over). `gates_ok` is
    False as well when an unseen alarm is pending (evaluate folds it into the coverage gate); `why` is recorded with
    a demotion."""
    same = (hyst.get("epoch"), hyst.get("recipe")) == (ledger["epoch"], ledger["recipe"])
    h = dict(hyst) if same else new_hysteresis(ledger["recipe"], ledger["epoch"])
    h["alarms_seen"] = sum(ledger["monitor"]["alarms"].values())
    bounds = ledger["totals"]["prospective_gate_bounded"]
    if h["trusted"]:
        if not gates_ok:                                  # demote at once
            h.update(trusted=False, demoted_at_s=now_s, bounds_at_demotion=bounds, gates_ok_since_s=None,
                     demotions=h.get("demotions", 0) + 1, last_demotion={"at_s": now_s, **(why or {})})
        return h
    if not gates_ok:
        h["gates_ok_since_s"] = None
        return h
    if h["gates_ok_since_s"] is None:
        h["gates_ok_since_s"] = now_s
    if not h["ever_trusted"]:
        h.update(trusted=True, ever_trusted=True)
        return h
    held = now_s - h["gates_ok_since_s"] >= t.repromotion_hours * HOUR and \
        h["demoted_at_s"] is not None and now_s > h["demoted_at_s"]
    if held and bounds - h["bounds_at_demotion"] >= t.repromotion_windows:
        h["trusted"] = True
    return h


def costs_bound(costs: Optional[Mapping], pairs: dict, ps: Mapping, policy_sha256: Optional[str]) -> bool:
    """The costs were simulated for these forecasts under this policy, over (at least) the retained pairs."""
    b = (costs or {}).get("binding")
    if not isinstance(b, Mapping) or policy_sha256 is None or ps["first"] is None:
        return False
    rec = pairs["recipes"]
    want = {"epoch": pairs["epoch"],
            "challenger": {"model": pairs["challenger"], "recipe": rec.get(pairs["challenger"])},
            "baseline": {"model": pairs["baseline"], "recipe": rec.get(pairs["baseline"])},
            "policy_sha256": policy_sha256}
    window = b.get("window")
    return {k: b.get(k) for k in want} == want and isinstance(window, (list, tuple)) and len(window) == 2 and \
        window[0] <= ps["first"] and window[1] >= ps["last"]


def cost_evidence_ok(costs: Mapping, t: TrustSettings) -> bool:
    """Enough matched decisions and enough truth behind the costs (module docstring); anything malformed fails."""
    try:
        paired, decisions = costs["paired_decisions"], costs["decisions"]
        unknown, seconds = float(costs["unknown_truth_seconds"]), float(costs["seconds"])
    except (KeyError, TypeError, ValueError):
        return False
    if not all(isinstance(v, int) and not isinstance(v, bool) for v in (paired, decisions)):
        return False
    floor = t.min_availability_bps / 10_000
    return (math.isfinite(unknown) and math.isfinite(seconds) and seconds > 0 and 0 < decisions
            and t.challenger_min_pairs <= paired <= decisions and paired >= floor * decisions
            and 0 <= unknown <= (1.0 - floor) * seconds)


def _paired_ok(pairs: Optional[dict], now_s: int, t: TrustSettings, costs: Optional[Mapping], loss_ratio_bps: int,
               policy_sha256: Optional[str]) -> bool:
    if pairs is None or costs is None:
        return False
    ps = pair_sums(pairs, now_s)
    if not costs_bound(costs, pairs, ps, policy_sha256) or not cost_evidence_ok(costs, t):
        return False
    if ps["n"] < t.challenger_min_pairs or ps["span"] < t.challenger_min_hours * HOUR:
        return False
    if ps["base"] <= 0:
        return False                                      # a zero baseline loss keeps the naive
    if ps["chal"] / ps["n"] > (loss_ratio_bps / 10_000) * ps["base"] / ps["n"]:
        return False
    for name in ("shortage", "replica_time"):
        if name not in costs:
            return False
        chal, base = costs[name]
        if not (math.isfinite(chal) and math.isfinite(base)) or chal > (t.challenger_cost_ratio_bps / 10_000) * base:
            return False
    return True


def challenger_promoted(pairs, now_s, t, costs, policy_sha256=None) -> bool:
    """The entry test of a challenger over the naive (module docstring)."""
    return _paired_ok(pairs, now_s, t, costs, t.challenger_loss_ratio_bps, policy_sha256)


def challenger_retained(pairs, now_s, t, costs, policy_sha256=None) -> bool:
    """The exit band: a selected challenger is kept while its loss is not worse than the exit ratio."""
    return _paired_ok(pairs, now_s, t, costs, t.challenger_exit_loss_ratio_bps, policy_sha256)


def select(sel: dict, verdicts: Mapping[str, CandidateVerdict], pairs: Mapping[str, dict],
           costs: Mapping[str, Mapping], now_s: int, t: TrustSettings,
           policy_sha256: Optional[str] = None) -> Tuple[dict, dict]:
    """(new selection state, entry flags) with selection hysteresis (module docstring)."""
    s = {**sel, "entry_since": dict(sel["entry_since"])}
    naive_ok = NAIVE in verdicts and verdicts[NAIVE].trusted
    entry = {}
    for m in verdicts:
        if m == NAIVE:
            continue
        ok = naive_ok and challenger_promoted(pairs.get(m), now_s, t, costs.get(m), policy_sha256)
        entry[m] = ok
        key = f"{m}:{verdicts[m].recipe}"
        if ok:
            s["entry_since"].setdefault(key, now_s)
        else:
            s["entry_since"].pop(key, None)
    current = s["selected"]
    if current is not None:
        v = verdicts.get(current)
        safe = v is not None and v.trusted and v.recipe == s["recipe"] and (current == NAIVE or naive_ok)
        kept = current == NAIVE or challenger_retained(pairs.get(current), now_s, t, costs.get(current), policy_sha256)
        if not (safe and kept):
            current = None                                # safety (or the exit band): fall back at once
    target = None
    for m in FALLBACK_ORDER:
        v = verdicts.get(m)
        if v is None or not v.trusted:
            continue
        if m == NAIVE:
            target = m
            break
        since = s["entry_since"].get(f"{m}:{v.recipe}")
        held = since is not None and now_s - since >= t.repromotion_hours * HOUR
        if (m == current and naive_ok) or held:
            target = m
            break
    if target != s["selected"]:
        s.update(selected=target, recipe=verdicts[target].recipe if target else None, since_s=now_s,
                 switches=s["switches"] + 1)
    return s, entry


def evaluate(cands: Sequence[CandidateInputs], hyst: Mapping[str, dict], g: Global, t: TrustSettings, *,
             pairs: Optional[Mapping[str, dict]] = None,
             costs: Optional[Mapping[str, Mapping[str, Tuple[float, float]]]] = None) -> Tuple[Verdict, Dict[str, dict]]:
    """(verdict, new state) for one autoscaler at g.now_s. The state maps each model to its hysteresis and SELECTION
    to the selection state."""
    by_model = {c.model: c for c in cands}
    if set(by_model) - set(FALLBACK_ORDER):
        raise TrustError(f"unknown candidates {set(by_model) - set(FALLBACK_ORDER)}")
    epochs = {c.ledger["epoch"] for c in cands}
    if len(epochs) > 1 or any(c.ledger["model"] != c.model for c in cands):
        raise TrustError("the candidates' evidence belongs to different epochs or models")
    epoch = next(iter(epochs), None)
    for m, p in (pairs or {}).items():
        if m not in by_model or m == NAIVE:
            continue
        identity = (p["epoch"], p["challenger"], p["baseline"], p["recipes"].get(m), p["recipes"].get(NAIVE))
        want = (epoch, m, NAIVE, by_model[m].recipe, by_model[NAIVE].recipe if NAIVE in by_model else None)
        if identity != want:
            raise TrustError(f"the paired evidence of {m} belongs to another epoch, model or recipe")
    new_h, verdicts = {}, {}
    for model, ci in by_model.items():
        gates, info = candidate_gates(ci, g, t)
        prior_h = hyst.get(model, new_hysteresis(ci.recipe, epoch))
        latched = unseen_alarm(prior_h, ci.ledger)
        if latched:                                       # an alarm since the last evaluation fails the coverage gate now
            gates = {**gates, "coverage": False}
        ok = all(gates.values())
        why = {"gates": [k for k, v in gates.items() if not v], "coverage": info["coverage"],
               "latched_alarm": ci.ledger["monitor"]["last_alarm"] if latched else None}
        h = step(prior_h, ok, ci.ledger, g.now_s, t, why)
        new_h[model] = h
        reason = next((r for gname, r in _GATE_REASON if not gates[gname]), "Ready")
        if h["trusted"]:
            phase = "Trusted"
        elif h["ever_trusted"]:
            phase = "Degraded"
        elif not gates["history"]:
            phase = "Collecting"
        elif not gates["calibration"]:
            phase = "Calibrating"
        else:
            phase = "Shadow"
        if reason == "Ready" and not h["trusted"]:
            reason = "ShadowInsufficient"                # gates hold, hysteresis still running
        verdicts[model] = CandidateVerdict(model, ci.recipe, gates, reason, phase, h["trusted"], info["coverage_gate"],
                                           info["coverage_all"], info["availability"],
                                           info["prediction_availability"], info["overdue"], info["prospective"],
                                           info["replay"], None if h["trusted"] else info["eta"],
                                           tuple(info["coverage"]["failing"]) + (("alarm",) if latched else ()))
    prior = hyst.get(SELECTION)
    if prior is None or prior.get("epoch") != epoch:          # a new epoch: no selection carries over
        prior = new_selection(epoch)
    sel, promoted = select(prior, verdicts, pairs or {}, costs or {}, g.now_s, t, g.policy_sha256)
    new_h[SELECTION] = sel
    selected = sel["selected"]
    if selected is None:                                  # report the candidate furthest along, in fallback order on ties
        ordered = [verdicts[m] for m in FALLBACK_ORDER if m in verdicts]
        best = max(ordered, key=lambda v: REASON_ORDER.index(v.reason)) if ordered else None
        reason = best.reason if best else "HistoryInsufficient"
        phase = best.phase if best else "Collecting"
        if best and best.trusted:                         # trusted but not (yet) selected over a trusted naive
            reason, phase = "ShadowInsufficient", "Shadow"
        return Verdict(None, phase, reason, False, verdicts, promoted), new_h
    if not g.configuration_validated:
        ready_reason = "ConfigurationNotValidated"
    elif not g.replay_suite_passed:
        ready_reason = "ReplayGateMissing"
    elif not g.startup_budget_valid:
        ready_reason = "LeadInsufficient"
    else:
        ready_reason = "Ready"
    return Verdict(selected, "Trusted", ready_reason, ready_reason == "Ready", verdicts, promoted), new_h

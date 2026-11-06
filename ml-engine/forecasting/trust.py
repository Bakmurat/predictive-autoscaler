"""Trust for forecast contract v2: evidence ledgers, per-candidate gates with hysteresis, paired challenger promotion,
the fallback order and the ready-to-activate verdict. Pure and replayable: every function maps (state, inputs, now) to
a new state or a verdict; nothing reads a clock, the network or Kubernetes (design C2, section 7).

Evidence ledger (one per epoch and model). Matured records and issuances are folded in exactly once (journal sequence
numbers) into hourly buckets (by window end, or by origin for issuances), kept for the 7-day retention. A bucket counts
issued windows, scored and unscorable outcomes, bounded/covered windows for all windows and for the nonoverlapping
gate cohort, and sums for realised error. PROSPECTIVE and REPLAY evidence are counted apart: replay may warm up
calibration and diagnostics, but every trust gate counts prospective evidence only (hindsight cannot authorize a
write). A recent-results ring (the last acute_recent_windows prospective gate bounds) and a consecutive-miss counter
drive acute demotion.

Gates per candidate (all thresholds are TrustSettings parameters, fingerprinted with the settings):
  identity/storage  identity valid and the journal healthy                                  else IdentityMismatch /
                                                                                                 StateUnavailable
  data              current input fresh, and the fit no older than max_fit_age (statistical)  else DataUnavailable
  history           the family's minimum history (naive: 1 day; E1-product: 7 days)          else HistoryInsufficient
  calibration       an upper bound is available now (>= min_windows scores, rank feasible)   else CalibrationInsufficient
  prospective       >= min_shadow_hours since the first prospective bound and >= min_prospective_windows prospective
                    gate bounds                                                              else ShadowInsufficient
  coverage          7-day prospective gate coverage >= nominal - tolerance; the worst UTC day with >= worst_day_min
                    outcomes too; acute: no acute_consecutive_misses in a row, and the last acute_recent_windows
                    bounds (once that many exist) at or above the threshold                  else CoverageLow
  availability      >= min_availability of the last day's matured windows scorable (None, no matured window, fails)
                                                                                             else DataUnavailable
Hysteresis: a trusted candidate failing any gate is demoted at once (Degraded). Once demoted, it is promoted again only
after its gates have held for repromotion_hours and >= repromotion_windows new prospective gate bounds have matured
since the demotion; a demotion and a promotion can never happen in the same evaluation. A candidate never trusted
before is promoted the first time its gates hold.

Challenger promotion (paired, against the trusted daily naive): >= challenger_min_pairs paired prospective gate bounds
(both models bounded at the same origins) over >= challenger_min_hours, mean pinball loss <= challenger_loss_ratio x
the naive's, and the same-policy replay costs (shortage and replica-time, supplied by the policy simulator) each <=
challenger_cost_ratio x the naive's. A naive loss of zero keeps the naive (no tie logic). Without the costs nothing is
promoted.

Fallback order: lstm (an optional challenger) -> statistical (E1-product) -> seasonal_naive -> reactive-only. A
candidate other than the naive is selected only when it is trusted AND promoted over a trusted naive; the naive is
selected when it is trusted; otherwise nothing (reactive-only). Never an untrusted naive because a richer model failed.
PredictionReady additionally needs the activation evidence this service can check (versioned replay suite, startup
budget): it means "ready to activate"; Recommend still makes no writes, and it says nothing about a workload's SLO.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Dict, List, Mapping, Optional, Sequence, Tuple

from forecasting.calibration import CalibrationRecord, in_cohort, pinball_rps
from forecasting.contracts import TrustSettings
from forecasting.maturation import candidate

LEDGER_SCHEMA = "evidence-ledger/v1"
HOUR = 3600
DAY = 86400
FALLBACK_ORDER = ("lstm", "statistical", "seasonal_naive")
NAIVE = "seasonal_naive"
MODES = ("prospective", "replay")
REASON_ORDER = ("IdentityMismatch", "StateUnavailable", "DataUnavailable", "HistoryInsufficient",
                "CalibrationInsufficient", "ShadowInsufficient", "CoverageLow", "LeadInsufficient",
                "ReplayGateMissing", "Ready")
# The candidate history minimums of the cold-start ladder (design C2, section 4); an optional LSTM brings its own.
REQUIRED_HISTORY_SECONDS = {"seasonal_naive": DAY, "statistical": 7 * DAY}
_FIELDS = ("iss", "g_iss", "sc", "un", "g_sc", "g_un", "a_bd", "a_cv", "g_bd", "g_cv", "r_bd", "r_cv", "pin", "abs",
           "bias", "n_step", "st0", "st1", "st2")


class TrustError(Exception):
    pass


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


def new_ledger(epoch: str, model: str) -> dict:
    return {"schema": LEDGER_SCHEMA, "epoch": epoch, "model": model, "buckets": {}, "recent": [],
            "consecutive_misses": 0, "first_prospective_s": None, "last_prospective_outcome_s": None,
            "last_outcome_seq": 0, "last_issuance_seq": 0, "newest_s": None,
            "totals": {"issued": 0, "covered": 0, "missed": 0, "unscorable": 0, "prospective_gate_bounded": 0,
                       "replay_gate_bounded": 0}}


def _bucket(ledger: dict, t: int) -> dict:
    key = str(t // HOUR)
    b = ledger["buckets"].get(key)
    if b is None:
        b = ledger["buckets"][key] = {f: 0 for f in _FIELDS}
        for f in ("pin", "abs", "bias", "st0", "st1", "st2"):
            b[f] = 0.0
    return b


def _prune(ledger: dict, now_s: int, retention_s: int) -> None:
    newest = max(now_s, ledger["newest_s"] or now_s)
    ledger["newest_s"] = newest
    floor = (newest - retention_s) // HOUR
    ledger["buckets"] = {k: v for k, v in ledger["buckets"].items() if int(k) > floor}


def _copy(ledger: dict) -> dict:
    return {**ledger, "buckets": {k: dict(v) for k, v in ledger["buckets"].items()}, "recent": list(ledger["recent"]),
            "totals": dict(ledger["totals"])}


def note_issuance(ledger: dict, *, seq: int, origin: int, lead_s: int, mode: str, bounded: bool,
                  retention_s: int = 7 * DAY) -> dict:
    """An issuance of this model at `origin` (exactly once by its journal sequence number)."""
    if seq <= ledger["last_issuance_seq"]:
        return ledger
    if mode not in MODES:
        raise TrustError(f"unknown evidence mode {mode!r}")
    s = _copy(ledger)
    s["last_issuance_seq"] = seq
    b = _bucket(s, origin)
    b["iss"] += 1
    if in_cohort(origin, lead_s):
        b["g_iss"] += 1
    s["totals"]["issued"] += 1
    if mode == "prospective" and bounded and s["first_prospective_s"] is None:
        s["first_prospective_s"] = origin
    _prune(s, origin, retention_s)
    return s


def note_outcome(ledger: dict, r: EvidenceRecord, *, lead_s: int, nominal_miscoverage: float, recent_n: int,
                 retention_s: int = 7 * DAY) -> dict:
    """A matured outcome of this model (exactly once by its journal sequence number)."""
    c = r.cal
    if (c.epoch, c.model) != (ledger["epoch"], ledger["model"]):
        raise TrustError("the record belongs to another epoch or model")
    if c.seq <= ledger["last_outcome_seq"]:
        return ledger
    s = _copy(ledger)
    s["last_outcome_seq"] = c.seq
    gate = in_cohort(c.decision_origin, lead_s)
    b = _bucket(s, c.window_end)
    if c.status == "unscorable":
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
        b["st0"] += r.step_abs_errors[0]
        b["st1"] += r.step_abs_errors[1]
        b["st2"] += r.step_abs_errors[2]
    if u is not None:
        covered = y <= u
        if r.mode == "replay":
            if gate:
                b["r_bd"] += 1
                b["r_cv"] += int(covered)
                s["totals"]["replay_gate_bounded"] += 1
        else:
            b["a_bd"] += 1
            b["a_cv"] += int(covered)
            s["totals"]["covered" if covered else "missed"] += 1
            if gate:
                b["g_bd"] += 1
                b["g_cv"] += int(covered)
                b["pin"] += pinball_rps(y, u, nominal_miscoverage)
                s["totals"]["prospective_gate_bounded"] += 1
                s["recent"] = (s["recent"] + [int(covered)])[-recent_n:]
                s["consecutive_misses"] = 0 if covered else s["consecutive_misses"] + 1
                s["last_prospective_outcome_s"] = c.window_end
    _prune(s, c.window_end, retention_s)
    return s


def _hours(now_s: int, seconds: int) -> Tuple[int, int]:
    """The hour buckets of a trailing window: from the first whole hour at or after now - seconds to the hour holding
    now (hour granularity; events after now do not exist yet)."""
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


def _ratio(num, den) -> Optional[float]:
    return num / den if den else None


def daily_gate_coverage(ledger: dict, now_s: int, seconds: int = 7 * DAY) -> List[Tuple[int, int, int]]:
    """[(utc_day, gate_bounded, gate_covered)] for the UTC days in the window."""
    days: Dict[int, List[int]] = {}
    lo, hi = _hours(now_s, seconds)
    for k, v in ledger["buckets"].items():
        if lo <= int(k) <= hi:
            d = days.setdefault(int(k) * HOUR // DAY, [0, 0])
            d[0] += v["g_bd"]
            d[1] += v["g_cv"]
    return [(d, bd, cv) for d, (bd, cv) in sorted(days.items())]


# --- paired challenger evidence -------------------------------------------------------------------------------------

def new_pairs(epoch: str, challenger: str, baseline: str = NAIVE) -> dict:
    return {"epoch": epoch, "challenger": challenger, "baseline": baseline, "pending": {}, "buckets": {},
            "first_pair_s": None, "last_seq": 0}


def note_pair(pairs: dict, r: EvidenceRecord, *, lead_s: int, nominal_miscoverage: float,
              retention_s: int = 7 * DAY) -> dict:
    """Pair prospective gate bounds of the challenger and the baseline at the same issuance (exactly once)."""
    c = r.cal
    if c.epoch != pairs["epoch"] or c.model not in (pairs["challenger"], pairs["baseline"]) or \
            c.seq <= pairs["last_seq"]:
        return pairs
    s = {**pairs, "pending": dict(pairs["pending"]), "buckets": {k: dict(v) for k, v in pairs["buckets"].items()},
         "last_seq": c.seq}
    if r.mode != "prospective" or c.status != "scored" or c.upper_rps is None or \
            not in_cohort(c.decision_origin, lead_s):
        return s
    loss = pinball_rps(c.y_rps, c.upper_rps, nominal_miscoverage)
    other = s["pending"].pop(c.issuance_id, None)
    if other is None or other[0] == c.model:
        s["pending"][c.issuance_id] = [c.model, loss, c.window_end]
    else:
        chal, base = (loss, other[1]) if c.model == s["challenger"] else (other[1], loss)
        b = s["buckets"].setdefault(str(c.window_end // HOUR), {"n": 0, "chal": 0.0, "base": 0.0})
        b["n"] += 1
        b["chal"] += chal
        b["base"] += base
        if s["first_pair_s"] is None:
            s["first_pair_s"] = c.window_end
    floor = (c.window_end - retention_s) // HOUR
    s["buckets"] = {k: v for k, v in s["buckets"].items() if int(k) > floor}
    s["pending"] = {k: v for k, v in s["pending"].items() if v[2] > c.window_end - retention_s}
    return s


def pair_sums(pairs: dict, now_s: int, seconds: int = 7 * DAY) -> Dict[str, float]:
    lo, hi = _hours(now_s, seconds)
    out = {"n": 0, "chal": 0.0, "base": 0.0}
    for k, v in pairs["buckets"].items():
        if lo <= int(k) <= hi:
            for f in out:
                out[f] += v[f]
    return out


# --- evaluation -----------------------------------------------------------------------------------------------------

@dataclass(frozen=True)
class CandidateInputs:
    model: str
    ledger: dict
    bound_available: bool                 # forecasting.calibration.upper_bound gave a bound now
    calibration_windows: int              # scores in the buffer now
    min_calibration_windows: int
    history_seconds: int
    required_history_seconds: int
    data_fresh: bool
    fit_age_seconds: Optional[int] = None         # None for kernels without a fit (seasonal-naive)


@dataclass(frozen=True)
class Global:
    now_s: int
    lead_seconds: int
    nominal_miscoverage: float
    identity_ok: bool = True
    store_healthy: bool = True
    replay_suite_passed: bool = False
    startup_budget_valid: bool = False


@dataclass(frozen=True)
class CandidateVerdict:
    model: str
    gates: Mapping[str, bool]
    reason: str
    phase: str
    trusted: bool
    coverage_gate: Optional[float]
    coverage_all: Optional[float]
    availability: Optional[float]
    prospective_gate_bounds: int
    replay_gate_bounds: int
    eta_seconds: Optional[int]


@dataclass(frozen=True)
class Verdict:
    selected: Optional[str]               # None: reactive-only
    phase: str
    reason: str
    prediction_ready: bool
    candidates: Mapping[str, CandidateVerdict]
    promoted: Mapping[str, bool] = field(default_factory=dict)


def new_hysteresis() -> dict:
    return {"trusted": False, "ever_trusted": False, "demoted_at_s": None, "bounds_at_demotion": 0,
            "gates_ok_since_s": None}


def candidate_gates(ci: CandidateInputs, g: Global, t: TrustSettings) -> Tuple[Dict[str, bool], dict]:
    led, now = ci.ledger, g.now_s
    threshold = (1.0 - g.nominal_miscoverage) - t.coverage_tolerance_bps / 10_000
    w7 = window_sums(led, now, 7 * DAY)
    d1 = window_sums(led, now, DAY)
    cov_gate = _ratio(w7["g_cv"], w7["g_bd"])
    cov_all = _ratio(w7["a_cv"], w7["a_bd"])
    avail = _ratio(d1["sc"], d1["sc"] + d1["un"])
    first = led["first_prospective_s"]
    shadow_ok = first is not None and now - first >= t.min_shadow_hours * HOUR and \
        w7["g_bd"] >= t.min_prospective_windows
    worst_ok = all(cv / bd >= threshold for _, bd, cv in daily_gate_coverage(led, now)
                   if bd >= t.worst_day_min_outcomes)
    recent = led["recent"]
    acute_ok = led["consecutive_misses"] < t.acute_consecutive_misses and \
        (len(recent) < t.acute_recent_windows or sum(recent) / len(recent) >= threshold)
    fresh_fit = ci.fit_age_seconds is None or ci.fit_age_seconds <= t.max_fit_age_hours * HOUR
    gates = {
        "identity": g.identity_ok, "state": g.store_healthy, "data": ci.data_fresh and fresh_fit,
        "history": ci.history_seconds >= ci.required_history_seconds,
        "calibration": ci.bound_available, "prospective": shadow_ok,
        "coverage": cov_gate is not None and cov_gate >= threshold and worst_ok and acute_ok,
        "availability": avail is not None and avail >= t.min_availability_bps / 10_000,
    }
    missing = max(ci.min_calibration_windows - ci.calibration_windows,
                  t.min_prospective_windows - int(w7["g_bd"]), 0)
    shadow_left = t.min_shadow_hours * HOUR - (now - first if first is not None else 0)
    # Advisory only: the windows still missing, never a promise. Low coverage with enough windows has no ETA.
    eta = max(missing * g.lead_seconds, shadow_left, 0)
    if not gates["coverage"] and w7["g_bd"] >= t.min_prospective_windows:
        eta = None
    return gates, {"coverage_gate": cov_gate, "coverage_all": cov_all, "availability": avail,
                   "prospective": int(w7["g_bd"]), "replay": int(w7["r_bd"]), "eta": eta}


_GATE_REASON = (("identity", "IdentityMismatch"), ("state", "StateUnavailable"), ("data", "DataUnavailable"),
                ("history", "HistoryInsufficient"), ("calibration", "CalibrationInsufficient"),
                ("prospective", "ShadowInsufficient"), ("coverage", "CoverageLow"),
                ("availability", "DataUnavailable"))


def step(hyst: dict, gates_ok: bool, ledger: dict, now_s: int, t: TrustSettings) -> dict:
    """The hysteresis transition for one evaluation."""
    h = dict(hyst)
    bounds = ledger["totals"]["prospective_gate_bounded"]
    if h["trusted"]:
        if not gates_ok:                                  # demote at once
            h.update(trusted=False, demoted_at_s=now_s, bounds_at_demotion=bounds, gates_ok_since_s=None)
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


def challenger_promoted(pairs: Optional[dict], now_s: int, t: TrustSettings,
                        costs: Optional[Mapping[str, Tuple[float, float]]]) -> bool:
    """Paired promotion of a challenger over the naive (see the module docstring)."""
    if pairs is None or costs is None or pairs["first_pair_s"] is None:
        return False
    ps = pair_sums(pairs, now_s)
    if ps["n"] < t.challenger_min_pairs or now_s - pairs["first_pair_s"] < t.challenger_min_hours * HOUR:
        return False
    if ps["base"] <= 0:
        return False                                      # a zero baseline loss keeps the naive
    if ps["chal"] / ps["n"] > (t.challenger_loss_ratio_bps / 10_000) * ps["base"] / ps["n"]:
        return False
    for name in ("shortage", "replica_time"):
        if name not in costs:
            return False
        chal, base = costs[name]
        if not (math.isfinite(chal) and math.isfinite(base)) or chal > (t.challenger_cost_ratio_bps / 10_000) * base:
            return False
    return True


def evaluate(cands: Sequence[CandidateInputs], hyst: Mapping[str, dict], g: Global, t: TrustSettings, *,
             pairs: Optional[Mapping[str, dict]] = None,
             costs: Optional[Mapping[str, Mapping[str, Tuple[float, float]]]] = None) -> Tuple[Verdict, Dict[str, dict]]:
    """(verdict, new hysteresis states) for one autoscaler at g.now_s."""
    by_model = {c.model: c for c in cands}
    if set(by_model) - set(FALLBACK_ORDER):
        raise TrustError(f"unknown candidates {set(by_model) - set(FALLBACK_ORDER)}")
    new_h, verdicts = {}, {}
    for model, ci in by_model.items():
        gates, info = candidate_gates(ci, g, t)
        ok = all(gates.values())
        h = step(hyst.get(model, new_hysteresis()), ok, ci.ledger, g.now_s, t)
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
        verdicts[model] = CandidateVerdict(model, gates, reason, phase, h["trusted"], info["coverage_gate"],
                                           info["coverage_all"], info["availability"], info["prospective"],
                                           info["replay"], None if h["trusted"] else info["eta"])
    naive_ok = NAIVE in verdicts and verdicts[NAIVE].trusted
    promoted = {m: naive_ok and challenger_promoted((pairs or {}).get(m), g.now_s, t, (costs or {}).get(m))
                for m in verdicts if m != NAIVE}
    selected = None
    for m in FALLBACK_ORDER:
        if m in verdicts and verdicts[m].trusted and (m == NAIVE or promoted.get(m)):
            selected = m
            break
    if selected is None:                                  # report the candidate furthest along, in fallback order on ties
        ordered = [verdicts[m] for m in FALLBACK_ORDER if m in verdicts]
        best = max(ordered, key=lambda v: REASON_ORDER.index(v.reason)) if ordered else None
        reason = best.reason if best else "HistoryInsufficient"
        phase = best.phase if best else "Collecting"
        if best and best.trusted:                         # trusted but not promoted over a trusted naive
            reason, phase = "ShadowInsufficient", "Shadow"
        return Verdict(None, phase, reason, False, verdicts, promoted), new_h
    if not g.replay_suite_passed:
        ready_reason = "ReplayGateMissing"
    elif not g.startup_budget_valid:
        ready_reason = "LeadInsufficient"
    else:
        ready_reason = "Ready"
    return Verdict(selected, "Trusted", ready_reason, ready_reason == "Ready", verdicts, promoted), new_h

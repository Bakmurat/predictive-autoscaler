"""C2.5: trust: zero denominators, missingness, replay versus prospective evidence, every fallback rung, hysteresis,
history minimums, the paired challenger gate and the ready-to-activate verdict."""

import dataclasses
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from forecasting import contracts as c  # noqa: E402
from forecasting import trust as T  # noqa: E402
from forecasting.calibration import CalibrationRecord  # noqa: E402
from forecasting.maturation import issued_outcome  # noqa: E402
from forecasting.store import Issuance, IssuanceKey, Outcome  # noqa: E402

EPOCH = "e" * 64
L = 600                                    # lead = cadence: every origin is in the gate cohort
H, DAY = 3600, 86400
START = 1_793_923_200 - 1_793_923_200 % DAY
TS = c.TrustSettings()
NOMINAL = 0.10


class Feed:
    """Builds evidence for one model: one issuance and one outcome per origin, with sequence numbers."""

    def __init__(self, model="seasonal_naive"):
        self.model, self.ledger, self.seq = model, T.new_ledger(EPOCH, model), 0

    def window(self, origin, *, covered=True, mode="prospective", status="scored", bounded=True, y=5.0):
        self.seq += 1
        self.ledger = T.note_issuance(self.ledger, seq=self.seq, origin=origin, lead_s=L, mode=mode, bounded=bounded)
        upper = (y + 1.0 if covered else y - 1.0) if bounded else None
        cal = CalibrationRecord(self.seq, EPOCH, self.model, "r", f"iss-{self.seq}", origin, origin + L, 4.0, upper,
                                status, y if status == "scored" else None)
        r = T.EvidenceRecord(cal, mode, (origin + L) * 1000, (1.0, 2.0, 3.0) if status == "scored" else None)
        self.ledger = T.note_outcome(self.ledger, r, lead_s=L, nominal_miscoverage=NOMINAL, recent_n=32)
        return r

    def run(self, hours, start=START, every=L, **kw):
        for t in range(start, start + hours * H, every):
            self.window(t, **{k: (v(t) if callable(v) else v) for k, v in kw.items()})
        return self


def inputs(feed, *, history=30 * DAY, bound=True, fresh=True, fit_age=None, cal_n=200):
    return T.CandidateInputs(feed.model, feed.ledger, bound, cal_n, 128, history,
                             T.REQUIRED_HISTORY_SECONDS.get(feed.model, 7 * DAY), fresh, fit_age)


def glob(now, **kw):
    return T.Global(now_s=now, lead_seconds=L, nominal_miscoverage=NOMINAL, **{"replay_suite_passed": True,
                                                                               "startup_budget_valid": True, **kw})


def mostly(t, every=10):
    """Covered except one window in `every` (90% coverage)."""
    return (t // L) % every != 0


def good(model="seasonal_naive", hours=72):
    return Feed(model).run(hours, covered=lambda t: mostly(t, 20))        # 95%: above 0.90 - 0.05


NOW = START + 72 * H


def verdict(*feeds, now=NOW, hyst=None, pairs=None, costs=None, **g):
    return T.evaluate([inputs(f) if isinstance(f, Feed) else f for f in feeds], hyst or {}, glob(now, **g), TS,
                      pairs=pairs, costs=costs)


# --- zero denominators and missingness ------------------------------------------------------------------------------

def test_no_evidence_means_no_ratios_and_no_trust():
    f = Feed()
    gates, info = T.candidate_gates(inputs(f, bound=False, history=0), glob(NOW), TS)
    assert info["coverage_gate"] is None and info["coverage_all"] is None and info["availability"] is None
    assert not gates["coverage"] and not gates["availability"] and not gates["prospective"]
    v, _ = verdict(T.CandidateInputs("seasonal_naive", f.ledger, False, 0, 128, 0, DAY, True))
    assert v.selected is None and v.phase == "Collecting" and v.reason == "HistoryInsufficient"


@pytest.mark.parametrize("unscorable_every,ok", [(25, True), (10, False)])
def test_missing_truth_over_five_percent_of_the_last_day_blocks_trust(unscorable_every, ok):
    f = Feed().run(48, covered=lambda t: mostly(t, 20))
    last = Feed()
    last.ledger, last.seq = f.ledger, f.seq
    for t in range(START + 48 * H, NOW, L):           # the last day: every n-th window unscorable
        last.window(t, status="unscorable" if (t // L) % unscorable_every == 0 else "scored",
                    covered=mostly(t, 20))
    gates, info = T.candidate_gates(inputs(last), glob(NOW), TS)
    assert gates["availability"] is ok and (info["availability"] >= 0.95) is ok
    if not ok:
        assert verdict(last)[0].candidates["seasonal_naive"].reason == "DataUnavailable"


@pytest.mark.parametrize("unscorable,ok", [(0, True), (1, True), (2, False)])
def test_the_availability_boundary_is_ninety_five_percent(unscorable, ok):
    f = Feed()
    for i in range(20):                                              # 20 windows in the last day: 1 missing = 95%
        f.window(NOW - DAY + (i + 1) * L, status="unscorable" if i < unscorable else "scored")
    gates, info = T.candidate_gates(inputs(f), glob(NOW), TS)
    assert info["availability"] == (20 - unscorable) / 20 and gates["availability"] is ok


# --- replay versus prospective evidence -----------------------------------------------------------------------------

def test_replay_evidence_never_establishes_trust():
    f = Feed().run(72, covered=lambda t: mostly(t, 20), mode="replay")
    v, _ = verdict(f)
    cv = v.candidates["seasonal_naive"]
    assert not cv.trusted and cv.reason == "ShadowInsufficient" and cv.replay_gate_bounds == 432
    assert cv.prospective_gate_bounds == 0 and cv.coverage_gate is None
    f.run(47, start=NOW, covered=lambda t: mostly(t, 20))           # 47 h prospective: still short of 48 h
    assert not verdict(f, now=NOW + 47 * H)[0].candidates["seasonal_naive"].trusted
    f.run(2, start=NOW + 47 * H, covered=lambda t: mostly(t, 20))
    assert verdict(f, now=NOW + 49 * H)[0].candidates["seasonal_naive"].trusted


def test_prospective_needs_48_hours_and_64_bounds():
    short = Feed().run(47, covered=lambda t: mostly(t, 20))
    assert verdict(short, now=START + 47 * H)[0].candidates["seasonal_naive"].reason == "ShadowInsufficient"
    sparse = Feed().run(72, every=2 * H, covered=True)              # 36 bounds over 72 h
    assert not T.candidate_gates(inputs(sparse), glob(NOW), TS)[0]["prospective"]
    assert verdict(good())[0].selected == "seasonal_naive"


# --- history, data, calibration and coverage gates ----------------------------------------------------------------

@pytest.mark.parametrize("change,reason,phase", [
    (dict(history=DAY - 1), "HistoryInsufficient", "Collecting"),
    (dict(bound=False, cal_n=100), "CalibrationInsufficient", "Calibrating"),
    (dict(fresh=False), "DataUnavailable", "Shadow"),
])
def test_each_gate_has_its_reason(change, reason, phase):
    v, _ = verdict(inputs(good(), **change))
    cv = v.candidates["seasonal_naive"]
    assert (cv.trusted, cv.reason, cv.phase, v.selected) == (False, reason, phase, None)


def test_identity_and_storage_come_first():
    v, _ = verdict(good(), identity_ok=False, store_healthy=False)
    assert v.candidates["seasonal_naive"].reason == "IdentityMismatch"
    v, _ = verdict(good(), store_healthy=False)
    assert v.candidates["seasonal_naive"].reason == "StateUnavailable" and v.selected is None


def test_the_statistical_fit_must_be_fresh():
    stat = good("statistical")
    assert T.candidate_gates(inputs(stat, fit_age=12 * H), glob(NOW), TS)[0]["data"]
    assert not T.candidate_gates(inputs(stat, fit_age=12 * H + 1), glob(NOW), TS)[0]["data"]
    assert not T.candidate_gates(inputs(stat, history=7 * DAY - 1), glob(NOW), TS)[0]["history"]


def test_coverage_below_nominal_minus_tolerance_fails():
    low = Feed().run(72, covered=lambda t: mostly(t, 8))             # 87.5%: at or above 0.85, passes
    assert T.candidate_gates(inputs(low), glob(NOW), TS)[0]["coverage"]
    lower = Feed().run(72, covered=lambda t: mostly(t, 5))           # 80% < 85%
    assert not T.candidate_gates(inputs(lower), glob(NOW), TS)[0]["coverage"]
    assert verdict(lower)[0].candidates["seasonal_naive"].reason == "CoverageLow"


def test_the_worst_day_must_also_meet_the_threshold():
    f = Feed().run(24, covered=lambda t: mostly(t, 4))               # day 1: 75%
    f.run(24 * 6, start=START + DAY, covered=True)                   # six perfect days: 7-day coverage ~96%
    gates, info = T.candidate_gates(inputs(f), glob(START + 7 * DAY), TS)
    assert info["coverage_gate"] > 0.95 and not gates["coverage"]


def test_acute_misses_demote_a_trusted_candidate_at_once():
    f = good()
    v, h = verdict(f)
    assert v.selected == "seasonal_naive"
    for i in range(5):                                               # five misses in a row
        f.window(NOW + i * L, covered=False)
    v2, h2 = verdict(f, now=NOW + 5 * L, hyst=h)
    assert v2.selected is None and v2.candidates["seasonal_naive"].phase == "Degraded"
    assert h2["seasonal_naive"]["demoted_at_s"] == NOW + 5 * L


def test_the_recent_32_bounds_must_hold_the_threshold():
    f = good()
    for i in range(32):                                              # 32 recent bounds at 75%, no 5-miss streak
        f.window(NOW + i * L, covered=i % 4 != 0)
    gates, _ = T.candidate_gates(inputs(f), glob(NOW + 32 * L), TS)
    assert not gates["coverage"] and f.ledger["consecutive_misses"] < 5


# --- hysteresis -----------------------------------------------------------------------------------------------------

def test_hysteresis_needs_24_hours_and_32_new_bounds():
    led = T.new_ledger(EPOCH, "seasonal_naive")
    h = T.step(T.new_hysteresis(), True, led, 0, TS)
    assert h["trusted"] and h["ever_trusted"]                        # the first promotion is immediate
    h = T.step(h, False, led, 100, TS)
    assert not h["trusted"] and h["demoted_at_s"] == 100
    h = T.step(h, True, led, 100, TS)                               # same tick: no oscillation
    assert not h["trusted"]
    h = T.step(h, True, led, 100 + 24 * H, TS)                       # 24 h held but no new bounds
    assert not h["trusted"]
    led["totals"]["prospective_gate_bounded"] = 31
    assert not T.step(h, True, led, 101 + 24 * H, TS)["trusted"]
    led["totals"]["prospective_gate_bounded"] = 32
    assert T.step(h, True, led, 101 + 24 * H, TS)["trusted"]
    broken = T.step(h, False, led, 102 + 24 * H, TS)                 # a failure restarts the 24 h hold
    assert broken["gates_ok_since_s"] is None
    again = T.step(broken, True, led, 103 + 24 * H, TS)
    assert not T.step(again, True, led, 103 + 48 * H - 1, TS)["trusted"]


# --- fallback rungs and the paired challenger gate ---------------------------------------------------------------

def pairs_for(challenger, chal_loss, base_loss, n=80, hours=60):
    p = T.new_pairs(EPOCH, challenger)
    seq, every = 0, max(1, hours * H // n)
    for i in range(n):
        origin = START + i * every // L * L
        for model, loss in ((challenger, chal_loss), ("seasonal_naive", base_loss)):
            seq += 1
            y = 5.0
            upper = y + loss / 0.1 if loss else y                    # pinball (1 - tau) x (U - Y) = 0.1 x margin
            cal = CalibrationRecord(seq, EPOCH, model, "r", f"iss-{i}", origin, origin + L, 4.0, upper, "scored", y)
            p = T.note_pair(p, T.EvidenceRecord(cal, "prospective", 0, None), lead_s=L, nominal_miscoverage=NOMINAL)
    return p


COSTS = {"shortage": (1.0, 1.0), "replica_time": (100.0, 100.0)}


@pytest.mark.parametrize("case,expected", [
    ("challenger_better", "statistical"),
    ("no_costs", "seasonal_naive"),
    ("costs_worse", "seasonal_naive"),
    ("loss_not_better", "seasonal_naive"),
    ("zero_baseline", "seasonal_naive"),
    ("statistical_untrusted", "seasonal_naive"),
    ("naive_untrusted", None),
    ("lstm_better", "lstm"),
    ("lstm_untrusted", "statistical"),
    ("nothing", None),
])
def test_every_fallback_rung(case, expected):
    naive, stat, lstm = good(), good("statistical"), good("lstm")
    pairs = {"statistical": pairs_for("statistical", 0.5, 1.0), "lstm": pairs_for("lstm", 0.4, 1.0)}
    costs = {"statistical": COSTS, "lstm": COSTS}
    cands = [inputs(naive), inputs(stat)]
    if case == "no_costs":
        costs = {}
    elif case == "costs_worse":
        costs = {"statistical": {"shortage": (1.03, 1.0), "replica_time": (100.0, 100.0)}}
    elif case == "loss_not_better":
        pairs["statistical"] = pairs_for("statistical", 0.96, 1.0)
    elif case == "zero_baseline":
        pairs["statistical"] = pairs_for("statistical", 0.0, 0.0)
    elif case == "statistical_untrusted":
        cands = [inputs(naive), inputs(stat, bound=False)]
    elif case == "naive_untrusted":
        cands = [inputs(naive, fresh=False), inputs(stat)]          # never the richer model without a trusted naive
    elif case in ("lstm_better", "lstm_untrusted"):
        cands = [inputs(naive), inputs(stat), inputs(lstm, bound=case == "lstm_better")]
    elif case == "nothing":
        cands = [inputs(naive, fresh=False), inputs(stat, fresh=False)]
    v, _ = T.evaluate(cands, {}, glob(NOW), TS, pairs=pairs, costs=costs)
    assert v.selected == expected
    assert v.prediction_ready is (expected is not None)


def test_the_paired_gate_needs_64_pairs_over_48_hours():
    assert T.pair_sums(pairs_for("statistical", 0.5, 1.0, n=63), NOW)["n"] == 63
    assert not T.challenger_promoted(pairs_for("statistical", 0.5, 1.0, n=63), NOW, TS, COSTS)
    assert not T.challenger_promoted(pairs_for("statistical", 0.5, 1.0, n=80, hours=40), START + 41 * H, TS, COSTS)
    assert T.challenger_promoted(pairs_for("statistical", 0.5, 1.0), NOW, TS, COSTS)


def test_pairs_need_both_models_at_the_same_origin():
    p = T.new_pairs(EPOCH, "statistical")
    one = CalibrationRecord(1, EPOCH, "statistical", "r", "iss-1", START, START + L, 4.0, 6.0, "scored", 5.0)
    other = CalibrationRecord(2, EPOCH, "seasonal_naive", "r", "iss-2", START, START + L, 4.0, 6.0, "scored", 5.0)
    replay = CalibrationRecord(3, EPOCH, "seasonal_naive", "r", "iss-1", START, START + L, 4.0, 6.0, "scored", 5.0)
    for r, mode in ((one, "prospective"), (other, "prospective"), (replay, "replay")):
        p = T.note_pair(p, T.EvidenceRecord(r, mode, 0, None), lead_s=L, nominal_miscoverage=NOMINAL)
    assert T.pair_sums(p, START + H)["n"] == 0                       # different issuances; replay never pairs
    match = CalibrationRecord(4, EPOCH, "seasonal_naive", "r", "iss-1", START, START + L, 4.0, 6.0, "scored", 5.0)
    p = T.note_pair(p, T.EvidenceRecord(match, "prospective", 0, None), lead_s=L, nominal_miscoverage=NOMINAL)
    assert T.pair_sums(p, START + H)["n"] == 1


@pytest.mark.parametrize("replay,startup,reason", [(False, True, "ReplayGateMissing"), (True, False, "LeadInsufficient"),
                                                   (True, True, "Ready")])
def test_prediction_ready_needs_the_activation_evidence(replay, startup, reason):
    v, _ = verdict(good(), replay_suite_passed=replay, startup_budget_valid=startup)
    assert (v.selected, v.reason, v.prediction_ready) == ("seasonal_naive", reason, reason == "Ready")


def test_the_eta_is_advisory_and_counts_missing_windows():
    young = Feed().run(10, covered=True)
    cv = verdict(inputs(young, bound=False, cal_n=100), now=START + 10 * H)[0].candidates["seasonal_naive"]
    assert cv.eta_seconds == max(28 * L, 38 * H)                      # 28 calibration windows / 38 h of shadow left
    low = Feed().run(72, covered=lambda t: mostly(t, 5))
    assert verdict(low)[0].candidates["seasonal_naive"].eta_seconds is None


# --- exactly once, journal binding ------------------------------------------------------------------------------

def test_ledger_updates_are_exactly_once():
    f = Feed()
    r = f.window(START)
    before = f.ledger
    assert T.note_outcome(before, r, lead_s=L, nominal_miscoverage=NOMINAL, recent_n=32) is before
    assert T.note_issuance(before, seq=1, origin=START, lead_s=L, mode="prospective", bounded=True) is before
    with pytest.raises(T.TrustError):
        T.note_outcome(T.new_ledger(EPOCH, "statistical"), dataclasses.replace(r, cal=dataclasses.replace(
            r.cal, seq=9)), lead_s=L, nominal_miscoverage=NOMINAL, recent_n=32)


def test_evidence_records_come_from_the_journal_with_their_mode_and_step_errors():
    origin = START
    targets = [origin + L, origin + 2 * L, origin + 3 * L]
    bundle = {"decision_origin": origin, "target_timestamps": targets, "sample_interval_seconds": L,
              "model_family": "seasonal_naive", "model_recipe": "r", "point_rps": [1.0, 2.0, 3.0],
              "base_window_max_rps": 3.0, "upper_window_max_rps": 5.0, "evidence_mode": "prospective"}
    data = c.canonical_json(bundle)
    iss = Issuance(1, "iss-1", IssuanceKey(EPOCH, origin, origin, targets[-1], "5" * 64), data, c.sha256_hex(data),
                   ("seasonal_naive",), 0)
    status, payload = issued_outcome(bundle, "seasonal_naive",
                                     received={t: (v, 0) for t, v in zip(targets, (2.0, 2.5, 1.0))},
                                     now_ms=(targets[-1] + 10) * 1000, settlement_s=0, grace_s=0)
    pb = c.canonical_json(payload)
    out = Outcome(3, "iss-1", EPOCH, targets[-1], "seasonal_naive", status, pb, c.sha256_hex(pb), 0)
    r = T.EvidenceRecord.from_journal(iss, out)
    assert r.mode == "prospective" and r.step_abs_errors == (1.0, 0.5, 2.0) and r.cal.y_rps == 2.5
    del bundle["evidence_mode"]
    data = c.canonical_json(bundle)
    with pytest.raises(T.TrustError):
        T.EvidenceRecord.from_journal(dataclasses.replace(iss, bundle=data, bundle_sha256=c.sha256_hex(data)), out)

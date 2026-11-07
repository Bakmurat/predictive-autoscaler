"""C2.5: trust: zero denominators, missingness and overdue outcomes, replay versus prospective evidence, recipe
isolation, every fallback rung, candidate and selection hysteresis, history minimums, the paired challenger gate, the
hour-granular windows and the ready-to-activate verdict."""

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
GRACE = 720                                # settlement 120 + grace 600


class Feed:
    """Builds evidence for one candidate: issuances and outcomes with journal sequence numbers."""

    def __init__(self, model="seasonal_naive", recipe="r", epoch=EPOCH):
        self.model, self.recipe, self.seq, self.epoch = model, recipe, 0, epoch
        self.ledger = T.new_ledger(epoch, model, recipe, L)

    def issue(self, origin, *, mode="prospective", bounded=True, recipe=None):
        self.seq += 1
        self.ledger = T.note_issuance(self.ledger, seq=self.seq, issuance_id=f"iss-{self.seq}", epoch=self.epoch,
                                      model=self.model, recipe=recipe or self.recipe, origin=origin,
                                      window_end=origin + L, deadline_s=origin + L + GRACE, lead_s=L, mode=mode,
                                      bounded=bounded)
        return self.seq

    def outcome(self, seq, origin, *, covered=True, mode="prospective", status="scored", bounded=True, y=5.0,
                recipe=None):
        upper = (y + 1.0 if covered else y - 1.0) if bounded else None
        cal = CalibrationRecord(seq, self.epoch, self.model, recipe or self.recipe, f"iss-{seq}", origin, origin + L, 4.0,
                                upper, status, y if status == "scored" else None)
        r = T.EvidenceRecord(cal, mode, (origin + L) * 1000, (1.0, 2.0, 3.0) if status == "scored" else None)
        self.ledger = T.note_outcome(self.ledger, r, lead_s=L, nominal_miscoverage=NOMINAL, recent_n=32)
        return r

    def window(self, origin, *, mode="prospective", bounded=True, **kw):
        return self.outcome(self.issue(origin, mode=mode, bounded=bounded), origin, mode=mode, bounded=bounded, **kw)

    def run(self, hours, start=START, every=L, **kw):
        for t in range(start, start + hours * H, every):
            self.window(t, **{k: (v(t) if callable(v) else v) for k, v in kw.items()})
        return self


def inputs(feed, *, history=30 * DAY, bound=True, fresh=True, fit_age=None, cal_n=200):
    return T.CandidateInputs(feed.model, feed.ledger, bound, cal_n, 128, history,
                             T.REQUIRED_HISTORY_SECONDS.get(feed.model, 7 * DAY), fresh, fit_age)


POLICY = "p" * 64


def glob(now, **kw):
    return T.Global(now_s=now, lead_seconds=L, nominal_miscoverage=NOMINAL, **{"replay_suite_passed": True,
                                                                               "startup_budget_valid": True,
                                                                               "policy_sha256": POLICY, **kw})


def mostly(t, every=10):
    """Covered except one window in `every`."""
    return (t // L) % every != 0


def good(model="seasonal_naive", hours=72, recipe="r", epoch=EPOCH):
    return Feed(model, recipe, epoch).run(hours, covered=lambda t: mostly(t, 20))  # 95%: above 0.90 - 0.05


NOW = START + 72 * H


def verdict(*feeds, now=NOW, hyst=None, pairs=None, costs=None, **g):
    return T.evaluate([inputs(f) if isinstance(f, Feed) else f for f in feeds], hyst or {}, glob(now, **g), TS,
                      pairs=pairs, costs=costs)


# --- zero denominators, missingness, overdue and missing issuances -------------------------------------------------

def test_no_evidence_means_no_ratios_and_no_trust():
    f = Feed()
    gates, info = T.candidate_gates(inputs(f, bound=False, history=0), glob(NOW), TS)
    assert info["coverage_gate"] is None and info["coverage_all"] is None and info["availability"] is None
    assert info["prediction_availability"] is None
    assert not gates["coverage"] and not gates["availability"] and not gates["prospective"]
    v, _ = verdict(T.CandidateInputs("seasonal_naive", f.ledger, False, 0, 128, 0, DAY, True))
    assert v.selected is None and v.phase == "Collecting" and v.reason == "HistoryInsufficient"


@pytest.mark.parametrize("unscorable_every,ok", [(25, True), (10, False)])
def test_missing_truth_over_five_percent_of_the_last_day_blocks_trust(unscorable_every, ok):
    f = Feed().run(48, covered=lambda t: mostly(t, 20))
    for t in range(START + 48 * H, NOW, L):
        f.window(t, status="unscorable" if (t // L) % unscorable_every == 0 else "scored", covered=mostly(t, 20))
    gates, info = T.candidate_gates(inputs(f), glob(NOW), TS)
    assert gates["availability"] is ok and (info["availability"] >= 0.95) is ok
    if not ok:
        assert verdict(f)[0].candidates["seasonal_naive"].reason == "DataUnavailable"


@pytest.mark.parametrize("unscorable,ok", [(0, True), (1, True), (2, False)])
def test_the_availability_boundary_is_ninety_five_percent(unscorable, ok):
    f = Feed()
    f.ledger["first_issue_s"] = NOW - 19 * L                      # 20 expected origins, all issued below
    for i in range(20):
        f.window(NOW - (19 - i) * L - L, status="unscorable" if i < unscorable else "scored")
    av = T.availability(f.ledger, NOW)
    assert av["outcomes"] == (20 - unscorable) / 20 and T.candidate_gates(inputs(f), glob(NOW), TS)[0][
        "availability"] is ok


def test_overdue_outcomes_count_against_availability():
    # Codex repro: after healthy history, 72 windows issued over 12 h with no outcome kept availability at 100 %.
    f = good()
    for i in range(72):
        f.issue(NOW + i * L)                                      # 72 windows, 12 h, outcomes never arrive
    later = NOW + 12 * H
    av = T.availability(f.ledger, later)
    assert av["overdue"] == 72 - (GRACE + L) // L                 # the last two are not due yet
    assert av["outcomes"] < 0.95
    v, _ = verdict(f, now=later, hyst=verdict(f)[1])
    assert v.selected is None and v.candidates["seasonal_naive"].reason == "DataUnavailable"
    assert v.candidates["seasonal_naive"].overdue == av["overdue"]


def test_a_window_is_overdue_only_after_its_deadline():
    f = Feed()
    f.issue(NOW)
    assert T.overdue(f.ledger, NOW + L + GRACE - 1, DAY) == 0
    assert T.overdue(f.ledger, NOW + L + GRACE, DAY) == 1
    f.outcome(1, NOW)
    assert T.overdue(f.ledger, NOW + L + GRACE, DAY) == 0


def test_missing_issuances_have_their_own_denominator():
    f = good()
    for t in range(NOW, NOW + 12 * H, L):                         # 12 h: only every third origin issued
        if (t // L) % 3 == 0:
            f.window(t, covered=True)
    av = T.availability(f.ledger, NOW + 12 * H)
    assert av["outcomes"] > 0.95 and av["predictions"] < 0.95
    assert not T.candidate_gates(inputs(f), glob(NOW + 12 * H), TS)[0]["availability"]


# --- replay versus prospective evidence -----------------------------------------------------------------------------

def test_replay_evidence_never_establishes_trust():
    f = Feed().run(72, covered=lambda t: mostly(t, 20), mode="replay")
    v, _ = verdict(f)
    cv = v.candidates["seasonal_naive"]
    assert not cv.trusted and cv.reason == "ShadowInsufficient" and cv.replay_gate_bounds == 432
    assert cv.prospective_gate_bounds == 0 and cv.coverage_gate is None and cv.availability is None
    f.run(47, start=NOW, covered=lambda t: mostly(t, 20))
    assert not verdict(f, now=NOW + 47 * H)[0].candidates["seasonal_naive"].trusted
    f.run(2, start=NOW + 47 * H, covered=lambda t: mostly(t, 20))
    assert verdict(f, now=NOW + 49 * H)[0].candidates["seasonal_naive"].trusted


def test_replay_successes_do_not_repair_prospective_availability_or_errors():
    # Codex repro: adding replay successes lifted availability from 89.66 % to 96.54 % and made it trusted.
    f = Feed().run(72, covered=lambda t: mostly(t, 20),
                   status=lambda t: "unscorable" if (t // L) % 10 == 0 and t >= NOW - DAY else "scored")
    before = T.availability(f.ledger, NOW)
    errors = {k: T.window_sums(f.ledger, NOW, 7 * DAY)[k] for k in ("sc", "abs", "bias", "n_step")}
    for i in range(200):                                          # replayed successes on the same day
        f.window(NOW - DAY + (i % 144) * L, mode="replay", covered=True)
    assert T.availability(f.ledger, NOW) == before and before["outcomes"] < 0.95
    assert {k: T.window_sums(f.ledger, NOW, 7 * DAY)[k] for k in errors} == errors
    assert not verdict(f)[0].candidates["seasonal_naive"].trusted


def test_prospective_needs_48_hours_and_64_bounds():
    short = Feed().run(47, covered=lambda t: mostly(t, 20))
    assert verdict(short, now=START + 47 * H)[0].candidates["seasonal_naive"].reason == "ShadowInsufficient"
    sparse = Feed().run(72, every=2 * H, covered=True)              # 36 bounds over 72 h
    assert not T.candidate_gates(inputs(sparse), glob(NOW), TS)[0]["prospective"]
    assert verdict(good())[0].selected == "seasonal_naive"


# --- recipe isolation -----------------------------------------------------------------------------------------------

def test_a_replacement_recipe_never_joins_established_evidence():
    # Codex repro: a replacement recipe's record raised an established ledger's prospective count to 433.
    f = good()
    count = f.ledger["totals"]["prospective_gate_bounded"]
    with pytest.raises(T.TrustError):
        f.window(NOW, recipe="r2")
    with pytest.raises(T.TrustError):
        f.outcome(9999, NOW, recipe="r2")
    assert f.ledger["totals"]["prospective_gate_bounded"] == count == 432


def test_hysteresis_and_pairs_are_bound_to_the_recipe():
    v, h = verdict(good())
    assert h["seasonal_naive"]["recipe"] == "r" and h["seasonal_naive"]["trusted"]
    replacement = Feed("seasonal_naive", "r2")                    # a new recipe starts with no trust at all
    v2, h2 = verdict(replacement, hyst=h)
    assert not h2["seasonal_naive"]["ever_trusted"] and h2["seasonal_naive"]["recipe"] == "r2"
    p = T.new_pairs(EPOCH, "statistical", "s1", "r")
    other = CalibrationRecord(1, EPOCH, "statistical", "s2", "iss-1", START, START + L, 4.0, 6.0, "scored", 5.0)
    with pytest.raises(T.TrustError):
        T.note_pair(p, T.EvidenceRecord(other, "prospective", 0, None), lead_s=L, nominal_miscoverage=NOMINAL)
    with pytest.raises(T.TrustError):                             # paired evidence of another recipe is refused
        verdict(good(), good("statistical", recipe="s9"), pairs={"statistical": p})


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
    ok = Feed().run(72, covered=lambda t: mostly(t, 8))              # 87.5%: at or above 0.85, passes
    assert T.candidate_gates(inputs(ok), glob(NOW), TS)[0]["coverage"]
    low = Feed().run(72, covered=lambda t: mostly(t, 5))             # 80% < 85%
    assert not T.candidate_gates(inputs(low), glob(NOW), TS)[0]["coverage"]
    assert verdict(low)[0].candidates["seasonal_naive"].reason == "CoverageLow"


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


# --- hour-granular windows (decision 1) -------------------------------------------------------------------------

def test_windows_are_whole_hours_from_the_first_hour_inside():
    f = Feed()
    for origin in (NOW - DAY - L, NOW - DAY, NOW - L):                # windows ending at now - 1 day, +Δ, and now
        f.window(origin)
    assert T.window_sums(f.ledger, NOW, DAY)["sc"] == 3               # aligned now: exactly [now - 1 day, now]
    g = Feed()
    g.window(NOW - DAY - 2 * L)                                       # ends one interval before now - 1 day
    assert T.window_sums(g.ledger, NOW, DAY)["sc"] == 0
    unaligned = NOW + 1800                                            # 30 min past the hour: starts at NOW - DAY + 1 h
    assert T._hours(unaligned, DAY) == ((NOW - DAY) // H + 1, NOW // H)
    assert T.window_sums(f.ledger, unaligned, DAY)["sc"] == 1                 # only the window ending at now


# --- candidate hysteresis -----------------------------------------------------------------------------------------

def test_hysteresis_needs_24_hours_and_32_new_bounds():
    led = T.new_ledger(EPOCH, "seasonal_naive", "r", L)
    h = T.step(T.new_hysteresis("r", EPOCH), True, led, 0, TS)
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


# --- fallback rungs, the paired gate and selection hysteresis -----------------------------------------------------

def pairs_for(challenger, chal_loss, base_loss, n=200, hours=96, start=START - 24 * H, recipe="r", epoch=EPOCH):
    p = T.new_pairs(epoch, challenger, recipe, "r")
    seq, every = 0, max(L, hours * H // n // L * L)
    for i in range(n):
        origin = start + i * every
        for model, loss, rc in ((challenger, chal_loss, recipe), ("seasonal_naive", base_loss, "r")):
            seq += 1
            y = 5.0
            upper = y + loss / 0.1 if loss else y                    # pinball (1 - tau) x (U - Y) = 0.1 x margin
            cal = CalibrationRecord(seq, epoch, model, rc, f"iss-{i}", origin, origin + L, 4.0, upper, "scored", y)
            p = T.note_pair(p, T.EvidenceRecord(cal, "prospective", 0, None), lead_s=L, nominal_miscoverage=NOMINAL)
    return p


def bound_costs(challenger="statistical", shortage=(1.0, 1.0), replica_time=(100.0, 100.0), epoch=EPOCH, recipe="r",
                policy=POLICY, window=(START - 30 * DAY, START + 30 * DAY)):
    """Same-policy costs as forecasting.policy_sim reports them, bound to the paired evidence and the policy."""
    return {"shortage": shortage, "replica_time": replica_time,
            "binding": {"epoch": epoch, "challenger": {"model": challenger, "recipe": recipe},
                        "baseline": {"model": "seasonal_naive", "recipe": "r"}, "policy_sha256": policy,
                        "window": list(window)}}


COSTS = bound_costs()
LSTM_COSTS = bound_costs("lstm")


def settled(cands, pairs, costs, now=NOW):
    """Evaluate a day earlier and then now: a challenger needs its entry criteria held for 24 hours."""
    _, h = T.evaluate(cands, {}, glob(now - 24 * H), TS, pairs=pairs, costs=costs)
    return T.evaluate(cands, h, glob(now), TS, pairs=pairs, costs=costs)


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
    costs = {"statistical": COSTS, "lstm": LSTM_COSTS}
    cands = [inputs(naive), inputs(stat)]
    if case == "no_costs":
        costs = {}
    elif case == "costs_worse":
        costs = {"statistical": bound_costs(shortage=(1.03, 1.0))}
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
    v, _ = settled(cands, pairs, costs)
    assert v.selected == expected
    assert v.prediction_ready is (expected is not None)


def test_the_paired_gate_measures_the_span_of_retained_pairs():
    assert not T.challenger_promoted(pairs_for("statistical", 0.5, 1.0, n=63), NOW, TS, COSTS, POLICY)
    assert T.challenger_promoted(pairs_for("statistical", 0.5, 1.0), NOW, TS, COSTS, POLICY)
    # Codex repro: 64 pairs spanning 12 h qualified after waiting until hour 72. The span decides, not the wait.
    short = pairs_for("statistical", 0.5, 1.0, n=64, hours=12, start=START)
    assert T.pair_sums(short, START + 72 * H)["span"] < 12 * H
    assert not T.challenger_promoted(short, START + 72 * H, TS, COSTS, POLICY)
    # An expired first pair does not establish the duration either.
    old = pairs_for("statistical", 0.5, 1.0, n=1, hours=1, start=START)
    for i in range(64):
        cal = [CalibrationRecord(10 + 2 * i + k, EPOCH, m, "r", f"late-{i}", START + 7 * DAY + i * L,
                                 START + 7 * DAY + (i + 1) * L, 4.0, 10.0 if m == "statistical" else 15.0,
                                 "scored", 5.0) for k, m in enumerate(("statistical", "seasonal_naive"))]
        for r in cal:
            old = T.note_pair(old, T.EvidenceRecord(r, "prospective", 0, None), lead_s=L, nominal_miscoverage=NOMINAL)
    now = START + 7 * DAY + 70 * L
    ps = T.pair_sums(old, now)
    assert ps["n"] == 64 and ps["span"] < 48 * H and not T.challenger_promoted(old, now, TS, COSTS, POLICY)


def test_pairs_need_both_models_at_the_same_origin():
    p = T.new_pairs(EPOCH, "statistical", "r", "r")
    one = CalibrationRecord(1, EPOCH, "statistical", "r", "iss-1", START, START + L, 4.0, 6.0, "scored", 5.0)
    other = CalibrationRecord(2, EPOCH, "seasonal_naive", "r", "iss-2", START, START + L, 4.0, 6.0, "scored", 5.0)
    replay = CalibrationRecord(3, EPOCH, "seasonal_naive", "r", "iss-1", START, START + L, 4.0, 6.0, "scored", 5.0)
    for r, mode in ((one, "prospective"), (other, "prospective"), (replay, "replay")):
        p = T.note_pair(p, T.EvidenceRecord(r, mode, 0, None), lead_s=L, nominal_miscoverage=NOMINAL)
    assert T.pair_sums(p, START + H)["n"] == 0                       # different issuances; replay never pairs
    # (One issuance per origin within an epoch: a second issuance at the same window replaces the unpaired entry.)
    match = CalibrationRecord(4, EPOCH, "statistical", "r", "iss-2", START, START + L, 4.0, 6.0, "scored", 5.0)
    p = T.note_pair(p, T.EvidenceRecord(match, "prospective", 0, None), lead_s=L, nominal_miscoverage=NOMINAL)
    assert T.pair_sums(p, START + H)["n"] == 1


def test_a_challenger_is_selected_only_after_its_entry_criteria_held_24_hours():
    cands = [inputs(good()), inputs(good("statistical"))]
    pairs, costs = {"statistical": pairs_for("statistical", 0.5, 1.0)}, {"statistical": COSTS}
    v1, h = T.evaluate(cands, {}, glob(NOW - 24 * H), TS, pairs=pairs, costs=costs)
    assert v1.selected == "seasonal_naive" and v1.promoted["statistical"]
    v2, h = T.evaluate(cands, h, glob(NOW - 1), TS, pairs=pairs, costs=costs)
    assert v2.selected == "seasonal_naive"
    v3, h = T.evaluate(cands, h, glob(NOW), TS, pairs=pairs, costs=costs)
    assert v3.selected == "statistical" and h[T.SELECTION]["switches"] == 2


def test_marginal_superiority_does_not_flap():
    cands = [inputs(good(hours=90)), inputs(good("statistical", hours=90))]      # evidence keeps flowing
    costs = {"statistical": COSTS}
    better, marginal = pairs_for("statistical", 0.5, 1.0), pairs_for("statistical", 0.97, 1.0)
    _, h = settled(cands, {"statistical": better}, costs)
    assert h[T.SELECTION]["selected"] == "statistical"
    switches = h[T.SELECTION]["switches"]
    for k in range(1, 13):                                           # alternating 0.5 / 0.97: inside the band
        p = marginal if k % 2 else better
        v, h = T.evaluate(cands, h, glob(NOW + k * H), TS, pairs={"statistical": p}, costs=costs)
        assert v.selected == "statistical"
    assert h[T.SELECTION]["switches"] == switches
    worse = pairs_for("statistical", 1.05, 1.0)                      # beyond the exit ratio: leaves at once
    v, h = T.evaluate(cands, h, glob(NOW + 13 * H), TS, pairs={"statistical": worse}, costs=costs)
    assert v.selected == "seasonal_naive"
    v, h = T.evaluate(cands, h, glob(NOW + 14 * H), TS, pairs={"statistical": better}, costs=costs)
    assert v.selected == "seasonal_naive"                            # re-entry waits for a fresh 24 h hold


def test_a_safety_failure_falls_back_at_once():
    naive, stat = good(), good("statistical")
    pairs, costs = {"statistical": pairs_for("statistical", 0.5, 1.0)}, {"statistical": COSTS}
    _, h = settled([inputs(naive), inputs(stat)], pairs, costs)
    assert h[T.SELECTION]["selected"] == "statistical"
    v, h2 = T.evaluate([inputs(naive), inputs(stat, fresh=False)], h, glob(NOW + 1), TS, pairs=pairs, costs=costs)
    assert v.selected == "seasonal_naive"                            # the statistical fit went stale
    v, _ = T.evaluate([inputs(naive, fresh=False), inputs(stat)], h, glob(NOW + 1), TS, pairs=pairs, costs=costs)
    assert v.selected is None                                        # its trusted naive basis failed


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


def test_the_exit_ratio_is_a_validated_setting():
    pa = {"spec": {"prediction": {"trust": {"challengerExitLossRatioBps": 9000}}}}
    with pytest.raises(c.ContractRefused):                            # below the entry ratio: no band
        c.resolve_settings(pa)
    assert c.resolve_settings({"spec": {"prediction": {}}}).trust.challenger_exit_loss_ratio_bps == 10_000


# --- exactly once, journal binding ------------------------------------------------------------------------------

def test_ledger_updates_are_exactly_once():
    f = Feed()
    r = f.window(START)
    before = f.ledger
    assert T.note_outcome(before, r, lead_s=L, nominal_miscoverage=NOMINAL, recent_n=32) is before
    assert T.note_issuance(before, seq=1, issuance_id="iss-1", epoch=EPOCH, model="seasonal_naive", recipe="r",
                           origin=START, window_end=START + L, deadline_s=START + L + GRACE, lead_s=L,
                           mode="prospective", bounded=True) is before
    with pytest.raises(T.TrustError):
        T.note_outcome(T.new_ledger(EPOCH, "statistical", "r", L), dataclasses.replace(r, cal=dataclasses.replace(
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


# --- epoch binding (Codex review on #24) -------------------------------------------------------------------------

OTHER = "f" * 64


def test_paired_evidence_of_another_epoch_is_refused_even_with_matching_recipes():
    # Codex repro: ledgers in epoch eeee..., pairs in ffff... with matching recipes; selected=statistical after the hold.
    cands = [inputs(good()), inputs(good("statistical"))]
    foreign = {"statistical": pairs_for("statistical", 0.5, 1.0, epoch=OTHER)}
    with pytest.raises(T.TrustError):
        settled(cands, foreign, {"statistical": COSTS})
    swapped = pairs_for("statistical", 0.5, 1.0)
    swapped["baseline"], swapped["challenger"] = "statistical", "seasonal_naive"
    with pytest.raises(T.TrustError):                                       # challenger/baseline identities too
        settled(cands, {"statistical": swapped}, {"statistical": COSTS})
    renamed = {**pairs_for("lstm", 0.5, 1.0), "recipes": {"lstm": "r", "seasonal_naive": "r"}}
    with pytest.raises(T.TrustError):                                       # lstm's pairs filed under statistical
        settled(cands, {"statistical": renamed}, {"statistical": COSTS})


def test_candidates_must_share_one_epoch():
    with pytest.raises(T.TrustError):
        verdict(good(), good("statistical", epoch=OTHER))


def test_an_epoch_change_resets_hysteresis_and_selection():
    cands = [inputs(good()), inputs(good("statistical"))]
    pairs, costs = {"statistical": pairs_for("statistical", 0.5, 1.0)}, {"statistical": COSTS}
    _, h = settled(cands, pairs, costs)
    assert h[T.SELECTION]["selected"] == "statistical" and h["statistical"]["trusted"]
    fresh = [inputs(good(epoch=OTHER)), inputs(good("statistical", epoch=OTHER))]
    fresh_pairs = {"statistical": pairs_for("statistical", 0.5, 1.0, epoch=OTHER)}
    v, h2 = T.evaluate(fresh, h, glob(NOW + 1), TS, pairs=fresh_pairs,
                       costs={"statistical": bound_costs(epoch=OTHER)})
    assert h2[T.SELECTION]["epoch"] == OTHER and h2[T.SELECTION]["entry_since"] == {"statistical:r": NOW + 1}
    assert v.selected == "seasonal_naive"                                  # the old epoch's selection did not carry
    assert all(h2[m]["epoch"] == OTHER for m in ("seasonal_naive", "statistical"))
    demoted = T.step({**h["statistical"], "trusted": True, "ever_trusted": True}, False, fresh[1].ledger, NOW, TS)
    assert demoted["epoch"] == OTHER and not demoted["ever_trusted"]       # old hysteresis never applies


# --- costs must be bound to the paired evidence and the policy (C2.8a) ---------------------------------------------

@pytest.mark.parametrize("bad", [
    dict(epoch="f" * 64), dict(recipe="other"), dict(challenger="lstm"), dict(policy="q" * 64),
    dict(window=(START + DAY, START + 30 * DAY)),                  # does not cover the retained pairs
])
def test_costs_that_do_not_match_promote_nothing(bad):
    pairs = pairs_for("statistical", 0.5, 1.0)
    assert T.challenger_promoted(pairs, NOW, TS, COSTS, POLICY)
    assert not T.challenger_promoted(pairs, NOW, TS, bound_costs(**bad), POLICY)
    unbound = {k: v for k, v in COSTS.items() if k != "binding"}
    assert not T.challenger_promoted(pairs, NOW, TS, unbound, POLICY)
    assert not T.challenger_promoted(pairs, NOW, TS, COSTS, None)      # no expected policy: nothing to match

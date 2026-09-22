#!/usr/bin/env python3
"""Known-answer tests for score.py v3 (run: python3 score_test.py). One case per Codex C-09 item."""
import json, os, sys, tempfile, unittest
from datetime import datetime, timedelta, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import score

T0 = datetime(2026, 9, 24, 0, 0, tzinfo=timezone.utc)
def iso(t): return t.strftime("%Y-%m-%dT%H:%M:%SZ")


class FakeProm:
    """Point-sample oracle: value(t) = fn(minutes since T0); None where `missing` says so.
    Records every instant() call so tests can assert what was read."""
    def __init__(self, fn, missing=lambda m: False):
        self.fn, self.missing, self.calls = fn, missing, []
    def instant(self, query, at):
        m = (at - T0).total_seconds() / 60.0
        self.calls.append(at)
        return None if self.missing(m) else self.fn(m)
    def range(self, query, start, end, step_s):
        out, t = [], start
        while t <= end:
            m = (t - T0).total_seconds() / 60.0
            if not self.missing(m):
                out.append((t.timestamp(), self.fn(m)))
            t += timedelta(seconds=step_s)
        return out


def rec(issued, steps=(1, 2, 3), rpm=lambda k: 100.0, cutoff=None, trained=None, anchor=None, version="m@abc"):
    base = anchor or issued
    r = {"issued_at": iso(issued), "application": "a", "namespace": "n", "horizon_minutes": 60, "step_minutes": 10.0,
         "model_version": version, "model_trained_at": iso(trained) if trained else "",
         "training_cutoff": iso(cutoff) if cutoff else "", "target_anchor": "inference_input_end" if anchor else "issued_at",
         "confidence": 0.9, "forecasts": [{"step": k, "target_at": iso(base + timedelta(minutes=10 * k)), "rpm": rpm(k)} for k in steps]}
    return r


class Alignment(unittest.TestCase):
    def test_exact_point_sample_at_plus_20_minutes(self):
        # ramp 100 + 10*minute; forecast for step 2 (T0+20) says 300 = exactly the point sample -> 0 error
        prom = FakeProm(lambda m: 100 + 10 * m)
        r = rec(T0, steps=(2,), rpm=lambda k: 300.0)
        out, rows = score.score([r], prom, "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(out["forecast_steps_scored"], 1)
        self.assertEqual(rows[0]["actual"], 300.0); self.assertEqual(out["overall"]["MAPE_percent"], 0.0)
        self.assertIn(T0 + timedelta(minutes=20), prom.calls)          # read exactly at target time

    def test_anchor_on_inference_input_end(self):
        prom = FakeProm(lambda m: 100 + 10 * m)
        anchor = T0 - timedelta(minutes=7)   # last grid sample 7 min before issuance
        r = rec(T0, steps=(1,), rpm=lambda k: 130.0, anchor=anchor)   # target = T0+3 min -> 130
        out, rows = score.score([r], prom, "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(rows[0]["actual"], 130.0); self.assertEqual(out["target_anchors_seen"], ["inference_input_end"])

    def test_known_error(self):
        prom = FakeProm(lambda m: 200.0)
        r = rec(T0, steps=(1, 2), rpm=lambda k: 250.0 if k == 1 else 150.0)
        out, _ = score.score([r], prom, "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(out["overall"]["MAPE_percent"], 25.0); self.assertEqual(out["overall"]["MAE_rpm"], 50.0)
        self.assertEqual(out["overall"]["under_predicted_share"], 0.5)


class Acceptance(unittest.TestCase):
    def test_sanity_rejection_never_removes_from_raw_set(self):
        # Codex C-16: a controller rejection is not a structural rejection; the raw-model set keeps the issuance.
        r1 = rec(T0); r2 = rec(T0 + timedelta(minutes=5))
        ev = {"event": "sanity_rejected", "application": "a", "namespace": "n", "issued_at": r1["issued_at"],
              "rejected_at": r1["issued_at"], "near_prediction_rpm": 12000.0, "current_rpm": 120.0, "max_sane_rpm": 1200.0}
        acc, rej = score.accept_records([r1, ev, r2])
        self.assertEqual([r["issued_at"] for r in acc], [r1["issued_at"], r2["issued_at"]])
        self.assertEqual(rej, [])
        # The rejection is a diagnostic: counted and listed, earliest rejected_at kept.
        rejections = score.controller_rejections([r1, ev, r2])
        self.assertEqual(len(rejections), 1)
        self.assertEqual(list(rejections.values())[0], score.parse_ts(r1["issued_at"]))

    def test_no_controller_used_subset_is_derived(self):
        # Codex C-19: no "controller-used" subset exists in the scorer; the raw set scores every step of a
        # rejected issuance, and rejections are reported only as diagnostics.
        self.assertFalse(hasattr(score, "controller_subset"))
        r1 = rec(T0)
        ev = {"event": "sanity_rejected", "application": "a", "namespace": "n", "issued_at": r1["issued_at"],
              "rejected_at": iso(T0 + timedelta(minutes=22))}
        acc, _ = score.accept_records([r1, ev])
        prom = FakeProm(lambda m: 100.0)
        out, rows = score.score(acc, prom, "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(out["forecast_steps_scored"], 3)
        self.assertNotIn("controller_accepted", out)

    def test_later_rejection_keeps_earliest_rejected_at(self):
        r1 = rec(T0, steps=(1, 2, 3, 4, 5, 6))
        ev1 = {"event": "sanity_rejected", "application": "a", "namespace": "n", "issued_at": r1["issued_at"],
               "rejected_at": iso(T0 + timedelta(minutes=50))}
        ev2 = dict(ev1, rejected_at=iso(T0 + timedelta(minutes=30)))
        rejections = score.controller_rejections([r1, ev1, ev2])
        self.assertEqual(list(rejections.values())[0], T0 + timedelta(minutes=30))

    def test_duplicate_rejected_and_not_counted(self):
        r1 = rec(T0); r2 = dict(rec(T0)); r2["forecasts"] = [dict(f, rpm=999.0) for f in r2["forecasts"]]
        acc, rej = score.accept_records([r1, r2])
        self.assertEqual(len(acc), 1); self.assertEqual(rej[0]["reason"], "duplicate")
        out, _ = score.score(acc, FakeProm(lambda m: 100.0), "a", "n", T0, T0 + timedelta(minutes=5), cadence_min=5)
        self.assertEqual(out["issuances_accepted"], 1); self.assertEqual(out["issuance_coverage"], 1.0)

    def test_stale_model_and_out_of_order_rejected(self):
        c1, c0 = T0 - timedelta(hours=1), T0 - timedelta(hours=2)
        # second: cutoff moves backwards -> stale; third: issued before the last ACCEPTED record -> out of order
        rs = [rec(T0, cutoff=c1), rec(T0 + timedelta(minutes=5), cutoff=c0), rec(T0 - timedelta(minutes=1), cutoff=c1)]
        acc, rej = score.accept_records(rs)
        self.assertEqual(len(acc), 1)
        self.assertEqual(sorted(x["reason"] for x in rej), ["out_of_order", "stale_model"])

    def test_target_must_be_after_issuance_and_cutoff(self):
        bad = rec(T0); bad["forecasts"][0]["target_at"] = iso(T0)                       # target == issuance
        late_cutoff = rec(T0 + timedelta(hours=1), cutoff=T0 + timedelta(hours=2))      # cutoff after targets
        acc, rej = score.accept_records([bad, late_cutoff])
        self.assertEqual(acc, []); self.assertEqual(sorted(x["reason"] for x in rej), ["target_before_cutoff", "target_not_in_future"])


class Coverage(unittest.TestCase):
    def test_missing_issuance_periods_lower_coverage(self):
        # window of 1 h at 5-min cadence expects 12 issuances; only 3 recorded -> 0.25
        recs = [rec(T0 + timedelta(minutes=5 * i)) for i in (0, 1, 2)]
        out, _ = score.score(recs, FakeProm(lambda m: 100.0), "a", "n", T0, T0 + timedelta(hours=1), cadence_min=5)
        self.assertEqual(out["issuances_expected"], 12); self.assertEqual(out["issuance_coverage"], 0.25)
        self.assertEqual(out["step_coverage"], 1.0)

    def test_missing_observation_is_a_gap(self):
        prom = FakeProm(lambda m: 100.0, missing=lambda m: 15 <= m < 25)   # no data for target at +20
        r = rec(T0, steps=(1, 2, 3))
        out, _ = score.score([r], prom, "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(out["forecast_steps_scored"], 2); self.assertEqual(out["gaps"], 1)
        self.assertAlmostEqual(out["step_coverage"], 2 / 3, places=4)

    def test_zero_observation_excluded_from_mape_kept_in_mae(self):
        prom = FakeProm(lambda m: 0.0 if 15 <= m < 25 else 100.0)
        r = rec(T0, steps=(1, 2), rpm=lambda k: 100.0)
        out, rows = score.score([r], prom, "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(out["zero_observations"], 1)
        self.assertEqual(out["overall"]["MAPE_percent"], 0.0)          # only the non-zero point
        self.assertEqual(out["overall"]["MAE_rpm"], 50.0)              # (0 + 100) / 2


class Baselines(unittest.TestCase):
    def test_persistence_uses_issuance_sample_only(self):
        prom = FakeProm(lambda m: 100 + 10 * m)
        r = rec(T0, steps=(1,), rpm=lambda k: 200.0)   # actual at +10 = 200; persistence = value at T0 = 100
        out, rows = score.score([r], prom, "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(rows[0]["persistence_ape"], 0.5)
        self.assertTrue(all(c <= T0 + timedelta(minutes=10) for c in prom.calls))

    def test_previous_day_baseline_reads_24h_before_target(self):
        prom = FakeProm(lambda m: 330.0 if m < 0 else 300.0)
        r = rec(T0, steps=(1,), rpm=lambda k: 300.0)
        out, _ = score.score([r], prom, "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(out["overall"]["prevday_MAPE_percent"], 10.0)
        self.assertIn(T0 + timedelta(minutes=10) - timedelta(hours=24), prom.calls)

    def test_baseline_can_never_peek_past_issuance(self):
        guard = score.NoPeeking(FakeProm(lambda m: 1.0))
        with self.assertRaises(ValueError):
            guard.at("q", T0 + timedelta(minutes=1), T0)


class Replicas(unittest.TestCase):
    def test_pod_minutes_interval_weighted_and_gap_capped(self):
        s = [(0, 2), (60, 2), (120, 4), (300, 4), (360, 1), (1000, 1)]   # 300->360 fine; 360->1000 is a 640 s gap
        pm, credited, gap = score.pod_minutes(s, max_gap_s=300)
        self.assertEqual(pm, 20.0); self.assertEqual(credited, 360.0); self.assertEqual(gap, 640.0)
        self.assertEqual(score.sampled_changes(s), 2)

    def test_series_gaps(self):
        prom = FakeProm(lambda m: 3.0, missing=lambda m: 10 <= m < 20)
        s = prom.range("q", T0, T0 + timedelta(minutes=59), 60)
        g = score.series_gaps(s, T0, T0 + timedelta(minutes=59))
        self.assertEqual(g["expected_samples"], 60); self.assertEqual(g["missing_samples"], 10)
        self.assertEqual(g["longest_gap_seconds"], 600.0)


class MainGate(unittest.TestCase):
    def test_main_fails_loudly_without_forecasts(self):
        class P:
            def __init__(self, *_): pass
            def range(self, q, s, e, st): return [(s.timestamp(), 100.0)]
            def instant(self, q, at): return 100.0
        score.Prom = P
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            path = f.name
        with self.assertRaises(SystemExit) as cm:
            score.main(["--prom", "http://x", "--forecast-log", path, "--start", iso(T0), "--end", iso(T0 + timedelta(hours=1))])
        os.unlink(path); self.assertIn("no forecast issuances", str(cm.exception))

    def test_main_fails_on_low_issuance_coverage(self):
        class P:
            def __init__(self, *_): pass
            def range(self, q, s, e, st):
                out, t = [], s
                while t <= e: out.append((t.timestamp(), 3.0)); t += timedelta(seconds=st)
                return out
            def instant(self, q, at): return 100.0
        score.Prom = P
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            f.write(json.dumps(rec(T0)) + "\n"); path = f.name
        with self.assertRaises(SystemExit) as cm:
            score.main(["--prom", "http://x", "--forecast-log", path, "--start", iso(T0), "--end", iso(T0 + timedelta(hours=1)),
                        "--app", "a", "--namespace", "n", "--controls", "", "--out", os.devnull])
        os.unlink(path); self.assertIn("issuance coverage", str(cm.exception))


FIXTURE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "testdata", "transition-forecasts.jsonl")
ART_A, ART_B = "a" * 64, "b" * 64


class ArtifactTransition(unittest.TestCase):
    """Codex round 28 / D-136: attribution by full artifact hash across a rolling-retraining transition.
    Fixture: artifact A issues every 5 min 00:00-00:55, artifact B 01:00-01:25; six 10-min steps each, so
    A's targets keep maturing (up to 01:55) after B has appeared. Observation is a constant 1000 rpm; A
    forecasts 1050 (AE 50), B forecasts 1010 (AE 10). Decision lines are interleaved every minute."""
    def load(self):
        recs = score.load_forecasts(FIXTURE, "nginx-test", "demo", T0, T0 + timedelta(minutes=90))
        acc, rej = score.accept_records(recs)
        return recs, acc, rej

    def test_decision_lines_are_not_issuances_or_rejections(self):
        recs, acc, rej = self.load()
        self.assertEqual(len(recs), 18); self.assertEqual(len(acc), 18); self.assertEqual(rej, [])
        self.assertEqual(score.controller_rejections(recs), {})
        self.assertEqual(len(score.load_decisions(FIXTURE, "nginx-test", "demo")), 90)

    def test_no_missing_or_duplicate_issuance_across_the_change(self):
        _, acc, _ = self.load()
        out, rows = score.score(acc, FakeProm(lambda m: 1000.0), "nginx-test", "demo", T0, T0 + timedelta(minutes=90),
                                cadence_min=5, as_of=T0 + timedelta(hours=3))
        pa = out["per_artifact"]
        self.assertEqual(set(pa), {ART_A, ART_B})
        self.assertEqual((pa[ART_A]["issuances"], pa[ART_B]["issuances"]), (12, 6))
        self.assertEqual(sum(v["issuances"] for v in pa.values()), out["issuances_accepted"])
        self.assertEqual(sum(v["steps_scored"] for v in pa.values()), out["forecast_steps_scored"])
        # every issuance appears exactly once, in exactly its own artifact's group
        by_art = {}
        for r in rows:
            by_art.setdefault(r["artifact_sha256"], set()).add(r["issued_at"])
        self.assertFalse(by_art[ART_A] & by_art[ART_B])
        self.assertEqual(by_art[ART_A] | by_art[ART_B], {r["issued_at"] for r in acc})
        per_iss = {}
        for r in rows:
            per_iss[(r["issued_at"], r["step"])] = per_iss.get((r["issued_at"], r["step"]), 0) + 1
        self.assertTrue(all(v == 1 for v in per_iss.values())); self.assertEqual(len(per_iss), 108)

    def test_old_artifact_targets_after_the_switch_stay_attributed_to_it(self):
        _, acc, _ = self.load()
        _, rows = score.score(acc, FakeProm(lambda m: 1000.0), "nginx-test", "demo", T0, T0 + timedelta(minutes=90),
                              cadence_min=5, as_of=T0 + timedelta(hours=3))
        late_a = [r for r in rows if r["artifact_sha256"] == ART_A and score.parse_ts(r["target_at"]) > T0 + timedelta(hours=1)]
        self.assertEqual(len(late_a), 36)              # retained, scored, and still A's
        self.assertTrue(all(r["ae"] == 50.0 for r in late_a))

    def test_pooled_aggregate_is_not_an_equal_average_of_artifacts(self):
        _, acc, _ = self.load()
        out, _ = score.score(acc, FakeProm(lambda m: 1000.0), "nginx-test", "demo", T0, T0 + timedelta(minutes=90),
                             cadence_min=5, as_of=T0 + timedelta(hours=3))
        self.assertEqual(out["per_artifact"][ART_A]["MAE_rpm"], 50.0); self.assertEqual(out["per_artifact"][ART_B]["MAE_rpm"], 10.0)
        self.assertEqual(out["overall"]["MAE_rpm"], 36.7)     # (72*50 + 36*10) / 108
        self.assertNotEqual(out["overall"]["MAE_rpm"], 30.0)  # the equal average it must not be

    def test_unmatured_targets_are_outstanding_not_gaps(self):
        _, acc, _ = self.load()
        out, _ = score.score(acc, FakeProm(lambda m: 1000.0), "nginx-test", "demo", T0, T0 + timedelta(minutes=90),
                             cadence_min=5, as_of=T0 + timedelta(hours=2))
        self.assertEqual(out["forecast_steps_outstanding"], 9); self.assertEqual(out["gaps"], 0)
        self.assertFalse(out["final_horizon_matured"])
        self.assertEqual(out["per_artifact"][ART_B]["outstanding"], 9); self.assertEqual(out["per_artifact"][ART_A]["outstanding"], 0)
        self.assertEqual(out["step_coverage"], 1.0)

    def test_missing_hash_goes_to_unknown(self):
        r = rec(T0); out, _ = score.score([r], FakeProm(lambda m: 100.0), "a", "n", T0, T0 + timedelta(hours=1), cadence_min=60)
        self.assertEqual(list(out["per_artifact"]), ["unknown"])

    def test_transition_events_are_separate_and_null_when_unknown(self):
        _, acc, _ = self.load()
        ev = score.transition_events(acc, {ART_B: {"published_at": "2026-09-24T00:52:00Z", "api_reloaded_at": "2026-09-24T00:53:10Z"}})
        self.assertEqual(ev[ART_A], {"published_at": None, "api_reloaded_at": None,
                                     "first_operator_issuance_at": "2026-09-24T00:00:00Z", "issuances": 12})
        self.assertEqual(ev[ART_B]["first_operator_issuance_at"], "2026-09-24T01:00:00Z")
        self.assertEqual(ev[ART_B]["api_reloaded_at"], "2026-09-24T00:53:10Z")


def dec(at, src, raw=5, adj=5, pred=5, status="used", guards=()):
    return {"event": "decision", "at": iso(at), "application": "a", "namespace": "n", "forecast_status": status,
            "raw_predicted_replicas": raw, "confidence_adjusted_replicas": adj, "predicted_replicas": pred,
            "safeguards": list(guards), "desired_source": src}


class Participation(unittest.TestCase):
    """D-140: fraction of reconciles whose decision the prediction SET, by daily phase, complete instances only."""
    PEAK = T0 + timedelta(hours=15)

    def full_peak(self):
        srcs = ["prediction"] * 20 + ["tie"] * 30 + ["reactive"] * 10
        return [dec(self.PEAK + timedelta(minutes=i), s, raw=9, adj=7 if i < 12 else 9, pred=7 if i < 12 else 9,
                    guards=("confidence_damping",) if i < 12 else ()) for i, s in enumerate(srcs)]

    def test_complete_peak_fraction_and_separate_ties(self):
        out = score.participation(self.full_peak())
        h = out["headline_complete_phases"]["peak"]
        self.assertEqual((h["reconciles"], h["prediction_set"], h["tie"], h["reactive"]), (60, 20, 30, 10))
        self.assertEqual(h["prediction_set_fraction"], 0.3333); self.assertEqual(h["damping_changed"], 12)
        self.assertEqual(h["safeguard_changed"], 12); self.assertEqual(h["safeguards"], {"confidence_damping": 12})
        self.assertEqual(out["incomplete_instances"], [])

    def test_gap_longer_than_three_intervals_makes_instance_incomplete(self):
        d = [x for i, x in enumerate(self.full_peak()) if not 30 <= i < 34]   # 5-minute hole
        out = score.participation(d)
        self.assertNotIn("peak", out["headline_complete_phases"])
        self.assertEqual(out["incomplete_instances"][0]["phase"], "peak")

    def test_partial_phase_is_not_headline(self):
        d = self.full_peak() + [dec(T0 + timedelta(hours=10, minutes=i), "prediction") for i in range(30)]
        out = score.participation(d)
        self.assertEqual(set(out["headline_complete_phases"]), {"peak"})
        self.assertEqual([x["phase"] for x in out["incomplete_instances"]], ["rising"])

    def test_phase_definition(self):
        P = lambda h: score.phase_instance(T0 + timedelta(hours=h))[0]
        self.assertEqual([P(h) for h in (5, 6, 14, 15, 16, 20, 21, 23)],
                         ["trough", "rising", "rising", "peak", "falling", "falling", "trough", "trough"])
        # the trough spans midnight as ONE instance
        self.assertEqual(score.phase_instance(T0 + timedelta(hours=23))[1:], score.phase_instance(T0 + timedelta(days=1, hours=2))[1:])

    def test_participation_only_cli(self):
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as f:
            path = f.name
        score.main(["--forecast-log", FIXTURE, "--start", iso(T0), "--end", iso(T0 + timedelta(hours=2)),
                    "--app", "nginx-test", "--namespace", "demo", "--participation-only", "--out", path])
        d = json.load(open(path)); os.unlink(path)
        self.assertEqual(d["decision_records"], 90)
        self.assertEqual(d["participation"]["incomplete_instances"][0]["phase"], "trough")


if __name__ == "__main__":
    unittest.main(verbosity=1)

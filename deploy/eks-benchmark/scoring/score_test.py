#!/usr/bin/env python3
"""Known-answer tests for score.py v3 (run: python3 score_test.py). One case per Codex C-09 item."""
import json, os, sys, tempfile, unittest
from datetime import datetime, timedelta, timezone
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import score
import forecast_log
import forecast_transfer
import hashlib
import atexit
import subprocess
from pathlib import Path
from unittest.mock import patch

T0 = datetime(2026, 9, 24, 0, 0, tzinfo=timezone.utc)
def iso(t): return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def fixture_args(path):
    data = Path(path).read_bytes()
    receipt = dict(schema_version=1, kind='fixture', bytes=len(data),
                   sha256=hashlib.sha256(data).hexdigest(), reader_fingerprint='f' * 64,
                   remote_probe_at='2026-09-30T00:00:00Z', collected_at='2026-09-30T00:00:01Z',
                   source={key: 'fixture' for key in forecast_log.SOURCE_FIELDS})
    with tempfile.NamedTemporaryFile('w', suffix='.receipt.json', delete=False) as f:
        json.dump(receipt, f)
    atexit.register(os.unlink, f.name)
    return ['--forecast-receipt', f.name, '--allow-fixture-receipt']


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
    def test_cli_refuses_log_without_transfer_receipt(self):
        with tempfile.TemporaryDirectory() as root:
            output = os.path.join(root, 'result.json')
            with self.assertRaises(SystemExit):
                score.main(['--forecast-log', FIXTURE, '--start', iso(T0),
                            '--end', iso(T0 + timedelta(hours=2)), '--app', 'nginx-test',
                            '--namespace', 'demo', '--participation-only', '--out', output])
            self.assertFalse(os.path.exists(output))

    def test_main_fails_loudly_without_forecasts(self):
        class P:
            def __init__(self, *_): pass
            def range(self, q, s, e, st): return [(s.timestamp(), 100.0)]
            def instant(self, q, at): return 100.0
        score.Prom = P
        with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as f:
            path = f.name
        with self.assertRaises(SystemExit) as cm:
            score.main(fixture_args(path) + ["--prom", "http://x", "--forecast-log", path, "--start", iso(T0), "--end", iso(T0 + timedelta(hours=1))])
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
            score.main(fixture_args(path) + ["--prom", "http://x", "--forecast-log", path, "--start", iso(T0), "--end", iso(T0 + timedelta(hours=1)),
                        "--app", "a", "--namespace", "n", "--controls", "", "--out", path + ".result"])
        os.unlink(path); os.unlink(path + ".result"); self.assertIn("issuance coverage", str(cm.exception))


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
        os.unlink(path)  # report destination must be new
        score.main(fixture_args(FIXTURE) + ["--forecast-log", FIXTURE, "--start", iso(T0), "--end", iso(T0 + timedelta(hours=2)),
                    "--app", "nginx-test", "--namespace", "demo", "--participation-only", "--out", path])
        d = json.load(open(path)); os.unlink(path)
        self.assertEqual(d["decision_records"], 90)
        self.assertEqual(d["participation"]["incomplete_instances"][0]["phase"], "trough")


class ReceiptIntegrity(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.log = Path(self.temp.name) / 'forecasts.jsonl'
        self.log.write_bytes(Path(FIXTURE).read_bytes())
        self.args = fixture_args(self.log)
        self.receipt = Path(self.args[1])
        self.end = T0 + timedelta(hours=2)

    def test_existing_report_is_never_overwritten(self):
        output = Path(self.temp.name) / 'original.json'
        output.write_text('original evidence')
        with patch.object(score, 'Prom', side_effect=AssertionError('network must not be called')):
            with self.assertRaisesRegex(SystemExit, 'output already exists'):
                score.main(self.args + ['--forecast-log', str(self.log), '--start', iso(T0),
                           '--end', iso(self.end), '--app', 'nginx-test', '--namespace', 'demo',
                           '--participation-only', '--out', str(output)])
        self.assertEqual(output.read_text(), 'original evidence')

    def test_concurrent_report_creation_is_not_overwritten(self):
        output = Path(self.temp.name) / 'result.json'
        real_link = os.link
        def concurrent_creator(source, destination):
            Path(destination).write_text('concurrent evidence')
            return real_link(source, destination)
        with patch.object(score.os, 'link', side_effect=concurrent_creator):
            with self.assertRaisesRegex(SystemExit, 'output already exists'):
                score.write_result('{"new": true}', str(output))
        self.assertEqual(output.read_text(), 'concurrent evidence')
        self.assertEqual(list(Path(self.temp.name).glob('.score-*')), [])

    def test_matching_fixture_is_explicit_and_bound(self):
        snapshot, provenance = forecast_log.load_verified(self.log, self.receipt, self.end, True)
        self.assertEqual(snapshot, self.log.read_bytes())
        self.assertTrue(provenance['fixture'])
        self.assertEqual(provenance['receipt_sha256'], hashlib.sha256(self.receipt.read_bytes()).hexdigest())

    def test_fixture_requires_explicit_opt_in(self):
        with self.assertRaises(SystemExit):
            forecast_log.load_verified(self.log, self.receipt, self.end)

    def test_truncated_and_same_length_modified_files_are_rejected(self):
        original = self.log.read_bytes()
        for data in (original[:-1], b'X' + original[1:]):
            self.log.write_bytes(data)
            with self.assertRaises(SystemExit):
                forecast_log.load_verified(self.log, self.receipt, self.end, True)

    def test_malformed_receipts_are_rejected(self):
        original = json.loads(self.receipt.read_text())
        for change in ({'bytes': True}, {'bytes': original['bytes'] + 1}, {'sha256': 'bad'},
                       {'kind': 'complete'}, {'schema_version': 2}, {'source': {}},
                       {'remote_probe_at': 'unknown'}, {'collected_at': '2020-01-01T00:00:00Z'}):
            with self.subTest(change=change):
                self.receipt.write_text(json.dumps(dict(original, **change)))
                with self.assertRaises(SystemExit):
                    forecast_log.load_verified(self.log, self.receipt, self.end, True)

    def test_end_after_probe_rejected_before_prometheus(self):
        with patch.object(score, 'Prom', side_effect=AssertionError('network must not be called')):
            with self.assertRaisesRegex(SystemExit, 'ends after remote probe'):
                score.main(self.args + ['--forecast-log', str(self.log), '--prom', 'http://unused',
                           '--start', iso(T0), '--end', '2026-10-01T00:00:00Z'])

    def test_replacing_source_after_verification_cannot_change_scored_bytes(self):
        output = Path(self.temp.name) / 'result.json'
        original_loader = score.load_decisions
        def replace_then_load(snapshot, *args):
            self.log.write_text('not the verified file')
            return original_loader(snapshot, *args)
        with patch.object(score, 'load_decisions', side_effect=replace_then_load):
            score.main(self.args + ['--forecast-log', str(self.log), '--start', iso(T0),
                       '--end', iso(self.end), '--app', 'nginx-test', '--namespace', 'demo',
                       '--participation-only', '--out', str(output)])
        result = json.loads(output.read_text())
        self.assertEqual(result['decision_records'], 90)
        self.assertTrue(result['forecast_log_provenance']['fixture'])

    def test_receipt_emission_checks_remote_hash_and_refuses_reuse(self):
        receipt = Path(self.temp.name) / 'remote.json'
        data = self.log.read_bytes()
        source = {k: 'test-' + k for k in forecast_log.SOURCE_FIELDS}
        with self.assertRaises(ValueError):
            forecast_log.emit(self.log, receipt, len(data), '0' * 64, '2026-09-23T00:00:00Z', source, 'f' * 64)
        self.assertFalse(receipt.exists())
        forecast_log.emit(self.log, receipt, len(data), hashlib.sha256(data).hexdigest(), '2026-09-23T00:00:00Z', source, 'f' * 64)
        before = receipt.read_bytes()
        with self.assertRaises(ValueError):
            forecast_log.emit(self.log, receipt, len(data), hashlib.sha256(data).hexdigest(), '2026-09-23T00:00:00Z', source, 'f' * 64)
        self.assertEqual(receipt.read_bytes(), before)


class ReaderTransport(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name); self.full = self.root / 'source'
        self.full.write_bytes(Path(FIXTURE).read_bytes())
        self.commands = self.root / 'commands'
        fake = self.root / 'kubectl'
        fake.write_text(r"""#!/usr/bin/env python3
import hashlib,json,os,pathlib,sys
args=sys.argv[1:]
with open(os.environ['COMMANDS'],'a') as f:f.write(' '.join(args)+'\n')
if 'get' in args and 'pods' in args:
    pod={'metadata':{'name':'operator','uid':'pod-uid'},'spec':{'nodeName':'node-a'}}
    print(json.dumps({'items':[pod,pod] if os.environ.get('MULTI') else [pod]}))
elif 'get' in args and 'deploy' in args:
    print(json.dumps({'spec':{'template':{'spec':{'containers':[{'env':[{'name':'FORECAST_LOG','value':'/data/forecasts.jsonl'}]}],'volumes':[{'name':'forecast-log','persistentVolumeClaim':{'claimName':'forecast-pvc'}}]}}}}))
elif 'get' in args and 'pvc' in args:
    changed=os.environ.get('UID_CHANGE') and pathlib.Path(os.environ['COMMANDS']).read_text().count('get pvc')>1
    print(json.dumps({'metadata':{'uid':'new-pvc-uid' if changed else 'pvc-uid'},'spec':{'volumeName':'pv-id'}}))
elif 'get' in args and 'pod' in args:
    changed=os.environ.get('READER_CHANGE') and pathlib.Path(os.environ['COMMANDS']).with_suffix('.attempted').exists()
    print(json.dumps({'metadata':{'uid':'reader-new' if changed else 'reader-uid'},'status':{'containerStatuses':[{'name':'reader','restartCount':0}]}}))
elif 'exec' in args:
    data=pathlib.Path(os.environ['FULL']).read_bytes()
    if any('__META__' in x for x in args):
        print(str(len(data))+' '+('0'*64 if os.environ.get('BAD_PREFIX_HASH') else hashlib.sha256(data).hexdigest()))
        if os.environ.get('APPEND'):
            with open(os.environ['FULL'],'ab') as f:f.write(b'new appended bytes\n')
    else:
        marker=pathlib.Path(os.environ['COMMANDS']).with_suffix('.attempted')
        first=not marker.exists();marker.touch()
        if first and os.environ.get('FAIL_ONCE'):
            print('fixture connection reset',file=sys.stderr);raise SystemExit(7)
        if 'forecast-chunk' in args:
            index,length=map(int,args[-2:]);data=data[index*1048576:index*1048576+length]
            sys.stdout.buffer.write(str(len(data)).encode()+b'\n'+hashlib.sha256(data).hexdigest().encode()+b'  chunk\n')
        if os.environ.get('CORRUPT') and data:data=bytes([data[0]^1])+data[1:]
        if os.environ.get('TRUNCATE') or (first and os.environ.get('TRUNCATE_ONCE')):data=data[:-10]
        sys.stdout.buffer.write(data)
""")
        fake.chmod(0o755)
        self.env=dict(os.environ, PATH=str(self.root)+':'+os.environ['PATH'],
                      FULL=str(self.full), COMMANDS=str(self.commands))
        self.script=Path(__file__).parent/'read-forecast-log.sh'
        self.out=self.root/'output'

    def run_reader(self, mode='--all', **env):
        return subprocess.run(['bash',str(self.script),mode,'--out',str(self.out)],
                              env=dict(self.env,**env),capture_output=True,text=True)

    def test_full_transfer_produces_remote_receipt_and_cleans_pod(self):
        p=self.run_reader();self.assertEqual(p.returncode,0,p.stdout+p.stderr)
        receipt=json.loads(Path(str(self.out)+'.receipt.json').read_text())
        self.assertEqual(receipt['source']['pvc_uid'],'pvc-uid')
        self.assertEqual(receipt['source']['operator_pod_uid'],'pod-uid')
        self.assertEqual(receipt['reader_fingerprint'],forecast_transfer.reader_fingerprint(self.script.parent))
        self.assertEqual(self.out.read_bytes(),self.full.read_bytes())
        self.assertIn('delete pod',self.commands.read_text())

    def test_truncation_with_success_exit_emits_no_receipt(self):
        p=self.run_reader(TRUNCATE='1');self.assertEqual(p.returncode,2,p.stdout+p.stderr)
        self.assertFalse(self.out.exists());self.assertFalse(Path(str(self.out)+'.receipt.json').exists())

    def test_transient_exec_failure_recovers_and_preserves_stderr(self):
        p=self.run_reader(FAIL_ONCE='1')
        self.assertEqual(p.returncode,0,p.stdout+p.stderr)
        self.assertEqual(self.out.read_bytes(),self.full.read_bytes())
        diagnostic=Path(str(self.out)+'.transfer')
        self.assertTrue(any('fixture connection reset' in f.read_text()
                            for f in diagnostic.glob('*.stderr')))
        attempts=[json.loads(line) for line in (diagnostic/'attempts.jsonl').read_text().splitlines()]
        failed=next(row for row in attempts if row['event']=='retry')
        self.assertEqual(failed['exit_code'],7)
        self.assertEqual(failed['offset'],0)

    def test_transient_success_exit_truncation_retries(self):
        p=self.run_reader(TRUNCATE_ONCE='1')
        self.assertEqual(p.returncode,0,p.stderr)
        self.assertEqual(self.out.read_bytes(),self.full.read_bytes())

    def test_append_after_probe_does_not_change_verified_prefix(self):
        before=self.full.read_bytes()
        p=self.run_reader(APPEND='1')
        self.assertEqual(p.returncode,0,p.stderr)
        self.assertEqual(self.out.read_bytes(),before)
        self.assertGreater(self.full.stat().st_size,len(before))

    def test_corrupt_chunk_and_bad_whole_hash_cannot_publish_receipt(self):
        for env in [{'CORRUPT':'1'}, {'BAD_PREFIX_HASH':'1'}]:
            self.out=self.root/next(iter(env))
            p=self.run_reader(**env)
            self.assertEqual(p.returncode,2,p.stderr)
            self.assertFalse(self.out.exists())
            self.assertFalse(Path(str(self.out)+'.receipt.json').exists())

    def test_changed_reader_identity_cannot_retry_another_reader(self):
        p=self.run_reader(FAIL_ONCE='1',READER_CHANGE='1')
        self.assertEqual(p.returncode,2)
        self.assertIn('reader identity',p.stderr)
        events=[json.loads(line) for line in Path(str(self.out)+'.transfer/attempts.jsonl').read_text().splitlines()]
        self.assertEqual(sum(x['event']=='retry' for x in events),1)

    def test_diagnostic_collision_refused_before_cluster_access(self):
        Path(str(self.out)+'.transfer').mkdir()
        p=self.run_reader()
        self.assertEqual(p.returncode,2)
        self.assertIn(str(self.out)+'.transfer',p.stderr)
        self.assertFalse(self.commands.exists())

    def test_clean_stdout_success_removes_temporary_diagnostics(self):
        p=subprocess.run(['bash',str(self.script),'--count'],
                         env=dict(self.env,TMPDIR=str(self.root)),capture_output=True,text=True)
        self.assertEqual(p.returncode,0,p.stderr)
        diagnostic=Path(next(line.removeprefix('transfer diagnostics: ') for line in p.stderr.splitlines()
                             if line.startswith('transfer diagnostics: ')))
        self.assertFalse(diagnostic.exists())

    def test_recovered_stdout_failure_keeps_logs_without_duplicate_prefix(self):
        p=subprocess.run(['bash',str(self.script),'--count'],
                         env=dict(self.env,TMPDIR=str(self.root),FAIL_ONCE='1'),capture_output=True,text=True)
        self.assertEqual(p.returncode,0,p.stderr)
        diagnostic=Path(next(line.removeprefix('transfer diagnostics: ') for line in p.stderr.splitlines()
                             if line.startswith('transfer diagnostics: ')))
        self.assertTrue((diagnostic/'attempts.jsonl').exists())
        self.assertFalse((diagnostic/'prefix.partial').exists())

    def test_exact_and_partial_chunk_boundaries(self):
        for size in [forecast_transfer.CHUNK*2,forecast_transfer.CHUNK+17]:
            self.out=self.root/str(size)
            self.full.write_bytes(bytes(i%251 for i in range(size)))
            p=self.run_reader(APPEND='1')
            self.assertEqual(p.returncode,0,p.stderr)
            self.assertEqual(self.out.read_bytes(),self.full.read_bytes()[:size])

    def test_new_receipt_is_accepted_by_existing_participation_cli(self):
        # Keep this real-receipt integration independent of the benchmark's
        # future-dated fixtures and of the machine's current date.
        sample=datetime(2000,1,1,0,30,tzinfo=timezone.utc)
        self.full.write_text(json.dumps(dict(dec(sample,'tie'),application='nginx-test',namespace='demo'))+'\n')
        p=self.run_reader()
        self.assertEqual(p.returncode,0,p.stderr)
        raw,meta=forecast_log.load_verified(self.out,Path(str(self.out)+'.receipt.json'),
                                          sample+timedelta(minutes=1))
        self.assertEqual(raw,self.full.read_bytes())
        result=subprocess.run([sys.executable,str(self.script.parent/'score.py'),
                               '--forecast-log',str(self.out),'--forecast-receipt',str(self.out)+'.receipt.json',
                               '--start',iso(sample),'--end',iso(sample+timedelta(minutes=1)),
                               '--participation-only'],capture_output=True,text=True)
        self.assertEqual(result.returncode,0,result.stderr)
        self.assertFalse(json.loads(result.stdout)['forecast_log_provenance']['fixture'])

    def test_recreated_pvc_same_name_rejected(self):
        p=self.run_reader(UID_CHANGE='1');self.assertEqual(p.returncode,2,p.stdout+p.stderr)
        self.assertIn('identity changed',p.stderr)
        self.assertFalse(self.out.exists())
        self.assertFalse(Path(str(self.out)+'.receipt.json').exists())

    def test_multiple_operator_pods_fail_before_reader_created(self):
        p=self.run_reader(MULTI='1');self.assertEqual(p.returncode,2,p.stdout+p.stderr)
        self.assertNotIn(' run ',self.commands.read_text())

    def test_existing_output_refused_before_cluster_access(self):
        self.out.write_text('preserve')
        p=self.run_reader();self.assertEqual(p.returncode,2)
        self.assertEqual(self.out.read_text(),'preserve');self.assertFalse(self.commands.exists())

    def test_partial_modes_emit_no_receipt(self):
        for mode in ['--last','--count']:
            with self.subTest(mode=mode):
                self.out=self.root/mode
                p=self.run_reader(mode);self.assertEqual(p.returncode,0,p.stdout+p.stderr)
                self.assertFalse(Path(str(self.out)+'.receipt.json').exists())


class TransferDeadline(unittest.TestCase):
    def test_deadline_checked_again_after_final_identity(self):
        clock=[0.0];identities=[0]
        def execute(command,**kwargs):
            if 'get' in command:
                identities[0]+=1
                raw=json.dumps({'metadata':{'uid':'uid'},'status':{'containerStatuses':[{'name':'reader','restartCount':0}]}}).encode()
                if identities[0]==2:clock[0]=901
            else:raw=b'1\n'+hashlib.sha256(b'x').hexdigest().encode()+b'  chunk\nx'
            return subprocess.CompletedProcess(command,0,raw,b'')
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with patch.object(forecast_transfer.time,'monotonic',side_effect=lambda:clock[0]), \
                 patch.object(forecast_transfer.time,'time',return_value=1000), \
                 patch.object(forecast_transfer.subprocess,'run',side_effect=execute):
                with self.assertRaises(TimeoutError):
                    forecast_transfer.transfer('c','n','pod','/file',1,hashlib.sha256(b'x').hexdigest(),
                                               root/'prefix',root,('uid',0))
            self.assertNotIn('"event": "complete"',(root/'attempts.jsonl').read_text())

    def test_suspended_monotonic_clock_does_not_extend_wall_deadline(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            with patch.object(forecast_transfer.time,'monotonic',return_value=0), \
                 patch.object(forecast_transfer.time,'time',side_effect=[1000,1901]), \
                 patch.object(forecast_transfer.subprocess,'run') as execute:
                with self.assertRaises(TimeoutError):
                    forecast_transfer.transfer('c','n','pod','/file',1,hashlib.sha256(b'x').hexdigest(),
                                               root/'prefix',root,('uid',0))
                execute.assert_not_called()

    def test_combined_fingerprint_changes_when_helper_changes(self):
        with tempfile.TemporaryDirectory() as directory:
            root=Path(directory)
            (root/'read-forecast-log.sh').write_text('reader')
            helper=root/'forecast_transfer.py';helper.write_text('helper')
            old=forecast_transfer.reader_fingerprint(root)
            helper.write_text('changed helper')
            self.assertNotEqual(old,forecast_transfer.reader_fingerprint(root))


if __name__ == "__main__":
    unittest.main(verbosity=1)

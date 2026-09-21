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
    def test_sanity_rejected_event_excludes_its_issuance(self):
        r1 = rec(T0); r2 = rec(T0 + timedelta(minutes=5))
        ev = {"event": "sanity_rejected", "application": "a", "namespace": "n", "issued_at": r1["issued_at"],
              "rejected_at": r1["issued_at"], "near_prediction_rpm": 12000.0, "current_rpm": 120.0, "max_sane_rpm": 1200.0}
        acc, rej = score.accept_records([r1, ev, r2])
        self.assertEqual([r["issued_at"] for r in acc], [r2["issued_at"]])
        self.assertEqual(rej[0]["reason"], "sanity_rejected")
        self.assertEqual(len(rej), 1)  # the event line itself is neither accepted nor counted as rejected

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


if __name__ == "__main__":
    unittest.main(verbosity=1)

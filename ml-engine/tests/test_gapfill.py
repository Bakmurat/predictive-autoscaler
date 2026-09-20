"""Known-answer tests for the validity mask and the bounded interior-gap rule (data/gapfill.py) and
their use by the training preflight. Run without TensorFlow (model and collector stubbed)."""
import sys, types, unittest
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
for name in ("models", "models.lstm_model", "data.victoriametrics_collector"):
    if name not in sys.modules:
        mod = types.ModuleType(name)
        if name == "models.lstm_model": mod.LSTMForecastModel = object
        if name == "data.victoriametrics_collector": mod.VictoriaMetricsCollector = object
        sys.modules[name] = mod
from data import gapfill  # noqa: E402
from training import train_lstm_from_vm as t  # noqa: E402

T0 = 1_800_000_000 - (1_800_000_000 % 600)   # some epoch on the grid
G = 600


def grid(n, start=T0, drop=(), value=lambda i: 100.0 + i):
    return [(start + i * G, value(i)) for i in range(n) if i not in drop]


class Mask(unittest.TestCase):
    def test_intervals_and_history_start(self):
        mask = {"version": 7, "benchmark_history_start": gapfill._iso(T0 + 10 * G),
                "intervals": [{"start": gapfill._iso(T0 + 20 * G), "end": gapfill._iso(T0 + 22 * G), "reason": "node roll"}]}
        kept, info = gapfill.apply_mask(grid(40), mask, role="benchmark")
        self.assertEqual(len(kept), 40 - 10 - 3)
        self.assertEqual(info["dropped_before_history_start"], 10); self.assertEqual(info["dropped_in_intervals"], 3)
        self.assertEqual(info["intervals"][0]["dropped"], 3); self.assertEqual(info["mask_version"], 7)

    def test_diagnostic_role_keeps_pre_history_but_not_intervals(self):
        mask = {"benchmark_history_start": gapfill._iso(T0 + 10 * G),
                "intervals": [{"start": gapfill._iso(T0 + 20 * G), "end": gapfill._iso(T0 + 22 * G)}]}
        kept, info = gapfill.apply_mask(grid(40), mask, role="diagnostic")
        self.assertEqual(len(kept), 37); self.assertEqual(info["dropped_before_history_start"], 0)
        self.assertIsNone(info["benchmark_history_start"])

    def test_masked_interval_is_never_interpolated(self):
        mask = {"intervals": [{"start": gapfill._iso(T0 + 20 * G), "end": gapfill._iso(T0 + 21 * G)}]}
        kept, _ = gapfill.apply_mask(grid(40), mask)
        # the two masked slots form a 2-slot interior gap: fillable by the rule -> must be rejected upstream.
        # The trainer's preflight therefore applies the mask FIRST and passes the masked series to the fill
        # step with the mask intervals as hard breaks (verified in test_preflight_masked_interval_breaks_run).
        run, flags, rec = gapfill.fill_interior_gaps(kept, cutoff=T0 + 39 * G, forbidden=gapfill.mask_intervals(mask))
        self.assertEqual(rec["gaps_filled"], 0); self.assertIn("validity-mask", rec["gaps_considered"][0]["reason"])


class Fill(unittest.TestCase):
    def test_fills_up_to_three_slots_linearly(self):
        pts = grid(100, drop=(10, 11, 12))           # endpoints 9 and 13: 40 minutes apart; 3 % of the window
        run, flags, rec = gapfill.fill_interior_gaps(pts, cutoff=T0 + 99 * G)
        self.assertEqual(len(run), 100); self.assertEqual(sum(flags), 3)
        vals = dict(run)
        self.assertAlmostEqual(vals[T0 + 10 * G], 110.0); self.assertAlmostEqual(vals[T0 + 12 * G], 112.0)
        g = rec["gaps_considered"][0]
        self.assertTrue(g["filled"]); self.assertEqual(g["endpoint_gap_s"], 2400); self.assertEqual(g["algorithm"], "linear-v1")
        self.assertEqual(g["endpoints"][0][1], 109.0); self.assertEqual(len(g["filled_values"]), 3)

    def test_four_slot_gap_is_not_filled(self):
        pts = grid(30, drop=(10, 11, 12, 13))         # endpoints 50 minutes apart
        run, flags, rec = gapfill.fill_interior_gaps(pts, cutoff=T0 + 29 * G)
        self.assertEqual(rec["gaps_filled"], 0); self.assertIn("longer than 3 slots", rec["gaps_considered"][0]["reason"])
        self.assertEqual(len(run), 16)                # longest run = slots 14..29

    def test_never_extrapolates_missing_target_at_the_end(self):
        pts = grid(30)[:-2]                           # last two slots missing: no right endpoint
        run, flags, rec = gapfill.fill_interior_gaps(pts, cutoff=T0 + 29 * G)
        self.assertEqual(rec["gaps_filled"], 0); self.assertEqual(len(run), 28); self.assertFalse(any(flags))

    def test_endpoint_after_cutoff_is_not_used(self):
        pts = grid(30, drop=(20,))
        run, flags, rec = gapfill.fill_interior_gaps(pts, cutoff=T0 + 20 * G)   # right endpoint (21) is after the cutoff
        self.assertEqual(rec["gaps_filled"], 0); self.assertIn("cutoff", rec["gaps_considered"][0]["reason"])

    def test_boundary_gap_is_not_filled(self):
        pts = grid(30, drop=(24,))
        boundary = T0 + 24 * G                         # partition boundary exactly at the missing slot
        run, flags, rec = gapfill.fill_interior_gaps(pts, cutoff=T0 + 29 * G, boundaries=[boundary])
        self.assertEqual(rec["gaps_filled"], 0); self.assertIn("boundary", rec["gaps_considered"][0]["reason"])

    def test_percentage_cap(self):
        # 40 slots, four separate one-slot gaps = 10 % filled -> cap 5 % removes the latest gaps
        pts = grid(40, drop=(5, 15, 25, 35))
        run, flags, rec = gapfill.fill_interior_gaps(pts, cutoff=T0 + 39 * G)
        self.assertLessEqual(rec["fill_fraction"], 0.05)
        self.assertTrue(rec["cap_removed_gaps"])
        self.assertTrue(all(g["filled"] or "reason" in g for g in rec["gaps_considered"]))

    def test_leakage_partition_rule(self):
        # a gap whose endpoints straddle the boundary would use a later-partition observation: refused
        pts = grid(30, drop=(23, 24))
        run, flags, rec = gapfill.fill_interior_gaps(pts, cutoff=T0 + 29 * G, boundaries=[T0 + 24 * G])
        self.assertEqual(rec["gaps_filled"], 0)


class Inference(unittest.TestCase):
    def test_latest_must_be_fresh_and_observed(self):
        now = T0 + 40 * G
        with self.assertRaises(ValueError):
            gapfill.check_inference_window(grid(30), now=now, sequence_length=10)        # stale by 10 slots
        window, rec = gapfill.check_inference_window(grid(40), now=now + 30, sequence_length=10)
        self.assertEqual(len(window), 10); self.assertFalse(rec["latest_is_imputed"]); self.assertEqual(rec["imputed_in_window"], 0)

    def test_interior_gap_in_window_is_filled_and_reported(self):
        now = T0 + 39 * G + 30
        window, rec = gapfill.check_inference_window(grid(40, drop=(35,)), now=now, sequence_length=10)
        self.assertEqual(rec["imputed_in_window"], 1); self.assertEqual(rec["gaps_filled"], 1)

    def test_unfillable_gap_in_window_is_refused(self):
        now = T0 + 39 * G + 30
        with self.assertRaises(ValueError):
            gapfill.check_inference_window(grid(40, drop=(30, 31, 32, 33)), now=now, sequence_length=10)


class PreflightIntegration(unittest.TestCase):
    def df(self, pts):
        return pd.DataFrame({"timestamp": [pd.Timestamp(tt, unit="s", tz="UTC") for tt, _ in pts], "value": [v for _, v in pts]})

    def test_preflight_masked_interval_breaks_run(self):
        pts = grid(400)
        mask = {"version": 1, "intervals": [{"start": gapfill._iso(T0 + 200 * G), "end": gapfill._iso(T0 + 201 * G), "reason": "reset"}]}
        ok, why, prepared, info = t.preflight_history(self.df(pts), mask=mask)
        # masked slots are dropped BEFORE the fill step and must not be re-filled: the run breaks there
        self.assertTrue(ok); self.assertEqual(info["mask"]["dropped_in_intervals"], 2)
        self.assertEqual(info["gap_fill"]["gaps_filled"], 0)
        self.assertEqual(len(prepared), 200)          # runs: slots 0..199 (200) and 202..399 (198); the masked hole is a hard break
        self.assertEqual(info["gap_fill"]["gaps_considered"][0]["reason"].split(" (")[0], "gap overlaps a validity-mask interval")

    def test_preflight_fills_and_flags_and_recounts(self):
        pts = grid(400, drop=(300,))                  # inside the training rows, reachable as a target label
        ok, why, prepared, info = t.preflight_history(self.df(pts), mask={"intervals": []})
        self.assertTrue(ok); self.assertEqual(len(prepared), 400); self.assertEqual(int(prepared["imputed"].sum()), 1)
        b = info["sequence_budget"]
        self.assertLess(b["train_sequences_genuine_targets"], b["sequences"])   # sequences targeting the filled slot excluded

    def test_preflight_history_start_enforced(self):
        pts = grid(400)
        mask = {"benchmark_history_start": gapfill._iso(T0 + 300 * G), "intervals": []}
        ok, why, prepared, info = t.preflight_history(self.df(pts), mask=mask, role="benchmark")
        self.assertFalse(ok); self.assertIn("have 100 of 189", why)
        ok2, _, prepared2, _ = t.preflight_history(self.df(pts), mask=mask, role="diagnostic")
        self.assertTrue(ok2); self.assertEqual(len(prepared2), 400)


if __name__ == "__main__":
    unittest.main(verbosity=1)

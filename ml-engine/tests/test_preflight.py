"""Known-answer tests for the training preflight (ten-minute grid, sequence budget).

Runs without TensorFlow: the model and collector modules are stubbed before import.
"""
import sys, types, unittest
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
for name in ("models", "models.lstm_model", "data.victoriametrics_collector"):
    if name not in sys.modules:
        mod = types.ModuleType(name)
        if name == "models.lstm_model":
            mod.LSTMForecastModel = object
        if name == "data.victoriametrics_collector":
            mod.VictoriaMetricsCollector = object
        sys.modules[name] = mod
from training import train_lstm_from_vm as t  # noqa: E402

T0 = pd.Timestamp("2026-09-21T00:00:00Z")


def grid(n, start=T0, step_min=10, value=100.0):
    ts = [start + pd.Timedelta(minutes=step_min * i) for i in range(n)]
    return pd.DataFrame({"timestamp": ts, "value": [value + i for i in range(n)]})


class SequenceBudget(unittest.TestCase):
    def test_min_points_is_189_for_default_model(self):
        self.assertEqual(t.min_points_for_training(144, 6), 189)
        b = t.sequence_budget(189, 144, 6)
        self.assertEqual((b["train_rows"], b["sequences"], b["train_sequences"], b["validation_sequences"]), (151, 2, 1, 1))

    def test_188_points_is_rejected_189_accepted(self):
        ok188, why, _, _ = t.preflight_history(grid(188))
        ok189, _, prepared, info = t.preflight_history(grid(189))
        self.assertFalse(ok188); self.assertIn("have 188 of 189", why)
        self.assertTrue(ok189); self.assertEqual(len(prepared), 189)
        self.assertEqual(info["missing_slots"], 0)

    def test_smoke_window_needs_fewer_points(self):
        need = t.min_points_for_training(36, 6)
        self.assertLess(need, 189)
        ok, _, _, _ = t.preflight_history(grid(need), sequence_length=36)
        self.assertTrue(ok)


class GridChecks(unittest.TestCase):
    def test_duplicates_and_nonfinite_are_dropped(self):
        df = grid(189)
        df = pd.concat([df, df.iloc[[5, 6]]])            # duplicate timestamps
        df.loc[df.index[10], "value"] = np.nan             # non-finite
        df.loc[df.index[11], "value"] = np.inf
        ok, why, prepared, info = t.preflight_history(df)
        self.assertEqual(info["duplicates_dropped"], 2)
        self.assertEqual(info["nonfinite_dropped"], 2)
        # two adjacent slots lost -> a 2-slot interior telemetry gap: filled under the bounded rule (2/189 < 5 %)
        self.assertTrue(ok); self.assertEqual(int(prepared["imputed"].sum()), 2)
        self.assertEqual(info["gap_fill"]["gaps_filled"], 1); self.assertEqual(len(prepared), 189)

    def test_missing_slot_breaks_the_run(self):
        df = grid(400).drop(index=[200])                   # one missing slot in the middle
        ok, _, prepared, info = t.preflight_history(df)
        self.assertTrue(ok)
        self.assertEqual(info["missing_slots"], 1); self.assertEqual(info["contiguous_runs"], 2)
        # a single interior missing slot is a telemetry gap: filled (1/400 < 5 %), so the whole series is one run
        self.assertEqual(len(prepared), 400); self.assertEqual(int(prepared["imputed"].sum()), 1)
        self.assertEqual(info["contiguous_run_points"], 400)

    def test_offgrid_samples_are_rejected_not_snapped(self):
        df = grid(189, start=T0 + pd.Timedelta(minutes=3))  # 3 minutes off the grid
        ok, why, _, info = t.preflight_history(df)
        self.assertFalse(ok); self.assertEqual(info["offgrid_dropped"], 189)

    def test_small_jitter_is_snapped(self):
        df = grid(189, start=T0 + pd.Timedelta(seconds=20))
        ok, _, prepared, info = t.preflight_history(df)
        self.assertTrue(ok); self.assertEqual(info["offgrid_dropped"], 0)
        self.assertEqual(prepared["timestamp"].iloc[0], T0.tz_localize(None))   # snapped to its grid slot

    def test_one_minute_cadence_is_rejected(self):
        df = grid(600, step_min=1)
        ok, _, _, info = t.preflight_history(df)
        self.assertFalse(ok)
        self.assertEqual(info["offgrid_dropped"], 600 - 60)  # only every tenth sample is on the grid


if __name__ == "__main__":
    unittest.main(verbosity=1)

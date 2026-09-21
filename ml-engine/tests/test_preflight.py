"""Known-answer tests for the training preflight (ten-minute grid, sequence budget).

Runs without TensorFlow: the model and collector modules are stubbed before import.
"""
import sys, types, unittest
from pathlib import Path
import numpy as np
import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
# Stub the TensorFlow-dependent modules ONLY when they cannot be imported (no TensorFlow on the
# machine). Installing stubs unconditionally poisoned sys.modules for every later test in the
# session (the whole suite failed in-cluster on 2026-09-20), so the real modules win when present.
import importlib
for name, attr, stub in (("models.lstm_model", "LSTMForecastModel", object),
                         ("data.victoriametrics_collector", "VictoriaMetricsCollector", object)):
    try:
        importlib.import_module(name)
    except Exception:  # ImportError from a missing tensorflow, or its transitive failures
        pkg = name.split(".")[0]
        if pkg not in sys.modules:
            sys.modules[pkg] = types.ModuleType(pkg)
        mod = types.ModuleType(name)
        setattr(mod, attr, stub)
        sys.modules[name] = mod
from training import train_lstm_from_vm as t  # noqa: E402

T0 = pd.Timestamp("2026-09-21T00:00:00Z")


def grid(n, start=T0, step_min=10, value=100.0):
    ts = [start + pd.Timedelta(minutes=step_min * i) for i in range(n)]
    return pd.DataFrame({"timestamp": ts, "value": [value + i for i in range(n)]})


class SequenceBudget(unittest.TestCase):
    def test_min_points_is_235_for_default_model(self):
        """Codex C-57: 189 was the old preflight's answer, computed from an 80/20 split of
        the sequence LIST. The deployed model splits on disjoint TARGET periods, which needs
        int(0.8*int(0.8*n)) >= 150, i.e. n >= 235."""
        self.assertEqual(t.min_points_for_training(144, 6), 235)
        b = t.sequence_budget(235, 144, 6)
        self.assertEqual(b["train_rows"], 188)
        self.assertGreaterEqual(b["train_sequences"], 1)
        self.assertGreaterEqual(b["validation_sequences"], 1)

    def test_189_points_yields_no_training_sequence(self):
        """The number the old preflight published: the model raises there."""
        b = t.sequence_budget(189, 144, 6)
        self.assertEqual(b["train_sequences"], 0)

    def test_234_points_is_rejected_235_accepted(self):
        ok234, why, _, _ = t.preflight_history(grid(234))
        ok235, _, prepared, info = t.preflight_history(grid(235))
        self.assertFalse(ok234); self.assertIn("have 234 of 235", why)
        self.assertTrue(ok235); self.assertEqual(len(prepared), 235)
        self.assertEqual(info["missing_slots"], 0)

    def test_smoke_window_needs_fewer_points(self):
        need = t.min_points_for_training(36, 6)
        self.assertLess(need, 235)
        ok, _, _, _ = t.preflight_history(grid(need), sequence_length=36)
        self.assertTrue(ok)


class GridChecks(unittest.TestCase):
    def test_duplicates_and_nonfinite_are_dropped(self):
        df = grid(235)
        df = pd.concat([df, df.iloc[[5, 6]]])            # duplicate timestamps
        df.loc[df.index[10], "value"] = np.nan             # non-finite
        df.loc[df.index[11], "value"] = np.inf
        ok, why, prepared, info = t.preflight_history(df)
        self.assertEqual(info["duplicates_dropped"], 2)
        self.assertEqual(info["nonfinite_dropped"], 2)
        # two adjacent slots lost -> a 2-slot interior telemetry gap: filled under the bounded rule (2/235 < 5 %)
        self.assertTrue(ok); self.assertEqual(int(prepared["imputed"].sum()), 2)
        self.assertEqual(info["gap_fill"]["gaps_filled"], 1); self.assertEqual(len(prepared), 235)

    def test_missing_slot_breaks_the_run(self):
        df = grid(400).drop(index=[200])                   # one missing slot in the middle
        ok, _, prepared, info = t.preflight_history(df)
        self.assertTrue(ok)
        self.assertEqual(info["missing_slots"], 1); self.assertEqual(info["contiguous_runs"], 2)
        # a single interior missing slot is a telemetry gap: filled (1/400 < 5 %), so the whole series is one run
        self.assertEqual(len(prepared), 400); self.assertEqual(int(prepared["imputed"].sum()), 1)
        self.assertEqual(info["contiguous_run_points"], 400)

    def test_offgrid_samples_are_rejected_not_snapped(self):
        df = grid(235, start=T0 + pd.Timedelta(minutes=3))  # 3 minutes off the grid
        ok, why, _, info = t.preflight_history(df)
        self.assertFalse(ok); self.assertEqual(info["offgrid_dropped"], 235)

    def test_small_jitter_is_snapped(self):
        df = grid(235, start=T0 + pd.Timedelta(seconds=20))
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

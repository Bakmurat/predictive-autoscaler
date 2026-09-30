"""D-1068: the benchmark trainer trains the hybrid's network with a bounded activation (tanh).

The live record (26 trainings, 2026-09-23 18Z -> 2026-09-30 00Z) had 7 non-finite and 4 collapsed networks with the
shipped ReLU BiLSTM; the declared experiment (3 live windows x 4 seeds) reproduced a non-finite ReLU run, gradient
clipping (clipnorm 1.0) did not prevent it, and tanh had zero failures with the lowest mean network MAE. The model
class keeps 'relu' as its default for other callers; the trainer chooses explicitly and records it.
"""
import os
import sys

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from training import train_lstm_from_vm as t  # noqa: E402


def test_the_trainer_activation_defaults_to_tanh(monkeypatch):
    monkeypatch.delenv("TRAIN_ACTIVATION", raising=False)
    assert t.training_activation() == "tanh"


def test_an_override_is_validated(monkeypatch):
    monkeypatch.setenv("TRAIN_ACTIVATION", "relu")
    assert t.training_activation() == "relu"
    monkeypatch.setenv("TRAIN_ACTIVATION", "sigmoid")
    with pytest.raises(ValueError):
        t.training_activation()


def test_the_trainer_passes_the_activation_to_the_model(monkeypatch, tmp_path):
    monkeypatch.delenv("TRAIN_ACTIVATION", raising=False)
    captured = {}

    class Stub:
        def __init__(self, sequence_length=144):
            pass

        def train(self, *args, **kwargs):
            captured.update(kwargs)
            raise RuntimeError("stop after capturing the training arguments")

    monkeypatch.setattr(t, "LSTMForecastModel", Stub)
    idx = pd.date_range("2026-09-20", periods=400, freq="10min")
    df = pd.DataFrame({"timestamp": idx, "value": 1000 + 100 * np.sin(np.arange(400) / 20.0)})
    try:
        t.train_lstm_for_metric(df, "nginx-test", "requests", tmp_path, epochs=1)
    except Exception:
        pass
    assert captured.get("activation") == "tanh"

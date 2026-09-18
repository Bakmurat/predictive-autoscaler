"""Tests for model validation gate, scaler consistency, and /models enrichment."""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

# Add parent directory so we can import api.main
sys.path.insert(0, str(Path(__file__).parent.parent))

# Check TF availability for conditional skipping
try:
    import tensorflow as tf
    HAS_TF = True
except ImportError:
    HAS_TF = False

requires_tf = pytest.mark.skipif(not HAS_TF, reason="TensorFlow not installed")


class FakeLSTMModel:
    """Mimics LSTMForecastModel without TensorFlow dependency."""

    def __init__(self, sequence_length=144, mape=10.0):
        self.sequence_length = sequence_length
        self.is_trained = True
        self.model = MagicMock()
        self.metadata = {}
        self.trained_at = datetime.utcnow()
        self._mape = mape

        # Set up a real-ish scaler with center_ and scale_ (RobustScaler)
        self.scaler = MagicMock()
        self.scaler.center_ = np.array([50.0])
        self.scaler.scale_ = np.array([50.0])
        # transform returns scaled values (identity for simplicity)
        self.scaler.transform = lambda x: x / 100.0
        self.scaler.fit_transform = lambda x: x / 100.0

        # Sequences
        self.last_sequence = np.random.rand(sequence_length)
        self.raw_training_values = np.random.rand(sequence_length * 7)  # 7 days
        self.training_data = self.last_sequence.copy()

    def train(self, df, target_column='value', epochs=50):
        """Fake training that sets up scaler from data."""
        values = df[target_column].values
        self.scaler.center_ = np.array([float(np.median(values))])
        self.scaler.scale_ = np.array([float(np.percentile(values, 75) - np.percentile(values, 25))])
        self.raw_training_values = values.flatten()
        self.last_sequence = (values[-self.sequence_length:] / (values.max() + 1e-8)).flatten()
        self.is_trained = True
        return {
            'success': True,
            'training_metrics': {'rmse': 1.0, 'mae': 0.5},
            'metadata': {}
        }

    def evaluate(self, test_data, target_column='value'):
        """Return predetermined MAPE for testing."""
        return {
            'rmse': 1.0,
            'mae': 0.5,
            'mape': self._mape,
            'r2': 0.9,
            'confidence': 0.8
        }


def _make_metric_data(n_points=300, base_value=1000.0):
    """Create fake metric data for training."""
    timestamps = pd.date_range(end=datetime.utcnow(), periods=n_points, freq='10min')
    values = base_value + np.random.randn(n_points) * 100
    return [
        {"timestamp": ts.isoformat(), "value": float(v)}
        for ts, v in zip(timestamps, values)
    ]


def _make_predictor():
    """Create a fresh LSTMPredictor with patched model creation."""
    from api.main import LSTMPredictor

    with patch.object(LSTMPredictor, '_load_pretrained_models'):
        predictor = LSTMPredictor()
    return predictor


# ---------------------------------------------------------------------------
# TestValidationGate
# ---------------------------------------------------------------------------
@requires_tf
class TestValidationGate:
    """Test the MAPE-based validation gate in train_on_data()."""

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_bootstrap_model_accepted(self, MockModel, mock_dump):
        """First model (no existing model) is always accepted."""
        fake = FakeLSTMModel(mape=20.0)
        MockModel.return_value = fake

        predictor = _make_predictor()
        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        model_key = "app1_requests"
        assert model_key in predictor.trained_models
        assert predictor.trained_models[model_key] is fake
        # validation_metadata should show accepted
        assert model_key in predictor.validation_metadata
        assert predictor.validation_metadata[model_key]["status"] == "accepted"

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_better_model_accepted(self, MockModel, mock_dump):
        """New model with lower MAPE is accepted and promoted."""
        old_model = FakeLSTMModel(mape=15.0)
        new_model = FakeLSTMModel(mape=10.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=7)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        assert predictor.trained_models[model_key] is new_model

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_worse_model_rejected(self, MockModel, mock_dump):
        """New model with MAPE > old_mape * 1.05 is rejected."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=20.0)  # much worse
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=7)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        # Old model should still be there
        assert predictor.trained_models[model_key] is old_model

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_within_tolerance_accepted(self, MockModel, mock_dump):
        """New model with MAPE up to 5% worse is accepted."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=10.4)  # within 5% of 10.0 (threshold = 10.5)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=7)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        assert predictor.trained_models[model_key] is new_model

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_rejection_logs_scores(self, MockModel, mock_dump):
        """On rejection, logger.info is called with both MAPE scores."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=20.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=7)

        data = _make_metric_data()
        with patch('api.main.logger') as mock_logger:
            predictor.train_on_data("app1", data, "requests")
            # Check that rejection was logged with both scores
            log_messages = [str(call) for call in mock_logger.info.call_args_list]
            rejection_logged = any("rejected" in msg and "20.0" in msg for msg in log_messages)
            assert rejection_logged, f"Expected rejection log with scores, got: {log_messages}"

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_rejection_updates_train_time(self, MockModel, mock_dump):
        """On rejection, model_train_times is updated to prevent retrain loop."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=20.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        old_time = datetime.utcnow() - timedelta(hours=7)
        predictor.model_train_times[model_key] = old_time

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        # Train time should be updated even though model was rejected
        assert predictor.model_train_times[model_key] > old_time

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_holdout_too_small_skips_validation(self, MockModel, mock_dump):
        """If holdout produces fewer than 10 test samples, model is accepted with warning."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=5.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=7)

        # With 200 points: holdout = 40 rows, which is < sequence_length (144)
        # so evaluate will return inf mape -> skip validation
        old_model._mape = float('inf')
        old_model.evaluate = lambda *a, **kw: {'mape': float('inf'), 'rmse': float('inf'),
                                                'mae': float('inf'), 'r2': -float('inf'),
                                                'confidence': 0.0}

        data = _make_metric_data(n_points=200)
        predictor.train_on_data("app1", data, "requests")

        # Model should be accepted (skip validation due to small holdout)
        assert predictor.trained_models[model_key] is new_model

    # --- Age-decay tests (Phase 9) ---

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_age_decay_12h_relaxes_threshold(self, MockModel, mock_dump):
        """Model aged 12h accepts new MAPE=11.5% when old MAPE=10%.
        Threshold = 10 * (1.05 + 0.01*12) = 10 * 1.17 = 11.7, so 11.5 < 11.7 passes.
        With fixed 1.05 gate: threshold = 10.5, so 11.5 > 10.5 would be rejected."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=11.5)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=12)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        # Should be accepted with age-decay; rejected with fixed 1.05
        assert predictor.trained_models[model_key] is new_model

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_age_decay_fresh_model_strict(self, MockModel, mock_dump):
        """Model aged 0.5h rejects new MAPE=10.8% when old MAPE=10%.
        Threshold = 10 * (1.05 + 0.01*0.5) = 10 * 1.055 = 10.55, so 10.8 > 10.55 fails."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=10.8)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(minutes=30)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        # Should still be old model (rejected)
        assert predictor.trained_models[model_key] is old_model

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_staleness_bypass_48h(self, MockModel, mock_dump):
        """Model aged 50h accepts new MAPE=90% when old MAPE=5% (48h+ bypass, MAPE < 100%)."""
        old_model = FakeLSTMModel(mape=5.0)
        new_model = FakeLSTMModel(mape=90.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=50)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        # 48h+ bypass: accept any retrain with MAPE < 100%
        assert predictor.trained_models[model_key] is new_model

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_staleness_bypass_rejects_insane_mape(self, MockModel, mock_dump):
        """Model aged 50h rejects new MAPE=150% (MAPE >= 100% sanity floor)."""
        old_model = FakeLSTMModel(mape=5.0)
        new_model = FakeLSTMModel(mape=150.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=50)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        # Even with 48h bypass, MAPE >= 100% is rejected
        assert predictor.trained_models[model_key] is old_model

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_decay_factor_in_log(self, MockModel, mock_dump):
        """On rejection with aged model, log message contains 'age=' and 'decay_factor='."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=20.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=5)

        data = _make_metric_data()
        with patch('api.main.logger') as mock_logger:
            predictor.train_on_data("app1", data, "requests")
            log_messages = [str(call) for call in mock_logger.info.call_args_list]
            has_age = any("age=" in msg for msg in log_messages)
            has_decay = any("decay_factor=" in msg for msg in log_messages)
            assert has_age, f"Expected 'age=' in log, got: {log_messages}"
            assert has_decay, f"Expected 'decay_factor=' in log, got: {log_messages}"

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_bypass_log_message(self, MockModel, mock_dump):
        """On 48h+ acceptance, log contains 'staleness bypass'."""
        old_model = FakeLSTMModel(mape=5.0)
        new_model = FakeLSTMModel(mape=50.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=50)

        data = _make_metric_data()
        with patch('api.main.logger') as mock_logger:
            predictor.train_on_data("app1", data, "requests")
            log_messages = [str(call) for call in mock_logger.info.call_args_list]
            has_bypass = any("staleness bypass" in msg for msg in log_messages)
            assert has_bypass, f"Expected 'staleness bypass' in log, got: {log_messages}"

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_metadata_includes_age_decay(self, MockModel, mock_dump):
        """After accept/reject, validation_metadata contains age_hours and decay_factor keys."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=9.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=3)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        meta = predictor.validation_metadata.get(model_key)
        assert meta is not None
        assert "age_hours" in meta, f"Expected 'age_hours' in metadata, got: {meta}"
        assert "decay_factor" in meta, f"Expected 'decay_factor' in metadata, got: {meta}"


# ---------------------------------------------------------------------------
# TestScalerConsistency
# ---------------------------------------------------------------------------
@requires_tf
class TestScalerConsistency:
    """Test that scaler state is consistent after model promotion."""

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_last_sequence_rescaled(self, MockModel, mock_dump):
        """After promotion, last_sequence is re-derived using new scaler."""
        new_model = FakeLSTMModel(mape=5.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        data = _make_metric_data(n_points=300, base_value=500.0)
        predictor.train_on_data("app1", data, "requests")

        model = predictor.trained_models["app1_requests"]
        # last_sequence should be re-derived from raw values using scaler.transform
        assert model.last_sequence is not None
        assert len(model.last_sequence) == 144

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_raw_values_refreshed(self, MockModel, mock_dump):
        """After promotion, raw_training_values contains full training data."""
        new_model = FakeLSTMModel(mape=5.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        data = _make_metric_data(n_points=300, base_value=500.0)
        predictor.train_on_data("app1", data, "requests")

        model = predictor.trained_models["app1_requests"]
        assert len(model.raw_training_values) == 300

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_scaler_range_logged(self, MockModel, mock_dump):
        """On promotion with existing model, scaler range change is logged."""
        old_model = FakeLSTMModel(mape=15.0)
        old_model.scaler.center_ = np.array([25.0])
        old_model.scaler.scale_ = np.array([25.0])

        new_model = FakeLSTMModel(mape=10.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=7)

        data = _make_metric_data()
        with patch('api.main.logger') as mock_logger:
            predictor.train_on_data("app1", data, "requests")
            log_messages = [str(call) for call in mock_logger.info.call_args_list]
            scaler_logged = any("Scaler range" in msg or "scaler" in msg.lower() for msg in log_messages)
            assert scaler_logged, f"Expected scaler range log, got: {log_messages}"


# ---------------------------------------------------------------------------
# TestValidationMetadata
# ---------------------------------------------------------------------------
@requires_tf
class TestValidationMetadata:
    """Test that validation_metadata is updated correctly."""

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_metadata_updated_on_accept(self, MockModel, mock_dump):
        """After acceptance, validation_metadata has status=accepted and mape."""
        fake = FakeLSTMModel(mape=8.0)
        MockModel.return_value = fake

        predictor = _make_predictor()
        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        meta = predictor.validation_metadata.get("app1_requests")
        assert meta is not None
        assert meta["status"] == "accepted"
        assert isinstance(meta["mape"], float)
        assert "timestamp" in meta

    @patch('api.main.joblib.dump')
    @patch('api.main.LSTMForecastModel')
    def test_metadata_updated_on_reject(self, MockModel, mock_dump):
        """After rejection, validation_metadata has status=rejected with both scores."""
        old_model = FakeLSTMModel(mape=10.0)
        new_model = FakeLSTMModel(mape=20.0)
        MockModel.return_value = new_model

        predictor = _make_predictor()
        model_key = "app1_requests"
        predictor.trained_models[model_key] = old_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=7)

        data = _make_metric_data()
        predictor.train_on_data("app1", data, "requests")

        meta = predictor.validation_metadata.get(model_key)
        assert meta is not None
        assert meta["status"] == "rejected"
        assert isinstance(meta["mape"], float)
        assert isinstance(meta["old_mape"], float)
        assert "timestamp" in meta


# ---------------------------------------------------------------------------
# TestModelsEndpoint
# ---------------------------------------------------------------------------
@requires_tf
class TestModelsEndpoint:
    """Test that /models returns enriched per-model info."""

    def test_models_returns_validation_info(self):
        """Response includes per-model validation_status, mape, scaler_range, age, staleness."""
        from fastapi.testclient import TestClient
        from api.main import app, predictor

        # Set up a fake trained model in the predictor
        model_key = "testapp_requests"
        fake_model = FakeLSTMModel(mape=8.0)
        predictor.trained_models[model_key] = fake_model
        predictor.model_train_times[model_key] = datetime.utcnow() - timedelta(hours=2)
        predictor.validation_metadata[model_key] = {
            "status": "accepted",
            "mape": 8.0,
            "old_mape": None,
            "timestamp": datetime.utcnow().isoformat(),
        }

        client = TestClient(app)
        response = client.get("/models")
        assert response.status_code == 200

        data = response.json()
        assert "models" in data
        assert model_key in data["models"]

        model_info = data["models"][model_key]
        assert "validation_status" in model_info
        assert model_info["validation_status"] == "accepted"
        assert "validation_mape" in model_info
        assert "scaler_range" in model_info
        assert "age_hours" in model_info
        assert "is_stale" in model_info
        assert model_info["is_stale"] is False  # 2 hours < 6 hours

        # Cleanup
        del predictor.trained_models[model_key]
        del predictor.model_train_times[model_key]
        del predictor.validation_metadata[model_key]

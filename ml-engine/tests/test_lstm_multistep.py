"""Tests for Dense(6) multi-step LSTM architecture upgrade.

Covers requirements PRED-04, PRED-05, INFRA-03, and decisions D-01 through D-16.
Tests: Dense(6) output shape, multi-step asymmetric loss, sequence creation,
time features, input shape, RobustScaler, model version detection, floor=0.0,
no autoregressive loop, inverse transform, evaluate multi-step.
"""

import sys
from pathlib import Path
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta

import numpy as np
import pandas as pd
import pytest

# Add parent directory so we can import models
sys.path.insert(0, str(Path(__file__).parent.parent))

# Check TF availability for conditional skipping
try:
    import tensorflow as tf
    HAS_TF = True
except ImportError:
    HAS_TF = False

requires_tf = pytest.mark.skipif(not HAS_TF, reason="TensorFlow not installed")


def _make_synthetic_data(n_points=300, base_value=1000.0):
    """Create synthetic sine wave + noise data with DatetimeIndex at 10-min intervals."""
    np.random.seed(42)
    timestamps = pd.date_range(end=datetime.utcnow(), periods=n_points, freq='10min')
    # Sine wave with daily period + noise for realistic training
    hours = np.arange(n_points) * 10 / 60.0  # hours
    values = base_value + 500 * np.sin(2 * np.pi * hours / 24.0) + np.random.randn(n_points) * 50
    values = np.maximum(values, 10)  # no negative values
    return pd.DataFrame({'value': values}, index=timestamps)


# ---------------------------------------------------------------------------
# TestDense6OutputShape (PRED-04)
# ---------------------------------------------------------------------------
@requires_tf
class TestDense6OutputShape:
    """Test that model builds with Dense(6) output."""

    def test_dense6_output_shape(self):
        """Build model, verify output_shape[-1] == 6."""
        from models.lstm_model import LSTMForecastModel

        df = _make_synthetic_data(n_points=250)
        model = LSTMForecastModel(sequence_length=144)
        model.train(df, target_column='value', epochs=1)

        output_shape = model.model.output_shape
        assert output_shape[-1] == 6, (
            f"Expected Dense(6) output shape ending in 6, got {output_shape}"
        )


# ---------------------------------------------------------------------------
# TestAsymmetricLossMultistep (PRED-04, D-03)
# ---------------------------------------------------------------------------
@requires_tf
class TestAsymmetricLossMultistep:
    """Test asymmetric loss with (batch, 6) shaped tensors and step decay."""

    def test_asymmetric_loss_multistep_is_scalar(self):
        """Loss on (4, 6) tensors produces a scalar."""
        from models.lstm_model import asymmetric_mse

        y_true = tf.constant(np.random.rand(4, 6).astype(np.float32))
        y_pred = tf.constant(np.random.rand(4, 6).astype(np.float32))
        loss = asymmetric_mse(y_true, y_pred)
        assert loss.shape == (), f"Expected scalar loss, got shape {loss.shape}"

    def test_asymmetric_loss_under_gt_over(self):
        """Under-prediction penalty > over-prediction for same absolute error."""
        from models.lstm_model import asymmetric_mse

        # Under-prediction: actual > predicted
        y_true_under = tf.constant([[10.0, 10.0, 10.0, 10.0, 10.0, 10.0]])
        y_pred_under = tf.constant([[8.0, 8.0, 8.0, 8.0, 8.0, 8.0]])
        loss_under = asymmetric_mse(y_true_under, y_pred_under).numpy()

        # Over-prediction: actual < predicted
        y_true_over = tf.constant([[10.0, 10.0, 10.0, 10.0, 10.0, 10.0]])
        y_pred_over = tf.constant([[12.0, 12.0, 12.0, 12.0, 12.0, 12.0]])
        loss_over = asymmetric_mse(y_true_over, y_pred_over).numpy()

        assert loss_under > loss_over, (
            f"Under-prediction loss ({loss_under}) should be > over-prediction ({loss_over})"
        )


# ---------------------------------------------------------------------------
# TestSequenceCreationMultistep (PRED-04, D-04)
# ---------------------------------------------------------------------------
@requires_tf
class TestSequenceCreationMultistep:
    """Test _create_sequences produces multi-step targets."""

    def test_sequence_creation_multistep(self):
        """200-point sequence produces X shape (N, 144, 5) and y shape (N, 6)."""
        from models.lstm_model import LSTMForecastModel, generate_time_features

        model = LSTMForecastModel(sequence_length=144)
        scaled_values = np.random.rand(200)
        timestamps = pd.date_range(end=datetime.utcnow(), periods=200, freq='10min')
        time_features = generate_time_features(timestamps)

        X, y = model._create_sequences(scaled_values, time_features)

        # Expected: 200 - 144 - 6 + 1 = 51 sequences
        assert X.shape[1] == 144, f"Expected seq_length 144, got {X.shape[1]}"
        assert X.shape[2] == 5, f"Expected 5 features, got {X.shape[2]}"
        assert y.shape[1] == 6, f"Expected 6 target steps, got {y.shape[1]}"
        assert X.shape[0] == y.shape[0], "X and y sample counts must match"


# ---------------------------------------------------------------------------
# TestTimeFeatures (PRED-05, D-05)
# ---------------------------------------------------------------------------
class TestTimeFeatures:
    """Test cyclical time feature generation."""

    def test_time_features_shape(self):
        """generate_time_features returns shape (N, 4) with values in [-1, 1]."""
        from models.lstm_model import generate_time_features

        timestamps = pd.date_range('2026-01-01', periods=50, freq='10min')
        features = generate_time_features(timestamps)

        assert features.shape == (50, 4), f"Expected (50, 4), got {features.shape}"
        assert np.all(features >= -1.0) and np.all(features <= 1.0), \
            "All sin/cos values should be in [-1, 1]"

    def test_time_features_known_values(self):
        """Midnight UTC -> hour_sin=0, hour_cos=1. 6 AM -> hour_sin=1, hour_cos~0."""
        from models.lstm_model import generate_time_features

        # Midnight UTC on a Monday (weekday=0)
        midnight = pd.Timestamp('2026-01-05 00:00:00')  # Monday
        features = generate_time_features([midnight])
        hour_sin, hour_cos, dow_sin, dow_cos = features[0]

        assert abs(hour_sin - 0.0) < 1e-6, f"Midnight hour_sin should be 0, got {hour_sin}"
        assert abs(hour_cos - 1.0) < 1e-6, f"Midnight hour_cos should be 1, got {hour_cos}"
        assert abs(dow_sin - 0.0) < 0.01, f"Monday dow_sin should be ~0, got {dow_sin}"
        assert abs(dow_cos - 1.0) < 0.01, f"Monday dow_cos should be ~1, got {dow_cos}"

        # 6 AM UTC
        six_am = pd.Timestamp('2026-01-05 06:00:00')
        features_6 = generate_time_features([six_am])
        h_sin, h_cos, _, _ = features_6[0]

        assert abs(h_sin - 1.0) < 1e-6, f"6AM hour_sin should be 1, got {h_sin}"
        assert abs(h_cos - 0.0) < 1e-5, f"6AM hour_cos should be ~0, got {h_cos}"


# ---------------------------------------------------------------------------
# TestInputShape (PRED-05, D-05)
# ---------------------------------------------------------------------------
@requires_tf
class TestInputShape:
    """Test that trained model has input_shape (None, 144, 5)."""

    def test_input_shape(self):
        """After train(), model input_shape == (None, 144, 5)."""
        from models.lstm_model import LSTMForecastModel

        df = _make_synthetic_data(n_points=250)
        model = LSTMForecastModel(sequence_length=144)
        model.train(df, target_column='value', epochs=1)

        input_shape = model.model.input_shape
        assert input_shape == (None, 144, 5), (
            f"Expected input shape (None, 144, 5), got {input_shape}"
        )


# ---------------------------------------------------------------------------
# TestRobustScaler (INFRA-03, D-08)
# ---------------------------------------------------------------------------
@requires_tf
class TestRobustScaler:
    """Test that RobustScaler is used instead of MinMaxScaler."""

    def test_robust_scaler(self):
        """After train(), scaler is RobustScaler with center_ and scale_ attributes."""
        from sklearn.preprocessing import RobustScaler
        from models.lstm_model import LSTMForecastModel

        df = _make_synthetic_data(n_points=250)
        model = LSTMForecastModel(sequence_length=144)
        model.train(df, target_column='value', epochs=1)

        assert isinstance(model.scaler, RobustScaler), (
            f"Expected RobustScaler, got {type(model.scaler).__name__}"
        )
        assert hasattr(model.scaler, 'center_'), "RobustScaler should have center_ attribute"
        assert hasattr(model.scaler, 'scale_'), "RobustScaler should have scale_ attribute"


# ---------------------------------------------------------------------------
# TestNoMinMaxAttrs (INFRA-03)
# ---------------------------------------------------------------------------
class TestNoMinMaxAttrs:
    """Verify MinMaxScaler is not referenced in source files."""

    def test_no_minmax_in_lstm_model(self):
        """lstm_model.py must not contain 'MinMaxScaler'."""
        lstm_path = Path(__file__).parent.parent / "models" / "lstm_model.py"
        content = lstm_path.read_text()
        assert "MinMaxScaler" not in content, (
            "lstm_model.py should not reference MinMaxScaler"
        )


# ---------------------------------------------------------------------------
# TestVersionDetection (D-10, D-12)
# ---------------------------------------------------------------------------
@requires_tf
class TestVersionDetection:
    """Test model version detection for old vs new format."""

    def test_old_model_detected(self):
        """Mock model with output_shape (None, 1) -> _is_old_model_format returns True."""
        from models.lstm_model import LSTMForecastModel

        mock_model_obj = MagicMock()
        mock_model_obj.model = MagicMock()
        mock_model_obj.model.output_shape = (None, 1)

        assert LSTMForecastModel._is_old_model_format(mock_model_obj) is True

    def test_new_model_detected(self):
        """Mock model with output_shape (None, 6) -> _is_old_model_format returns False."""
        from models.lstm_model import LSTMForecastModel

        mock_model_obj = MagicMock()
        mock_model_obj.model = MagicMock()
        mock_model_obj.model.output_shape = (None, 6)

        assert LSTMForecastModel._is_old_model_format(mock_model_obj) is False

    def test_no_model_attr_is_old(self):
        """Object without .model attribute -> returns True."""
        from models.lstm_model import LSTMForecastModel

        mock_obj = MagicMock(spec=[])  # no attributes
        assert LSTMForecastModel._is_old_model_format(mock_obj) is True


# ---------------------------------------------------------------------------
# TestFloorZero (D-13, D-15)
# ---------------------------------------------------------------------------
@requires_tf
class TestFloorZero:
    """Test that safety floor is set to 0.0."""

    def test_floor_zero(self):
        """predict() returns floor_pct == 0.0."""
        from models.lstm_model import LSTMForecastModel

        df = _make_synthetic_data(n_points=250)
        model = LSTMForecastModel(sequence_length=144)
        model.train(df, target_column='value', epochs=1)

        result = model.predict(steps_ahead=6)
        assert result['floor_pct'] == 0.0, (
            f"Expected floor_pct=0.0, got {result['floor_pct']}"
        )


# ---------------------------------------------------------------------------
# TestPredictNoAutoregressive (PRED-04)
# ---------------------------------------------------------------------------
@requires_tf
class TestPredictNoAutoregressive:
    """Verify predict() makes exactly 1 call to model.predict (not 6)."""

    def test_predict_no_autoregressive(self):
        """predict() makes exactly 1 model.predict call."""
        from models.lstm_model import LSTMForecastModel

        df = _make_synthetic_data(n_points=250)
        model = LSTMForecastModel(sequence_length=144)
        model.train(df, target_column='value', epochs=1)

        # Wrap the keras model's predict to count calls
        original_predict = model.model.predict
        call_count = [0]

        def counting_predict(*args, **kwargs):
            call_count[0] += 1
            return original_predict(*args, **kwargs)

        model.model.predict = counting_predict
        model.predict(steps_ahead=6)

        assert call_count[0] == 1, (
            f"Expected exactly 1 model.predict call, got {call_count[0]}"
        )


# ---------------------------------------------------------------------------
# TestMultistepInverseTransform (PRED-04)
# ---------------------------------------------------------------------------
@requires_tf
class TestMultistepInverseTransform:
    """Test that Dense(6) scaled output is correctly inverse-transformed."""

    def test_multistep_inverse_transform(self):
        """predict() returns 6 RPM values (not scaled, positive)."""
        from models.lstm_model import LSTMForecastModel

        df = _make_synthetic_data(n_points=250, base_value=5000.0)
        model = LSTMForecastModel(sequence_length=144)
        model.train(df, target_column='value', epochs=1)

        result = model.predict(steps_ahead=6)
        predictions = result['predictions']

        assert len(predictions) == 6, f"Expected 6 predictions, got {len(predictions)}"
        # All predictions should be positive RPM values
        for i, p in enumerate(predictions):
            assert p > 0, f"Prediction step {i} should be positive, got {p}"


# ---------------------------------------------------------------------------
# TestEvaluateMultistep (PRED-04)
# ---------------------------------------------------------------------------
@requires_tf
class TestEvaluateMultistep:
    """Test evaluate() works with Dense(6) model."""

    def test_evaluate_multistep(self):
        """evaluate() on Dense(6) model returns valid metrics."""
        from models.lstm_model import LSTMForecastModel

        # The test partition must hold at least sequence_length + STEPS_AHEAD points, otherwise
        # evaluate() reports "unavailable" (None metrics) instead of infinities - see the next test.
        df = _make_synthetic_data(n_points=600, base_value=3000.0)
        train_df = df.iloc[:400]
        test_df = df.iloc[400:]

        model = LSTMForecastModel(sequence_length=144)
        model.train(train_df, target_column='value', epochs=1)

        eval_result = model.evaluate(test_df, target_column='value')
        assert 'rmse' in eval_result
        assert 'mae' in eval_result
        assert 'mape' in eval_result
        assert eval_result['rmse'] is not None and eval_result['rmse'] >= 0
        assert eval_result['mae'] is not None and eval_result['mae'] >= 0

    def test_evaluate_short_partition_is_unavailable_not_infinite(self):
        """A test partition shorter than sequence_length + STEPS_AHEAD yields None metrics and an
        explicit 'unavailable' reason (never inf), per the training-preflight contract."""
        from models.lstm_model import LSTMForecastModel

        df = _make_synthetic_data(n_points=400, base_value=3000.0)
        model = LSTMForecastModel(sequence_length=144)
        model.train(df.iloc[:300], target_column='value', epochs=1)

        eval_result = model.evaluate(df.iloc[300:], target_column='value')
        assert eval_result['rmse'] is None and eval_result['mae'] is None and eval_result['mape'] is None
        assert str(eval_result.get('evaluation', '')).startswith('unavailable')

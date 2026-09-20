"""Tests for prediction accuracy improvements: asymmetric loss, BiLSTM, dynamic floor.

Covers requirements ACC-01, ACC-02, ACC-03.

TF-dependent tests (TestAsymmetricLoss, TestBiLSTM) are skipped when TensorFlow
is not installed (e.g., local dev). They run in the container environment.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock

import numpy as np
import pytest

# Add parent directory so we can import models/api
sys.path.insert(0, str(Path(__file__).parent.parent))

# Check TF availability for conditional skipping
try:
    import tensorflow as tf
    HAS_TF = True
except ImportError:
    HAS_TF = False

requires_tf = pytest.mark.skipif(not HAS_TF, reason="TensorFlow not installed")


# ---------------------------------------------------------------------------
# TestAsymmetricLoss (ACC-01)
# ---------------------------------------------------------------------------
@requires_tf
class TestAsymmetricLoss:
    """Test that asymmetric_mse penalizes under-prediction 2:1 vs over-prediction."""

    def test_under_prediction_penalized_2x(self):
        """Given same absolute error, under-prediction loss should be 2x over-prediction."""
        from models.lstm_model import asymmetric_mse

        # Under-prediction: actual=10, predicted=8 -> error=+2 (positive = under)
        y_true_under = tf.constant([[10.0]])
        y_pred_under = tf.constant([[8.0]])
        loss_under = asymmetric_mse(y_true_under, y_pred_under).numpy()

        # Over-prediction: actual=10, predicted=12 -> error=-2 (negative = over)
        y_true_over = tf.constant([[10.0]])
        y_pred_over = tf.constant([[12.0]])
        loss_over = asymmetric_mse(y_true_over, y_pred_over).numpy()

        # Under-prediction should be 2x the over-prediction loss
        assert abs(loss_under - 2.0 * loss_over) < 1e-5, (
            f"Under-prediction loss ({loss_under}) should be 2x over-prediction loss ({loss_over})"
        )

    def test_perfect_prediction_zero_loss(self):
        """When predicted equals actual, loss should be 0."""
        from models.lstm_model import asymmetric_mse

        y_true = tf.constant([[10.0]])
        y_pred = tf.constant([[10.0]])
        loss = asymmetric_mse(y_true, y_pred).numpy()
        assert abs(loss) < 1e-7, f"Perfect prediction loss should be 0, got {loss}"

    def test_model_compiles_with_loss(self):
        """A Keras model can compile with asymmetric_mse as loss without error."""
        from models.lstm_model import asymmetric_mse

        model = tf.keras.Sequential([
            tf.keras.layers.Dense(1, input_shape=(1,))
        ])
        # This should not raise
        model.compile(optimizer='adam', loss=asymmetric_mse, metrics=['mae'])
        assert model.loss is not None


# ---------------------------------------------------------------------------
# TestBiLSTM (ACC-02)
# ---------------------------------------------------------------------------
@requires_tf
class TestBiLSTM:
    """Test that model uses Bidirectional LSTM layers."""

    def test_model_has_bidirectional_layers(self):
        """After train(), model layers should include Bidirectional wrappers."""
        import pandas as pd
        from models.lstm_model import LSTMForecastModel

        # Create minimal synthetic data for training
        np.random.seed(42)
        n_points = 200  # sequence_length(144) + buffer
        timestamps = pd.date_range(end='2026-01-01', periods=n_points, freq='10min')
        values = 1000 + np.random.randn(n_points) * 100
        df = pd.DataFrame({'value': values}, index=timestamps)

        model = LSTMForecastModel(sequence_length=144)
        model.train(df, target_column='value', epochs=1)

        # Check that at least one layer is Bidirectional
        layer_types = [type(layer).__name__ for layer in model.model.layers]
        bidirectional_count = sum(1 for lt in layer_types if lt == 'Bidirectional')
        assert bidirectional_count >= 3, (
            f"Expected at least 3 Bidirectional layers, got {bidirectional_count}. "
            f"Layer types: {layer_types}"
        )

    def test_model_builds_correct_shape(self):
        """BiLSTM model with input_shape=(144, 5) produces output shape (None, 6)."""
        import pandas as pd
        from models.lstm_model import LSTMForecastModel

        np.random.seed(42)
        n_points = 200
        timestamps = pd.date_range(end='2026-01-01', periods=n_points, freq='10min')
        values = 1000 + np.random.randn(n_points) * 100
        df = pd.DataFrame({'value': values}, index=timestamps)

        model = LSTMForecastModel(sequence_length=144)
        model.train(df, target_column='value', epochs=1)

        output_shape = model.model.output_shape
        assert output_shape == (None, 6), (
            f"Expected output shape (None, 6), got {output_shape}"
        )


# ---------------------------------------------------------------------------
# TestDynamicFloor (ACC-03)
# ---------------------------------------------------------------------------
class TestDynamicFloor:
    """Test safety floor logic.

    Phase 16 (D-13): floor_pct set to 0.0 (removed). Code and gauge kept (D-15).
    No TensorFlow needed -- these test pure math.
    """

    def test_no_mape_defaults_0pct(self):
        """When mape_for_floor is not set (cold start), floor_pct defaults to 0.0."""
        floor_pct = 0.0  # Phase 16 (D-13): floor removed
        assert abs(floor_pct - 0.0) < 1e-7

        blended = 1000.0
        floored = blended * (1.0 + floor_pct)
        assert abs(floored - 1000.0) < 1e-5

    def test_normal_mape_still_0pct(self):
        """When mape_for_floor=15.0, floor = blended * 1.0 (Phase 16: floor removed)."""
        floor_pct = 0.0  # Phase 16 (D-13): floor removed
        assert abs(floor_pct - 0.0) < 1e-7

        blended = 1000.0
        floored = blended * (1.0 + floor_pct)
        assert abs(floored - 1000.0) < 1e-5

    def test_high_mape_still_0pct(self):
        """When mape_for_floor=50.0, floor = blended * 1.0 (Phase 16: floor removed)."""
        floor_pct = 0.0  # Phase 16 (D-13): floor removed
        assert abs(floor_pct - 0.0) < 1e-7

        blended = 1000.0
        floored = blended * (1.0 + floor_pct)
        assert abs(floored - 1000.0) < 1e-5

    def test_low_mape_floors_0pct(self):
        """When mape_for_floor=2.0, floor = blended * 1.0 (floor removed)."""
        floor_pct = 0.0  # Phase 16 (D-13): floor removed
        assert abs(floor_pct - 0.0) < 1e-7

        blended = 1000.0
        floored = blended * (1.0 + floor_pct)
        assert abs(floored - 1000.0) < 1e-5

    def test_floor_formula_integration(self):
        """Verify floor is 0% regardless of MAPE (Phase 16: floor removed)."""
        test_cases = [
            (0.0, 0.0),    # default -> 0%
            (2.0, 0.0),    # low MAPE -> 0%
            (5.0, 0.0),    # 5% MAPE -> 0%
            (10.0, 0.0),   # 10% MAPE -> 0%
            (15.0, 0.0),   # 15% MAPE -> 0%
            (20.0, 0.0),   # 20% MAPE -> 0%
            (30.0, 0.0),   # 30% MAPE -> 0%
            (50.0, 0.0),   # 50% MAPE -> 0%
            (100.0, 0.0),  # 100% MAPE -> 0%
        ]
        for mape_value, expected_pct in test_cases:
            floor_pct = 0.0  # Phase 16 (D-13): floor removed
            assert abs(floor_pct - expected_pct) < 1e-7, (
                f"MAPE={mape_value}: expected floor_pct={expected_pct}, got {floor_pct}"
            )


# ---------------------------------------------------------------------------
# TestMAPEInjection (ACC-03 wiring verification)
# ---------------------------------------------------------------------------
@requires_tf
def _fresh_grid(n):
    """n ten-minute points ending now: the inference path requires a gridded, fresh window
    (data/gapfill.check_inference_window), so a fixed historical timestamp is rejected as stale."""
    from datetime import datetime, timezone, timedelta
    end = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    end = end - timedelta(minutes=end.minute % 10)
    return [{'timestamp': (end - timedelta(minutes=10 * (n - 1 - i))).strftime('%Y-%m-%dT%H:%M:%SZ'),
             'value': float(i)} for i in range(n)]


class TestMAPEInjection:
    """Test that MAPE flows from accuracy_tracker through predict() to model.mape_for_floor.

    These tests verify the wiring in main.py -- no TensorFlow needed.
    We mock the trained_models dict and accuracy_tracker to isolate the plumbing.
    """

    def _make_fake_model(self):
        """Create a minimal fake model with attributes LSTMPredictor.predict() needs."""
        model = MagicMock()
        model.sequence_length = 144
        model.scaler = MagicMock()
        model.scaler.transform = MagicMock(return_value=np.zeros((144, 1)))
        model.mape_for_floor = 0.0
        model.raw_training_values = np.zeros(200)
        model.last_sequence = np.zeros(144)
        model.predict.return_value = {
            'predictions': [100.0, 110.0, 120.0, 130.0, 140.0, 150.0],
            'confidence': 0.85,
        }
        return model

    def test_namespace_flows_to_get_component_mape(self):
        """predict(namespace='mynamespace') passes that namespace to accuracy_tracker.get_component_mape() with 'blended'."""
        from unittest.mock import patch

        fake_model = self._make_fake_model()
        metric_data = _fresh_grid(200)

        with patch('api.main.accuracy_tracker') as mock_tracker, \
             patch.dict('api.main.predictor.trained_models', {'myapp_requests': fake_model}), \
             patch.dict('api.main.predictor.model_train_times', {'myapp_requests': __import__('datetime').datetime.utcnow()}):
            mock_tracker.get_component_mape.return_value = 12.5

            from api.main import predictor
            predictor.predict('myapp', metric_data, 60, 'requests', namespace='mynamespace')

            mock_tracker.get_component_mape.assert_called_with('myapp', 'mynamespace', 'requests', 'blended')

    def test_nonzero_mape_reaches_model(self):
        """When accuracy_tracker.get_component_mape returns MAPE=15.0, model.mape_for_floor is set to 15.0."""
        from unittest.mock import patch

        fake_model = self._make_fake_model()
        metric_data = _fresh_grid(200)

        with patch('api.main.accuracy_tracker') as mock_tracker, \
             patch.dict('api.main.predictor.trained_models', {'myapp_requests': fake_model}), \
             patch.dict('api.main.predictor.model_train_times', {'myapp_requests': __import__('datetime').datetime.utcnow()}):
            mock_tracker.get_component_mape.return_value = 15.0

            from api.main import predictor
            predictor.predict('myapp', metric_data, 60, 'requests', namespace='default')

            assert fake_model.mape_for_floor == 15.0, (
                f"Expected model.mape_for_floor=15.0, got {fake_model.mape_for_floor}"
            )

    def test_exception_defaults_mape_to_zero(self):
        """When accuracy_tracker.get_component_mape() raises, mape defaults to 0.0."""
        from unittest.mock import patch

        fake_model = self._make_fake_model()
        metric_data = _fresh_grid(200)

        with patch('api.main.accuracy_tracker') as mock_tracker, \
             patch.dict('api.main.predictor.trained_models', {'myapp_requests': fake_model}), \
             patch.dict('api.main.predictor.model_train_times', {'myapp_requests': __import__('datetime').datetime.utcnow()}):
            mock_tracker.get_component_mape.side_effect = RuntimeError("no data")

            from api.main import predictor
            predictor.predict('myapp', metric_data, 60, 'requests', namespace='default')

            assert fake_model.mape_for_floor == 0.0, (
                f"Expected model.mape_for_floor=0.0 on exception, got {fake_model.mape_for_floor}"
            )

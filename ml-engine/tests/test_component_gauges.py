"""Tests for OBS-02: Per-component gauges and predict() passthrough.

Verifies that:
- PREDICTION_RPM_GAUGE, FLOOR_PCT_GAUGE, COMPONENT_MAPE_GAUGE are declared in main.py
- predict() return dict from LSTMPredictor includes 'components' and 'floor_pct' fields
- lstm_model.py predict() return contains lstm/pattern/blended/final arrays and floor_pct
"""

import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(Path(__file__).parent.parent))


class TestGaugeDeclarations:
    """Test that the three new Prometheus gauges are declared in main.py."""

    @pytest.fixture(scope="class")
    def main_py_content(self):
        main_py = PROJECT_ROOT / "ml-engine" / "api" / "main.py"
        return main_py.read_text()

    def test_prediction_rpm_gauge_declared(self, main_py_content):
        """PREDICTION_RPM_GAUGE must be declared with metric name 'ml_api_prediction_rpm'."""
        assert "ml_api_prediction_rpm" in main_py_content, (
            "PREDICTION_RPM_GAUGE with name 'ml_api_prediction_rpm' not found in main.py"
        )

    def test_floor_pct_gauge_declared(self, main_py_content):
        """FLOOR_PCT_GAUGE must be declared with metric name 'ml_api_floor_pct'."""
        assert "ml_api_floor_pct" in main_py_content, (
            "FLOOR_PCT_GAUGE with name 'ml_api_floor_pct' not found in main.py"
        )

    def test_component_mape_gauge_declared(self, main_py_content):
        """COMPONENT_MAPE_GAUGE must be declared with metric name 'ml_api_component_mape'."""
        assert "ml_api_component_mape" in main_py_content, (
            "COMPONENT_MAPE_GAUGE with name 'ml_api_component_mape' not found in main.py"
        )

    def test_prediction_rpm_gauge_has_component_and_step_labels(self, main_py_content):
        """PREDICTION_RPM_GAUGE must have 'component' and 'step' labels."""
        assert "component" in main_py_content, "PREDICTION_RPM_GAUGE missing 'component' label"
        assert "step" in main_py_content, "PREDICTION_RPM_GAUGE missing 'step' label"

    def test_components_passthrough_in_predict_return(self, main_py_content):
        """LSTMPredictor.predict() must pass through 'components' from prediction_result."""
        assert 'prediction_result.get("components"' in main_py_content or \
               "prediction_result.get('components'" in main_py_content, (
            "LSTMPredictor.predict() return dict missing components passthrough from prediction_result"
        )

    def test_floor_pct_passthrough_in_predict_return(self, main_py_content):
        """LSTMPredictor.predict() must pass through 'floor_pct' from prediction_result."""
        assert 'prediction_result.get("floor_pct"' in main_py_content or \
               "prediction_result.get('floor_pct'" in main_py_content, (
            "LSTMPredictor.predict() return dict missing floor_pct passthrough from prediction_result"
        )


class TestLstmModelComponentsReturn:
    """Test that lstm_model.py predict() returns the required component dict structure."""

    @pytest.fixture(scope="class")
    def lstm_model_content(self):
        lstm_model = PROJECT_ROOT / "ml-engine" / "models" / "lstm_model.py"
        return lstm_model.read_text()

    def test_predict_returns_components_key(self, lstm_model_content):
        """predict() must return a dict with 'components' key."""
        assert "'components'" in lstm_model_content or '"components"' in lstm_model_content, (
            "lstm_model.py predict() return dict missing 'components' key"
        )

    def test_predict_returns_floor_pct_key(self, lstm_model_content):
        """predict() must return a dict with 'floor_pct' key."""
        assert "'floor_pct'" in lstm_model_content or '"floor_pct"' in lstm_model_content, (
            "lstm_model.py predict() return dict missing 'floor_pct' key"
        )

    def test_blended_pre_floor_captured(self, lstm_model_content):
        """blended_pre_floor list must be populated before the floor multiplication."""
        assert "blended_pre_floor" in lstm_model_content, (
            "lstm_model.py missing 'blended_pre_floor' for pre-floor capture"
        )
        assert "blended_pre_floor.append" in lstm_model_content, (
            "lstm_model.py missing 'blended_pre_floor.append()' call"
        )

    def test_components_has_four_arrays(self, lstm_model_content):
        """components dict must include lstm, pattern, blended, and final arrays."""
        for component in ("'lstm'", "'pattern'", "'blended'", "'final'"):
            assert component in lstm_model_content, (
                f"lstm_model.py components dict missing {component} key"
            )

    def test_operator_contract_predictions_key_preserved(self, lstm_model_content):
        """'predictions' key must still exist in return dict (Go operator contract)."""
        assert "'predictions'" in lstm_model_content or '"predictions"' in lstm_model_content, (
            "lstm_model.py predict() return must still contain 'predictions' key for Go operator"
        )

    def test_operator_contract_confidence_key_preserved(self, lstm_model_content):
        """'confidence' key must still exist in return dict (Go operator contract)."""
        assert "'confidence'" in lstm_model_content or '"confidence"' in lstm_model_content, (
            "lstm_model.py predict() return must still contain 'confidence' key for Go operator"
        )

    def test_blended_pre_floor_append_before_floor_multiplication(self, lstm_model_content):
        """blended_pre_floor.append must occur before floor multiplication line."""
        append_pos = lstm_model_content.find("blended_pre_floor.append")
        floor_mult_pos = lstm_model_content.find("blended * (1.0 + floor_pct)")
        assert append_pos != -1, "blended_pre_floor.append not found in lstm_model.py"
        assert floor_mult_pos != -1, "floor multiplication 'blended * (1.0 + floor_pct)' not found"
        assert append_pos < floor_mult_pos, (
            "blended_pre_floor.append must appear BEFORE floor multiplication "
            "(pre-floor semantics required for Phase 14 analysis)"
        )

"""Tests for OBS-03: Grafana dashboard JSON structure validation.

Verifies that grafana/dashboards/prediction-components.json:
- Is valid JSON
- Has uid 'pred-components-v5'
- Has exactly 5 panels
- Uses datasource UID ${DS_PROMETHEUS} in all panel targets
- Contains all required PromQL queries
- Has templating variables for 'application' and 'namespace'
"""

import json
import sys
from pathlib import Path

import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
DASHBOARD_PATH = PROJECT_ROOT / "grafana" / "dashboards" / "prediction-components.json"

REQUIRED_DATASOURCE_UID = "${DS_PROMETHEUS}"

REQUIRED_PROMQL_QUERIES = [
    "predictive_autoscaler_predicted_rpm",
    "predictive_autoscaler_current_rpm",
    "ml_api_prediction_rpm",
    "ml_api_floor_pct",
    "ml_api_component_mape",
    "kube_deployment_spec_replicas",
    "predictive_autoscaler_overestimate_streak",
]

EXPECTED_PANEL_TITLES = [
    "Predicted vs Actual RPM",
    "Prediction Components (per-step)",
    "Floor % and MAPE by Component",
    "Replica Comparison",
    "Overestimate Detection",
]


@pytest.fixture(scope="module")
def dashboard_data():
    """Load and parse the dashboard JSON file."""
    assert DASHBOARD_PATH.exists(), (
        f"Dashboard JSON not found at {DASHBOARD_PATH}. "
        "Expected grafana/dashboards/prediction-components.json to be created by Plan 13-02."
    )
    content = DASHBOARD_PATH.read_text()
    return json.loads(content)


@pytest.fixture(scope="module")
def dashboard_dict(dashboard_data):
    """Return the inner 'dashboard' dict (strips Grafana API envelope if present)."""
    if "dashboard" in dashboard_data:
        return dashboard_data["dashboard"]
    return dashboard_data


@pytest.fixture(scope="module")
def dashboard_json_str(dashboard_data):
    """Return the full dashboard content as a string for substring searching."""
    return json.dumps(dashboard_data)


class TestDashboardJsonIsValid:
    """Test that the file is valid JSON."""

    def test_file_exists(self):
        assert DASHBOARD_PATH.exists(), f"Dashboard file missing: {DASHBOARD_PATH}"

    def test_file_is_valid_json(self):
        content = DASHBOARD_PATH.read_text()
        try:
            json.loads(content)
        except json.JSONDecodeError as exc:
            pytest.fail(f"prediction-components.json is not valid JSON: {exc}")


class TestDashboardUid:
    """Test dashboard UID."""

    def test_uid_is_pred_components_v5(self, dashboard_dict):
        uid = dashboard_dict.get("uid")
        assert uid == "pred-components-v5", (
            f"Dashboard uid must be 'pred-components-v5', got '{uid}'"
        )


class TestDashboardPanels:
    """Test the dashboard has exactly 5 panels with the required titles."""

    def test_dashboard_has_5_panels(self, dashboard_dict):
        panels = dashboard_dict.get("panels", [])
        assert len(panels) == 5, (
            f"Dashboard must have exactly 5 panels, found {len(panels)}: "
            f"{[p.get('title') for p in panels]}"
        )

    def test_predicted_vs_actual_rpm_panel_exists(self, dashboard_dict):
        titles = [p.get("title") for p in dashboard_dict.get("panels", [])]
        assert "Predicted vs Actual RPM" in titles, (
            f"Panel 'Predicted vs Actual RPM' not found. Panels: {titles}"
        )

    def test_prediction_components_panel_exists(self, dashboard_dict):
        titles = [p.get("title") for p in dashboard_dict.get("panels", [])]
        assert "Prediction Components (per-step)" in titles, (
            f"Panel 'Prediction Components (per-step)' not found. Panels: {titles}"
        )

    def test_floor_mape_panel_exists(self, dashboard_dict):
        titles = [p.get("title") for p in dashboard_dict.get("panels", [])]
        assert "Floor % and MAPE by Component" in titles, (
            f"Panel 'Floor % and MAPE by Component' not found. Panels: {titles}"
        )

    def test_replica_comparison_panel_exists(self, dashboard_dict):
        titles = [p.get("title") for p in dashboard_dict.get("panels", [])]
        assert "Replica Comparison" in titles, (
            f"Panel 'Replica Comparison' not found. Panels: {titles}"
        )

    def test_overestimate_detection_panel_exists(self, dashboard_dict):
        titles = [p.get("title") for p in dashboard_dict.get("panels", [])]
        assert "Overestimate Detection" in titles, (
            f"Panel 'Overestimate Detection' not found. Panels: {titles}"
        )


class TestDashboardDatasource:
    """Test that the correct VictoriaMetrics datasource UID is used."""

    def test_datasource_uid_present_in_dashboard(self, dashboard_json_str):
        assert REQUIRED_DATASOURCE_UID in dashboard_json_str, (
            f"Datasource UID '{REQUIRED_DATASOURCE_UID}' not found in dashboard JSON. "
            "All panel targets must reference this VictoriaMetrics datasource."
        )

    def test_panel_1_uses_correct_datasource(self, dashboard_dict):
        panels = dashboard_dict.get("panels", [])
        panel_1 = next((p for p in panels if p.get("title") == "Predicted vs Actual RPM"), None)
        assert panel_1 is not None
        panel_str = json.dumps(panel_1)
        assert REQUIRED_DATASOURCE_UID in panel_str, (
            "Panel 'Predicted vs Actual RPM' missing datasource UID ${DS_PROMETHEUS}"
        )

    def test_panel_2_uses_correct_datasource(self, dashboard_dict):
        panels = dashboard_dict.get("panels", [])
        panel_2 = next((p for p in panels if p.get("title") == "Prediction Components (per-step)"), None)
        assert panel_2 is not None
        panel_str = json.dumps(panel_2)
        assert REQUIRED_DATASOURCE_UID in panel_str, (
            "Panel 'Prediction Components (per-step)' missing datasource UID ${DS_PROMETHEUS}"
        )


class TestDashboardPromqlQueries:
    """Test that all required PromQL metric names appear in the dashboard."""

    @pytest.mark.parametrize("metric_name", REQUIRED_PROMQL_QUERIES)
    def test_required_promql_metric_present(self, dashboard_json_str, metric_name):
        assert metric_name in dashboard_json_str, (
            f"Required PromQL metric '{metric_name}' not found in dashboard JSON"
        )


class TestDashboardTemplating:
    """Test that the dashboard has templating variables for application and namespace."""

    def test_templating_has_application_variable(self, dashboard_dict):
        templating = dashboard_dict.get("templating", {})
        variables = templating.get("list", [])
        var_names = [v.get("name") for v in variables]
        assert "application" in var_names, (
            f"Templating variable 'application' not found. Variables: {var_names}"
        )

    def test_templating_has_namespace_variable(self, dashboard_dict):
        templating = dashboard_dict.get("templating", {})
        variables = templating.get("list", [])
        var_names = [v.get("name") for v in variables]
        assert "namespace" in var_names, (
            f"Templating variable 'namespace' not found. Variables: {var_names}"
        )

    def test_application_variable_uses_correct_datasource(self, dashboard_dict):
        templating = dashboard_dict.get("templating", {})
        variables = templating.get("list", [])
        app_var = next((v for v in variables if v.get("name") == "application"), None)
        assert app_var is not None
        var_str = json.dumps(app_var)
        assert REQUIRED_DATASOURCE_UID in var_str, (
            "Templating variable 'application' must use datasource UID ${DS_PROMETHEUS}"
        )

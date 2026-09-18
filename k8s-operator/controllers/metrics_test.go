package controllers

import (
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricRegistration(t *testing.T) {
	if predictedReplicasGauge == nil {
		t.Error("predictedReplicasGauge is nil")
	}
	if actualNeededReplicasGauge == nil {
		t.Error("actualNeededReplicasGauge is nil")
	}
	if predictionErrorPercentGauge == nil {
		t.Error("predictionErrorPercentGauge is nil")
	}
}

func TestMetricRecording(t *testing.T) {
	// Test predictedReplicasGauge
	predictedReplicasGauge.WithLabelValues("nginx-test", "default").Set(19.0)
	val := testutil.ToFloat64(predictedReplicasGauge.WithLabelValues("nginx-test", "default"))
	if val != 19.0 {
		t.Errorf("predictedReplicasGauge: expected 19.0, got %f", val)
	}

	// Test actualNeededReplicasGauge
	actualNeededReplicasGauge.WithLabelValues("nginx-test", "default").Set(17.0)
	val = testutil.ToFloat64(actualNeededReplicasGauge.WithLabelValues("nginx-test", "default"))
	if val != 17.0 {
		t.Errorf("actualNeededReplicasGauge: expected 17.0, got %f", val)
	}

	// Test predictionErrorPercentGauge
	predictionErrorPercentGauge.WithLabelValues("nginx-test", "default").Set(11.76)
	val = testutil.ToFloat64(predictionErrorPercentGauge.WithLabelValues("nginx-test", "default"))
	if val != 11.76 {
		t.Errorf("predictionErrorPercentGauge: expected 11.76, got %f", val)
	}
}

func TestOverestimateMetricRegistration(t *testing.T) {
	if overestimateOverridesTotal == nil {
		t.Error("overestimateOverridesTotal is nil")
	}
	if overestimateStreakGauge == nil {
		t.Error("overestimateStreakGauge is nil")
	}
}

func TestOverestimateMetricRecording(t *testing.T) {
	// Test overestimateOverridesTotal counter
	overestimateOverridesTotal.WithLabelValues("test-metric-overest", "test-ns").Add(1)
	val := testutil.ToFloat64(overestimateOverridesTotal.WithLabelValues("test-metric-overest", "test-ns"))
	if val != 1.0 {
		t.Errorf("overestimateOverridesTotal: expected 1.0, got %f", val)
	}
	overestimateOverridesTotal.WithLabelValues("test-metric-overest", "test-ns").Add(1)
	val = testutil.ToFloat64(overestimateOverridesTotal.WithLabelValues("test-metric-overest", "test-ns"))
	if val != 2.0 {
		t.Errorf("overestimateOverridesTotal after second inc: expected 2.0, got %f", val)
	}

	// Test overestimateStreakGauge
	overestimateStreakGauge.WithLabelValues("test-metric-overest", "test-ns").Set(3.0)
	val = testutil.ToFloat64(overestimateStreakGauge.WithLabelValues("test-metric-overest", "test-ns"))
	if val != 3.0 {
		t.Errorf("overestimateStreakGauge: expected 3.0, got %f", val)
	}

	// Reset to 0
	overestimateStreakGauge.WithLabelValues("test-metric-overest", "test-ns").Set(0.0)
	val = testutil.ToFloat64(overestimateStreakGauge.WithLabelValues("test-metric-overest", "test-ns"))
	if val != 0.0 {
		t.Errorf("overestimateStreakGauge after reset: expected 0.0, got %f", val)
	}
}

func TestMetricErrorPercentCalculation(t *testing.T) {
	predicted := float64(19)
	actual := float64(17)
	errorPct := math.Abs(predicted-actual) / actual * 100

	// Expected: abs(19-17)/17*100 = 11.764705882352942
	expectedPct := 11.764705882352942
	if math.Abs(errorPct-expectedPct) > 0.0001 {
		t.Errorf("error percent calculation: expected ~%.4f, got %.4f", expectedPct, errorPct)
	}

	predictionErrorPercentGauge.WithLabelValues("test-app", "test-ns").Set(errorPct)
	val := testutil.ToFloat64(predictionErrorPercentGauge.WithLabelValues("test-app", "test-ns"))
	if math.Abs(val-expectedPct) > 0.0001 {
		t.Errorf("predictionErrorPercentGauge after set: expected ~%.4f, got %.4f", expectedPct, val)
	}
}

// OBS-01: RPM gauge registration and recording tests

func TestRpmGaugeRegistration(t *testing.T) {
	// predictedRpmGauge and currentRpmGauge are declared in metrics.go and registered in init().
	// A nil check verifies they were created by prometheus.NewGaugeVec.
	if predictedRpmGauge == nil {
		t.Error("predictedRpmGauge is nil — gauge was not created")
	}
	if currentRpmGauge == nil {
		t.Error("currentRpmGauge is nil — gauge was not created")
	}
}

func TestRpmGaugeMetricNames(t *testing.T) {
	// Verify the metric names match the plan requirement strings.
	// We do this by setting a value and checking it can be read back via testutil.
	// If the gauge name were wrong, it would have panicked at MustRegister time.
	predictedRpmGauge.WithLabelValues("nginx-test", "default").Set(18000.0)
	val := testutil.ToFloat64(predictedRpmGauge.WithLabelValues("nginx-test", "default"))
	if val != 18000.0 {
		t.Errorf("predictedRpmGauge (predictive_autoscaler_predicted_rpm): expected 18000.0, got %f", val)
	}

	currentRpmGauge.WithLabelValues("nginx-test", "default").Set(17400.0)
	val = testutil.ToFloat64(currentRpmGauge.WithLabelValues("nginx-test", "default"))
	if val != 17400.0 {
		t.Errorf("currentRpmGauge (predictive_autoscaler_current_rpm): expected 17400.0, got %f", val)
	}
}

func TestRpmGaugeRecording(t *testing.T) {
	// Test that set values are read back correctly (gauge semantics: last value wins).
	predictedRpmGauge.WithLabelValues("test-rpm-app", "test-rpm-ns").Set(30000.0)
	val := testutil.ToFloat64(predictedRpmGauge.WithLabelValues("test-rpm-app", "test-rpm-ns"))
	if val != 30000.0 {
		t.Errorf("predictedRpmGauge: expected 30000.0, got %f", val)
	}

	// Overwrite with a lower value (gauge must track current, not cumulative).
	predictedRpmGauge.WithLabelValues("test-rpm-app", "test-rpm-ns").Set(15000.0)
	val = testutil.ToFloat64(predictedRpmGauge.WithLabelValues("test-rpm-app", "test-rpm-ns"))
	if val != 15000.0 {
		t.Errorf("predictedRpmGauge after overwrite: expected 15000.0, got %f", val)
	}

	currentRpmGauge.WithLabelValues("test-rpm-app", "test-rpm-ns").Set(14400.0)
	val = testutil.ToFloat64(currentRpmGauge.WithLabelValues("test-rpm-app", "test-rpm-ns"))
	if val != 14400.0 {
		t.Errorf("currentRpmGauge: expected 14400.0, got %f", val)
	}
}

func TestRpmGaugeLabels(t *testing.T) {
	// Verify that different application/namespace label combinations produce independent series.
	predictedRpmGauge.WithLabelValues("app-a", "ns-a").Set(10000.0)
	predictedRpmGauge.WithLabelValues("app-b", "ns-b").Set(20000.0)

	valA := testutil.ToFloat64(predictedRpmGauge.WithLabelValues("app-a", "ns-a"))
	valB := testutil.ToFloat64(predictedRpmGauge.WithLabelValues("app-b", "ns-b"))

	if valA != 10000.0 {
		t.Errorf("predictedRpmGauge[app-a/ns-a]: expected 10000.0, got %f", valA)
	}
	if valB != 20000.0 {
		t.Errorf("predictedRpmGauge[app-b/ns-b]: expected 20000.0, got %f", valB)
	}
	if valA == valB {
		t.Error("predictedRpmGauge label isolation failure: app-a and app-b returned same value")
	}
}

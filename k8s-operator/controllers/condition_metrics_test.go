package controllers

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The condition metric the chart's alert rules read: one series per (autoscaler, type, status), 1 for the persisted
// status and 0 for the other two; removed with the autoscaler.

// conditionSeries returns status → value for one autoscaler and condition type, without creating series.
func conditionSeries(t *testing.T, ns, name, typ string) map[string]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric)
	go func() { conditionGauge.Collect(ch); close(ch) }() // other tests' reconciles leave series too: never block
	out := map[string]float64{}
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatal(err)
		}
		l := map[string]string{}
		for _, p := range d.GetLabel() {
			l[p.GetName()] = p.GetValue()
		}
		if l["namespace"] == ns && l["autoscaler"] == name && l["type"] == typ {
			out[l["status"]] = d.GetGauge().GetValue()
		}
	}
	return out
}

func wantSeries(t *testing.T, got map[string]float64, current string) {
	t.Helper()
	if len(got) != 3 {
		t.Fatalf("want the three status series, got %v", got)
	}
	for s, v := range got {
		if want := map[bool]float64{true: 1, false: 0}[s == current]; v != want {
			t.Fatalf("status %s = %v, want %v (current %s): %v", s, v, want, current, got)
		}
	}
}

func TestConditionMetricsFollowThePersistedConditions(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	t.Cleanup(func() { forgetConditionMetrics(req.Namespace, req.Name) })
	runRPMReconcile(t, r, req, path)
	wantSeries(t, conditionSeries(t, req.Namespace, req.Name, "Ready"), "True")
	wantSeries(t, conditionSeries(t, req.Namespace, req.Name, "TelemetryAvailable"), "True")
	wantSeries(t, conditionSeries(t, req.Namespace, req.Name, "ScalingActive"), "False") // Recommend writes nothing
	wantSeries(t, conditionSeries(t, req.Namespace, req.Name, "ConflictDetected"), "False")
	for _, typ := range conditionMetricTypes {
		if got := conditionSeries(t, req.Namespace, req.Name, typ); len(got) != 3 {
			t.Fatalf("%s: %v", typ, got)
		}
	}

	if v := testutil.ToFloat64(forecastingEnabledGauge.WithLabelValues(req.Namespace, req.Name)); v != 1 {
		t.Fatalf("forecasting enabled by default, gauge %v", v)
	}
	a := getPA(t, r, req)
	off := false
	a.Spec.Prediction.Enabled = &off
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	runRPMReconcile(t, r, req, path)
	if v := testutil.ToFloat64(forecastingEnabledGauge.WithLabelValues(req.Namespace, req.Name)); v != 0 {
		t.Fatalf("forecasting disabled, gauge %v", v)
	}
	wantSeries(t, conditionSeries(t, req.Namespace, req.Name, "ForecastAvailable"), "False")

	if err := r.Delete(context.Background(), getPA(t, r, req)); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for _, typ := range conditionMetricTypes {
		if got := conditionSeries(t, req.Namespace, req.Name, typ); len(got) != 0 {
			t.Fatalf("a deleted autoscaler keeps its %s series: %v", typ, got)
		}
	}
	if forecastingEnabledGauge.DeleteLabelValues(req.Namespace, req.Name) { // true only if the series still existed
		t.Fatal("a deleted autoscaler keeps its forecasting-enabled series")
	}
}

func TestConditionMetricsTelemetryLoss(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "") // no current request rate
	t.Cleanup(func() { forgetConditionMetrics(req.Namespace, req.Name) })
	runRPMReconcile(t, r, req, path)
	wantSeries(t, conditionSeries(t, req.Namespace, req.Name, "TelemetryAvailable"), "False")
	wantSeries(t, conditionSeries(t, req.Namespace, req.Name, "ScalingActive"), "False")
}

func TestConditionMetricsTypeAbsentHasNoSeries(t *testing.T) {
	t.Cleanup(func() { forgetConditionMetrics("ns-a", "pa-a") })
	recordConditionMetrics("ns-a", "pa-a", true, []metav1.Condition{{Type: "Ready", Status: metav1.ConditionUnknown}})
	wantSeries(t, conditionSeries(t, "ns-a", "pa-a", "Ready"), "Unknown")
	recordConditionMetrics("ns-a", "pa-a", true, nil)
	if got := conditionSeries(t, "ns-a", "pa-a", "Ready"); len(got) != 0 {
		t.Fatalf("a condition the status no longer carries keeps its series: %v", got)
	}
}

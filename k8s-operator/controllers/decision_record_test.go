package controllers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

func decisionTestAutoscaler(min, max int32) *autoscalerv1alpha1.PredictiveAutoscaler {
	a := &autoscalerv1alpha1.PredictiveAutoscaler{}
	a.Spec.TargetDeployment.Name = "nginx-test"
	a.Spec.TargetDeployment.Namespace = "demo"
	a.Spec.MinReplicas, a.Spec.MaxReplicas = min, max
	a.Spec.Metrics.Requests = &autoscalerv1alpha1.RequestsMetric{TargetRPS: 10}
	a.Spec.Prediction.HorizonMinutes = 60
	a.Spec.Prediction.LeadTimeMinutes = 20
	return a
}

// The live 2026-09-22 14:08Z cycle (Codex C-109): peak 4993 rpm at 600 rpm/pod -> raw 9, confidence
// 0.677 damps it to ceil(1 + 0.677*8) = 7, reactive 10 sets the decision. The record must carry
// every one of those values and attribute the decision to the reactive floor.
func TestDecisionRecord_DampingAndSource(t *testing.T) {
	a := decisionTestAutoscaler(1, 12)
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard()}
	now := time.Now()
	p := &MLPredictionResponse{Predictions: []float64{4993, 4900, 5000, 5100, 5200, 5300}, Confidence: 0.677,
		ArtifactSHA256: "abc", ModelVersion: "m@abc", issuedAt: now, anchorAt: now}
	n, ok, det := r.calculatePredictedReplicasDetail(a, p)
	if !ok || n != 7 || det.Raw != 9 || det.Damped != 7 || det.Clamped != 7 {
		t.Fatalf("got n=%d ok=%v det=%+v, want raw 9 damped 7", n, ok, det)
	}
	d := newDecisionRecord(a, true, 10)
	d.setForecast(p, det, ok)
	d.ReactiveReplicas = 10
	d.setDecision(n, 10, false)
	if d.DesiredSource != "reactive" || d.ForecastStatus != "used" || !d.hasSafeguard("confidence_damping") {
		t.Fatalf("record %+v", d)
	}
	if *d.RawPredicted != 9 || *d.ConfidenceAdj != 7 || *d.Confidence != 0.677 {
		t.Fatalf("intermediate values lost: %+v", d)
	}
}

func TestDecisionRecord_SourceCases(t *testing.T) {
	cases := []struct {
		name                  string
		status                string
		safeguards            []string
		pred, react, min, max int32
		keep                  bool
		want                  string
	}{
		{"prediction wins", "used", nil, 8, 6, 1, 12, false, "prediction"},
		{"tie", "used", nil, 6, 6, 1, 12, false, "tie"},
		{"reactive wins", "used", nil, 5, 6, 1, 12, false, "reactive"},
		{"prediction equals the floor", "used", []string{"min_clamp"}, 2, 1, 2, 12, false, "min_replicas"},
		{"both below the floor", "used", nil, 1, 1, 2, 12, false, "min_replicas"},
		{"prediction cut to max", "used", []string{"max_clamp"}, 12, 6, 1, 12, false, "max_replicas"},
		{"prediction exactly max, not clamped", "used", nil, 12, 6, 1, 12, false, "prediction"},
		{"reactive above max", "used", nil, 5, 14, 1, 12, false, "max_replicas"},
		{"rejected forecast never wins", "sanity_rejected", nil, 6, 6, 1, 12, false, "reactive"},
		{"forecasting disabled", "disabled", nil, 0, 4, 1, 12, false, "reactive"},
		{"no input", "unavailable", nil, 0, 0, 1, 12, true, "keep_current"},
	}
	for _, c := range cases {
		d := &decisionRecord{ForecastStatus: c.status, Safeguards: c.safeguards, ReactiveReplicas: c.react, MinReplicas: c.min, MaxReplicas: c.max}
		d.setDecision(c.pred, 0, c.keep)
		if d.DesiredSource != c.want {
			t.Errorf("%s: source %q, want %q", c.name, d.DesiredSource, c.want)
		}
	}
}

// recordDecision appends one "event":"decision" JSON line with the applied action.
func TestDecisionRecord_AppendsEventLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "forecasts.jsonl")
	t.Setenv("FORECAST_LOG", path)
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard()}
	d := newDecisionRecord(decisionTestAutoscaler(1, 12), false, 3)
	d.ReactiveReplicas = 4
	d.setDecision(0, 4, false)
	r.recordDecision(d, "scale_up", 4)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(b)), "\n")
	if len(lines) != 1 {
		t.Fatalf("want 1 line, got %d", len(lines))
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(lines[0]), &m); err != nil {
		t.Fatal(err)
	}
	if m["event"] != "decision" || m["action"] != "scale_up" || m["applied_replicas"].(float64) != 4 ||
		m["forecast_status"] != "disabled" || m["desired_source"] != "reactive" || m["raw_predicted_replicas"] != nil {
		t.Fatalf("record %v", m)
	}
}

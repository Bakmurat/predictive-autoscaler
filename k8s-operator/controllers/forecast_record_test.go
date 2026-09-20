package controllers

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"

	"github.com/go-logr/logr"
)

// TestRecordForecastFromCapturedAPIResponse feeds the operator's forecast-record path with a real
// /predict payload captured from the ml-api (testdata/smoke_predict.json, produced by the benchmark
// smoke test) and checks that the record is forward-looking and carries the provenance fields.
func TestRecordForecastFromCapturedAPIResponse(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "smoke_predict.json"))
	if err != nil {
		t.Skipf("no captured payload: %v", err)
	}
	var resp MLPredictionResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("captured payload does not decode into MLPredictionResponse: %v", err)
	}
	if len(resp.Predictions) != 6 {
		t.Fatalf("expected 6 horizon steps, got %d", len(resp.Predictions))
	}
	if resp.ArtifactSHA256 == "" || resp.TrainingCutoff == "" || resp.InferenceInputEnd == "" {
		t.Fatalf("provenance missing in payload: sha=%q cutoff=%q input_end=%q", resp.ArtifactSHA256, resp.TrainingCutoff, resp.InferenceInputEnd)
	}
	if !strings.Contains(resp.ModelVersion, "@"+resp.ArtifactSHA256[:12]) {
		t.Fatalf("model_version %q is not derived from the artifact hash", resp.ModelVersion)
	}

	logPath := filepath.Join(t.TempDir(), "forecasts.jsonl")
	t.Setenv("FORECAST_LOG", logPath)
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard(), predictionCache: map[string]*cachedPrediction{}}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{}
	a.Spec.TargetDeployment.Name = "nginx-test"
	a.Spec.TargetDeployment.Namespace = "demo"
	a.Spec.Prediction.HorizonMinutes = 60
	issued := time.Now().UTC()
	r.recordForecast(a, &resp, issued)

	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatalf("forecast log not written: %v", err)
	}
	var rec forecastRecord
	if err := json.Unmarshal([]byte(strings.TrimSpace(string(data))), &rec); err != nil {
		t.Fatalf("record is not valid JSON: %v", err)
	}
	if rec.TrainingCutoff != normalizeRFC3339(resp.TrainingCutoff) && rec.TrainingCutoff != resp.TrainingCutoff {
		t.Fatalf("training_cutoff not carried: %q", rec.TrainingCutoff)
	}
	if rec.TargetAnchor != "inference_input_end" {
		t.Fatalf("target anchor should be the inference input end, got %q", rec.TargetAnchor)
	}
	cutoff, _ := time.Parse(time.RFC3339, normalizeRFC3339(resp.TrainingCutoff))
	anchor, _ := time.Parse(time.RFC3339, normalizeRFC3339(resp.InferenceInputEnd))
	for i, f := range rec.Forecasts {
		target, err := time.Parse(time.RFC3339, f.TargetAt)
		if err != nil {
			t.Fatalf("step %d target_at unparsable: %v", i+1, err)
		}
		if want := anchor.Add(time.Duration(i+1) * 10 * time.Minute); !target.Equal(want) {
			t.Fatalf("step %d target %s, want anchor+%dmin = %s", i+1, target, 10*(i+1), want)
		}
		if !target.After(cutoff) {
			t.Fatalf("step %d target %s is not after the training cutoff %s", i+1, target, cutoff)
		}
	}
}

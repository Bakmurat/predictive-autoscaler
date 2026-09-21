package controllers

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The API-to-operator contract for served forecast steps (Codex C-86 / D-108).
//
// The defect: encoding/json decodes a JSON null into []float64 as the ZERO VALUE. A step the
// model could not produce, served as null, reaches the controller as a forecast of zero
// requests per minute -- indistinguishable from a genuine quiet period, and able to drive a
// scale-down. The fix is on the API side (refuse with 422 instead of serving a null). These
// tests prove why nothing on the operator side can compensate, and that the refusal the API
// now returns is one the operator already routes to reactive-only scaling.

func testAutoscaler() *autoscalerv1alpha1.PredictiveAutoscaler {
	return &autoscalerv1alpha1.PredictiveAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "nginx-test-autoscaler", Namespace: "demo"},
		Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{
			TargetDeployment: autoscalerv1alpha1.TargetDeployment{
				Name: "nginx-test", Namespace: "demo",
			},
			Metrics: autoscalerv1alpha1.MetricsConfig{
				Requests: &autoscalerv1alpha1.RequestsMetric{Enabled: true, TargetRPS: 30},
			},
			Prediction: autoscalerv1alpha1.PredictionConfig{HorizonMinutes: 60, LeadTimeMinutes: 20},
		},
	}
}

// TestNullPredictionDecodesAsZero documents WHY the API must never serve a null step. It is a
// property of encoding/json, not a bug in the operator: []float64 cannot represent "absent".
func TestNullPredictionDecodesAsZero(t *testing.T) {
	var withNull MLPredictionResponse
	if err := json.Unmarshal([]byte(`{"predictions":[1200.0,null,1400.0]}`), &withNull); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(withNull.Predictions) != 3 {
		t.Fatalf("want 3 steps, got %d", len(withNull.Predictions))
	}
	if withNull.Predictions[1] != 0 {
		t.Fatalf("expected a null step to decode as the zero value, got %v", withNull.Predictions[1])
	}

	var genuineZero MLPredictionResponse
	if err := json.Unmarshal([]byte(`{"predictions":[1200.0,0.0,1400.0]}`), &genuineZero); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if genuineZero.Predictions[1] != withNull.Predictions[1] {
		t.Fatal("a null step and a genuine zero decode identically -- that is the whole problem")
	}
}

// TestRefusedForecastIsClassifiedAsRefusal drives the real getPrediction against a server
// returning the API's new 422, and proves it reaches the operator as a refusal -- which
// routes it to reactive-only scaling rather than to a cached forecast (C-17).
func TestRefusedForecastIsClassifiedAsRefusal(t *testing.T) {
	const detail = "forecast refused: no finite value for step(s) 3 of 6"
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_, _ = w.Write([]byte(`{"detail":"` + detail + `"}`))
	}))
	defer srv.Close()
	t.Setenv("ML_API_URL", srv.URL)

	r := &PredictiveAutoscalerReconciler{}
	_, err := r.getPrediction(context.Background(), testAutoscaler())
	if err == nil {
		t.Fatal("expected an error for a 422 response")
	}
	if !isForecastRefusal(err) {
		t.Fatalf("a 422 must be classified as a forecast refusal, got %T: %v", err, err)
	}
	if !strings.Contains(err.Error(), "no finite value for step") {
		t.Fatalf("the refusal must carry the API's detail so the operator log says why; got %q", err)
	}
}

// TestValidResponseDecodesEveryStepFinite is the positive half of the contract: a served
// forecast arrives with every step a usable number, and the target anchor survives.
func TestValidResponseDecodesEveryStepFinite(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"predictions":[1200.5,1250.25,1300.0,1350.0,1400.0,1450.0],
			"confidence":0.87,"model_name":"nginx-test_requests",
			"model_version":"nginx-test_requests@abc123456789",
			"inference_input_end":"2026-09-21T21:40:00Z","sequence_length":144,
			"horizon_minutes":60}`))
	}))
	defer srv.Close()
	t.Setenv("ML_API_URL", srv.URL)

	r := &PredictiveAutoscalerReconciler{}
	resp, err := r.getPrediction(context.Background(), testAutoscaler())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(resp.Predictions) != 6 {
		t.Fatalf("want 6 steps, got %d", len(resp.Predictions))
	}
	for i, v := range resp.Predictions {
		if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 {
			t.Fatalf("step %d is not a usable forecast: %v", i+1, v)
		}
	}
	if resp.InferenceInputEnd == "" {
		t.Fatal("the target anchor must survive the decode")
	}
}

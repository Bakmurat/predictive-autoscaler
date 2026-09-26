package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// These responses came from the immutable API image with synthetic input data
// and a preserved checkpoint, not from a live request or scored observation.
func apiComponentFixture(t *testing.T, name string) map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile("testdata/component-api-responses.json")
	if err != nil {
		t.Fatal(err)
	}
	var all map[string]interface{}
	if err = json.Unmarshal(raw, &all); err != nil {
		t.Fatal(err)
	}
	return all[name].(map[string]interface{})
}

func fixturePrediction(t *testing.T, body map[string]interface{}, namespace string) (*PredictiveAutoscalerReconciler, *autoscalerv1alpha1.PredictiveAutoscaler, *MLPredictionResponse) {
	t.Helper()
	raw := componentBody(t, body)
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, raw) }))
	t.Cleanup(s.Close)
	t.Setenv("ML_API_URL", s.URL)
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard()}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{TargetDeployment: autoscalerv1alpha1.TargetDeployment{Name: body["application"].(string), Namespace: namespace}, Prediction: autoscalerv1alpha1.PredictionConfig{HorizonMinutes: 60}}}
	p, err := r.getPrediction(context.Background(), a)
	if err != nil {
		t.Fatal(err)
	}
	return r, a, p
}

func TestForecastComponentActualAPIResponses(t *testing.T) {
	for _, tc := range []struct{ arm, ns string }{{"baseline", "demo"}, {"seasonal", "demo"}, {"baseline", "other"}} {
		t.Run(tc.arm+"/"+tc.ns, func(t *testing.T) {
			b := apiComponentFixture(t, tc.arm)
			r, a, p := fixturePrediction(t, b, tc.ns)
			anchor, err := time.Parse(time.RFC3339, normalizeRFC3339(p.InferenceInputEnd))
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "forecasts.jsonl")
			t.Setenv("FORECAST_LOG", path)
			r.recordForecast(a, p, anchor.Add(time.Minute))
			row := componentRows(t, path)[0]
			d := row["components"].(map[string]interface{})
			if d["status"] != "ok" {
				t.Fatalf("actual API response unavailable: %v", d)
			}
			if row["application"] != b["application"] || row["namespace"] != tc.ns || row["model_version"] != b["model_version"] || row["artifact_sha256"] != b["artifact_sha256"] {
				t.Fatal("response and request identity lost")
			}
			original := b["components"].(map[string]interface{})
			for _, key := range []string{"pattern", "lstm", "blended", "final", "pattern_weights", "pattern_available_per_step", "network_finite_per_step", "network_failed", "pattern_source"} {
				if !reflect.DeepEqual(d[key], original[key]) {
					t.Fatalf("changed %s", key)
				}
			}
			for i, fc := range row["forecasts"].([]interface{}) {
				if fc.(map[string]interface{})["target_at"] != d["target_timestamps"].([]interface{})[i] {
					t.Fatal("targets not bound")
				}
			}
		})
	}
}

func TestForecastComponentTypedNonfiniteKeepsCoreIssuance(t *testing.T) {
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		t.Run(fmt.Sprint(v), func(t *testing.T) {
			b := apiComponentFixture(t, "baseline")
			r, a, p := fixturePrediction(t, b, "demo")
			p.components.LSTM[0] = &v
			path := filepath.Join(t.TempDir(), "forecasts.jsonl")
			t.Setenv("FORECAST_LOG", path)
			r.recordForecast(a, p, time.Now())
			row := componentRows(t, path)[0]
			d := row["components"].(map[string]interface{})
			if d["status"] != "invalid" || d["reason"] != "nonfinite_component" {
				t.Fatalf("%v", d)
			}
			if _, ok := d["lstm"]; ok {
				t.Fatal("unsafe number leaked")
			}
			if len(row["forecasts"].([]interface{})) != 6 {
				t.Fatal("core line lost")
			}
		})
	}
}

func TestForecastComponentFractionalAnchorUsesSerializedTargets(t *testing.T) {
	anchor := time.Date(2026, 9, 25, 8, 0, 0, 123000000, time.UTC)
	b := componentResponse(anchor)
	b["inference_input_end"] = anchor.Format(time.RFC3339Nano)
	for i := 0; i < 6; i++ {
		b["target_timestamps"].([]string)[i] = anchor.Add(time.Duration(i+1) * 10 * time.Minute).Format(time.RFC3339Nano)
	}
	_, d := recordedComponent(t, componentBody(t, b), 60)
	if d["status"] != "misaligned" {
		t.Fatalf("fractional targets silently aligned: %v", d)
	}
	if _, ok := d["pattern"]; ok {
		t.Fatal("misaligned values exposed")
	}
}

func TestForecastComponentLogFailureDoesNotChangeScaling(t *testing.T) {
	r, _, req, _ := rpmGaugeReconciler(t)
	body := componentBody(t, componentResponse(time.Now().UTC().Truncate(10*time.Minute)))
	s := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
	defer s.Close()
	t.Setenv("ML_API_URL", s.URL)
	t.Setenv("FORECAST_LOG", t.TempDir()) // A directory cannot be appended as a log.
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	var deployment appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &deployment); err != nil {
		t.Fatal(err)
	}
	if *deployment.Spec.Replicas != 2 {
		t.Fatalf("log failure changed scaling to %d", *deployment.Spec.Replicas)
	}
}

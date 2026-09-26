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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

func componentResponse(anchor time.Time) map[string]interface{} {
	nums := func(v float64) []interface{} { return []interface{}{v, v, v, v, v, v} }
	flags := []interface{}{true, true, true, true, true, true}
	targets := []string{}
	for i := 1; i <= 6; i++ {
		targets = append(targets, anchor.Add(time.Duration(i)*10*time.Minute).Format("2006-01-02T15:04:05"))
	}
	return map[string]interface{}{
		"predictions": nums(750), "confidence": .9, "model_version": "hybrid@abc", "artifact_sha256": strings.Repeat("a", 64),
		"inference_input_end": anchor.Format(time.RFC3339), "horizon_minutes": 60, "target_timestamps": targets,
		"components": map[string]interface{}{"pattern": nums(750), "pattern_available_per_step": append([]interface{}{}, flags...),
			"pattern_weights": nums(1), "lstm": nums(600), "network_finite_per_step": append([]interface{}{}, flags...),
			"network_failed": nil, "blended": nums(750), "final": nums(750), "pattern_source": "seasonal_history"},
	}
}

func componentBody(t *testing.T, body map[string]interface{}) string {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	return strings.ReplaceAll(string(raw), `"OVERFLOW"`, `1e309`)
}

func componentRows(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	rows := []map[string]interface{}{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var r map[string]interface{}
		if err = json.Unmarshal([]byte(line), &r); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, r)
	}
	return rows
}

func recordedComponent(t *testing.T, body string, horizon int32) (map[string]interface{}, map[string]interface{}) {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
	defer server.Close()
	t.Setenv("ML_API_URL", server.URL)
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard()}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: "hybrid", Namespace: "demo"}, Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{TargetDeployment: autoscalerv1alpha1.TargetDeployment{Name: "hybrid", Namespace: "demo"}, Prediction: autoscalerv1alpha1.PredictionConfig{HorizonMinutes: horizon}}}
	prediction, err := r.getPrediction(context.Background(), a)
	if err != nil {
		t.Fatalf("optional diagnostic changed core decode: %v", err)
	}
	path := filepath.Join(t.TempDir(), "forecasts.jsonl")
	t.Setenv("FORECAST_LOG", path)
	r.recordForecast(a, prediction, time.Date(2026, 9, 25, 8, 1, 0, 0, time.UTC))
	rows := componentRows(t, path)
	if len(rows) != 1 {
		t.Fatalf("issuances=%d", len(rows))
	}
	d, ok := rows[0]["components"].(map[string]interface{})
	if !ok {
		t.Fatalf("missing typed component evidence: %v", rows[0])
	}
	return rows[0], d
}

func TestForecastComponentRecording(t *testing.T) {
	anchor := time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC)
	cases := []struct {
		name, status string
		mutate       func(map[string]interface{})
	}{
		{"valid", "ok", func(b map[string]interface{}) {}},
		{"absent", "absent", func(b map[string]interface{}) { delete(b, "components") }},
		{"null", "absent", func(b map[string]interface{}) { b["components"] = nil }},
		{"wrong_type", "invalid", func(b map[string]interface{}) { b["components"] = "bad" }},
		{"short_array", "invalid", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["pattern"] = []interface{}{750}
		}},
		{"null_boolean", "invalid", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["pattern_available_per_step"].([]interface{})[0] = nil
		}},
		{"missing_required", "invalid", func(b map[string]interface{}) { delete(b["components"].(map[string]interface{}), "network_failed") }},
		{"overflow_optional", "invalid", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["lstm"].([]interface{})[0] = "OVERFLOW"
		}},
		{"weight_out_of_range", "invalid", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["pattern_weights"].([]interface{})[0] = 1.1
		}},
		{"pattern_flag_contradiction", "invalid", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["pattern"].([]interface{})[0] = nil
		}},
		{"network_flag_contradiction", "invalid", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["network_finite_per_step"].([]interface{})[0] = false
		}},
		{"bad_timestamp", "invalid", func(b map[string]interface{}) { b["target_timestamps"].([]string)[0] = "not-time" }},
		{"missing_targets", "invalid", func(b map[string]interface{}) { delete(b, "target_timestamps") }},
		{"shifted_target", "misaligned", func(b map[string]interface{}) { b["target_timestamps"].([]string)[0] = "2026-09-25T08:11:00Z" }},
		{"wrong_final", "invalid", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["final"].([]interface{})[0] = 751
		}},
		{"null_final", "invalid", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["final"].([]interface{})[0] = nil
		}},
		{"partial_pattern", "ok", func(b map[string]interface{}) {
			c := b["components"].(map[string]interface{})
			c["pattern"].([]interface{})[0] = nil
			c["pattern_available_per_step"].([]interface{})[0] = false
			c["pattern_weights"].([]interface{})[0] = 0
		}},
		{"failed_network", "ok", func(b map[string]interface{}) {
			c := b["components"].(map[string]interface{})
			c["network_failed"] = "RuntimeError: synthetic network failure"
			for i := 0; i < 6; i++ {
				c["lstm"].([]interface{})[i] = nil
				c["network_finite_per_step"].([]interface{})[i] = false
			}
		}},
		{"zero_pattern", "ok", func(b map[string]interface{}) {
			b["components"].(map[string]interface{})["pattern"].([]interface{})[0] = 0
		}},
		{"offset_targets", "ok", func(b map[string]interface{}) { b["target_timestamps"].([]string)[0] = "2026-09-25T10:10:00+02:00" }},
	}
	baseline := componentResponse(anchor)
	baselineBody := componentBody(t, baseline)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			b := componentResponse(anchor)
			tc.mutate(b)
			row, d := recordedComponent(t, componentBody(t, b), 60)
			if d["schema"] != "component-v1" || d["status"] != tc.status {
				t.Fatalf("diagnostics=%v", d)
			}
			// A bad optional field must not change any core issuance field.
			var original MLPredictionResponse
			if err := json.Unmarshal([]byte(baselineBody), &original); err != nil {
				t.Fatal(err)
			}
			p := filepath.Join(t.TempDir(), "core.jsonl")
			t.Setenv("FORECAST_LOG", p)
			r := &PredictiveAutoscalerReconciler{Log: logr.Discard()}
			a := &autoscalerv1alpha1.PredictiveAutoscaler{Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{TargetDeployment: autoscalerv1alpha1.TargetDeployment{Name: "hybrid", Namespace: "demo"}, Prediction: autoscalerv1alpha1.PredictionConfig{HorizonMinutes: 60}}}
			r.recordForecast(a, &original, time.Date(2026, 9, 25, 8, 1, 0, 0, time.UTC))
			core := componentRows(t, p)[0]
			delete(core, "components")
			delete(row, "components")
			if !reflect.DeepEqual(core, row) {
				t.Fatalf("core issuance changed\n%v\n%v", core, row)
			}
			if tc.status != "ok" {
				for _, key := range []string{"pattern", "lstm", "blended", "final", "pattern_weights"} {
					if _, ok := d[key]; ok {
						t.Fatalf("invalid evidence exposes %s", key)
					}
				}
				if d["reason"] == nil {
					t.Fatal("missing unavailable reason")
				}
				return
			}
			targets := d["target_timestamps"].([]interface{})
			if targets[0] != "2026-09-25T08:10:00Z" {
				t.Fatalf("noncanonical target: %v", targets)
			}
			if tc.name == "partial_pattern" {
				if d["pattern"].([]interface{})[0] != nil || d["pattern_weights"].([]interface{})[0] != float64(0) {
					t.Fatal("partial pattern/weight lost")
				}
			}
			if tc.name == "zero_pattern" && d["pattern"].([]interface{})[0] != float64(0) {
				t.Fatal("zero lost")
			}
		})
	}
	t.Run("unsupported_geometry", func(t *testing.T) {
		_, d := recordedComponent(t, baselineBody, 30)
		if d["status"] != "invalid" {
			t.Fatalf("%v", d)
		}
	})
	t.Run("first_value_only", func(t *testing.T) {
		_, d := recordedComponent(t, baselineBody+" trailing not JSON", 60)
		if d["status"] != "ok" {
			t.Fatalf("%v", d)
		}
	})
}

func TestForecastComponentPythonRounding(t *testing.T) {
	b := componentResponse(time.Date(2026, 9, 25, 8, 0, 0, 0, time.UTC))
	raw := []interface{}{.125, -.125, 2.675, 1.005, -.005, math.Copysign(0, -1)}
	rounded := []interface{}{.12, -.12, 2.67, 1.0, -.01, math.Copysign(0, -1)}
	b["predictions"] = rounded
	b["components"].(map[string]interface{})["final"] = raw
	_, d := recordedComponent(t, componentBody(t, b), 60)
	if d["status"] != "ok" {
		t.Fatalf("Python rounding rejected: %v", d)
	}
	if !reflect.DeepEqual(d["final"], raw) {
		t.Fatalf("raw final values changed: %v", d["final"])
	}
}

func TestForecastComponentCacheAndIdentity(t *testing.T) {
	anchor := time.Now().UTC().Truncate(10 * time.Minute)
	b := componentResponse(anchor)
	var response atomic.Value
	response.Store(componentBody(t, b))
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); fmt.Fprint(w, response.Load().(string)) }))
	defer server.Close()
	t.Setenv("ML_API_URL", server.URL)
	r := &PredictiveAutoscalerReconciler{Log: logr.Discard(), predictionCache: map[string]*cachedPrediction{}}
	a := &autoscalerv1alpha1.PredictiveAutoscaler{Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{TargetDeployment: autoscalerv1alpha1.TargetDeployment{Name: "hybrid", Namespace: "demo"}, Prediction: autoscalerv1alpha1.PredictionConfig{HorizonMinutes: 60}}}
	path := filepath.Join(t.TempDir(), "forecasts.jsonl")
	t.Setenv("FORECAST_LOG", path)
	for i := 0; i < 2; i++ {
		if _, err := r.getCachedPrediction(context.Background(), a, "demo/hybrid"); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 || len(componentRows(t, path)) != 1 {
		t.Fatal("cache reuse recorded/requested another issuance")
	}
	r.predictionCache["demo/hybrid"].fetchedAt = time.Now().Add(-6 * time.Minute)
	b["model_version"] = "hybrid@new"
	b["artifact_sha256"] = strings.Repeat("b", 64)
	response.Store(componentBody(t, b))
	if _, err := r.getCachedPrediction(context.Background(), a, "demo/hybrid"); err != nil {
		t.Fatal(err)
	}
	a.Spec.TargetDeployment.Name = "seasonal"
	b["model_version"] = "seasonal-pattern@new"
	b["components"].(map[string]interface{})["pattern"].([]interface{})[0] = 800
	response.Store(componentBody(t, b))
	if _, err := r.getCachedPrediction(context.Background(), a, "demo/seasonal"); err != nil {
		t.Fatal(err)
	}
	rows := componentRows(t, path)
	if len(rows) != 3 || calls.Load() != 3 {
		t.Fatal("fresh issuance count")
	}
	for _, row := range rows {
		d, ok := row["components"].(map[string]interface{})
		if !ok || d["status"] != "ok" {
			t.Fatalf("missing valid component: %v", row)
		}
	}
	if rows[0]["artifact_sha256"] == rows[1]["artifact_sha256"] || rows[1]["artifact_sha256"] != rows[2]["artifact_sha256"] || rows[2]["application"] != "seasonal" || rows[1]["model_version"] == rows[2]["model_version"] {
		t.Fatal("identity binding lost")
	}
}

func TestForecastComponentCoreFailuresUnchanged(t *testing.T) {
	for _, body := range []string{`{"predictions":"bad","components":{}}`, `{"predictions":[1],"components":NaN}`} {
		t.Run(body, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }))
			defer server.Close()
			t.Setenv("ML_API_URL", server.URL)
			r := &PredictiveAutoscalerReconciler{Log: logr.Discard()}
			if _, err := r.getPrediction(context.Background(), &autoscalerv1alpha1.PredictiveAutoscaler{}); err == nil || !strings.Contains(err.Error(), "failed to decode response") {
				t.Fatalf("core decode error changed: %v", err)
			}
		})
	}
}

func TestForecastComponentReconcileDecisionUnchanged(t *testing.T) {
	for _, bad := range []bool{false, true} {
		t.Run(fmt.Sprint(bad), func(t *testing.T) {
			r, _, req, path := rpmGaugeReconciler(t)
			body := componentResponse(time.Now().UTC().Truncate(10 * time.Minute))
			if bad {
				body["components"] = "malformed"
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, componentBody(t, body)) }))
			defer server.Close()
			t.Setenv("ML_API_URL", server.URL)
			d := runRPMReconcile(t, r, req, path)
			if d.ForecastStatus != "used" || (d.RawPredicted == nil || *d.RawPredicted != 2) || d.DesiredReplicas != 2 || d.AppliedReplicas != 2 || d.Action != "scale_up" {
				t.Fatalf("decision changed: %+v", d)
			}
			found := false
			for _, row := range componentRows(t, path) {
				if row["event"] == nil {
					found = true
					diag, ok := row["components"].(map[string]interface{})
					if !ok {
						t.Fatal("no component evidence")
					}
					want := "ok"
					if bad {
						want = "invalid"
					}
					if diag["status"] != want {
						t.Fatalf("%v", diag)
					}
				}
			}
			if !found {
				t.Fatal("core issuance lost")
			}
		})
	}
}

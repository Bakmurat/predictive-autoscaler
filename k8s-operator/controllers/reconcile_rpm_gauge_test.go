package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Gather does not create a new GaugeVec child, unlike WithLabelValues.
func observedPredictedRPM(t *testing.T, app, namespace string) (float64, bool) {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(predictedRpmGauge)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, metric := range family.Metric {
			labels := map[string]string{}
			for _, label := range metric.Label {
				labels[label.GetName()] = label.GetValue()
			}
			if labels["application"] == app && labels["namespace"] == namespace {
				return metric.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

func rpmGaugeReconciler(t *testing.T) (*PredictiveAutoscalerReconciler, *autoscalerv1alpha1.PredictiveAutoscaler, ctrl.Request, string) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := autoscalerv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	name := "rpm-" + strings.ReplaceAll(strings.ToLower(t.Name()), "/", "-")
	a := &autoscalerv1alpha1.PredictiveAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "gauge-test"},
		Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{
			TargetDeployment: autoscalerv1alpha1.TargetDeployment{Name: name, Namespace: "gauge-test"},
			MinReplicas:      1, MaxReplicas: 12,
			Metrics:    autoscalerv1alpha1.MetricsConfig{Requests: &autoscalerv1alpha1.RequestsMetric{TargetRPS: 10}},
			Prediction: autoscalerv1alpha1.PredictionConfig{HorizonMinutes: 60, LeadTimeMinutes: 20},
		},
	}
	replicas := int32(1)
	d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: a.Namespace}, Spec: appsv1.DeploymentSpec{Replicas: &replicas}}
	client := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(a).WithObjects(a, d).Build()
	r := &PredictiveAutoscalerReconciler{Client: client, Scheme: scheme, Log: logr.Discard(), predictionCache: map[string]*cachedPrediction{}}
	vm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"300"]}]}}`)
	}))
	t.Cleanup(vm.Close)
	t.Setenv("VICTORIAMETRICS_URL", vm.URL)
	logPath := filepath.Join(t.TempDir(), "decisions.jsonl")
	t.Setenv("FORECAST_LOG", logPath)
	t.Cleanup(func() { predictedRpmGauge.DeleteLabelValues(name, a.Namespace) })
	return r, a, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: a.Namespace, Name: a.Name}}, logPath
}

func runRPMReconcile(t *testing.T, r *PredictiveAutoscalerReconciler, req ctrl.Request, logPath string) decisionRecord {
	t.Helper()
	delete(r.lastReconcileMap, req.NamespacedName.String())
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	var last decisionRecord
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var record decisionRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatal(err)
		}
		if record.Event == "decision" {
			last = record
		}
	}
	if last.Event != "decision" {
		t.Fatal("missing persisted scaling decision")
	}
	var deployment appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &deployment); err != nil {
		t.Fatal(err)
	}
	if *deployment.Spec.Replicas != last.AppliedReplicas {
		t.Fatalf("deployment %d disagrees with applied decision %d", *deployment.Spec.Replicas, last.AppliedReplicas)
	}
	return last
}

func TestReconcileRPMGaugeUsesSelectedWindow(t *testing.T) {
	for _, tc := range []struct {
		name string
		age  time.Duration
		lead int32
		pred []float64
		peak float64
		n    int32
	}{
		{"cached_elapsed_steps", 25 * time.Minute, 20, []float64{300, 600, 900, 1200, 1500, 1800}, 1200, 2},
		{"thirty_minute_lead", time.Minute, 30, []float64{300, 600, 900, 1200, 1500, 1800}, 900, 2},
		{"valid_zero", time.Minute, 20, []float64{0, 0, 0, 0, 0, 0}, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, a, req, path := rpmGaugeReconciler(t)
			a.Spec.Prediction.LeadTimeMinutes = tc.lead
			if err := r.Update(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			now := time.Now()
			r.predictionCache[req.NamespacedName.String()] = &cachedPrediction{fetchedAt: now, response: &MLPredictionResponse{
				Predictions: tc.pred, Confidence: 0.9, anchorAt: now.Add(-tc.age), issuedAt: now,
			}}
			d := runRPMReconcile(t, r, req, path)
			if d.ForecastStatus != "used" || d.LeadWindowPeak == nil || *d.LeadWindowPeak != tc.peak || d.AppliedReplicas != tc.n {
				t.Fatalf("unexpected unchanged scaling behavior: %+v", d)
			}
			if value, present := observedPredictedRPM(t, a.Name, a.Namespace); !present || value != tc.peak {
				t.Fatalf("metric=%v present=%v, want persisted decision peak %v", value, present, tc.peak)
			}
		})
	}
}

func TestReconcileRPMGaugeRemovesUnusedForecast(t *testing.T) {
	for _, tc := range []struct{ name, status string }{
		{"disabled", "disabled"}, {"refused", "unavailable"}, {"expired_transport", "unavailable"},
		{"elapsed_horizon", "horizon_elapsed"}, {"sanity_rejected", "sanity_rejected"}, {"empty", "used"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, a, req, path := rpmGaugeReconciler(t)
			now := time.Now()
			cached := &cachedPrediction{fetchedAt: now, response: &MLPredictionResponse{
				Predictions: []float64{600, 600, 600, 600, 600, 600}, Confidence: 0.9, anchorAt: now.Add(-time.Minute), issuedAt: now,
			}}
			r.predictionCache[req.NamespacedName.String()] = cached
			runRPMReconcile(t, r, req, path)
			if value, present := observedPredictedRPM(t, a.Name, a.Namespace); !present || value != 600 {
				t.Fatalf("precondition metric=%v present=%v", value, present)
			}
			switch tc.name {
			case "disabled":
				if err := r.Get(context.Background(), req.NamespacedName, a); err != nil {
					t.Fatal(err)
				}
				no := false
				a.Spec.Prediction.Enabled = &no
				if err := r.Update(context.Background(), a); err != nil {
					t.Fatal(err)
				}
			case "refused", "expired_transport":
				code := http.StatusUnprocessableEntity
				cached.fetchedAt = now.Add(-6 * time.Minute)
				if tc.name == "expired_transport" {
					code = http.StatusServiceUnavailable
					cached.fetchedAt = now.Add(-11 * time.Minute)
				}
				ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(code) }))
				defer ml.Close()
				t.Setenv("ML_API_URL", ml.URL)
			case "elapsed_horizon":
				cached.response.anchorAt = now.Add(-50 * time.Minute)
			case "sanity_rejected":
				cached.response.Predictions = []float64{9000, 9000, 9000, 9000, 9000, 9000}
			case "empty":
				cached.response.Predictions = nil
			}
			d := runRPMReconcile(t, r, req, path)
			if d.ForecastStatus != tc.status || d.AppliedReplicas != 1 {
				t.Fatalf("unexpected unchanged scaling behavior: %+v", d)
			}
			if value, present := observedPredictedRPM(t, a.Name, a.Namespace); present {
				t.Fatalf("unused/empty forecast left metric %v; must be absent", value)
			}
		})
	}
}

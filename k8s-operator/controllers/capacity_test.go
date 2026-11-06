package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	ctrl "sigs.k8s.io/controller-runtime"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The per-replica capacity contract (item 1, gap G1): one resolver for the reactive rule, the forecast rule and the
// headroom floor; no default; without a capacity the decision holds and Recommend reports an incomplete recommendation.

func quantity(s string) *resource.Quantity {
	q := resource.MustParse(s)
	return &q
}

func TestPerReplicaCapacityRPM(t *testing.T) {
	for _, tc := range []struct {
		name   string
		req    *autoscalerv1alpha1.RequestsMetric
		rpm    float64
		source string
		ok     bool
	}{
		{"capacityPerReplica", &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("40")}, 2400, capacitySourcePerReplica, true},
		{"fractional", &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("12.5")}, 750, capacitySourcePerReplica, true},
		{"milli-units", &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("500m")}, 30, capacitySourcePerReplica, true},
		{"the upper bound", &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("1M")}, 6e7, capacitySourcePerReplica, true},
		{"legacy targetRPS", &autoscalerv1alpha1.RequestsMetric{TargetRPS: 10}, 600, capacitySourceTargetRPS, true},
		{"no requests block", nil, 0, "", false},
		{"neither field", &autoscalerv1alpha1.RequestsMetric{Enabled: true}, 0, "", false},
		{"both fields (the API rejects it)", &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("40"), TargetRPS: 10}, 0, "", false},
		{"zero", &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("0")}, 0, "", false},
		{"negative", &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("-5")}, 0, "", false},
		{"above the bound", &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("1000001")}, 0, "", false},
	} {
		spec := autoscalerv1alpha1.PredictiveAutoscalerSpec{Metrics: autoscalerv1alpha1.MetricsConfig{Requests: tc.req}}
		rpm, source, ok := perReplicaCapacityRPM(spec)
		if rpm != tc.rpm || source != tc.source || ok != tc.ok {
			t.Errorf("%s: got (%v, %q, %v), want (%v, %q, %v)", tc.name, rpm, source, ok, tc.rpm, tc.source, tc.ok)
		}
		cok, reason, _ := capacityCondition(spec)
		if cok != tc.ok || reason == "" {
			t.Errorf("%s: the condition (%v, %q) disagrees with the resolver", tc.name, cok, reason)
		}
	}
}

// A forecast is never converted to replicas on a guessed capacity (the former 30,000 rpm fallback).
func TestAForecastIsNotConvertedWithoutACapacity(t *testing.T) {
	r, a, _, _ := rpmGaugeReconciler(t)
	a.Spec.Metrics.Requests = nil
	p := &MLPredictionResponse{Predictions: []float64{6000, 6000, 6000, 6000, 6000, 6000}, Confidence: 0.9}
	if n, ok := r.calculatePredictedReplicas(a, p); ok || n != 0 {
		t.Fatalf("no capacity must give no forecast count, got %d %v", n, ok)
	}
	a.Spec.Metrics.Requests = &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("12.5")}
	if n, ok := r.calculatePredictedReplicas(a, p); !ok || n != 8 { // 6000 / 750
		t.Fatalf("6000 rpm at 12.5 rps per replica needs 8, got %d %v", n, ok)
	}
}

// capacityHarness is recommendHarness with spec.metrics.requests replaced by req (nil removes the block).
func capacityHarness(t *testing.T, mode, rpm string, req *autoscalerv1alpha1.RequestsMetric) (*PredictiveAutoscalerReconciler, ctrl.Request, string, func() []string) {
	t.Helper()
	r, creq, path, rec := recommendHarness(t, mode, rpm)
	a := getPA(t, r, creq)
	a.Spec.Metrics.Requests = req
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		desiredReplicasGauge.DeleteLabelValues(creq.Name, creq.Namespace)
		predictedReplicasGauge.DeleteLabelValues(creq.Name, creq.Namespace)
	})
	return r, creq, path, func() []string { return drain(rec) }
}

func TestCapacityPerReplicaAloneDrivesTheDecision(t *testing.T) {
	// 3000 rpm at 12.5 rps (750 rpm) per replica needs exactly 4; a capacity rounded down to 12 would need 5.
	r, req, path, events := capacityHarness(t, autoscalerv1alpha1.ModeActive, "3000",
		&autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("12.5")})
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "scale_up" || d.ReactiveReplicas != 4 || d.AppliedReplicas != 4 {
		t.Fatalf("decision: %+v", d)
	}
	if d.CapacitySource != capacitySourcePerReplica || d.CapacityRPM == nil || *d.CapacityRPM != 750 {
		t.Fatalf("the ledger must record the capacity: %q %v", d.CapacitySource, d.CapacityRPM)
	}
	c := meta.FindStatusCondition(getPA(t, r, req).Status.Conditions, "CapacityConfigured")
	if c == nil || c.Status != "True" || c.Reason != "CapacityPerReplica" || !strings.Contains(c.Message, "12.5 requests per second") {
		t.Fatalf("CapacityConfigured: %+v", c)
	}
	if ev := strings.Join(events(), "\n"); strings.Contains(ev, "Capacity") {
		t.Fatalf("a configured capacity is not news: %s", ev)
	}
}

func TestTargetRPSAloneKeepsItsLegacyMeaning(t *testing.T) {
	r, req, path, _ := capacityHarness(t, autoscalerv1alpha1.ModeActive, "3000", &autoscalerv1alpha1.RequestsMetric{TargetRPS: 10})
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "scale_up" || d.ReactiveReplicas != 5 || d.AppliedReplicas != 5 || d.CapacitySource != capacitySourceTargetRPS ||
		d.CapacityRPM == nil || *d.CapacityRPM != 600 {
		t.Fatalf("decision: %+v", d)
	}
	c := meta.FindStatusCondition(getPA(t, r, req).Status.Conditions, "CapacityConfigured")
	if c == nil || c.Status != "True" || c.Reason != "DeprecatedTargetRPS" || !strings.Contains(c.Message, "capacityPerReplica") {
		t.Fatalf("CapacityConfigured: %+v", c)
	}
}

// Neither field: no capacity is invented. The decision holds the current count, nothing is written, no forecast is
// requested, and the status says why. The measured rate itself is still published.
func TestNoCapacityHoldsAndSaysWhy(t *testing.T) {
	for _, tc := range []struct {
		name string
		req  *autoscalerv1alpha1.RequestsMetric
	}{
		{"no requests block", nil},
		{"requests block without a capacity", &autoscalerv1alpha1.RequestsMetric{Enabled: true}},
	} {
		for _, mode := range []string{autoscalerv1alpha1.ModeActive, autoscalerv1alpha1.ModeRecommend} {
			t.Run(tc.name+"/"+mode, func(t *testing.T) {
				r, req, path, events := capacityHarness(t, mode, "3000", tc.req) // 3000 rpm: any invented capacity gives a number
				var calls atomic.Int32
				ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1); w.WriteHeader(503) }))
				t.Cleanup(ml.Close)
				t.Setenv("ML_API_URL", ml.URL)
				d := runRPMReconcile(t, r, req, path)
				want := "keep_current"
				if mode == autoscalerv1alpha1.ModeRecommend {
					want = "recommend"
				}
				if d.Action != want || d.AppliedReplicas != 1 || d.ReactiveReplicas != 0 || d.DesiredSource != "keep_current" ||
					!hasString(d.Safeguards, "capacity_missing_hold") || hasString(d.Safeguards, "telemetry_unavailable_hold") {
					t.Fatalf("decision: %+v", d)
				}
				if d.CapacitySource != capacitySourceMissing || d.CapacityRPM != nil || d.ForecastStatus != "capacity_missing" ||
					d.TelemetryStatus != "measured" || d.CurrentRPM != 3000 {
					t.Fatalf("ledger: capacity %q %v forecast %q telemetry %q rpm %v", d.CapacitySource, d.CapacityRPM,
						d.ForecastStatus, d.TelemetryStatus, d.CurrentRPM)
				}
				if n := calls.Load(); n != 0 {
					t.Fatalf("no forecast may be requested without a capacity: %d calls", n)
				}
				var dep appsv1.Deployment
				if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil || *dep.Spec.Replicas != 1 {
					t.Fatalf("the target was written: %v %v", dep.Spec.Replicas, err)
				}
				st := r.scaleStates[req.NamespacedName.String()]
				if mode == autoscalerv1alpha1.ModeRecommend && (!st.lastScaleUp.IsZero() || !st.lastScaleDown.IsZero()) {
					t.Fatalf("Recommend advanced the scale history: %+v", st)
				}
				s := getPA(t, r, req).Status
				if s.CalculatedReplicas != 0 || s.PredictedReplicas != 0 || s.ForecastReplicas != 0 || s.AppliedReplicas != 0 ||
					s.CurrentReplicas != 1 || s.Mode != mode {
					t.Fatalf("the recommendation must be incomplete, not a number: %+v", s)
				}
				for typ, want := range map[string][3]string{
					"CapacityConfigured": {"False", "CapacityMissing", "capacityPerReplica"},
					"Ready":              {"True", "ReconcileCompleted", "calculated=incomplete"},
					"TelemetryAvailable": {"True", "Measured", ""},
					"ForecastAvailable":  {"False", "CapacityMissing", ""},
				} {
					c := meta.FindStatusCondition(s.Conditions, typ)
					if c == nil || string(c.Status) != want[0] || c.Reason != want[1] || !strings.Contains(c.Message, want[2]) {
						t.Fatalf("%s: %+v", typ, c)
					}
				}
				sa := meta.FindStatusCondition(s.Conditions, "ScalingActive")
				if wantReason := map[string]string{autoscalerv1alpha1.ModeActive: "CapacityMissing",
					autoscalerv1alpha1.ModeRecommend: "RecommendMode"}[mode]; sa == nil || sa.Status != "False" || sa.Reason != wantReason {
					t.Fatalf("ScalingActive: %+v", sa)
				}
				if ev := strings.Join(events(), "\n"); !strings.Contains(ev, "Warning CapacityNotConfigured CapacityMissing:") {
					t.Fatalf("no Warning event: %s", ev)
				}
				for _, g := range []interface{ DeleteLabelValues(...string) bool }{desiredReplicasGauge, predictedReplicasGauge,
					actualNeededReplicasGauge, predictionErrorPercentGauge} {
					if g.DeleteLabelValues(req.Name, req.Namespace) {
						t.Fatal("a replica series was published without a capacity")
					}
				}
				if v, ok := gaugeValue(t, currentRpmGauge, req.Name, req.Namespace); !ok || v != 3000 {
					t.Fatalf("the measured rate must still be published: %v %v", v, ok)
				}
				// The hold repeats without a second event; configuring a capacity resumes the calculation.
				runRPMReconcile(t, r, req, path)
				if ev := strings.Join(events(), "\n"); strings.Contains(ev, "CapacityNotConfigured") {
					t.Fatalf("the Warning repeats: %s", ev)
				}
				a := getPA(t, r, req)
				a.Spec.Metrics.Requests = &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("10")}
				if err := r.Update(context.Background(), a); err != nil {
					t.Fatal(err)
				}
				if d := runRPMReconcile(t, r, req, path); d.DesiredReplicas != 5 || d.CapacitySource != capacitySourcePerReplica {
					t.Fatalf("after configuring a capacity: %+v", d)
				}
				if s := getPA(t, r, req).Status; s.CalculatedReplicas != 5 {
					t.Fatalf("calculatedReplicas after configuring a capacity: %d", s.CalculatedReplicas)
				}
				if ev := strings.Join(events(), "\n"); !strings.Contains(ev, "Normal CapacityConfigured") {
					t.Fatalf("no recovery event: %s", ev)
				}
			})
		}
	}
}

// Without a capacity an Active autoscaler far above the measured need does not scale down, and records no floor.
func TestNoCapacityNeverScalesDown(t *testing.T) {
	r, req, path, _ := capacityHarness(t, autoscalerv1alpha1.ModeActive, "60", nil) // 1 rps: one replica of any real capacity
	setReplicas(t, r, req, 8)
	agedState(t, r, req, path)
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "keep_current" || d.AppliedReplicas != 8 || d.ScaleDownHeadroomFloor != nil {
		t.Fatalf("decision: %+v", d)
	}
}

// The reactive rule, the forecast rule and the headroom floor use the one resolved capacity, whichever field set it.
func TestReactiveForecastAndHeadroomShareOneCapacity(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		req                       *autoscalerv1alpha1.RequestsMetric
		reactive, forecast, floor int32
		capacityRPM               float64
		desired, applied          int32
	}{
		// 2900 rpm / 750 → 4; 4500 / 750 → 6; 2900 / (750 x 0.9) → 5. From 8 the gradual step removes 2 → 6.
		{name: "capacityPerReplica 12.5", req: &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: quantity("12.5")},
			reactive: 4, forecast: 6, floor: 5, capacityRPM: 750, desired: 6, applied: 6},
		// 2900 / 720 → 5; 4500 / 720 → 7; 2900 / 648 → 5. From 8 → 7 (the forecast bounds it).
		{name: "targetRPS 12", req: &autoscalerv1alpha1.RequestsMetric{TargetRPS: 12},
			reactive: 5, forecast: 7, floor: 5, capacityRPM: 720, desired: 7, applied: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, req, path, _ := capacityHarness(t, autoscalerv1alpha1.ModeActive, "2900", tc.req)
			setReplicas(t, r, req, 8)
			level := forecastServer(t)
			agedState(t, r, req, path)
			level.Store(4500.0)
			d := runRPMReconcile(t, r, req, path)
			if d.ForecastStatus != "used" || d.CapacityRPM == nil || *d.CapacityRPM != tc.capacityRPM {
				t.Fatalf("forecast %q capacity %v", d.ForecastStatus, d.CapacityRPM)
			}
			if d.ReactiveReplicas != tc.reactive || d.RawPredicted == nil || *d.RawPredicted != tc.forecast ||
				d.ScaleDownHeadroomFloor == nil || *d.ScaleDownHeadroomFloor != tc.floor {
				t.Fatalf("reactive %d forecast %v floor %v, want %d %d %d", d.ReactiveReplicas, d.RawPredicted,
					d.ScaleDownHeadroomFloor, tc.reactive, tc.forecast, tc.floor)
			}
			if d.DesiredReplicas != tc.desired || d.Action != "scale_down" || d.AppliedReplicas != tc.applied {
				t.Fatalf("desired %d %s %d", d.DesiredReplicas, d.Action, d.AppliedReplicas)
			}
		})
	}
}

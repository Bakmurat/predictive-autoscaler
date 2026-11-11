package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The forecast v2 path through Reconcile, against a stub statistical forecasting service that answers like the real
// one: it reads the autoscaler (settings, published lead) and echoes a coherent window for the requested origin.

// v2Origin is a grid origin on the default 300 s cadence; the test clock starts 10 s into it.
var v2Origin = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

// v2Mode is what the stub service answers.
type v2Mode struct {
	Kind  string  // eligible, shadow, collecting, status:<code>, malformed, wrong-epoch
	Upper float64 // upper window max RPS
}

type v2Stub struct {
	mode     atomic.Value // v2Mode
	requests atomic.Int32
	mu       sync.Mutex
	last     v2Request
	after    func() // runs after each answer (a test moving the clock while the request is in flight)
}

func (s *v2Stub) set(kind string, upper float64) { s.mode.Store(v2Mode{kind, upper}) }

// v2Wire builds a coherent v2 answer for a request, the way the service would.
func v2Wire(req v2Request, pa *autoscalerv1alpha1.PredictiveAutoscaler, mode v2Mode, now time.Time) map[string]interface{} {
	s, _ := resolveV2Settings(pa.Spec)
	lead := pa.Status.Prediction.Lead
	origin, _ := v2ParseTS(req.DecisionOrigin, "origin")
	dt, L := s.SampleIntervalSeconds, int64(lead.Seconds)
	w := v2Window{origin, dt, L}
	var targets []string
	var points []float64
	for _, t := range w.targets() {
		targets = append(targets, v2FormatTS(t))
		points = append(points, mode.Upper*0.8)
	}
	nowMs := now.UnixMilli()
	eligible := mode.Kind == "eligible" && w.actionableAt(nowMs, int64(lead.StartupDelaySeconds), int64(lead.ActuationAllowanceSeconds))
	state, reason := "Trusted", "Ready"
	var upper interface{} = mode.Upper
	switch {
	case mode.Kind == "collecting":
		state, reason, upper = "Collecting", "HistoryInsufficient", nil
	case !eligible:
		state, reason = "Shadow", "ShadowInsufficient"
	}
	epoch := v2EvidenceKey(req.AutoscalerUID, req.TargetUID, req.MetricQuerySHA256, s, L)
	if mode.Kind == "wrong-epoch" {
		epoch = strings.Repeat("0", 64)
	}
	ms := func(v int64) string { return time.UnixMilli(v).UTC().Format("2006-01-02T15:04:05.000Z") }
	return map[string]interface{}{
		"autoscaler_namespace": req.AutoscalerNamespace, "autoscaler_name": req.AutoscalerName,
		"autoscaler_uid": req.AutoscalerUID, "autoscaler_generation": req.AutoscalerGeneration,
		"target_namespace": req.Namespace, "target_name": req.Application, "target_uid": req.TargetUID,
		"metric_query_sha256": req.MetricQuerySHA256, "contract": metricContract, "settings_sha256": req.SettingsSHA256,
		"forecast_contract": forecastContractV2, "unit": v2Unit, "issuance_id": "iss-" + req.DecisionOrigin,
		"issued_at": ms(nowMs), "input_end": v2FormatTS(origin - dt), "input_received_through": ms(nowMs - 500),
		"fit_cutoff": v2FormatTS(origin - 3600), "model_family": "seasonal_naive", "model_recipe": "seasonal-naive/daily@1",
		"artifact_sha256": nil, "calibration_epoch": epoch, "checkpoint_sha256": nil,
		"decision_origin": v2FormatTS(origin), "window_start": v2FormatTS(origin), "window_end": v2FormatTS(origin + L),
		"target_timestamps": targets, "sample_interval_seconds": dt, "lead_seconds": L,
		"startup_delay_seconds": lead.StartupDelaySeconds, "actuation_allowance_seconds": lead.ActuationAllowanceSeconds,
		"valid_until": v2FormatTS(w.validUntil(int64(lead.StartupDelaySeconds), int64(lead.ActuationAllowanceSeconds))),
		"point_rps":   points, "base_window_max_rps": mode.Upper * 0.8, "upper_window_max_rps": upper,
		"nominal_miscoverage_bps": s.MiscoverageBps, "working_miscoverage_ratio": 0.1, "calibration_windows": 140,
		"prospective_windows": 70, "gate_coverage_ratio": 0.9, "missing_outcome_ratio": 0.0,
		"selected_candidate": "seasonal_naive", "trust_state": state, "reason": reason, "predictive_eligible": eligible,
		"evidence_cutoff": v2FormatTS(origin - dt),
	}
}

// startV2Stub serves /v2/predict; /predict (the v1 path) fails the test.
func startV2Stub(t *testing.T, reader client.Reader, clock func() time.Time) *v2Stub {
	t.Helper()
	stub := &v2Stub{}
	stub.set("collecting", 0)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/v2/predict" {
			t.Errorf("unexpected request to %s on the v2 path", req.URL.Path)
			w.WriteHeader(404)
			return
		}
		stub.requests.Add(1)
		var body v2Request
		dec := json.NewDecoder(req.Body)
		dec.DisallowUnknownFields()
		if err := dec.Decode(&body); err != nil {
			t.Errorf("request body: %v", err)
		}
		stub.mu.Lock()
		stub.last = body
		stub.mu.Unlock()
		mode := stub.mode.Load().(v2Mode)
		switch {
		case strings.HasPrefix(mode.Kind, "status:"):
			code := 503
			if mode.Kind == "status:422" {
				code = 422
			}
			w.WriteHeader(code)
			_, _ = w.Write([]byte(`{"detail":"stub"}`))
			return
		case mode.Kind == "malformed":
			_, _ = w.Write([]byte(`{"predictions":[1,2,3]}`))
			return
		}
		var pa autoscalerv1alpha1.PredictiveAutoscaler
		if err := reader.Get(context.Background(), types.NamespacedName{Namespace: body.AutoscalerNamespace, Name: body.AutoscalerName}, &pa); err != nil {
			t.Errorf("stub read: %v", err)
			w.WriteHeader(503)
			return
		}
		if pa.Status.Prediction == nil || pa.Status.Prediction.Lead == nil {
			w.WriteHeader(422) // LeadInsufficient: the service needs the published lead
			return
		}
		_ = json.NewEncoder(w).Encode(v2Wire(body, &pa, mode, clock()))
		stub.mu.Lock()
		after := stub.after
		stub.mu.Unlock()
		if after != nil {
			after()
		}
	}))
	t.Cleanup(srv.Close)
	t.Setenv("ML_API_URL", srv.URL)
	return stub
}

type v2Fixture struct {
	r      *PredictiveAutoscalerReconciler
	req    ctrl.Request
	path   string
	stub   *v2Stub
	now    time.Time
	events func() []string
}

func (f *v2Fixture) at(sec int) { f.now = v2Origin.Add(time.Duration(sec) * time.Second) }

// v2Harness: Active or Recommend, 1 replica, capacity 10 rps (600 rpm) per replica, the rate `rpm`, engine statistical
// on the 300 s grid, a configured startup budget of 120 s (lead L = 900 s, valid until origin + 285 s), clock at
// origin + 10 s.
func v2Harness(t *testing.T, mode, rpm string) *v2Fixture {
	t.Helper()
	r, req, path, rec := recommendHarness(t, mode, rpm)
	f := &v2Fixture{r: r, req: req, path: path, events: func() []string { return drain(rec) }}
	f.at(10)
	r.Clock = func() time.Time { return f.now }
	a := getPA(t, r, req)
	capacity := resource.MustParse("10")
	a.Spec.Metrics.Requests = &autoscalerv1alpha1.RequestsMetric{Enabled: true, CapacityPerReplica: &capacity}
	a.Spec.Prediction.Engine = autoscalerv1alpha1.EngineStatistical
	fallback := int32(120)
	a.Spec.Behavior = &autoscalerv1alpha1.BehaviorConfig{Startup: &autoscalerv1alpha1.StartupBehavior{FallbackDelaySeconds: &fallback}}
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	f.stub = startV2Stub(t, r.Client, r.now)
	t.Cleanup(func() { forgetConditionMetrics(req.Namespace, req.Name) })
	return f
}

func (f *v2Fixture) reconcile(t *testing.T) decisionRecord {
	t.Helper()
	return runRPMReconcile(t, f.r, f.req, f.path)
}

func (f *v2Fixture) pa(t *testing.T) *autoscalerv1alpha1.PredictiveAutoscaler {
	return getPA(t, f.r, f.req)
}

func (f *v2Fixture) cond(t *testing.T, typ string) *metav1Condition {
	c := meta.FindStatusCondition(f.pa(t).Status.Conditions, typ)
	if c == nil {
		t.Fatalf("condition %s missing", typ)
	}
	return &metav1Condition{Status: string(c.Status), Reason: c.Reason, Message: c.Message}
}

type metav1Condition struct{ Status, Reason, Message string }

func TestV2PublishesTheLeadBeforeAsking(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	f.reconcile(t)
	p := f.pa(t).Status.Prediction
	if p == nil || p.Lead == nil {
		t.Fatalf("status.prediction: %+v", p)
	}
	want := autoscalerv1alpha1.PredictionLead{Seconds: 900, StartupDelaySeconds: 120, ActuationAllowanceSeconds: 15,
		Method: "configured", SampleCount: 0, ObservedGeneration: f.pa(t).Generation}
	if *p.Lead != want || p.FrozenLeadSeconds != 900 || p.SettingsSHA256 == "" || p.Engine != "statistical" {
		t.Fatalf("lead %+v, status %+v", *p.Lead, p)
	}
	if n := f.stub.requests.Load(); n != 1 {
		t.Fatalf("v2 requests: %d", n)
	}
	if f.stub.last.SettingsSHA256 != p.SettingsSHA256 || f.stub.last.DecisionOrigin != "2026-11-06T12:00:00Z" ||
		f.stub.last.ForecastContract != forecastContractV2 {
		t.Fatalf("request: %+v", f.stub.last)
	}
}

func TestV2ColdStartIsReactiveAndSaysWhy(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000") // 3000 rpm at 600 per replica: 5
	f.stub.set("collecting", 0)
	d := f.reconcile(t)
	if d.Action != "scale_up" || d.DesiredReplicas != 5 || d.DesiredSource != "reactive" || d.ForecastStatus != "not_eligible" {
		t.Fatalf("decision: %+v", d)
	}
	if d.ForecastContract != forecastContractV2 || d.V2 == nil || d.V2.Accepted || d.V2.IssuanceID == nil ||
		*d.V2.IssuanceID != "iss-2026-11-06T12:00:00Z" || d.V2.Verdict != "HistoryInsufficient" {
		t.Fatalf("ledger v2: %+v", d.V2)
	}
	if c := f.cond(t, "PredictionReady"); c.Status != "False" || c.Reason != "HistoryInsufficient" {
		t.Fatalf("PredictionReady: %+v", c)
	}
	if c := f.cond(t, "ForecastAvailable"); c.Reason != "NotTrusted" {
		t.Fatalf("ForecastAvailable: %+v", c)
	}
	if ev := strings.Join(f.events(), "\n"); strings.Contains(ev, "Warning PredictionNotReady") {
		t.Fatalf("collecting history is not a warning: %s", ev)
	}
}

func TestV2ForecastOutageStaysReactive(t *testing.T) {
	for _, kind := range []string{"status:503", "status:422", "malformed", "wrong-epoch"} {
		t.Run(kind, func(t *testing.T) {
			f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
			f.stub.set(kind, 1000)
			d := f.reconcile(t)
			if d.Action != "scale_up" || d.AppliedReplicas != 5 || d.DesiredSource != "reactive" || d.V2.Accepted {
				t.Fatalf("decision: %+v / %+v", d, d.V2)
			}
			want := map[string]string{"status:503": "DataUnavailable", "status:422": "RequestRefused",
				"malformed": "ForecastRejected", "wrong-epoch": "ForecastRejected"}[kind]
			if c := f.cond(t, "PredictionReady"); c.Status != "False" || c.Reason != want {
				t.Fatalf("PredictionReady: %+v", c)
			}
		})
	}
}

func TestV2TelemetryOutageHoldsEvenWithAnEligibleForecast(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "") // the rate cannot be measured
	f.stub.set("eligible", 1000)
	d := f.reconcile(t)
	if d.Action != "keep_current" || d.AppliedReplicas != 1 || !hasString(d.Safeguards, "telemetry_unavailable_hold") {
		t.Fatalf("decision: %+v", d)
	}
}

func TestV2EligibleForecastRaisesTheDecisionWithinTheCap(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000") // R = 5
	f.stub.set("eligible", 1000)                             // ceil(1000/10) = 100, E = min(2, ceil(50% x 5)) = 2
	d := f.reconcile(t)
	if d.Action != "scale_up" || d.DesiredReplicas != 7 || d.DesiredSource != "prediction" || d.ForecastStatus != "used" {
		t.Fatalf("decision: %+v", d)
	}
	v := d.V2
	if !v.Accepted || *v.PredictiveReplicas != 100 || *v.PredictiveAllowance != 2 || *v.CappedReplicas != 7 || !v.CapBinding ||
		*v.UpperWindowMaxRPS != 1000 || *v.CapacityRPM != 600 || !hasString(d.Safeguards, "predictive_extra_cap") {
		t.Fatalf("ledger v2: %+v", v)
	}
	st := f.pa(t).Status
	if p := st.Prediction; !p.Eligible || *p.PredictiveReplicas != 100 || *p.CappedReplicas != 7 || !p.CapBinding ||
		p.UpperWindowMaxRPS != "1000" || p.Phase != "Trusted" || p.IssuanceID == "" {
		t.Fatalf("status.prediction: %+v", p)
	}
	if st.CalculatedReplicas != 7 || st.ForecastReplicas != 7 {
		t.Fatalf("status: %+v", st)
	}
	if c := f.cond(t, "PredictionReady"); c.Status != "True" || c.Reason != "Ready" {
		t.Fatalf("PredictionReady: %+v", c)
	}
	if v, ok := gaugeValue(t, predictedRpmGauge, f.req.Name, f.req.Namespace); !ok || v != 60000 {
		t.Fatalf("predicted rpm gauge %v %v", v, ok)
	}
	if got := testGauge(t, predictiveExtraReplicasGauge, f.req.Namespace, f.req.Name); got != 2 {
		t.Fatalf("predictive extra %v", got)
	}
	if got := testGauge(t, predictiveCappedGauge, f.req.Namespace, f.req.Name); got != 1 {
		t.Fatalf("capped %v", got)
	}
	if got := testGauge(t, predictionGateGauge, f.req.Namespace, f.req.Name, "Trusted"); got != 1 {
		t.Fatalf("gate %v", got)
	}
}

// v2 forecasts far above the measured need for many reconciles: no confidence damping, no overestimate cap, no
// divergence rejection: the decision is the capped forecast every time, and no v1 overestimate state builds up.
func TestV2HasNoConfidenceOrOverestimateInfluence(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "600") // R = 1; a forecast of 500 rps is 50x the rate
	a := f.pa(t)
	extra, pct := int32(20), int32(1000)
	a.Spec.Prediction.PredictiveExtra = &autoscalerv1alpha1.PredictiveExtra{MaxReplicas: &extra, MaxPercent: &pct}
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	f.stub.set("eligible", 50) // ceil(50/10) = 5, E = min(20, 10) = 10: uncapped
	for i := 0; i < 6; i++ {
		f.at(10 + 300*i)
		d := f.reconcile(t)
		if d.DesiredReplicas != 5 || d.ForecastStatus != "used" {
			t.Fatalf("reconcile %d: %+v", i, d)
		}
		for _, g := range []string{"overestimate_cap", "confidence_damping", "sanity_rejected"} {
			if hasString(d.Safeguards, g) {
				t.Fatalf("reconcile %d: v1 safeguard %s on the v2 path", i, g)
			}
		}
		if d.Confidence != nil || d.RawPredicted != nil {
			t.Fatalf("reconcile %d: v1 forecast fields on the v2 path: %+v", i, d)
		}
	}
	if st := f.r.scaleStates[f.req.NamespacedName.String()]; st.overestimateStreak != 0 || st.overrideActive {
		t.Fatalf("overestimate state on the v2 path: %+v", st)
	}
}

// A cached answer keeps its original window: reused only while actionable, never past its deadline, never for the next
// origin; the next origin asks again and gets a later window.
func TestV2CacheNeverSlidesTheWindow(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	f.stub.set("eligible", 100)
	f.reconcile(t) // origin 12:00, valid until 12:04:45
	f.at(200)
	d := f.reconcile(t)
	if n := f.stub.requests.Load(); n != 1 || !d.V2.Cached || *d.V2.DecisionOrigin != "2026-11-06T12:00:00Z" || !d.V2.Accepted {
		t.Fatalf("within the deadline the verified answer is reused: requests %d, %+v", n, d.V2)
	}
	f.at(290) // past valid_until, same origin: no reuse, and a fresh answer for this origin is not actionable either
	d = f.reconcile(t)
	if n := f.stub.requests.Load(); n != 2 || d.V2.Cached || d.V2.Accepted || d.ForecastStatus == "used" {
		t.Fatalf("past the deadline: requests %d, %+v", n, d.V2)
	}
	f.at(310) // the next origin: a new window, never the old one moved forward
	d = f.reconcile(t)
	if n := f.stub.requests.Load(); n != 3 || *d.V2.DecisionOrigin != "2026-11-06T12:05:00Z" || !d.V2.Accepted {
		t.Fatalf("next origin: requests %d, %+v", n, d.V2)
	}
	if p := f.pa(t).Status.Prediction; p.DecisionOrigin.Time.Unix() != v2Origin.Unix()+300 {
		t.Fatalf("status origin %v", p.DecisionOrigin)
	}
}

func TestV2RecommendNeverWritesInAnyState(t *testing.T) {
	for _, kind := range []string{"eligible", "shadow", "collecting", "status:503", "status:422", "malformed", "wrong-epoch"} {
		t.Run(kind, func(t *testing.T) {
			f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "3000")
			f.stub.set(kind, 1000)
			writes := 0
			wrapClient(f.r, interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
				if sub == "scale" {
					writes++
				}
				return c.SubResource(sub).Update(ctx, obj, opts...)
			}})
			d := f.reconcile(t)
			if d.Action != "recommend" || d.AppliedReplicas != 1 || writes != 0 {
				t.Fatalf("Recommend wrote: %+v (writes %d)", d, writes)
			}
			if kind == "eligible" {
				c := f.cond(t, "PredictionReady")
				if c.Status != "True" || !strings.Contains(c.Message, "Recommend mode still makes no writes") || d.DesiredReplicas != 7 {
					t.Fatalf("ready to activate, recommendation 7: %+v %d", c, d.DesiredReplicas)
				}
			}
		})
	}
}

func TestV2WriteStillNeedsTheScaleGuards(t *testing.T) {
	t.Run("resourceVersion conflict", func(t *testing.T) {
		f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
		f.stub.set("eligible", 1000)
		wrapClient(f.r, interceptor.Funcs{SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, obj.GetName(), nil)
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		}})
		reconcileOnce(t, f.r, f.req)
		if d := lastDecision(t, f.path); d.Action != "guard_abort" || d.AppliedReplicas != 1 || !d.V2.Accepted {
			t.Fatalf("a conflicting v2 write must abort: %+v", d)
		}
	})
	t.Run("target replaced", func(t *testing.T) {
		f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
		f.stub.set("eligible", 1000)
		wrapClient(f.r, interceptor.Funcs{SubResourceGet: func(ctx context.Context, c client.Client, sub string, obj client.Object, out client.Object, opts ...client.SubResourceGetOption) error {
			if err := c.SubResource(sub).Get(ctx, obj, out, opts...); err != nil {
				return err
			}
			if s, ok := out.(*autoscalingv1.Scale); ok {
				s.UID = "uid-replacement"
			}
			return nil
		}})
		reconcileOnce(t, f.r, f.req)
		var dep appsv1.Deployment
		if err := f.r.Get(context.Background(), f.req.NamespacedName, &dep); err != nil {
			t.Fatal(err)
		}
		if d := lastDecision(t, f.path); d.Action != "guard_abort" || *dep.Spec.Replicas != 1 {
			t.Fatalf("a replaced target must not be written: %+v", d)
		}
	})
}

// The headroom floor and the restart holds apply to v2 decisions exactly as to v1 ones.
func TestV2KeepsHeadroomAndRestartSafeguards(t *testing.T) {
	t.Run("headroom", func(t *testing.T) {
		f := v2Harness(t, autoscalerv1alpha1.ModeActive, "2900") // R = 5, floor ceil(2900/540) = 6
		setReplicas(t, f.r, f.req, 6)
		f.stub.set("eligible", 20) // P = 2: below R
		agedState(t, f.r, f.req, f.path)
		d := f.reconcile(t)
		if d.Action != "hold_headroom" || d.AppliedReplicas != 6 || d.ScaleDownHeadroomFloor == nil || *d.ScaleDownHeadroomFloor != 6 {
			t.Fatalf("decision: %+v", d)
		}
	})
	t.Run("restart", func(t *testing.T) {
		f := v2Harness(t, autoscalerv1alpha1.ModeActive, "600") // R = 1
		setReplicas(t, f.r, f.req, 6)
		f.stub.set("eligible", 10) // P = 1
		f.r.scaleStates = map[string]*scaleState{}
		if d := f.reconcile(t); d.AppliedReplicas != 6 || d.Action != "hold_stabilizing" {
			t.Fatalf("a fresh Active state must not scale down at once: %+v", d)
		}
	})
}

func TestV2LeadIsFrozenAndNeedsABudget(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	f.reconcile(t)
	// No budget at all: no lead, no request, reactive.
	a := f.pa(t)
	a.Spec.Behavior = nil
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	before := f.stub.requests.Load()
	d := f.reconcile(t)
	if d.ForecastStatus != "lead_insufficient" || f.stub.requests.Load() != before || f.pa(t).Status.Prediction.Lead != nil {
		t.Fatalf("without a budget: %+v", d)
	}
	if c := f.cond(t, "PredictionReady"); c.Reason != "LeadInsufficient" || !strings.Contains(c.Message, "fallbackDelaySeconds") {
		t.Fatalf("PredictionReady: %+v", c)
	}
	// A budget whose lead exceeds the horizon: no lead either.
	big := int32(3500)
	a = f.pa(t)
	a.Spec.Behavior = &autoscalerv1alpha1.BehaviorConfig{Startup: &autoscalerv1alpha1.StartupBehavior{FallbackDelaySeconds: &big}}
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if d := f.reconcile(t); d.ForecastStatus != "lead_insufficient" {
		t.Fatalf("lead beyond the horizon: %+v", d)
	}
	// Within one basis a longer measurement raises the lead and a shorter one never lowers it.
	fb := int32(120)
	a = f.pa(t)
	a.Spec.Behavior = &autoscalerv1alpha1.BehaviorConfig{Startup: &autoscalerv1alpha1.StartupBehavior{FallbackDelaySeconds: &fb}}
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	serveCurrentReplicaSet(t, f)
	f.reconcile(t)
	tr := f.r.startupTrackers[f.req.NamespacedName.String()]
	if tr.rsUID == "" {
		t.Fatal("precondition: the current ReplicaSet is resolved")
	}
	for i := 0; i < startupMinSamples; i++ {
		tr.samples[types.UID(fmt.Sprintf("pod-%d", i))] = startupSample{created: f.now.Add(-time.Hour), seconds: 600}
	}
	f.reconcile(t) // measured: ceil(600 x 1.25) = 750 → L = ceil((750+300+15+300)/300)x300 = 1500
	if l := f.pa(t).Status.Prediction.Lead; l.Seconds != 1500 || l.Method != "measured" || l.SampleCount != startupMinSamples {
		t.Fatalf("raised lead: %+v", l)
	}
	for uid := range tr.samples {
		tr.samples[uid] = startupSample{created: f.now.Add(-time.Hour), seconds: 30}
	}
	f.reconcile(t)
	if l := f.pa(t).Status.Prediction.Lead; l.Seconds != 1500 || l.StartupDelaySeconds != 38 {
		t.Fatalf("the lead must not shrink automatically: %+v", l)
	}
}

func TestV2InvalidSettingsStayReactive(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
	a := f.pa(t)
	a.Spec.Prediction.HorizonMinutes = 7 // not a multiple of the 300 s cadence
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	d := f.reconcile(t)
	if d.ForecastStatus != "settings_invalid" || d.AppliedReplicas != 5 || f.stub.requests.Load() != 0 {
		t.Fatalf("decision: %+v", d)
	}
	if c := f.cond(t, "PredictionReady"); c.Reason != "SettingsInvalid" {
		t.Fatalf("PredictionReady: %+v", c)
	}
	if ev := strings.Join(f.events(), "\n"); !strings.Contains(ev, "Warning PredictionNotReady SettingsInvalid") {
		t.Fatalf("invalid settings warn: %s", ev)
	}
}

func TestV2LeavingTheEngineClearsItsState(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	f.stub.set("eligible", 100)
	f.reconcile(t)
	a := f.pa(t)
	a.Spec.Prediction.Engine = autoscalerv1alpha1.EngineLegacyLSTM
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	t.Cleanup(ml.Close)
	t.Setenv("ML_API_URL", ml.URL)
	d := f.reconcile(t)
	st := f.pa(t).Status
	if d.ForecastContract != "" || d.V2 != nil || st.Prediction != nil || meta.FindStatusCondition(st.Conditions, "PredictionReady") != nil {
		t.Fatalf("v1 again: %+v / %+v", d, st)
	}
	if predictiveExtraReplicasGauge.DeleteLabelValues(f.req.Namespace, f.req.Name) ||
		predictionGateGauge.DeleteLabelValues(f.req.Namespace, f.req.Name, "Trusted") {
		t.Fatal("v2 series kept after leaving the engine")
	}
}

func testGauge(t *testing.T, g *prometheus.GaugeVec, labels ...string) float64 {
	t.Helper()
	return testutil.ToFloat64(g.WithLabelValues(labels...))
}

// Codex #26 repro: the forecast's deadline is 12:04:45; the decision is made at 12:04:44, but the scale read finishes
// at 12:04:46. The write must not happen on the expired forecast: abort, retry soon, decide again without it.
func TestV2AnExpiredForecastNeverAuthorizesTheWrite(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000") // R = 5, an eligible bound of 1000 rps gives 7
	f.stub.set("eligible", 1000)
	f.at(284)
	wrapClient(f.r, interceptor.Funcs{SubResourceGet: func(ctx context.Context, c client.Client, sub string, obj client.Object, out client.Object, opts ...client.SubResourceGetOption) error {
		if sub == "scale" {
			f.at(286) // the slow read
		}
		return c.SubResource(sub).Get(ctx, obj, out, opts...)
	}})
	res := reconcileOnce(t, f.r, f.req)
	d := lastDecision(t, f.path)
	var dep appsv1.Deployment
	if err := f.r.Get(context.Background(), f.req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	if d.Action != "guard_abort" || *dep.Spec.Replicas != 1 || d.V2.Accepted || d.V2.Verdict != "DeadlinePassed" ||
		res.RequeueAfter != staleDecisionRetry {
		t.Fatalf("an expired forecast was written: %+v / %+v, replicas %d, requeue %v", d, d.V2, *dep.Spec.Replicas, res.RequeueAfter)
	}
	if c := f.cond(t, "ScalingActive"); c.Reason != "GuardAborted" || !strings.Contains(c.Message, "forecast expired") {
		t.Fatalf("ScalingActive: %+v", c)
	}
	if p := f.pa(t).Status.Prediction; p.Eligible {
		t.Fatalf("status.prediction still eligible: %+v", p)
	}
	f.at(287) // the retry: past the deadline, the decision is reactive and is written
	if d := f.reconcile(t); d.AppliedReplicas != 5 || d.V2.Accepted {
		t.Fatalf("retry: %+v", d)
	}
}

// The verdict uses the clock after the request: a forecast that expired while it was being fetched is not used.
func TestV2VerdictUsesTheTimeAfterTheRequest(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
	f.stub.set("eligible", 1000)
	f.at(284)
	f.stub.after = func() { f.at(286) }
	if d := f.reconcile(t); d.AppliedReplicas != 5 || d.V2.Accepted || d.V2.Verdict != "DeadlinePassed" {
		t.Fatalf("decision: %+v / %+v", d, d.V2)
	}
}

// v2 decisions are anchored to grid origins: the next one is C after this decision's origin (plus the 1 s offset), net
// of the time the decision took, and a reconcile triggered in between (a status update) decides nothing.
func TestV2SchedulingIsAnchoredToTheDecisionOrigin(t *testing.T) {
	for _, c := range []struct {
		name     string
		interval int32 // spec.prediction.sampleIntervalSeconds
		start    int   // seconds after the origin
		took     int
		wait     time.Duration
	}{
		{"60 s grid, 2 s of work (Codex #26)", 60, 1, 2, 58 * time.Second},
		{"300 s grid, 60 s reconcile interval", 300, 1, 2, 298 * time.Second},
		{"started late in the interval", 300, 40, 5, 256 * time.Second},
	} {
		t.Run(c.name, func(t *testing.T) {
			f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "3000")
			a := f.pa(t)
			dt := c.interval
			a.Spec.Prediction.SampleIntervalSeconds = &dt
			if err := f.r.Update(context.Background(), a); err != nil {
				t.Fatal(err)
			}
			f.stub.set("eligible", 100)
			f.at(c.start)
			f.stub.after = func() { f.at(c.start + c.took) }
			delete(f.r.lastReconcileMap, f.req.NamespacedName.String())
			res, err := f.r.Reconcile(context.Background(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			if res.RequeueAfter != c.wait {
				t.Fatalf("requeue after %v, want %v", res.RequeueAfter, c.wait)
			}
			f.stub.after = nil
			// A status-triggered reconcile 20 s later: no decision, no request, the same next start.
			requests := f.stub.requests.Load()
			f.at(c.start + c.took + 20)
			res, err = f.r.Reconcile(context.Background(), f.req)
			if err != nil {
				t.Fatal(err)
			}
			if res.RequeueAfter != c.wait-20*time.Second || f.stub.requests.Load() != requests {
				t.Fatalf("an in-between reconcile: requeue %v (want %v), requests %d -> %d", res.RequeueAfter, c.wait-20*time.Second,
					requests, f.stub.requests.Load())
			}
			// At the scheduled start, the next decision runs for the next origin.
			f.at(int(dt) + 1)
			if _, err := f.r.Reconcile(context.Background(), f.req); err != nil {
				t.Fatal(err)
			}
			if f.stub.requests.Load() != requests+1 || f.stub.last.DecisionOrigin != v2FormatTS(v2Origin.Unix()+int64(dt)) {
				t.Fatalf("the scheduled decision: requests %d, origin %s", f.stub.requests.Load(), f.stub.last.DecisionOrigin)
			}
		})
	}
}

// The lead covers the decision interval the scheduler actually keeps: C plus the start offset and jitter.
// The lead is the grid-rounded larger of the configured minimum (spec.prediction.minLeadSeconds) and what startup,
// decision interval and allowance need. The minimum lengthens the window only: the published startup budget stays
// what was measured or configured, the setting is part of the settings fingerprint, and a minimum beyond the horizon
// is refused with the settings.
func TestV2LeadIsNeverBelowTheConfiguredMinimum(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	f.reconcile(t)
	base := f.pa(t).Status.Prediction
	if base.Lead == nil {
		t.Fatal("precondition: a lead from the configured budget")
	}
	s, err := resolveV2Settings(f.pa(t).Spec)
	if err != nil {
		t.Fatal(err)
	}
	dt := int32(s.SampleIntervalSeconds)
	min := base.Lead.Seconds + 1 // rounds up to the next grid step
	a := f.pa(t)
	a.Spec.Prediction.MinLeadSeconds = &min
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	got := f.pa(t).Status.Prediction
	if got.Lead == nil || got.Lead.Seconds != base.Lead.Seconds+dt {
		t.Fatalf("lead with minimum %d on the %d s grid: %+v (was %d)", min, dt, got.Lead, base.Lead.Seconds)
	}
	if got.Lead.StartupDelaySeconds != base.Lead.StartupDelaySeconds || got.Lead.Method != base.Lead.Method ||
		got.Lead.ActuationAllowanceSeconds != base.Lead.ActuationAllowanceSeconds {
		t.Fatalf("a minimum lead must not alter the startup budget: %+v, was %+v", got.Lead, base.Lead)
	}
	if got.SettingsSHA256 == base.SettingsSHA256 || got.LeadBasis == base.LeadBasis {
		t.Fatal("the minimum lead is not part of the settings fingerprint and the lead basis")
	}
	// A minimum below what startup needs changes nothing but the fingerprint.
	low := int32(1)
	a = f.pa(t)
	a.Spec.Prediction.MinLeadSeconds = &low
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	f.reconcile(t)
	if l := f.pa(t).Status.Prediction.Lead; l == nil || l.Seconds != base.Lead.Seconds {
		t.Fatalf("a small minimum must leave the required lead: %+v", l)
	}
	// Beyond the horizon: refused with the settings, on the grid-rounded value.
	spec := f.pa(t).Spec
	horizon := int32(s.HorizonSeconds)
	over := horizon - dt + 1
	spec.Prediction.MinLeadSeconds = &over
	if _, err := resolveV2Settings(spec); err != nil {
		t.Fatalf("a minimum that rounds to the horizon is valid: %v", err)
	}
	over = horizon + 1
	if _, err := resolveV2Settings(spec); err == nil || (horizon < v2MaxHorizonSeconds && !strings.Contains(err.Error(), "exceeds the horizon")) {
		t.Fatalf("a minimum beyond the horizon must be refused: %v", err)
	}
	// The forecaster's rule, mirrored: a published lead below the minimum is not accepted.
	s.MinLeadSeconds = int64(base.Lead.Seconds) + 1
	if p := v2LeadProblem(base.Lead, s, base.Lead.ObservedGeneration); !strings.Contains(p, "below the configured minimum lead") {
		t.Fatalf("lead problem: %q", p)
	}
}

func TestV2LeadCoversTheScheduledDecisionInterval(t *testing.T) {
	for _, c := range []struct {
		interval time.Duration
		dt, want int64
	}{{60 * time.Second, 60, 65}, {60 * time.Second, 300, 305}, {90 * time.Second, 60, 125}, {300 * time.Second, 300, 305}} {
		if got := v2LeadDecisionInterval(c.interval, c.dt); got != c.want {
			t.Errorf("%+v: %d", c, got)
		}
	}
}

// serveCurrentReplicaSet answers the target's ReplicaSet and pod lists: one current ReplicaSet, no pods.
func serveCurrentReplicaSet(t *testing.T, f *v2Fixture) {
	t.Helper()
	serveReplicaSetPods(t, f)
}

// serveReplicaSetPods answers the target's ReplicaSet and pod lists: one current ReplicaSet and the pods given to the
// returned function (startupPod fixtures, re-owned by that ReplicaSet; none until it is called).
func serveReplicaSetPods(t *testing.T, f *v2Fixture) func(...podSpec) {
	t.Helper()
	var dep appsv1.Deployment
	if err := f.r.Get(context.Background(), f.req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}
	if err := f.r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	ctrlTrue := true
	rs := appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: dep.Name + "-h", Namespace: dep.Namespace, UID: "rs-current",
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: dep.Name, UID: dep.UID, Controller: &ctrlTrue}}},
		Spec: appsv1.ReplicaSetSpec{Template: *dep.Spec.Template.DeepCopy()}}
	rs.Spec.Template.Labels = map[string]string{podTemplateHashLabel: "h"}
	var pods []corev1.Pod
	wrapClient(f.r, interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		switch l := list.(type) {
		case *appsv1.ReplicaSetList:
			l.Items = []appsv1.ReplicaSet{rs}
			return nil
		case *corev1.PodList:
			l.Items = pods
			return nil
		}
		return c.List(ctx, list, opts...)
	}})
	return func(specs ...podSpec) {
		pods = nil
		for _, spec := range specs {
			p := startupPod(spec) // created relative to startupNow, which is v2Origin
			p.Namespace = dep.Namespace
			p.OwnerReferences[0].Name, p.OwnerReferences[0].UID = rs.Name, rs.UID
			pods = append(pods, *p)
		}
	}
}

// leadAndEpoch is the published lead, the frozen lead and the evidence key the forecasting service files evidence under.
func (f *v2Fixture) leadAndEpoch(t *testing.T) (lead, frozen int32, epoch string) {
	t.Helper()
	a := f.pa(t)
	p := a.Status.Prediction
	if p == nil || p.Lead == nil {
		return 0, p.FrozenLeadSeconds, ""
	}
	s, err := resolveV2Settings(a.Spec)
	if err != nil {
		t.Fatal(err)
	}
	return p.Lead.Seconds, p.FrozenLeadSeconds, v2EvidenceKey(string(a.UID), a.Status.MetricSource.TargetUID, a.Status.MetricSource.SHA256, s, int64(p.Lead.Seconds))
}

// R3-03 through Reconcile: pods of the current ReplicaSet that are not Ready but are no pending start (a readiness
// loss, an evicted pod still listed, a terminating pod) leave the lead, the frozen lead and the evidence epoch alone,
// also across an operator restart.
func TestV2ReadinessLossAndDeadPodsKeepTheLeadAndTheEpoch(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	serve := serveReplicaSetPods(t, f)
	served := podSpec{name: "web-a", hash: "h", createdAgo: 20 * time.Minute, startedAfter: 2 * time.Second, readyAfter: 20 * time.Second}
	flap, evicted := served, podSpec{name: "web-e", hash: "h", createdAgo: 20 * time.Minute, readyAfter: 20 * time.Second,
		lostReadyAgo: time.Minute, phase: corev1.PodFailed}
	flap.lostReadyAgo = -5 * time.Minute // not Ready from origin + 300 s on (the clock is past it in the steps below)
	terminating := podSpec{name: "web-t", hash: "h", createdAgo: 20 * time.Minute, terminating: true}
	serve(served)
	f.reconcile(t)
	lead0, frozen0, epoch0 := f.leadAndEpoch(t)
	if lead0 != 900 || frozen0 != 900 || epoch0 == "" {
		t.Fatalf("precondition: lead %d frozen %d", lead0, frozen0)
	}
	serve(flap, evicted, terminating)
	for i, at := range []int{301, 601, 901, 1201, 1501, 1801, 3301, 6901} {
		if i == 3 {
			f.r.startupTrackers = nil // an operator restart: only the pod objects say what the pods were
		}
		f.at(at)
		before := f.stub.requests.Load()
		d := f.reconcile(t)
		lead, frozen, epoch := f.leadAndEpoch(t)
		st := f.pa(t).Status.Prediction.Startup
		if lead != lead0 || frozen != frozen0 || epoch != epoch0 || st.PendingSeconds != nil || st.PendingEvidence != "" ||
			st.DelaySeconds == nil || *st.DelaySeconds != 120 {
			t.Fatalf("t=+%d s: lead %d frozen %d startup %+v (same epoch: %v)", at, lead, frozen, st, epoch == epoch0)
		}
		if d.ForecastStatus == "lead_insufficient" || f.stub.requests.Load() != before+1 {
			t.Fatalf("t=+%d s: the forecast must still be requested: %+v", at, d)
		}
	}
}

// A start that is really pending still raises the budget (R2-02 keeps this), and one longer than the horizon is never
// cut to fit: there is no lead, no forecast is requested, and the reactive rule carries on. The lead does not come
// back down by itself afterwards; a change of the startup configuration starts a new lead basis.
func TestV2PendingStartBeyondTheHorizonDisablesPredictionNotTheReactiveRule(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000") // 3000 rpm at 600 per replica: 5
	serve := serveReplicaSetPods(t, f)
	serve(podSpec{name: "web-n", hash: "h", createdAgo: 400 * time.Second, startedAfter: 3 * time.Second})
	if d := f.reconcile(t); d.Action != "scale_up" || d.AppliedReplicas != 5 {
		t.Fatalf("reactive scale-up: %+v", d)
	}
	// Pending for 410 s: D = 410, L = ceil((410 + 305 + 15 + 300) / 300) x 300 = 1200.
	lead, frozen, epoch1200 := f.leadAndEpoch(t)
	if st := f.pa(t).Status.Prediction.Startup; lead != 1200 || frozen != 1200 || st.PendingSeconds == nil || *st.PendingSeconds != 410 ||
		st.PendingEvidence != "neverReady" || *st.DelaySeconds != 410 {
		t.Fatalf("a pending start raises the lead: lead %d frozen %d startup %+v", lead, frozen, st)
	}
	// Pending for 3701 s: no lead fits the 3600 s horizon.
	f.at(3301)
	before := f.stub.requests.Load()
	d := f.reconcile(t)
	if d.ForecastStatus != "lead_insufficient" || d.Action != "at_target" || d.DesiredReplicas != 5 || d.DesiredSource != "reactive" ||
		d.TelemetryStatus != "measured" || f.stub.requests.Load() != before {
		t.Fatalf("beyond the horizon the decision is reactive and no forecast is requested: %+v", d)
	}
	p := f.pa(t).Status.Prediction
	if p.Lead != nil || p.FrozenLeadSeconds != 1200 || *p.Startup.PendingSeconds != 3701 || *p.Startup.DelaySeconds != 3701 {
		t.Fatalf("no lead, and the pending start is reported uncut: %+v startup %+v", p, p.Startup)
	}
	if c := f.cond(t, "PredictionReady"); c.Status != "False" || c.Reason != "LeadInsufficient" ||
		!strings.Contains(c.Message, "exceeds the 3600 s horizon") || !strings.Contains(c.Message, "has not become Ready for 3701 s") {
		t.Fatalf("PredictionReady: %+v", c)
	}
	// The pod is gone: the lead is back, at the frozen value (no automatic shortening within the basis).
	serve()
	f.at(3601)
	f.reconcile(t)
	if lead, frozen, epoch := f.leadAndEpoch(t); lead != 1200 || frozen != 1200 || epoch != epoch1200 {
		t.Fatalf("the lead must not shrink by itself: lead %d frozen %d", lead, frozen)
	}
	basis := f.pa(t).Status.Prediction.LeadBasis
	// A spec change outside the lead basis keeps it.
	a := f.pa(t)
	a.Spec.MaxReplicas++
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	f.at(3901)
	f.reconcile(t)
	if lead, _, _ := f.leadAndEpoch(t); lead != 1200 || f.pa(t).Status.Prediction.LeadBasis != basis {
		t.Fatalf("maxReplicas is not part of the lead basis: lead %d", lead)
	}
	// The reset route: a change of spec.behavior.startup is a new lead basis, chosen from the current budget.
	a = f.pa(t)
	one := int32(1)
	a.Spec.Behavior.Startup.MinimumDelaySeconds = &one
	if err := f.r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	f.at(4201)
	f.reconcile(t)
	if lead, frozen, _ := f.leadAndEpoch(t); lead != 900 || frozen != 900 || f.pa(t).Status.Prediction.LeadBasis == basis {
		t.Fatalf("a new lead basis starts from the current budget: lead %d frozen %d", lead, frozen)
	}
}

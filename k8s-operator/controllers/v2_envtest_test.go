package controllers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Forecast contract v2 against a real API server: the CRD schema of the v2 settings and the startup budget, the
// published lead as the forecasting service would read it, and Recommend writing nothing in any v2 state.

// v2EnvFixture's clock starts 10 s into v2Origin; next() moves it to the next grid origin (a new forecast window).
func v2EnvFixture(t *testing.T, mode string) (*envFixture, *v2Stub, func()) {
	t.Helper()
	f := envHarness(t, mode)
	now := v2Origin.Add(10 * time.Second)
	f.r.Clock = func() time.Time { return now }
	next := func() { now = now.Add(300 * time.Second) }
	ctx := context.Background()
	var pa autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(ctx, f.req.NamespacedName, &pa); err != nil {
		t.Fatal(err)
	}
	capacity := resource.MustParse("10")
	fallback := int32(120)
	pa.Spec.Metrics.Requests = &autoscalerv1alpha1.RequestsMetric{Enabled: true, CapacityPerReplica: &capacity}
	pa.Spec.Prediction.Engine = autoscalerv1alpha1.EngineStatistical
	pa.Spec.Behavior = &autoscalerv1alpha1.BehaviorConfig{Startup: &autoscalerv1alpha1.StartupBehavior{FallbackDelaySeconds: &fallback}}
	if err := f.c.Update(ctx, &pa); err != nil {
		t.Fatal(err)
	}
	stub := startV2Stub(t, f.c, f.r.now)
	return f, stub, next
}

func TestEnvtestV2RecommendNeverWritesInAnyState(t *testing.T) {
	f, stub, next := v2EnvFixture(t, autoscalerv1alpha1.ModeRecommend) // 3000 rpm at 10 rps per replica: 5 needed
	for _, kind := range []string{"collecting", "shadow", "status:503", "status:422", "malformed", "wrong-epoch", "eligible"} {
		next() // each state on its own origin: one answer per window
		stub.set(kind, 1000)
		reconcileOnce(t, f.r, f.req)
		d := lastDecision(t, f.path)
		if d.Action != "recommend" || *f.deployment(t).Spec.Replicas != 1 {
			t.Fatalf("%s: Recommend wrote: %+v (replicas %d)", kind, d, *f.deployment(t).Spec.Replicas)
		}
	}
	var pa autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(context.Background(), f.req.NamespacedName, &pa); err != nil {
		t.Fatal(err)
	}
	c := meta.FindStatusCondition(pa.Status.Conditions, "PredictionReady")
	if c == nil || c.Status != "True" || !strings.Contains(c.Message, "Recommend mode still makes no writes") ||
		pa.Status.CalculatedReplicas != 7 || pa.Status.Prediction == nil || !pa.Status.Prediction.Eligible {
		t.Fatalf("ready to activate with a recommendation of 7: %+v / %+v", c, pa.Status)
	}
	// The lead as stored by the API server: exactly the keys contracts.resolve_lead requires, zeros included.
	var u unstructured.Unstructured
	u.SetGroupVersionKind(autoscalerv1alpha1.GroupVersion.WithKind("PredictiveAutoscaler"))
	if err := f.c.Get(context.Background(), f.req.NamespacedName, &u); err != nil {
		t.Fatal(err)
	}
	lead, _, _ := unstructured.NestedMap(u.Object, "status", "prediction", "lead")
	raw, _ := json.Marshal(lead)
	if want := `{"actuationAllowanceSeconds":15,"method":"configured","observedGeneration":` +
		jsonInt(pa.Generation) + `,"sampleCount":0,"seconds":900,"startupDelaySeconds":120}`; string(raw) != want {
		t.Fatalf("stored lead %s, want %s", raw, want)
	}
}

func jsonInt(v int64) string { b, _ := json.Marshal(v); return string(b) }

func TestEnvtestV2ActiveWritesOnlyAVerifiedForecast(t *testing.T) {
	f, stub, next := v2EnvFixture(t, autoscalerv1alpha1.ModeActive)
	stub.set("shadow", 1000)
	reconcileOnce(t, f.r, f.req)
	if got := *f.deployment(t).Spec.Replicas; got != 5 {
		t.Fatalf("an untrusted forecast leaves the reactive 5, got %d", got)
	}
	next()
	stub.set("eligible", 1000) // P = 5 + min(2, ceil(2.5)) = 7
	reconcileOnce(t, f.r, f.req)
	if got := *f.deployment(t).Spec.Replicas; got != 7 {
		t.Fatalf("a verified forecast raises the count to the capped 7, got %d", got)
	}
}

// The CRD's rule on spec.prediction and the operator's resolver decide historyDays alike: engine statistical needs two
// days (the calibration scale is fitted from residuals one day apart), the legacy engine keeps one. An absent engine
// is the v1 path: admitted, and the v2 resolver is never consulted for it.
func TestEnvtestV2HistoryDaysAdmissionMatchesResolver(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeRecommend)
	ctx := context.Background()
	for _, c := range []struct {
		name     string
		pred     map[string]interface{}
		admitted bool
		v2Path   bool
	}{
		{"hist-statistical-1", map[string]interface{}{"engine": "statistical", "historyDays": int64(1)}, false, true},
		{"hist-statistical-2", map[string]interface{}{"engine": "statistical", "historyDays": int64(2)}, true, true},
		{"hist-legacy-1", map[string]interface{}{"engine": "legacyLstm", "sampleIntervalSeconds": int64(600), "historyDays": int64(1)}, true, true},
		{"hist-no-engine-1", map[string]interface{}{"historyDays": int64(1)}, true, false},
	} {
		u := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": autoscalerv1alpha1.GroupVersion.String(), "kind": "PredictiveAutoscaler",
			"metadata": map[string]interface{}{"name": c.name, "namespace": f.req.Namespace},
			"spec": map[string]interface{}{"targetDeployment": map[string]interface{}{"name": "web", "namespace": f.req.Namespace},
				"minReplicas": int64(1), "maxReplicas": int64(3), "prediction": c.pred}}}
		err := f.c.Create(ctx, u)
		if (err == nil) != c.admitted {
			t.Errorf("%s: admission accepted=%v, want %v (%v)", c.name, err == nil, c.admitted, err)
		}
		if err != nil && !strings.Contains(err.Error(), "historyDays must be at least 2 with engine statistical") {
			t.Errorf("%s: the refusal does not name the rule: %v", c.name, err)
		}
		if !c.v2Path {
			continue
		}
		raw, _ := json.Marshal(c.pred)
		var spec autoscalerv1alpha1.PredictiveAutoscalerSpec
		if jerr := json.Unmarshal(raw, &spec.Prediction); jerr != nil {
			t.Fatal(jerr)
		}
		if _, rerr := resolveV2Settings(spec); (rerr == nil) != c.admitted {
			t.Errorf("%s: the resolver accepted=%v but admission accepted=%v (%v)", c.name, rerr == nil, c.admitted, rerr)
		}
	}
}

func TestEnvtestV2SchemaValidates(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeRecommend)
	ctx := context.Background()
	create := func(name string, spec map[string]interface{}) error {
		base := map[string]interface{}{"targetDeployment": map[string]interface{}{"name": "web", "namespace": f.req.Namespace},
			"minReplicas": int64(1), "maxReplicas": int64(3)}
		for k, v := range spec {
			base[k] = v
		}
		u := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": autoscalerv1alpha1.GroupVersion.String(), "kind": "PredictiveAutoscaler",
			"metadata": map[string]interface{}{"name": name, "namespace": f.req.Namespace}, "spec": base}}
		return f.c.Create(ctx, u)
	}
	pred := func(m map[string]interface{}) map[string]interface{} { return map[string]interface{}{"prediction": m} }
	for name, spec := range map[string]map[string]interface{}{
		"unknown-engine":   pred(map[string]interface{}{"engine": "prophet"}),
		"seven-minutes":    pred(map[string]interface{}{"sampleIntervalSeconds": int64(420)}),
		"weekly":           pred(map[string]interface{}{"seasonality": "dailyWeekly"}),
		"history-29":       pred(map[string]interface{}{"historyDays": int64(29)}),
		"risk-3000":        pred(map[string]interface{}{"risk": map[string]interface{}{"miscoverageBps": int64(3000)}}),
		"cost-ratio-low":   pred(map[string]interface{}{"trust": map[string]interface{}{"challengerCostRatioBps": int64(9000)}}),
		"extra-negative":   pred(map[string]interface{}{"predictiveExtra": map[string]interface{}{"maxReplicas": int64(-1)}}),
		"fallback-zero":    {"behavior": map[string]interface{}{"startup": map[string]interface{}{"fallbackDelaySeconds": int64(0)}}},
		"allowance-301":    {"behavior": map[string]interface{}{"startup": map[string]interface{}{"actuationAllowanceSeconds": int64(301)}}},
		"unknown-v2-field": pred(map[string]interface{}{"calibration": map[string]interface{}{"minWindow": int64(100)}}),
	} {
		err := create(name, spec)
		if name == "unknown-v2-field" {
			// Structural pruning drops an unknown field silently; it must not survive to be read as a setting.
			if err != nil {
				t.Errorf("%s: %v", name, err)
			}
			continue
		}
		if err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
	var pruned unstructured.Unstructured
	pruned.SetGroupVersionKind(autoscalerv1alpha1.GroupVersion.WithKind("PredictiveAutoscaler"))
	if err := f.c.Get(ctx, types.NamespacedName{Namespace: f.req.Namespace, Name: "unknown-v2-field"}, &pruned); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(pruned.Object, "spec", "prediction", "calibration", "minWindow"); found {
		t.Fatal("an unknown v2 field was stored")
	}
	if err := create("full", pred(map[string]interface{}{"engine": "statistical", "sampleIntervalSeconds": int64(60),
		"seasonality": "daily", "historyDays": int64(14), "settlementSeconds": int64(120), "outcomeGraceSeconds": int64(600),
		"risk": map[string]interface{}{"miscoverageBps": int64(1000)}, "predictiveExtra": map[string]interface{}{"maxReplicas": int64(0)}})); err != nil {
		t.Fatalf("valid v2 settings must be accepted: %v", err)
	}
}

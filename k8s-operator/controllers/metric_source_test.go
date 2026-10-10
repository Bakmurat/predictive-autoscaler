package controllers

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// B4a — the metric source: per-autoscaler query templates, compiled and published by the operator (DESIGN-B4).

func TestTheIstioPresetIsTodaysQuery(t *testing.T) {
	c, err := compileMetricQuery(nil, "demo", "nginx-test")
	if err != nil {
		t.Fatal(err)
	}
	// The query the operator ran before B4, minus the "* 60" that now happens once in Go.
	want := `sum(rate(istio_requests_total{reporter="destination",destination_workload="nginx-test",destination_workload_namespace="demo"}[1m]))`
	if c.Query != want {
		t.Fatalf("istio preset changed:\n got %s\nwant %s", c.Query, want)
	}
	same, _ := compileMetricQuery(&autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetIstio}, "demo", "nginx-test")
	if same != c {
		t.Fatal("an explicit istio preset must equal the default")
	}
}

func TestQueryTemplatesAllowOnlyTheTwoSubstitutions(t *testing.T) {
	prom := func(q string) *autoscalerv1alpha1.MetricSource {
		return &autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetPrometheus, Query: q}
	}
	for _, tc := range []struct {
		name string
		src  *autoscalerv1alpha1.MetricSource
		want string // empty = must fail
	}{
		{"fields", prom(`sum(rate(http_requests_total{namespace="{{ .Namespace }}",deployment="{{.Name}}"}[1m]))`),
			`sum(rate(http_requests_total{namespace="shop",deployment="web"}[1m]))`},
		{"comment", prom(`{{/* per second */}}sum(rate(x{d="{{ .Name }}"}[1m]))`), `sum(rate(x{d="web"}[1m]))`},
		{"plain_text", prom(`sum(rate(x[1m]))`), `sum(rate(x[1m]))`},
		{"predefined_function", prom(`{{ printf "%s" .Name }}`), ""},
		{"pipeline", prom(`{{ .Name | printf "%q" }}`), ""},
		{"len", prom(`{{ len .Name }}`), ""},
		{"if", prom(`{{ if .Name }}x{{ end }}`), ""},
		{"range", prom(`{{ range .Name }}x{{ end }}`), ""},
		{"with", prom(`{{ with .Name }}x{{ end }}`), ""},
		{"variable", prom(`{{ $n := .Name }}{{ $n }}`), ""},
		{"define", prom(`{{ define "x" }}y{{ end }}z`), ""},
		{"define_named_query", prom(`{{ define "query" }}sum(x){{ end }}`), ""},
		{"block", prom(`{{ block "query" . }}sum(x){{ end }}`), ""},
		{"trimmed_define", prom(`{{- define "query" -}}sum(x){{- end -}}`), ""},
		{"template_call", prom(`{{ template "query" }}`), ""},
		{"other_field", prom(`{{ .Other }}`), ""},
		{"nested_field", prom(`{{ .Name.Length }}`), ""},
		{"dot", prom(`{{ . }}`), ""},
		{"parse_error", prom(`{{ .Name `), ""},
		{"blank", prom("  \n "), ""},
		{"only_a_comment", prom(`{{/* nothing */}}`), ""},
		{"too_long", prom(`sum(` + strings.Repeat("x", maxRenderedQuery) + `)`), ""},
		{"istio_with_query", &autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetIstio, Query: "x"}, ""},
		{"prometheus_without_query", &autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetPrometheus}, ""},
		{"unknown_preset", &autoscalerv1alpha1.MetricSource{Preset: "nginx-ingress"}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, err := compileMetricQuery(tc.src, "shop", "web")
			if tc.want == "" {
				if err == nil || !stderrors.Is(err, errInvalidMetricQuery) {
					t.Fatalf("must be rejected as an invalid metric query: %q, %v", c.Query, err)
				}
				return
			}
			if err != nil || c.Query != tc.want {
				t.Fatalf("got %q, %v; want %q", c.Query, err, tc.want)
			}
		})
	}
}

func TestQueryValuesAreEscapedAndTheHashCoversTheExactBytes(t *testing.T) {
	src := &autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetPrometheus, Query: `x{n="{{ .Name }}"}`}
	c, err := compileMetricQuery(src, "shop", `we"b\x`)
	if err != nil {
		t.Fatal(err)
	}
	if c.Query != `x{n="we\"b\\x"}` {
		t.Fatalf("escaping: %s", c.Query)
	}
	spaced := &autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetPrometheus, Query: `x{n="{{ .Name }}"} `}
	d, _ := compileMetricQuery(spaced, "shop", `we"b\x`)
	if d.SHA256 == c.SHA256 || len(c.SHA256) != 64 {
		t.Fatal("a whitespace edit is another query: it must have another hash (no normalization)")
	}
}

func TestPrometheusURLPrefersTheNewSetting(t *testing.T) {
	t.Setenv("PROMETHEUS_URL", "")
	t.Setenv("VICTORIAMETRICS_URL", "http://old")
	if u := prometheusURL(); u != "http://old" {
		t.Fatalf("fallback: %s", u)
	}
	t.Setenv("PROMETHEUS_URL", "http://new")
	if u := prometheusURL(); u != "http://new" {
		t.Fatalf("PROMETHEUS_URL must win: %s", u)
	}
}

// The compiled query is in status, with the generation and target it belongs to, before the forecasting service is
// asked; the request carries the identity the service checks, and the reactive read uses the same query.
func TestTheCompiledQueryIsPublishedBeforeTheServiceIsAsked(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	var seen []MLPredictionRequest
	var statusAtCall *autoscalerv1alpha1.MetricSourceStatus
	ml := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, hr *http.Request) {
		var in MLPredictionRequest
		_ = json.NewDecoder(hr.Body).Decode(&in)
		seen = append(seen, in)
		statusAtCall = getPA(t, r, req).Status.MetricSource
		fmt.Fprint(w, `{"predictions":[600,600,600,600,600,600],"confidence":0.9}`)
	}))
	t.Cleanup(ml.Close)
	t.Setenv("ML_API_URL", ml.URL)
	var vmQueries []string
	vm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, hr *http.Request) {
		vmQueries = append(vmQueries, hr.URL.Query().Get("query"))
		fmt.Fprint(w, rateBody(t, "3000"))
	}))
	t.Cleanup(vm.Close)
	t.Setenv("PROMETHEUS_URL", vm.URL)

	d := runRPMReconcile(t, r, req, path)
	a := getPA(t, r, req)
	ms := a.Status.MetricSource
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	if ms == nil || ms.ObservedGeneration != a.Generation || ms.TargetUID != string(dep.UID) || ms.Contract != metricContract {
		t.Fatalf("status.metricSource: %+v (generation %d, target %s)", ms, a.Generation, dep.UID)
	}
	if statusAtCall == nil || statusAtCall.SHA256 != ms.SHA256 {
		t.Fatalf("the compiled query must be persisted before the service is called: %+v", statusAtCall)
	}
	if len(seen) != 1 || seen[0].MetricQuerySHA256 != ms.SHA256 || seen[0].AutoscalerName != a.Name ||
		seen[0].AutoscalerNamespace != a.Namespace || seen[0].AutoscalerGeneration != a.Generation ||
		seen[0].TargetUID != ms.TargetUID || seen[0].Contract != metricContract {
		t.Fatalf("request identity: %+v", seen)
	}
	if len(vmQueries) == 0 || vmQueries[0] != ms.Query {
		t.Fatalf("the reactive read must run the compiled query: %v", vmQueries)
	}
	if d.ForecastStatus != "used" || d.CurrentRPM != 3000 {
		t.Fatalf("decision: %+v", d)
	}
}

func TestAnInvalidQueryWithdrawsTheCompiledSourceAndWritesNothing(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	runRPMReconcile(t, r, req, path) // publishes a valid source and scales 1 → 5
	key := req.NamespacedName.String()
	r.predictionCache[key] = &cachedPrediction{binding: "anything", fetchedAt: time.Now()}
	a := getPA(t, r, req)
	a.Spec.Metrics.Requests.Source = &autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetPrometheus, Query: `{{ printf "x" }}`}
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	two := int32(2)
	dep.Spec.Replicas = &two // would be scaled to 5 if anything were written
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, r, req)
	got := getPA(t, r, req)
	if got.Status.MetricSource != nil {
		t.Fatalf("an invalid configuration must withdraw the compiled query: %+v", got.Status.MetricSource)
	}
	if c := condition(t, r, req.NamespacedName, "Ready"); c.Reason != "InvalidMetricQuery" {
		t.Fatalf("Ready: %+v", c)
	}
	if _, cached := r.predictionCache[key]; cached {
		t.Fatal("the cached forecast must be dropped")
	}
	if n := replicasOf(t, r, req.NamespacedName); n != 2 {
		t.Fatalf("nothing may be written: replicas %d", n)
	}
}

// A forecast computed on another signal, or from a service that does not report its signal, is never used or cached.
// (1200 req/min would mean 2 replicas against the measured 300: if used, the decision would change.)
func TestAForecastForAnotherQueryIsRefused(t *testing.T) {
	for name, body := range map[string]string{
		"other_hash":  `{"predictions":[1200,1200,1200,1200,1200,1200],"confidence":0.9,"metric_query_sha256":"0000","contract":"requests-per-second/v1"}`,
		"old_service": `{"predictions":[1200,1200,1200,1200,1200,1200],"confidence":0.9}`,
		"other_units": `{"predictions":[1200,1200,1200,1200,1200,1200],"confidence":0.9,"metric_query_sha256":"%s","contract":"requests-per-minute/v0"}`,
	} {
		t.Run(name, func(t *testing.T) {
			r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "300")
			ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, hr *http.Request) {
				var in MLPredictionRequest
				_ = json.NewDecoder(hr.Body).Decode(&in)
				if strings.Contains(body, "%s") {
					fmt.Fprintf(w, body, in.MetricQuerySHA256)
					return
				}
				fmt.Fprint(w, body)
			}))
			t.Cleanup(ml.Close)
			t.Setenv("ML_API_URL", ml.URL)
			d := runRPMReconcile(t, r, req, path)
			if d.ForecastStatus == "used" || d.AppliedReplicas != 1 {
				t.Fatalf("the forecast must not drive the decision: %+v", d)
			}
			if _, cached := r.predictionCache[req.NamespacedName.String()]; cached {
				t.Fatal("a refused forecast must not be cached")
			}
		})
	}
}

// A replaced target changes the binding: a forecast cached for the old object is not reused.
func TestAReplacedTargetInvalidatesTheCachedForecast(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "300") // reactive: 1 replica
	key := req.NamespacedName.String()
	now := time.Now()
	r.predictionCache[key] = &cachedPrediction{binding: reconcileBinding(t, r, req), fetchedAt: now, response: &MLPredictionResponse{
		Predictions: []float64{1200, 1200, 1200, 1200, 1200, 1200}, Confidence: 0.9, anchorAt: now.Add(-time.Minute), issuedAt: now,
	}}
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	dep.UID = types.UID("replacement")
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	d := runRPMReconcile(t, r, req, path)
	if d.ForecastStatus == "used" || d.AppliedReplicas != 1 {
		t.Fatalf("the old target's forecast must not be used: %+v", d)
	}
}

// Codex r11: a huge but finite rate must clamp to maxReplicas, never wrap through int32 into a small count.
func TestAHugeRateClampsToMaxReplicas(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	vmServer(t, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1e15"]}]}}`, 200)
	d := runRPMReconcile(t, r, req, path)
	if d.ReactiveReplicas != math.MaxInt32 || d.DesiredReplicas != 12 || d.AppliedReplicas != 12 {
		t.Fatalf("a huge rate must saturate and clamp to max: %+v", d)
	}
}

// Codex r11 BLOCKER: the overestimate override, its streak, the ramp reference and the scale-down timer belong to one
// signal. After a query edit they restart, so an inherited override cannot bypass stabilization on the new signal.
func TestAQueryEditRestartsTheSignalState(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "300") // needs 1 replica
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	five := int32(5)
	dep.Spec.Replicas = &five
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	if d := runRPMReconcile(t, r, req, path); d.Action != "hold_stabilizing" {
		t.Fatalf("precondition: %+v", d)
	}
	st := r.scaleStates[req.NamespacedName.String()]
	scaledUp := time.Now().Add(-2 * time.Hour)
	st.overrideActive, st.overestimateStreak, st.lastRPM = true, 3, 999
	st.belowCurrentSince = time.Now().Add(-time.Hour) // stabilization long passed for the OLD signal
	st.lastScaleUp = scaledUp

	a := getPA(t, r, req)
	a.Spec.Metrics.Requests.Source = &autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetPrometheus,
		Query: `sum(rate(http_requests_total{namespace="{{ .Namespace }}",deployment="{{ .Name }}"}[1m]))`}
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "hold_stabilizing" || d.AppliedReplicas != 5 {
		t.Fatalf("the new signal must start its own stabilization, not inherit the old one: %+v", d)
	}
	st = r.scaleStates[req.NamespacedName.String()]
	if st.overrideActive || st.overestimateStreak != 0 || st.lastRPM == 999 || !st.lastScaleUp.Equal(scaledUp) {
		t.Fatalf("signal state not restarted or write history lost: %+v", st)
	}
}

// The same with a usable forecast on the new signal (Codex r12): the reset, not the forecast-unavailable fallback,
// clears the inherited override.
func TestAQueryEditRestartsTheOverrideWithAUsableForecast(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "300") // reactive: 1 replica
	ml := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"predictions":[540,540,540,540,540,540],"confidence":0.9}`) // 1 replica: no overestimate
	}))
	t.Cleanup(ml.Close)
	t.Setenv("ML_API_URL", ml.URL)
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	five := int32(5)
	dep.Spec.Replicas = &five
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	if d := runRPMReconcile(t, r, req, path); d.ForecastStatus != "used" || d.Action != "hold_stabilizing" {
		t.Fatalf("precondition: %+v", d)
	}
	st := r.scaleStates[req.NamespacedName.String()]
	st.overrideActive, st.overestimateStreak = true, 3
	st.belowCurrentSince = time.Now().Add(-time.Hour)

	a := getPA(t, r, req)
	a.Spec.Metrics.Requests.Source = &autoscalerv1alpha1.MetricSource{Preset: autoscalerv1alpha1.PresetPrometheus,
		Query: `sum(rate(http_requests_total{namespace="{{ .Namespace }}",deployment="{{ .Name }}"}[1m]))`}
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	d := runRPMReconcile(t, r, req, path)
	if d.ForecastStatus != "used" || d.Action != "hold_stabilizing" || d.AppliedReplicas != 5 {
		t.Fatalf("the inherited override must not let the new signal skip stabilization: %+v", d)
	}
	if st = r.scaleStates[req.NamespacedName.String()]; st.overrideActive {
		t.Fatal("the override must restart with the signal")
	}
}

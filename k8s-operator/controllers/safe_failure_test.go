package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	appsv1 "k8s.io/api/apps/v1"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

func hasString(xs []string, x string) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}

// gaugeValue reads one child of a GaugeVec without creating it (Gather, not WithLabelValues).
func gaugeValue(t *testing.T, g *prometheus.GaugeVec, app, namespace string) (float64, bool) {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(g)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		for _, m := range family.Metric {
			labels := map[string]string{}
			for _, l := range m.Label {
				labels[l.GetName()] = l.GetValue()
			}
			if labels["application"] == app && labels["namespace"] == namespace {
				return m.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// Missing metrics must hold the current replica count, never read as zero traffic (plan item #0; user decision
// 2026-10-09: missing or failed metrics → HOLD).

// rateBody is the metrics answer for a measured rate given in requests per minute; the query contract is per second.
func rateBody(t *testing.T, rpm string) string {
	t.Helper()
	v, err := strconv.ParseFloat(rpm, 64)
	if err != nil {
		t.Fatal(err)
	}
	return `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"` + strconv.FormatFloat(v/60, 'f', -1, 64) + `"]}]}}`
}

// echoProvenance wraps a stub forecasting service: like the real service, it reports the request's compiled-query hash
// and contract in a successful JSON answer (spliced in, so the rest of the stub's bytes are unchanged).
func echoProvenance(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(req.Body)
		req.Body = io.NopCloser(bytes.NewReader(raw))
		var in MLPredictionRequest
		_ = json.Unmarshal(raw, &in)
		rec := httptest.NewRecorder()
		next(rec, req)
		out := rec.Body.Bytes()
		if rec.Code == http.StatusOK && bytes.HasPrefix(bytes.TrimSpace(out), []byte("{")) &&
			!bytes.Contains(out, []byte(`"metric_query_sha256"`)) {
			trimmed := bytes.TrimSpace(out)
			fields := fmt.Sprintf(`{"metric_query_sha256":%q,"contract":%q`, in.MetricQuerySHA256, in.Contract)
			if !bytes.HasPrefix(bytes.TrimSpace(trimmed[1:]), []byte("}")) {
				fields += ","
			}
			out = append([]byte(fields), trimmed[1:]...)
		}
		for k, v := range rec.Header() {
			w.Header()[k] = v
		}
		w.WriteHeader(rec.Code)
		_, _ = w.Write(out)
	}
}

// withSource gives a test autoscaler the compiled metric source a reconcile would have published (once).
func withSource(t *testing.T, a *autoscalerv1alpha1.PredictiveAutoscaler) *autoscalerv1alpha1.PredictiveAutoscaler {
	t.Helper()
	if a.Status.MetricSource == nil {
		c, err := compileMetricQuery(requestsSource(a), a.Spec.TargetDeployment.Namespace, a.Spec.TargetDeployment.Name)
		if err != nil {
			t.Fatal(err)
		}
		a.Status.MetricSource = &autoscalerv1alpha1.MetricSourceStatus{Query: c.Query, SHA256: c.SHA256, Contract: metricContract}
	}
	return a
}

func vmServer(t *testing.T, body string, code int) {
	t.Helper()
	vm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(code)
		fmt.Fprint(w, body)
	}))
	t.Cleanup(vm.Close)
	t.Setenv("VICTORIAMETRICS_URL", vm.URL)
}

func TestQueryCurrentRPMOnlyAcceptsOneFiniteMeasurement(t *testing.T) {
	r := &PredictiveAutoscalerReconciler{}
	for _, tc := range []struct {
		name, body string
		code       int
		want       float64
		ok         bool
	}{
		{"measured", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1234.5"]}]}}`, 200, 1234.5, true},
		{"real_zero", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"0"]}]}}`, 200, 0, true},
		{"no_series", `{"status":"success","data":{"resultType":"vector","result":[]}}`, 200, 0, false},
		{"not_success", `{"status":"error","errorType":"timeout","data":{"resultType":"vector","result":[]}}`, 200, 0, false},
		{"partial", `{"status":"success","isPartial":true,"data":{"resultType":"vector","result":[{"value":[1,"5"]}]}}`, 200, 0, false},
		{"two_series", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"5"]},{"value":[1,"6"]}]}}`, 200, 0, false},
		{"nan", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"NaN"]}]}}`, 200, 0, false},
		{"inf", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"+Inf"]}]}}`, 200, 0, false},
		{"overflow_after_conversion", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"1e308"]}]}}`, 200, 0, false},
		{"negative", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"-3"]}]}}`, 200, 0, false},
		{"not_a_number", `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"x"]}]}}`, 200, 0, false},
		{"http_error", `oops`, 503, 0, false},
		{"matrix_result", `{"status":"success","data":{"resultType":"matrix","result":[{"value":[1,"5"]}]}}`, 200, 0, false},
		{"malformed_timestamp", `{"status":"success","data":{"resultType":"vector","result":[{"value":["x","5"]}]}}`, 200, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			vmServer(t, tc.body, tc.code)
			got, err := r.queryCurrentRPM(context.Background(), `sum(rate(requests_total[1m]))`)
			if tc.ok {
				if err != nil || got != tc.want*60 { // the query answers per second; the operator works per minute
					t.Fatalf("got %v, %v; want %v", got, err, tc.want)
				}
				return
			}
			if err == nil {
				t.Fatalf("got a measurement %v from %s; must be unavailable", got, tc.name)
			}
			if tc.code == 200 && !errors.Is(err, errMetricsUnavailable) {
				t.Fatalf("error %v is not errMetricsUnavailable", err)
			}
		})
	}
}

func TestUnifiedDesired(t *testing.T) {
	for _, tc := range []struct {
		name                       string
		pred, react, min, max, cur int32
		used, telemetry            bool
		want                       int32
		hold                       bool
	}{
		{"forecast_above_reactive", 6, 4, 1, 12, 3, true, true, 6, false},
		{"reactive_above_forecast", 3, 5, 1, 12, 3, true, true, 5, false},
		{"unused_forecast_ignored", 9, 2, 1, 12, 3, false, true, 2, false},
		{"zero_traffic_measured_scales_to_min", 0, 0, 1, 12, 5, false, true, 1, false},
		{"clamped_to_max", 20, 0, 1, 12, 3, true, true, 12, false},
		{"telemetry_missing_no_forecast_holds", 0, 0, 1, 12, 5, false, false, 5, true},
		{"telemetry_missing_forecast_lower_holds", 2, 0, 1, 12, 5, true, false, 5, true},
		{"telemetry_missing_forecast_higher_still_holds", 9, 0, 1, 12, 5, true, false, 5, true},
		{"telemetry_missing_respects_max", 0, 0, 1, 4, 6, false, false, 4, true},
		{"telemetry_missing_respects_min", 0, 0, 2, 12, 1, false, false, 2, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, hold := unifiedDesired(tc.pred, tc.react, tc.min, tc.max, tc.cur, tc.used, tc.telemetry)
			if got != tc.want || hold != tc.hold {
				t.Fatalf("got (%d, %v), want (%d, %v)", got, hold, tc.want, tc.hold)
			}
		})
	}
}

// The live reconcile: with the metrics answer unavailable, a workload at 5 replicas stays at 5 — with no forecast
// (formerly: both inputs failing) and with a fresh forecast lower or higher than the current count.
func TestReconcileHoldsWhenTheCurrentRateIsUnavailable(t *testing.T) {
	for _, tc := range []struct {
		name     string
		vmBody   string
		forecast []float64
	}{
		{"empty_result_no_forecast", `{"status":"success","data":{"resultType":"vector","result":[]}}`, nil},
		{"partial_with_low_forecast", `{"status":"success","isPartial":true,"data":{"resultType":"vector","result":[{"value":[1,"600"]}]}}`, []float64{600, 600, 600, 600, 600, 600}},
		{"error_status_with_high_forecast", `{"status":"error","data":{"resultType":"vector","result":[]}}`, []float64{6000, 6000, 6000, 6000, 6000, 6000}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, _, req, path := rpmGaugeReconciler(t)
			var d appsv1.Deployment
			if err := r.Get(context.Background(), req.NamespacedName, &d); err != nil {
				t.Fatal(err)
			}
			five := int32(5)
			d.Spec.Replicas = &five
			if err := r.Update(context.Background(), &d); err != nil {
				t.Fatal(err)
			}
			vmServer(t, tc.vmBody, 200)
			ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
			t.Cleanup(ml.Close)
			t.Setenv("ML_API_URL", ml.URL)
			if tc.forecast != nil {
				now := time.Now()
				r.predictionCache[req.NamespacedName.String()] = &cachedPrediction{binding: reconcileBinding(t, r, req), fetchedAt: now, response: &MLPredictionResponse{
					Predictions: tc.forecast, Confidence: 0.9, anchorAt: now.Add(-time.Minute), issuedAt: now,
				}}
			}
			currentRpmGauge.WithLabelValues(req.Name, req.Namespace).Set(4321) // a previous measurement
			t.Cleanup(func() { currentRpmGauge.DeleteLabelValues(req.Name, req.Namespace) })
			rec := runRPMReconcile(t, r, req, path)
			if rec.AppliedReplicas != 5 || rec.DesiredReplicas != 5 || rec.DesiredSource != "keep_current" {
				t.Fatalf("metrics unavailable must hold 5 replicas: %+v", rec)
			}
			if rec.TelemetryStatus != "unavailable" || rec.TelemetryError == nil ||
				!hasString(rec.Safeguards, "telemetry_unavailable_hold") {
				t.Fatalf("the record must say the telemetry was unavailable and the decision held: %+v", rec)
			}
			for name, g := range map[string]*prometheus.GaugeVec{"current_rpm": currentRpmGauge,
				"actual_needed": actualNeededReplicasGauge, "error_pct": predictionErrorPercentGauge} {
				if v, present := gaugeValue(t, g, req.Name, req.Namespace); present {
					t.Fatalf("%s gauge %v must be absent without a measurement", name, v)
				}
			}
		})
	}
}

// With the rate measured, the reconcile still follows the normal rule (a measured zero is a real zero).
func TestReconcileStillScalesOnAMeasuredRate(t *testing.T) {
	r, _, req, path := rpmGaugeReconciler(t)
	vmServer(t, rateBody(t, "3000"), 200)
	ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	t.Cleanup(ml.Close)
	t.Setenv("ML_API_URL", ml.URL)
	rec := runRPMReconcile(t, r, req, path)
	t.Cleanup(func() { currentRpmGauge.DeleteLabelValues(req.Name, req.Namespace) })
	if rec.AppliedReplicas != 5 || rec.DesiredSource == "keep_current" || rec.TelemetryStatus != "measured" ||
		rec.TelemetryError != nil { // 3000 rpm / 600 rpm per pod (targetRPS 10)
		t.Fatalf("a measured rate must drive the reactive rule: %+v", rec)
	}
	if v, present := gaugeValue(t, currentRpmGauge, req.Name, req.Namespace); !present || v != 3000 {
		t.Fatalf("current_rpm gauge %v present=%v, want 3000", v, present)
	}
}

// A measured zero is real zero traffic: the gauge shows 0 and the decision follows the normal rule.
func TestReconcilePublishesAMeasuredZero(t *testing.T) {
	r, _, req, path := rpmGaugeReconciler(t)
	vmServer(t, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"0"]}]}}`, 200)
	ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	t.Cleanup(ml.Close)
	t.Setenv("ML_API_URL", ml.URL)
	rec := runRPMReconcile(t, r, req, path)
	t.Cleanup(func() { currentRpmGauge.DeleteLabelValues(req.Name, req.Namespace) })
	if rec.TelemetryStatus != "measured" || rec.DesiredSource == "keep_current" || rec.AppliedReplicas != 1 {
		t.Fatalf("a measured zero must follow the normal rule: %+v", rec)
	}
	if v, present := gaugeValue(t, currentRpmGauge, req.Name, req.Namespace); !present || v != 0 {
		t.Fatalf("current_rpm gauge %v present=%v, want a present 0", v, present)
	}
}

package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/go-logr/logr"
	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

func attemptRows(t *testing.T, path string) []map[string]interface{} {
	t.Helper()
	data, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	var rows []map[string]interface{}
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if line == "" {
			continue
		}
		var row map[string]interface{}
		if err := json.Unmarshal([]byte(line), &row); err != nil {
			t.Fatal(err)
		}
		rows = append(rows, row)
	}
	return rows
}

func attemptEvents(rows []map[string]interface{}, event string) []map[string]interface{} {
	var out []map[string]interface{}
	for _, row := range rows {
		if row["event"] == event {
			out = append(out, row)
		}
	}
	return out
}

func attemptFixture(t *testing.T) (*PredictiveAutoscalerReconciler, *autoscalerv1alpha1.PredictiveAutoscaler, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "forecast.jsonl")
	t.Setenv("FORECAST_LOG", path)
	a := &autoscalerv1alpha1.PredictiveAutoscaler{}
	a.Name, a.Namespace = "predictive", "control"
	a.Spec.TargetDeployment.Name, a.Spec.TargetDeployment.Namespace = "app", "demo"
	a.Spec.Prediction.HorizonMinutes = 60
	compiled, err := compileMetricQuery(nil, "demo", "app")
	if err != nil {
		t.Fatal(err)
	}
	a.Status.MetricSource = &autoscalerv1alpha1.MetricSourceStatus{Query: compiled.Query, SHA256: compiled.SHA256,
		Contract: metricContract}
	return &PredictiveAutoscalerReconciler{Log: logr.Discard(), predictionCache: map[string]*cachedPrediction{}}, a, path
}

func TestForecastAttemptLedgerFreshCacheAndStale(t *testing.T) {
	r, a, path := attemptFixture(t)
	var calls, status int32
	srv := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, req *http.Request) {
		atomic.AddInt32(&calls, 1)
		if atomic.LoadInt32(&status) != 0 {
			w.WriteHeader(502)
			return
		}
		fmt.Fprint(w, `{"predictions":[100,100,100,100,100,100],"confidence":0.9}`)
	}))
	defer srv.Close()
	t.Setenv("ML_API_URL", srv.URL)
	first, err := r.getCachedPrediction(context.Background(), a, "k")
	if err != nil {
		t.Fatal(err)
	}
	hit, err := r.getCachedPrediction(context.Background(), a, "k")
	if err != nil || hit != first || atomic.LoadInt32(&calls) != 1 {
		t.Fatal("cache behavior changed")
	}
	r.predictionCache["k"].fetchedAt = time.Now().Add(-6 * time.Minute)
	atomic.StoreInt32(&status, 502)
	stale, err := r.getCachedPrediction(context.Background(), a, "k")
	if err != nil || stale != first || atomic.LoadInt32(&calls) != 2 {
		t.Fatal("stale behavior changed")
	}
	rows := attemptRows(t, path)
	starts, ends := attemptEvents(rows, "forecast_attempt_started"), attemptEvents(rows, "forecast_attempt_completed")
	if len(starts) != 2 || len(ends) != 2 {
		t.Fatalf("missing fresh-attempt ledger: starts=%d ends=%d", len(starts), len(ends))
	}
	if starts[0]["attempt_id"] == starts[1]["attempt_id"] || ends[1]["outcome"] != "http_error" {
		t.Fatal("attempt identity/outcome lost")
	}
	if ends[0]["attempt_id"] != starts[0]["attempt_id"] || ends[1]["attempt_id"] != starts[1]["attempt_id"] {
		t.Fatal("completion cross-linked")
	}
	for _, row := range append(starts, ends...) {
		if _, ok := row["issued_at"]; ok {
			t.Fatal("ledger event masquerades as issuance")
		}
	}
}

func TestForecastAttemptLedgerRawServedStatusesAndEmpty(t *testing.T) {
	for _, tc := range []struct {
		name, body, state string
		wantSteps         int
		wantErr           bool
	}{
		{"null_step", `{"predictions":[0,null,2],"confidence":0.9,"components":{"network_finite_per_step":[false,true,false]}}`, "array", 3, false},
		{"empty", `{"predictions":[],"confidence":0.9}`, "array", 0, false},
		{"null_array", `{"predictions":null,"confidence":0.9}`, "null", 0, false},
		{"malformed_step", `{"predictions":[0,"bad"],"confidence":0.9}`, "array", 2, true},
		{"overflow_step", `{"predictions":[1e999],"confidence":0.9}`, "array", 1, true},
		{"trailing_garbage", `{"predictions":[0,null,2],"confidence":0.9} trailing garbage`, "array", 3, false},
		{"second_document", `{"predictions":[1],"confidence":0.9} {"predictions":[null]}`, "array", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, a, path := attemptFixture(t)
			srv := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, req *http.Request) { fmt.Fprint(w, tc.body) }))
			defer srv.Close()
			t.Setenv("ML_API_URL", srv.URL)
			p, err := r.getCachedPrediction(context.Background(), a, "k")
			if (err != nil) != tc.wantErr {
				t.Fatalf("decode behavior changed: %v", err)
			}
			if tc.name == "null_step" && p.Predictions[1] != 0 {
				t.Fatal("legacy null decode changed")
			}
			rows := attemptRows(t, path)
			ends := attemptEvents(rows, "forecast_attempt_completed")
			if len(ends) != 1 {
				t.Fatalf("missing completion for %s", tc.name)
			}
			if ends[0]["served_predictions_status"] != tc.state || ends[0]["served_step_count"] != float64(tc.wantSteps) {
				t.Fatal(ends[0])
			}
			if (tc.name == "null_step" || tc.name == "trailing_garbage") && ends[0]["served_step_status"].([]interface{})[1] != "null" {
				t.Fatal("raw null became finite")
			}
			if tc.name == "second_document" && (p.Predictions[0] != 1 || ends[0]["served_step_status"].([]interface{})[0] != "finite") {
				t.Fatal("diagnostics did not bind accepted first document")
			}
			if tc.name == "overflow_step" && ends[0]["served_step_status"].([]interface{})[0] != "non_finite" {
				t.Fatal("overflow was not distinguished")
			}
			if tc.wantSteps == 0 {
				for _, row := range rows {
					if row["issued_at"] != nil {
						t.Fatal("empty response invented issuance")
					}
				}
			}
		})
	}
}

func TestForecastAttemptLedgerDecisionLinks(t *testing.T) {
	r, _, req, path := rpmGaugeReconciler(t)
	srv := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, req *http.Request) {
		fmt.Fprint(w, `{"predictions":[600,600,600,600,600,600],"confidence":0.9}`)
	}))
	defer srv.Close()
	t.Setenv("ML_API_URL", srv.URL)
	d := runRPMReconcile(t, r, req, path)
	if d.ForecastStatus != "used" || d.AppliedReplicas != 1 {
		t.Fatal("scaling behavior changed", d)
	}
	rows := attemptRows(t, path)
	decisions := attemptEvents(rows, "decision")
	lookup, ok := decisions[0]["forecast_lookup"].(map[string]interface{})
	if !ok {
		t.Fatal("decision has no lookup evidence")
	}
	if lookup["resolution"] != "fresh_response" || lookup["returned_issuance_id"] == nil {
		t.Fatal(lookup)
	}
	runRPMReconcile(t, r, req, path)
	rows = attemptRows(t, path)
	decisions = attemptEvents(rows, "decision")
	second := decisions[1]["forecast_lookup"].(map[string]interface{})
	if second["resolution"] != "cache_hit" || second["returned_issuance_id"] != lookup["returned_issuance_id"] || second["fresh_attempt_id"] != nil {
		t.Fatal(second)
	}
}

func TestForecastAttemptLedgerMarshalFailureDoesNotAppendBlankLine(t *testing.T) {
	r, _, path := attemptFixture(t)
	r.appendForecastLog(map[string]interface{}{"value": math.NaN()})
	data, err := os.ReadFile(path)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if len(data) != 0 {
		t.Fatalf("marshal failure appended corrupt evidence: %q", data)
	}
}

type attemptRoundTripper func(*http.Request) (*http.Response, error)

func (f attemptRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type attemptTimeoutBody struct{}

func (attemptTimeoutBody) Read([]byte) (int, error) { return 0, context.DeadlineExceeded }
func (attemptTimeoutBody) Close() error             { return nil }

func TestForecastAttemptLedgerTimeoutStagesPreserveCacheClass(t *testing.T) {
	for _, status := range []int{0, 200, 422, 502} {
		t.Run(fmt.Sprint(status), func(t *testing.T) {
			r, a, path := attemptFixture(t)
			oldTransport := http.DefaultTransport
			http.DefaultTransport = attemptRoundTripper(func(req *http.Request) (*http.Response, error) {
				if status == 0 {
					return nil, context.DeadlineExceeded
				}
				return &http.Response{StatusCode: status, Body: attemptTimeoutBody{}, Header: make(http.Header)}, nil
			})
			defer func() { http.DefaultTransport = oldTransport }()
			cached := &MLPredictionResponse{Predictions: []float64{1, 2}}
			r.predictionCache["k"] = &cachedPrediction{response: cached, fetchedAt: time.Now().Add(-6 * time.Minute), binding: forecastBinding(a)}
			p, err, lookup := r.getCachedPredictionObserved(context.Background(), a, "k")
			if status == 422 {
				if p != nil || !isForecastRefusal(err) || lookup.Resolution != "unavailable" {
					t.Fatal("refusal fallback changed")
				}
			} else if err != nil || p != cached || lookup.Resolution != "stale_after_error" {
				t.Fatal("nonrefusal fallback changed")
			}
			ends := attemptEvents(attemptRows(t, path), "forecast_attempt_completed")
			if len(ends) != 1 || ends[0]["error_class"] != "timeout" {
				t.Fatal(ends)
			}
			wantStage, wantOutcome := "request", "timeout"
			if status == 200 {
				wantStage = "decode"
			}
			if status == 422 {
				wantStage, wantOutcome = "response_body", "http_refusal"
			}
			if status == 502 {
				wantStage, wantOutcome = "response_body", "http_error"
			}
			if ends[0]["error_stage"] != wantStage || ends[0]["outcome"] != wantOutcome {
				t.Fatal(ends[0])
			}
		})
	}
}

type attemptWriteCloser struct {
	writes, closes     int
	short              bool
	writeErr, closeErr error
}

func (f *attemptWriteCloser) Write(p []byte) (int, error) {
	f.writes++
	if f.short {
		return len(p) - 1, f.writeErr
	}
	return len(p), f.writeErr
}
func (f *attemptWriteCloser) Close() error { f.closes++; return f.closeErr }

type attemptLogSink struct{ errors *int }

func (s attemptLogSink) Init(logr.RuntimeInfo)                  {}
func (s attemptLogSink) Enabled(int) bool                       { return false }
func (s attemptLogSink) Info(int, string, ...interface{})       {}
func (s attemptLogSink) Error(error, string, ...interface{})    { *s.errors++ }
func (s attemptLogSink) WithValues(...interface{}) logr.LogSink { return s }
func (s attemptLogSink) WithName(string) logr.LogSink           { return s }

func TestForecastAttemptLedgerWriterFailuresAreObservedOnly(t *testing.T) {
	for _, mode := range []string{"open", "short", "write", "close"} {
		t.Run(mode, func(t *testing.T) {
			r, a, _ := attemptFixture(t)
			count := 0
			r.Log = logr.New(attemptLogSink{&count})
			file := &attemptWriteCloser{short: mode == "short"}
			if mode == "write" {
				file.writeErr = errors.New("write fixture")
			}
			if mode == "close" {
				file.closeErr = errors.New("close fixture")
			}
			r.forecastLedger.open = func(string) (io.WriteCloser, error) {
				if mode == "open" {
					return nil, errors.New("open fixture")
				}
				return file, nil
			}
			srv := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"predictions":[1,2],"confidence":0.9}`) }))
			defer srv.Close()
			t.Setenv("ML_API_URL", srv.URL)
			p, err := r.getCachedPrediction(context.Background(), a, "k")
			if err != nil || p == nil || len(p.Predictions) != 2 || r.predictionCache["k"].response != p {
				t.Fatal("logging changed prediction/cache", err)
			}
			if count == 0 {
				t.Fatal("write failure not reported")
			}
			if mode != "open" && (file.writes != file.closes || file.writes == 0) {
				t.Fatal("writer not closed", file)
			}
		})
	}
}

func TestForecastAttemptLedgerDisabledAndIdentityFailure(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			r, a, path := attemptFixture(t)
			r.forecastLedger.random = bytes.NewReader(nil)
			if disabled {
				t.Setenv("FORECAST_LOG", "")
			}
			srv := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"predictions":[1,2],"confidence":0.9}`) }))
			defer srv.Close()
			t.Setenv("ML_API_URL", srv.URL)
			p, err, lookup := r.getCachedPredictionObserved(context.Background(), a, "k")
			want := "initialization_failed"
			if disabled {
				want = "disabled"
			}
			if err != nil || p == nil || lookup.InstrumentationStatus != want || lookup.LookupID != "" || lookup.FreshAttemptID != "" || p.issuanceID != "" || lookup.ReturnedLinkStatus != "unavailable" {
				t.Fatal("identity failure changed behavior or fabricated links", lookup, err)
			}
			if len(attemptEvents(attemptRows(t, path), "forecast_attempt_started")) != 0 {
				t.Fatal("invented attempt identity")
			}
		})
	}
}

func TestForecastAttemptLedgerWriterSequencesConcurrent(t *testing.T) {
	r, a, path := attemptFixture(t)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			lookup := r.newForecastLookup(a)
			r.beginForecastAttempt(lookup, []byte(`{}`))
		}()
	}
	wg.Wait()
	rows := attemptRows(t, path)
	if len(rows) != 33 {
		t.Fatal("lost event", len(rows))
	}
	seen := map[string]bool{}
	for i, row := range rows {
		if row["event_seq"] != float64(i+1) {
			t.Fatal("unordered event sequence", row)
		}
		if id, ok := row["attempt_id"].(string); ok {
			if seen[id] {
				t.Fatal("duplicate id")
			}
			seen[id] = true
		}
	}
}

func TestForecastAttemptLedgerRequestIdentityAndSyntheticFixture(t *testing.T) {
	r, a, req, path := rpmGaugeReconciler(t)
	var body atomic.Value
	var status int32
	srv := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, request *http.Request) {
		rawBody, _ := io.ReadAll(request.Body)
		body.Store(rawBody)
		if s := atomic.LoadInt32(&status); s != 0 {
			w.WriteHeader(int(s))
			fmt.Fprint(w, `{"detail":"synthetic refusal"}`)
			return
		}
		fmt.Fprint(w, `{"predictions":[600,600,600,600,600,600],"confidence":0.9}`)
	}))
	defer srv.Close()
	t.Setenv("ML_API_URL", srv.URL)
	runRPMReconcile(t, r, req, path)
	runRPMReconcile(t, r, req, path)
	r.predictionCache[req.NamespacedName.String()].fetchedAt = time.Now().Add(-6 * time.Minute)
	atomic.StoreInt32(&status, 502)
	runRPMReconcile(t, r, req, path)
	atomic.StoreInt32(&status, 422)
	runRPMReconcile(t, r, req, path)
	rows := attemptRows(t, path)
	starts := attemptEvents(rows, "forecast_attempt_started")
	decisions := attemptEvents(rows, "decision")
	if len(starts) != 3 || len(decisions) != 4 {
		t.Fatal("wrong counts")
	}
	hash := sha256.Sum256(body.Load().([]byte))
	for _, start := range starts {
		if start["request_body_sha256"] != hex.EncodeToString(hash[:]) || start["application"] != a.Spec.TargetDeployment.Name || start["request_namespace"] != a.Spec.TargetDeployment.Namespace || start["autoscaler_name"] != a.Name {
			t.Fatal("request identity mismatch", start)
		}
	}
	fresh := decisions[0]["forecast_lookup"].(map[string]interface{})
	stale := decisions[2]["forecast_lookup"].(map[string]interface{})
	refused := decisions[3]["forecast_lookup"].(map[string]interface{})
	if stale["resolution"] != "stale_after_error" || stale["returned_issuance_id"] != fresh["returned_issuance_id"] || refused["resolution"] != "unavailable" || refused["cache_action"] != "delete" {
		t.Fatal("cache links changed")
	}
	if out := os.Getenv("FORECAST_ATTEMPT_FIXTURE_OUT"); out != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		file, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = file.Write(data); err != nil {
			t.Fatal(err)
		}
		if err = file.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestForecastAttemptLedgerEmptyWireNamespacePreservesLegacyIssuance(t *testing.T) {
	r, a, path := attemptFixture(t)
	a.Spec.TargetDeployment.Namespace = ""
	srv := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, req *http.Request) {
		var request MLPredictionRequest
		if err := json.NewDecoder(req.Body).Decode(&request); err != nil {
			t.Error(err)
		}
		if request.Namespace != "" {
			t.Error("wire namespace changed")
		}
		fmt.Fprint(w, `{"predictions":[1,2],"confidence":0.9}`)
	}))
	defer srv.Close()
	t.Setenv("ML_API_URL", srv.URL)
	_, err, lookup := r.getCachedPredictionObserved(context.Background(), a, "k")
	if err != nil {
		t.Fatal(err)
	}
	if lookup.Namespace != a.Namespace || lookup.RequestNamespace != "" {
		t.Fatal("identity mismatch", lookup)
	}
	rows := attemptRows(t, path)
	for _, row := range rows {
		if row["issued_at"] != nil && row["namespace"] != "" {
			t.Fatal("legacy namespace changed", row)
		}
		if row["event"] == "forecast_attempt_started" && (row["namespace"] != a.Namespace || row["request_namespace"] != "") {
			t.Fatal(row)
		}
	}
}

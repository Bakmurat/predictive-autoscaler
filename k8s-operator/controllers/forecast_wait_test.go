package controllers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The wait allowances (R3-01, forecast_wait.go): what reconciles wait for the forecasting service and for the metrics
// backend is accounted cumulatively, whatever the answers are, and every decision is still made.

// The figures the allowance is documented with (forecast_wait.go, docs/configuration.md).
func TestWaitPolicyFigures(t *testing.T) {
	for name, p := range map[string]waitPolicy{"forecast": forecastWaitPolicy, "metrics": metricsWaitPolicy} {
		if p.Request != 5*time.Second || p.Fleet != time.Duration(minReconcileSeconds)*time.Second || p.Key != time.Minute ||
			p.Minimum != 250*time.Millisecond || p.Request+p.Fleet/6 != 10*time.Second { // a full allowance plus one round's refill
			t.Fatalf("%s: %+v", name, p)
		}
	}
	if mlAPITimeout < forecastWaitPolicy.Request || vmQueryTimeout < metricsWaitPolicy.Request {
		t.Fatal("a client's own limit must not undercut the deadline the allowance grants")
	}
}

// fakeClock drives a waitAllowance in unit tests.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time { return c.t }

// ask is one request of key that waits for `takes` (or its whole deadline when takes is longer): it returns the
// deadline it was given (0: not sent).
func (c *fakeClock) ask(a *waitAllowance, key string, takes time.Duration) time.Duration {
	d, ok := a.grant(key)
	if !ok {
		return 0
	}
	if takes > d {
		takes = d
	}
	c.t = c.t.Add(takes)
	a.charge(key, takes)
	return d
}

// The reviewer's repro on PR #65: four consecutive answers that each take 70 % of the request deadline. Only timeouts
// started the cooldown, so each of the four got the full deadline (283 ms against a claimed 104 ms; at production
// figures twenty 4.9 s answers cost 98 s). Now a slow success, a slow error status and a timeout are charged alike.
func TestWaitAllowanceChargesSlowAnswersLikeTimeouts(t *testing.T) {
	policy := waitPolicy{Request: 100 * time.Millisecond, Fleet: 600 * time.Millisecond, Key: 1200 * time.Millisecond, Minimum: 5 * time.Millisecond}
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	a := &waitAllowance{policy: &policy, now: clock.now}
	start, full := clock.t, 0
	for i := 0; i < 4; i++ {
		if d := clock.ask(a, fmt.Sprintf("pa-%d", i), 70*time.Millisecond); d == policy.Request {
			full++
		}
	}
	elapsed := clock.t.Sub(start)
	if bound := policy.Request + elapsed*policy.Request/policy.Fleet; full != 1 || a.waited > bound {
		t.Fatalf("%d requests got the full deadline and %s was waited in %s, bound %s", full, a.waited, elapsed, bound)
	}
	// Production figures: twenty autoscalers, every answer takes 4.9 s (a success or an error status: the allowance
	// does not see which). One round costs at most the allowance, not 98 s.
	clock.t, a = time.Unix(1_800_000_000, 0), &waitAllowance{policy: &forecastWaitPolicy, now: clock.now}
	for round := 0; round < 4; round++ {
		began, before := clock.t, a.waited
		for i := 0; i < 20; i++ {
			clock.ask(a, fmt.Sprintf("pa-%d", i), 4900*time.Millisecond)
			clock.t = clock.t.Add(20 * time.Millisecond) // the rest of the reconcile
		}
		spent, took := a.waited-before, clock.t.Sub(began)
		if bound := 5*time.Second + took/6; spent > bound {
			t.Fatalf("round %d: %s waited in a round of %s, bound %s", round, spent, took, bound)
		}
		clock.t = began.Add(30 * time.Second) // the next round
	}
	if total, window := a.waited, clock.t.Sub(time.Unix(1_800_000_000, 0)); total > 5*time.Second+window/6 {
		t.Fatalf("%s waited in %s", total, window)
	}
}

// One autoscaler whose requests hang, next to healthy ones, in the same queue order every round: the hanging one takes
// the fleet allowance once, then only what its own allowance has regained, and the healthy ones are asked with real
// deadlines. When it stops hanging it gets the full deadline back by itself.
func TestWaitAllowanceIsFairToHealthyAutoscalersAndRecovers(t *testing.T) {
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	a := &waitAllowance{policy: &forecastWaitPolicy, now: clock.now}
	healthy := []string{"b", "c", "d"}
	for round := 0; round < 6; round++ {
		began, before := clock.t, a.waited
		hung := clock.ask(a, "a", time.Hour) // never answers: waits its whole deadline
		for _, k := range healthy {
			d := clock.ask(a, k, 50*time.Millisecond)
			if round == 0 && d != 0 {
				t.Fatalf("round 0: the allowance is gone, %s must not be sent (got %s)", k, d)
			}
			if round > 0 && d < 2*time.Second {
				t.Fatalf("round %d: healthy %s got %s although only one autoscaler hangs", round, k, d)
			}
		}
		// Its own allowance regains 5 s in 60 s: less than half of it by the next round.
		if (round == 0 && hung != 5*time.Second) || (round > 0 && (hung > 2500*time.Millisecond || hung < time.Second)) {
			t.Fatalf("round %d: the hanging autoscaler got %s", round, hung)
		}
		if spent := a.waited - before; spent > 5*time.Second+clock.t.Sub(began)/6 {
			t.Fatalf("round %d: %s waited", round, spent)
		}
		clock.t = began.Add(30 * time.Second)
	}
	// It answers again: after one quiet minute its own allowance is full, and so is its deadline.
	clock.t = clock.t.Add(time.Minute)
	if d := clock.ask(a, "a", 50*time.Millisecond); d != 5*time.Second {
		t.Fatalf("after recovery the deadline is %s, want the full 5 s (never a fixed probe)", d)
	}
	// An overrun past the deadline is charged too and repaid before anything else is sent.
	d, _ := a.grant("b")
	clock.t = clock.t.Add(d + 400*time.Millisecond)
	a.charge("b", d+400*time.Millisecond)
	if d, ok := a.grant("c"); ok {
		t.Fatalf("after an overrun the fleet allowance is in debt, yet %s was granted", d)
	}
}

// shortWaits shortens both wait policies for a test and restores them.
func shortWaits(t *testing.T, p waitPolicy) {
	t.Helper()
	oldF, oldM := forecastWaitPolicy, metricsWaitPolicy
	forecastWaitPolicy, metricsWaitPolicy = p, p
	t.Cleanup(func() { forecastWaitPolicy, metricsWaitPolicy = oldF, oldM })
}

// The policy of the fleet tests: one request waits at most 400 ms; the fleet allowance regains 100 ms per second.
var testWaits = waitPolicy{Request: 400 * time.Millisecond, Fleet: 4 * time.Second, Key: 8 * time.Second, Minimum: 40 * time.Millisecond}

// waitBound is the most all requests to one service may have waited since began, plus slack for one request's overrun
// past its deadline in a heavily loaded test run. The unfixed operator spent a full client timeout per reconcile.
func waitBound(p waitPolicy, began time.Time) time.Duration {
	return p.Request + time.Duration(float64(p.Request)*(float64(time.Since(began))/float64(p.Fleet))) + 2*time.Second
}

// serviceWaits replaces the default transport: requests to host go through via (nil: the real transport) after
// before(req) returned, and the deadline each one carried and the time it took are noted; every other request is
// untouched.
type serviceWaits struct {
	mu     sync.Mutex
	given  []time.Duration
	spent  time.Duration
	host   string
	direct http.RoundTripper
	via    http.RoundTripper
	before func(*http.Request) error
}

func (f *serviceWaits) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host != f.host {
		return f.direct.RoundTrip(req)
	}
	wait, start := time.Duration(-1), time.Now() // -1: no deadline at all
	if d, ok := req.Context().Deadline(); ok {
		wait = time.Until(d)
	}
	defer func() {
		f.mu.Lock()
		f.given, f.spent = append(f.given, wait), f.spent+time.Since(start)
		f.mu.Unlock()
	}()
	if f.before != nil {
		if err := f.before(req); err != nil {
			return nil, err
		}
	}
	via := f.via
	if via == nil {
		via = f.direct
	}
	return via.RoundTrip(req)
}

func (f *serviceWaits) take() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.given
	f.given = nil
	return out
}

func (f *serviceWaits) total() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.spent
}

// watchService installs a serviceWaits for the service at rawURL.
func watchService(t *testing.T, rawURL string) *serviceWaits {
	t.Helper()
	u, err := url.Parse(rawURL)
	if err != nil {
		t.Fatal(err)
	}
	f := &serviceWaits{host: u.Host, direct: http.DefaultTransport}
	http.DefaultTransport = f
	t.Cleanup(func() { http.DefaultTransport = f.direct })
	return f
}

// deafService is the URL of a service that never answers. accepts: the connection is accepted and the request read,
// and nothing comes back (a stuck service). Otherwise the connection is never established, as when the SYN is dropped
// (a default-deny egress policy, a Service without endpoints on a CNI that drops rather than refuses).
func deafService(t *testing.T, accepts bool) (string, *serviceWaits) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var conns []net.Conn
	var mu sync.Mutex
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c) // held open, never answered
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	rawURL := "http://" + ln.Addr().String()
	f := watchService(t, rawURL)
	via := &http.Transport{}
	if !accepts {
		via.DialContext = func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done() // no SYN-ACK ever arrives
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
		}
	}
	f.via = via
	t.Cleanup(via.CloseIdleConnections)
	return rawURL, f
}

// fleet adds n-1 copies of the harness autoscaler and its Deployment, so that n autoscalers share the one worker.
func fleet(t *testing.T, r *PredictiveAutoscalerReconciler, req ctrl.Request, n int) []ctrl.Request {
	t.Helper()
	ctx := context.Background()
	var a autoscalerv1alpha1.PredictiveAutoscaler
	var dep appsv1.Deployment
	if err := r.Get(ctx, req.NamespacedName, &a); err != nil {
		t.Fatal(err)
	}
	if err := r.Get(ctx, req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	reqs := []ctrl.Request{req}
	for i := 1; i < n; i++ {
		name := req.Name + "-" + string(rune('a'+i))
		pa := &autoscalerv1alpha1.PredictiveAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: req.Namespace}, Spec: *a.Spec.DeepCopy()}
		pa.Spec.TargetDeployment.Name = name
		d := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: req.Namespace, UID: types.UID("uid-" + name)},
			Spec: *dep.Spec.DeepCopy()}
		if err := r.Create(ctx, pa); err != nil {
			t.Fatal(err)
		}
		if err := r.Create(ctx, d); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			for _, g := range []interface{ DeleteLabelValues(...string) bool }{currentRpmGauge, actualNeededReplicasGauge,
				predictionErrorPercentGauge, predictedRpmGauge, predictedReplicasGauge, desiredReplicasGauge} {
				g.DeleteLabelValues(name, req.Namespace)
			}
			forgetConditionMetrics(req.Namespace, name)
			forgetV2Metrics(req.Namespace, name)
		})
		reqs = append(reqs, ctrl.Request{NamespacedName: types.NamespacedName{Namespace: req.Namespace, Name: name}})
	}
	return reqs
}

// countScaleWrites counts the /scale updates per Deployment.
func countScaleWrites(r *PredictiveAutoscalerReconciler) map[string]int {
	writes := map[string]int{}
	wrapClient(r, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				writes[obj.GetName()]++
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	return writes
}

// fleetHarness is n Active autoscalers at 1 replica with a rate that needs 5, on the v1 or the v2 forecast path.
func fleetHarness(t *testing.T, engine string, n int) (*PredictiveAutoscalerReconciler, []ctrl.Request, string, *v2Fixture) {
	t.Helper()
	var r *PredictiveAutoscalerReconciler
	var req ctrl.Request
	var path string
	var f *v2Fixture
	if engine == "" {
		r, req, path, _ = recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000") // 3000 rpm at 600 per replica: 5
	} else {
		f = v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
		r, req, path = f.r, f.req, f.path
	}
	r.MLAPI = newMLAPIClient("", "", 30*time.Second) // the client's own limit is far away: the allowance decides
	return r, fleet(t, r, req, n), path, f
}

// A forecasting service that accepts and never answers, and one that cannot be reached at all, with several autoscalers
// on both forecast paths: all forecast requests of a round together wait at most the fleet allowance, the requests the
// allowance does not cover are not sent, and each autoscaler's reactive decision is made and written once.
func TestADeafForecasterCostsTheFleetOneAllowanceAndDelaysNoReactiveDecision(t *testing.T) {
	const n = 4
	for _, engine := range []string{"", autoscalerv1alpha1.EngineStatistical} {
		for _, accepts := range []bool{true, false} {
			name := "engine=" + engine + "/the service accepts and never answers"
			if !accepts {
				name = "engine=" + engine + "/the connection is never established"
			}
			t.Run(name, func(t *testing.T) {
				shortWaits(t, testWaits)
				r, reqs, path, _ := fleetHarness(t, engine, n)
				rawURL, waits := deafService(t, accepts)
				t.Setenv("ML_API_URL", rawURL)
				writes := countScaleWrites(r)
				began, sent := time.Now(), 0
				for round := 1; round <= 3; round++ {
					for _, q := range reqs {
						d := runRPMReconcile(t, r, q, path)
						wantAction := "scale_up"
						if round > 1 {
							wantAction = "at_target"
						}
						if d.Application != q.Name || d.Action != wantAction || d.AppliedReplicas != 5 || d.DesiredReplicas != 5 ||
							d.DesiredSource != "reactive" || d.ForecastStatus != "unavailable" || d.TelemetryStatus != "measured" {
							t.Fatalf("round %d, %s: the reactive decision must stand: %+v", round, q.Name, d)
						}
						given := waits.take()
						if len(given) > 1 || (len(given) == 1 && (given[0] > testWaits.Request || given[0] < 0)) {
							t.Fatalf("round %d, %s: the forecast request carried %v, want at most one deadline of at most %s", round, q.Name, given, testWaits.Request)
						}
						if round == 1 && q == reqs[0] && (len(given) != 1 || given[0] < testWaits.Request/2) {
							t.Fatalf("the first request has the whole allowance: %v", given)
						}
						if d.V2 != nil && d.V2.Request != map[bool]string{true: "timeout", false: "not_sent"}[len(given) == 1] {
							t.Fatalf("round %d, %s: ledger v2.request %q with %d requests sent", round, q.Name, d.V2.Request, len(given))
						}
						sent += len(given)
					}
					// The bound, on what the requests took on the wire and on what the allowance charged.
					if bound := waitBound(testWaits, began); waits.total() > bound || r.forecastWait.waited > bound {
						t.Fatalf("round %d: the forecast requests waited %s (charged %s) in %s, bound %s", round, waits.total(),
							r.forecastWait.waited, time.Since(began), bound)
					}
				}
				if sent >= 3*n {
					t.Fatalf("all %d requests were sent: nothing was held back by the allowance", sent)
				}
				for _, q := range reqs {
					if writes[q.Name] != 1 {
						t.Fatalf("%s was written %d times, want once (1 to 5)", q.Name, writes[q.Name])
					}
					c := condition(t, r, q.NamespacedName, "ForecastAvailable")
					if c.Status != "False" || c.Reason != "Unavailable" {
						t.Fatalf("%s ForecastAvailable: %+v", q.Name, c)
					}
					if engine != "" {
						c := condition(t, r, q.NamespacedName, "PredictionReady")
						if c.Status != "False" || c.Reason != "DataUnavailable" ||
							!(strings.Contains(c.Message, "did not answer within") || strings.Contains(c.Message, "not asked")) {
							t.Fatalf("%s PredictionReady: %+v", q.Name, c)
						}
					}
				}
			})
		}
	}
}

// requestName is the autoscaler a forecast request (v1 or v2) is for; the body is restored.
func requestName(req *http.Request) string {
	raw, _ := io.ReadAll(req.Body)
	req.Body = io.NopCloser(strings.NewReader(string(raw)))
	var in struct {
		Name string `json:"autoscaler_name"`
	}
	_ = json.Unmarshal(raw, &in)
	return in.Name
}

// Slow answers are charged like timeouts (the reviewer's finding on PR #65): a service that answers every request
// correctly but slowly (v2: an eligible forecast; v1: an error status), and a fleet in which some autoscalers'
// requests hang and the others' are slow. Over three rounds the requests together wait at most the allowance; the
// unfixed operator gave every slow answer the full deadline (here 6 x 3 x 300 ms = 5.4 s for the slow fleets).
func TestSlowAndMixedForecastFleetsWaitAtMostTheAllowance(t *testing.T) {
	const n, slow = 6, 300 * time.Millisecond
	for _, tc := range []struct {
		name, engine string
		hung         int // how many of the n autoscalers' requests never answer; the others answer after `slow`
	}{
		{"slow successes (v2)", autoscalerv1alpha1.EngineStatistical, 0},
		{"slow error statuses (v1)", "", 0},
		{"some hung, the others slow (v2)", autoscalerv1alpha1.EngineStatistical, 2},
		{"some hung, the others slow (v1)", "", 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shortWaits(t, testWaits)
			r, reqs, path, f := fleetHarness(t, tc.engine, n)
			if f != nil {
				f.stub.set("eligible", 1000) // used: 7 replicas instead of the reactive 5
			}
			hung := map[string]bool{}
			for _, q := range reqs[:tc.hung] {
				hung[q.Name] = true
			}
			waits := watchService(t, mlAPIBaseURL())
			waits.before = func(req *http.Request) error {
				wait := time.After(slow)
				if hung[requestName(req)] {
					wait = nil // never
				}
				select {
				case <-wait:
					return nil // then the real answer: the stub's eligible forecast (v2) or its 503 (v1)
				case <-req.Context().Done():
					return req.Context().Err()
				}
			}
			began, answered, sent := time.Now(), 0, 0
			for round := 1; round <= 3; round++ {
				for _, q := range reqs {
					d := runRPMReconcile(t, r, q, path)
					given := waits.take()
					sent += len(given)
					if d.TelemetryStatus != "measured" || d.ReactiveReplicas != 5 || d.AppliedReplicas < 5 || len(given) > 1 {
						t.Fatalf("round %d, %s: the reactive need must be met: %+v (requests %v)", round, q.Name, d, given)
					}
					if d.ForecastStatus == "used" {
						// Used only when it was received in time: sent and answered now, or the verified answer of this origin
						// reused without a request; never for a request that hangs.
						fresh := len(given) == 1 && d.V2.Request == "answered" && !d.V2.Cached
						reused := len(given) == 0 && d.V2.Request == "cached" && d.V2.Cached
						if hung[q.Name] || !(fresh || reused) || !d.V2.Accepted || d.V2.IssuanceID == nil || d.AppliedReplicas != 7 {
							t.Fatalf("round %d, %s: a forecast was used without a timely answer: %+v / %+v", round, q.Name, d, d.V2)
						}
						if fresh {
							answered++
						}
					} else if d.V2 != nil && (d.V2.Accepted || d.V2.Request == "answered" || d.V2.IssuanceID != nil) {
						t.Fatalf("round %d, %s: not used, yet the ledger reads as delivered: %+v", round, q.Name, d.V2)
					}
				}
				if bound := waitBound(testWaits, began); waits.total() > bound || r.forecastWait.waited > bound {
					t.Fatalf("round %d: the forecast requests waited %s (charged %s) in %s, bound %s", round, waits.total(),
						r.forecastWait.waited, time.Since(began), bound)
				}
			}
			if sent >= 3*n {
				t.Fatalf("all %d requests were sent: slow answers were not charged", sent)
			}
			if tc.engine != "" && tc.hung == 0 && answered == 0 {
				t.Fatal("the first slow answer fits the allowance and must be used")
			}
		})
	}
}

// A hung metrics backend with several autoscalers: the queries together wait at most the metrics allowance (the
// unfixed operator waited its 10 s client timeout in every reconcile), every decision records the rate as unavailable
// and holds the current count, nothing is written, and no zero stands in for the missing rate.
func TestAHungMetricsBackendCostsTheFleetOneAllowanceAndEveryDecisionHolds(t *testing.T) {
	const n = 5
	for _, accepts := range []bool{true, false} {
		t.Run(fmt.Sprintf("accepts=%v", accepts), func(t *testing.T) {
			shortWaits(t, testWaits)
			r, reqs, path, _ := fleetHarness(t, "", n)
			for _, q := range reqs {
				setReplicas(t, r, q, 5) // zero traffic would ask for 1
			}
			rawURL, waits := deafService(t, accepts)
			t.Setenv("PROMETHEUS_URL", rawURL)
			writes := countScaleWrites(r)
			began, sent := time.Now(), 0
			for round := 1; round <= 3; round++ {
				for _, q := range reqs {
					start := time.Now()
					d := runRPMReconcile(t, r, q, path)
					given := waits.take()
					sent += len(given)
					if d.Action != "keep_current" || d.DesiredSource != "keep_current" || d.AppliedReplicas != 5 || d.DesiredReplicas != 5 ||
						d.TelemetryStatus != "unavailable" || d.TelemetryError == nil || !hasString(d.Safeguards, "telemetry_unavailable_hold") {
						t.Fatalf("round %d, %s: the decision must hold on an unavailable rate: %+v", round, q.Name, d)
					}
					if msg := *d.TelemetryError; !(strings.Contains(msg, "did not answer within") || strings.Contains(msg, "not asked")) {
						t.Fatalf("round %d, %s: telemetry error %q", round, q.Name, msg)
					}
					if len(given) > 1 || (len(given) == 1 && given[0] > testWaits.Request) || time.Since(start) > testWaits.Request+5*time.Second {
						t.Fatalf("round %d, %s: the query carried %v and the reconcile took %s", round, q.Name, given, time.Since(start))
					}
					if _, present := gaugeValue(t, currentRpmGauge, q.Name, q.Namespace); present {
						t.Fatalf("%s: a rate is published although none was measured", q.Name)
					}
					c := condition(t, r, q.NamespacedName, "TelemetryAvailable")
					if c.Status != "False" || c.Reason != "MetricsUnavailable" {
						t.Fatalf("%s TelemetryAvailable: %+v", q.Name, c)
					}
				}
				if bound := waitBound(testWaits, began); waits.total() > bound || r.metricsWait.waited > bound {
					t.Fatalf("round %d: the metrics queries waited %s (charged %s) in %s, bound %s", round, waits.total(),
						r.metricsWait.waited, time.Since(began), bound)
				}
			}
			if len(writes) != 0 || sent >= 3*n {
				t.Fatalf("writes %v; %d of %d queries sent", writes, sent, 3*n)
			}
		})
	}
}

// The request deadline is a compatibility change for the legacy (v1) path, whose client used to wait up to 120 s: a
// valid forecast that arrives after the deadline is not received. With a cached forecast under ten minutes old that
// forecast stands in (stale_after_error); without one the decision is reactive. The attempt is recorded as a timeout.
func TestV1AValidAnswerSlowerThanTheDeadlineFallsBackToTheCacheOrTheReactiveRule(t *testing.T) {
	for _, cached := range []bool{true, false} {
		t.Run(fmt.Sprintf("cached=%v", cached), func(t *testing.T) {
			shortWaits(t, testWaits)
			r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000") // reactive: 5
			served := make(chan struct{}, 4)
			srv := httptest.NewServer(echoProvenance(func(w http.ResponseWriter, hr *http.Request) {
				select {
				case <-time.After(3 * testWaits.Request): // a valid forecast for 10 replicas, too late
				case <-hr.Context().Done():
				}
				b := componentResponse(time.Now().UTC().Truncate(10 * time.Minute))
				b["predictions"] = []interface{}{6000.0, 6000.0, 6000.0, 6000.0, 6000.0, 6000.0}
				fmt.Fprint(w, componentBody(t, b))
				served <- struct{}{}
			}))
			t.Cleanup(srv.Close)
			t.Setenv("ML_API_URL", srv.URL)
			key := req.NamespacedName.String()
			if cached {
				runRPMReconcile(t, r, req, path) // publishes the metric source the cache is bound to; scales 1 to 5
				r.forecastWait = waitAllowance{}
				old := time.Now().Add(-6 * time.Minute) // past the 5-minute refresh, inside the 10-minute reuse
				r.predictionCache[key] = &cachedPrediction{binding: reconcileBinding(t, r, req), fetchedAt: old, response: &MLPredictionResponse{
					Predictions: []float64{4200, 4200, 4200, 4200, 4200, 4200}, Confidence: 0.9, anchorAt: time.Now().Add(-time.Minute), issuedAt: old}}
			}
			start := time.Now()
			d := runRPMReconcile(t, r, req, path)
			if took := time.Since(start); took > testWaits.Request+5*time.Second {
				t.Fatalf("the reconcile waited %s for the slow answer", took)
			}
			ends := attemptEvents(attemptRows(t, path), "forecast_attempt_completed")
			if last := ends[len(ends)-1]; last["outcome"] != "timeout" || last["error_class"] != "timeout" {
				t.Fatalf("attempt: %v", last)
			}
			if cached {
				if d.ForecastStatus != "used" || d.ForecastLookup == nil || d.ForecastLookup.Resolution != "stale_after_error" ||
					d.DesiredReplicas != 7 || d.AppliedReplicas != 7 { // 4200 rpm at 600 per replica
					t.Fatalf("the cached forecast must stand in: %+v / %+v", d, d.ForecastLookup)
				}
			} else if d.ForecastStatus != "unavailable" || d.ForecastLookup.Resolution != "unavailable" || d.DesiredSource != "reactive" || d.AppliedReplicas != 5 {
				t.Fatalf("without a cache the decision is reactive: %+v / %+v", d, d.ForecastLookup)
			}
			if _, kept := r.predictionCache[key]; kept != cached {
				t.Fatalf("the late answer must not reach the cache (cached before: %v, now: %v)", cached, kept)
			}
			select {
			case <-served: // the service finished its answer after the operator had gone: issued there, not received here
			case <-time.After(10 * time.Second):
				t.Fatal("the stub never finished")
			}
		})
	}
}

// An answer that needs longer than its deadline is not waited for and never used: the request is cancelled, nothing is
// cached, and the decision of that origin is reactive. A later decision asks again and uses only what it verified
// itself.
func TestV2AnAnswerSlowerThanTheDeadlineIsNeverUsed(t *testing.T) {
	// The fleet allowance refills quickly here, so that the second request is sent at all.
	shortWaits(t, waitPolicy{Request: 300 * time.Millisecond, Fleet: 300 * time.Millisecond, Key: 300 * time.Millisecond, Minimum: 20 * time.Millisecond})
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000") // R = 5
	f.stub.set("eligible", 1000)                             // P = 7 when it is used
	slow := make(chan struct{})
	waits := watchService(t, mlAPIBaseURL())
	waits.before = func(req *http.Request) error {
		select {
		case <-slow: // released: answer at once
			return nil
		case <-req.Context().Done():
			return req.Context().Err()
		}
	}
	writes := countScaleWrites(f.r)
	d := f.reconcile(t)
	if d.Action != "scale_up" || d.AppliedReplicas != 5 || d.DesiredSource != "reactive" || d.ForecastStatus != "unavailable" ||
		d.V2 == nil || d.V2.Accepted || d.V2.IssuanceID != nil || d.V2.Request != "timeout" {
		t.Fatalf("the decision must not wait for the slow answer: %+v / %+v", d, d.V2)
	}
	if _, cached := f.r.v2Cache[f.req.NamespacedName.String()]; cached || f.stub.requests.Load() != 0 {
		t.Fatal("a request that ran out of its deadline must leave no answer behind")
	}
	// The same origin a little later, the service answering in time: a fresh request, a verified answer for a window
	// that is still actionable, one more write.
	close(slow)
	forecastWaitPolicy.Request = 20 * time.Second // the answering stub must not be cut off in a loaded test run
	f.r.forecastWait = waitAllowance{}
	f.at(20)
	d = f.reconcile(t)
	if d.Action != "scale_up" || d.AppliedReplicas != 7 || d.ForecastStatus != "used" || !d.V2.Accepted || d.V2.Cached ||
		d.V2.Request != "answered" || f.stub.requests.Load() != 1 {
		t.Fatalf("a later decision asks again: %+v / %+v", d, d.V2)
	}
	if n := writes[f.req.Name]; n != 2 {
		t.Fatalf("scale writes: %d, want 2 (1 to 5, then 5 to 7)", n)
	}
}

// A refused connection and an error status that come at once cost next to nothing: the next request still has
// (almost) the whole allowance.
func TestForecastFailuresThatAnswerAtOnceLeaveTheAllowance(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000") // its forecaster answers 503
	for i := 0; i < 2; i++ {
		if d := runRPMReconcile(t, r, req, path); d.ForecastStatus != "unavailable" || d.AppliedReplicas != 5 {
			t.Fatalf("decision: %+v", d)
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there any more: the connection is refused at once
	t.Setenv("ML_API_URL", "http://"+addr)
	if d := runRPMReconcile(t, r, req, path); d.ForecastStatus != "unavailable" || d.Action != "at_target" {
		t.Fatalf("decision: %+v", d)
	}
	if left, ok := r.forecastWait.grant(req.NamespacedName.String()); !ok || left < forecastWaitPolicy.Request-2*time.Second {
		t.Fatalf("three immediate failures left only %s of the allowance", left)
	}
}

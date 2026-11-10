package controllers

import (
	"context"
	"net"
	"net/http"
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

// The forecast wait budget (R3-01): a forecasting service that does not answer costs a round of decisions one full
// budget and otherwise one probe budget per autoscaler, and every reactive decision is still made and written.

// The arithmetic the budget is documented with (forecast_wait.go, docs/configuration.md).
func TestForecastWaitBudgetArithmetic(t *testing.T) {
	share := time.Duration(minReconcileSeconds) * time.Second / 3
	if got := forecastWaitBudget + forecastSupportedAutoscalers*forecastProbeBudget; got != share || share != 10*time.Second {
		t.Fatalf("one full budget and %d probe budgets are %s, a third of the shortest cadence is %s", forecastSupportedAutoscalers, got, share)
	}
	if forecastRearmInterval < time.Duration(defaultReconcileSeconds)*time.Second || mlAPITimeout < forecastWaitBudget {
		t.Fatal("the full budget must come at most once per default round, and the client's own limit must not undercut it")
	}
}

// shortForecastBudgets shortens the budgets for a test and restores them.
func shortForecastBudgets(t *testing.T, full, probe time.Duration) {
	t.Helper()
	oldFull, oldProbe := forecastWaitBudget, forecastProbeBudget
	forecastWaitBudget, forecastProbeBudget = full, probe
	t.Cleanup(func() { forecastWaitBudget, forecastProbeBudget = oldFull, oldProbe })
}

// forecasterWaits replaces the default transport: requests to the forecasting service go through forecaster, and the
// wait each one was given (its context deadline) is noted; every other request (the metrics query) is untouched.
type forecasterWaits struct {
	mu     sync.Mutex
	given  []time.Duration
	host   string
	direct http.RoundTripper
	via    http.RoundTripper
}

func (f *forecasterWaits) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Host != f.host {
		return f.direct.RoundTrip(req)
	}
	wait := time.Duration(-1) // no deadline at all
	if d, ok := req.Context().Deadline(); ok {
		wait = time.Until(d)
	}
	f.mu.Lock()
	f.given = append(f.given, wait)
	f.mu.Unlock()
	return f.via.RoundTrip(req)
}

func (f *forecasterWaits) take() []time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := f.given
	f.given = nil
	return out
}

// deafForecaster points ML_API_URL at a forecasting service that never answers and returns the waits its requests
// were given. accepts: the connection is accepted and the request read, and nothing comes back (a stuck service).
// Otherwise the connection is never established, as when the SYN is dropped (a default-deny egress policy, a Service
// without endpoints on a CNI that drops rather than refuses).
func deafForecaster(t *testing.T, accepts bool) *forecasterWaits {
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
	direct := http.DefaultTransport
	f := &forecasterWaits{host: ln.Addr().String(), direct: direct, via: &http.Transport{}}
	if !accepts {
		f.via = &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			<-ctx.Done() // no SYN-ACK ever arrives
			return nil, &net.OpError{Op: "dial", Net: "tcp", Err: ctx.Err()}
		}}
	}
	http.DefaultTransport = f
	t.Cleanup(func() {
		http.DefaultTransport = direct
		f.via.(*http.Transport).CloseIdleConnections()
	})
	t.Setenv("ML_API_URL", "http://"+f.host)
	return f
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

// A forecasting service that accepts and never answers, and one that cannot be reached at all, with several autoscalers
// on both forecast paths: the first request of the round waits the full budget, every other one the probe budget, and
// each autoscaler's reactive decision is made and written once.
func TestADeafForecasterCostsARoundOneFullBudgetAndDelaysNoReactiveDecision(t *testing.T) {
	const n, full, probe = 4, 600 * time.Millisecond, 60 * time.Millisecond
	// How long a reconcile, or a round, may take beyond its forecast waits in a heavily loaded test run. The unfixed
	// operator waited the HTTP client's limit (here 30 s) in every reconcile, 120 s per round.
	const slack = 15 * time.Second
	for _, engine := range []string{"", autoscalerv1alpha1.EngineStatistical} {
		for _, accepts := range []bool{true, false} {
			name := "engine=" + engine + "/the service accepts and never answers"
			if !accepts {
				name = "engine=" + engine + "/the connection is never established"
			}
			t.Run(name, func(t *testing.T) {
				shortForecastBudgets(t, full, probe)
				var r *PredictiveAutoscalerReconciler
				var req ctrl.Request
				var path string
				if engine == "" {
					r, req, path, _ = recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000") // 3000 rpm at 600 per replica: 5
				} else {
					f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
					r, req, path = f.r, f.req, f.path
				}
				r.MLAPI = newMLAPIClient("", "", 30*time.Second) // the client's own limit is far away: the budget decides
				reqs := fleet(t, r, req, n)
				waits := deafForecaster(t, accepts)
				writes := countScaleWrites(r)
				for round := 1; round <= 3; round++ {
					if round == 3 {
						r.forecastFullBudgetAt = time.Now().Add(-time.Second) // the re-arm interval has passed
					}
					began := time.Now()
					for i, q := range reqs {
						start := time.Now()
						d := runRPMReconcile(t, r, q, path)
						took := time.Since(start)
						wantAction, wantWait := "scale_up", probe
						if round > 1 {
							wantAction = "at_target"
						}
						if i == 0 && round != 2 {
							wantWait = full // the first request of the round, unless a full budget ran out a moment ago
						}
						if d.Application != q.Name || d.Action != wantAction || d.AppliedReplicas != 5 || d.DesiredReplicas != 5 ||
							d.DesiredSource != "reactive" || d.ForecastStatus != "unavailable" || d.TelemetryStatus != "measured" {
							t.Fatalf("round %d, %s: the reactive decision must stand: %+v", round, q.Name, d)
						}
						// The deadline the request carried: at most the probe budget, or more than it (the full budget).
						given := waits.take()
						if len(given) != 1 || given[0] > wantWait || (wantWait == full && given[0] <= probe) {
							t.Fatalf("round %d, %s: the forecast request was given %v, want one wait of at most %s", round, q.Name, given, wantWait)
						}
						if took < given[0] || took > wantWait+slack {
							t.Fatalf("round %d, %s: the reconcile took %s with a forecast wait of %s", round, q.Name, took, wantWait)
						}
					}
					bound := n*probe + slack
					if round != 2 {
						bound += full
					}
					if took := time.Since(began); took > bound {
						t.Fatalf("round %d took %s, bound %s", round, took, bound)
					}
				}
				for _, q := range reqs {
					if writes[q.Name] != 1 {
						t.Fatalf("%s was written %d times, want once (1 to 5)", q.Name, writes[q.Name])
					}
					c := condition(t, r, q.NamespacedName, "ForecastAvailable")
					if c.Status != "False" || c.Reason != "Unavailable" {
						t.Fatalf("%s ForecastAvailable: %+v", q.Name, c)
					}
				}
				if engine != "" {
					c := condition(t, r, req.NamespacedName, "PredictionReady")
					if c.Status != "False" || c.Reason != "DataUnavailable" || !strings.Contains(c.Message, "did not answer within") {
						t.Fatalf("PredictionReady: %+v", c)
					}
				}
			})
		}
	}
}

// An answer that needs longer than the budget is not waited for and never used: the request is cancelled, nothing is
// cached, and the decision of that origin is reactive. A later decision asks again and uses only what it verified
// itself.
func TestV2AnAnswerSlowerThanTheBudgetIsNeverUsed(t *testing.T) {
	// The probe budget is long here, so that the answering stub is never cut off in a loaded test run.
	shortForecastBudgets(t, 300*time.Millisecond, 20*time.Second)
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000") // R = 5
	f.stub.set("eligible", 1000)                             // P = 7 when it is used
	slow := make(chan struct{})
	direct := http.DefaultTransport
	target, err := url.Parse(mlAPIBaseURL())
	if err != nil {
		t.Fatal(err)
	}
	http.DefaultTransport = attemptRoundTripper(func(req *http.Request) (*http.Response, error) {
		if req.URL.Host == target.Host {
			select {
			case <-slow: // released: answer at once
			case <-time.After(5 * time.Second): // far beyond the budget
			case <-req.Context().Done():
				return nil, req.Context().Err()
			}
		}
		return direct.RoundTrip(req)
	})
	t.Cleanup(func() { http.DefaultTransport = direct })
	writes := countScaleWrites(f.r)
	d := f.reconcile(t)
	if d.Action != "scale_up" || d.AppliedReplicas != 5 || d.DesiredSource != "reactive" || d.ForecastStatus != "unavailable" ||
		d.V2 == nil || d.V2.Accepted || d.V2.IssuanceID != nil {
		t.Fatalf("the decision must not wait for the slow answer: %+v / %+v", d, d.V2)
	}
	if _, cached := f.r.v2Cache[f.req.NamespacedName.String()]; cached || f.stub.requests.Load() != 0 {
		t.Fatal("a request that ran out of its budget must leave no answer behind")
	}
	// The same origin a little later, the service answering in time: a fresh request, a verified answer for a window
	// that is still actionable, one more write.
	close(slow)
	f.at(20)
	d = f.reconcile(t)
	if d.Action != "scale_up" || d.AppliedReplicas != 7 || d.ForecastStatus != "used" || !d.V2.Accepted || d.V2.Cached ||
		f.stub.requests.Load() != 1 {
		t.Fatalf("a later decision asks again: %+v / %+v", d, d.V2)
	}
	if n := writes[f.req.Name]; n != 2 {
		t.Fatalf("scale writes: %d, want 2 (1 to 5, then 5 to 7)", n)
	}
}

// A refused connection and an error status are answers of a kind: they cost no wait and keep the full budget.
func TestForecastFailuresThatAnswerKeepTheFullBudget(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000") // its forecaster answers 503
	for i := 0; i < 2; i++ {
		if d := runRPMReconcile(t, r, req, path); d.ForecastStatus != "unavailable" || d.AppliedReplicas != 5 {
			t.Fatalf("decision: %+v", d)
		}
		if !r.forecastFullBudgetAt.IsZero() {
			t.Fatal("an error status is not an unanswered request")
		}
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there any more: the connection is refused at once
	t.Setenv("ML_API_URL", "http://"+addr)
	start := time.Now()
	if d := runRPMReconcile(t, r, req, path); d.ForecastStatus != "unavailable" || d.Action != "at_target" {
		t.Fatalf("decision: %+v", d)
	}
	if !r.forecastFullBudgetAt.IsZero() || time.Since(start) > forecastWaitBudget {
		t.Fatalf("a refused connection must fail at once and keep the full budget (took %s)", time.Since(start))
	}
}

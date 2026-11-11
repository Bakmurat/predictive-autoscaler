package controllers

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The stage metrics: mode, reasons, the last decision, the replica count from the calculation to the Ready pods,
// every write attempt's outcome, the configured capacity and the forecast the operator looked at. A value that does
// not exist has no series, and no label carries free text.

// seriesOf returns one autoscaler's series of a collector as "label=value,..." (without namespace and autoscaler; ""
// when there is no other label) -> value, without creating any series.
func seriesOf(t *testing.T, c prometheus.Collector, ns, name string) map[string]float64 {
	t.Helper()
	ch := make(chan prometheus.Metric)
	go func() { c.Collect(ch); close(ch) }()
	out := map[string]float64{}
	for m := range ch {
		var d dto.Metric
		if err := m.Write(&d); err != nil {
			t.Fatal(err)
		}
		labels, other := map[string]string{}, []string{}
		for _, p := range d.GetLabel() {
			labels[p.GetName()] = p.GetValue()
			if p.GetName() != "namespace" && p.GetName() != "autoscaler" {
				other = append(other, p.GetName()+"="+p.GetValue())
			}
		}
		if labels["namespace"] != ns || labels["autoscaler"] != name {
			continue
		}
		sort.Strings(other)
		v := d.GetGauge().GetValue()
		if d.GetCounter() != nil {
			v = d.GetCounter().GetValue()
		}
		out[strings.Join(other, ",")] = v
	}
	return out
}

func wantOnly(t *testing.T, what string, got map[string]float64, key string, value float64) {
	t.Helper()
	if len(got) != 1 || got[key] != value {
		t.Fatalf("%s: want exactly {%s: %v}, got %v", what, key, value, got)
	}
}

func wantAbsent(t *testing.T, what string, got map[string]float64) {
	t.Helper()
	if len(got) != 0 {
		t.Fatalf("%s: want no series, got %v", what, got)
	}
}

func wantWrites(t *testing.T, got map[string]float64, counted map[string]float64) {
	t.Helper()
	if len(got) != len(writeResults) {
		t.Fatalf("every outcome must have a series from the first reconcile on, got %v", got)
	}
	for _, r := range writeResults {
		if got["result="+r] != counted[r] {
			t.Fatalf("result %s = %v, want %v: %v", r, got["result="+r], counted[r], got)
		}
	}
}

func stageHarness(t *testing.T, mode, rpm string) (*PredictiveAutoscalerReconciler, ctrl.Request, string) {
	t.Helper()
	r, req, path, _ := recommendHarness(t, mode, rpm)
	t.Cleanup(func() { forgetConditionMetrics(req.Namespace, req.Name) })
	return r, req, path
}

func TestRecommendPublishesTheCalculationAndRequestsNothing(t *testing.T) {
	r, req, path := stageHarness(t, autoscalerv1alpha1.ModeRecommend, "3000") // 3000 req/min at 600 per replica: 5
	runRPMReconcile(t, r, req, path)
	ns, name := req.Namespace, req.Name
	if got := seriesOf(t, modeGauge, ns, name); len(got) != 2 || got["mode=Recommend"] != 1 || got["mode=Active"] != 0 {
		t.Fatalf("mode: %v", got)
	}
	wantOnly(t, "decision", seriesOf(t, decisionActionGauge, ns, name), "action=recommend", 1)
	wantOnly(t, "calculated", seriesOf(t, calculatedReplicasGauge, ns, name), "", 5)
	wantOnly(t, "target", seriesOf(t, targetReplicasGauge, ns, name), "", 1)
	wantOnly(t, "ready", seriesOf(t, targetReadyReplicasGauge, ns, name), "", 0)
	wantAbsent(t, "requested (Recommend asks for nothing)", seriesOf(t, requestedReplicasGauge, ns, name))
	wantAbsent(t, "applied", seriesOf(t, appliedReplicasGauge, ns, name))
	wantAbsent(t, "applied timestamp", seriesOf(t, appliedTimestampGauge, ns, name))
	wantWrites(t, seriesOf(t, scaleWritesTotal, ns, name), nil)
	wantOnly(t, "capacity", seriesOf(t, capacityPerReplicaGauge, ns, name), "source=targetRPS", 10)
	reasons := seriesOf(t, conditionReasonGauge, ns, name)
	if reasons["reason=RecommendMode,type=ScalingActive"] != 1 || reasons["reason=ReconcileCompleted,type=Ready"] != 1 {
		t.Fatalf("condition reasons: %v", reasons)
	}
	perType := map[string]int{}
	for k := range reasons {
		perType[k[strings.Index(k, "type="):]]++
	}
	for typ, n := range perType {
		if n != 1 {
			t.Fatalf("%s has %d reason series, want one: %v", typ, n, reasons)
		}
	}
	wantAbsent(t, "no forecast was verified", seriesOf(t, selectedFamilyGauge, ns, name))
	wantAbsent(t, "no forecast was verified", seriesOf(t, predictionLeadGauge, ns, name))
}

func TestAConfirmedWriteBecomesTheAppliedStage(t *testing.T) {
	r, req, path := stageHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	ns, name := req.Namespace, req.Name
	if d := runRPMReconcile(t, r, req, path); d.Action != "scale_up" {
		t.Fatalf("setup: %+v", d)
	}
	if got := seriesOf(t, modeGauge, ns, name); got["mode=Active"] != 1 || got["mode=Recommend"] != 0 {
		t.Fatalf("mode: %v", got)
	}
	wantOnly(t, "decision", seriesOf(t, decisionActionGauge, ns, name), "action=scale_up", 1)
	wantOnly(t, "calculated", seriesOf(t, calculatedReplicasGauge, ns, name), "", 5)
	wantOnly(t, "requested", seriesOf(t, requestedReplicasGauge, ns, name), "", 5)
	wantOnly(t, "applied", seriesOf(t, appliedReplicasGauge, ns, name), "", 5)
	wantOnly(t, "target as read before the write", seriesOf(t, targetReplicasGauge, ns, name), "", 1)
	if at := seriesOf(t, appliedTimestampGauge, ns, name)[""]; at <= 0 {
		t.Fatalf("applied timestamp: %v", at)
	}
	wantWrites(t, seriesOf(t, scaleWritesTotal, ns, name), map[string]float64{writeApplied: 1})
	// The next reconcile holds at the target: it asks for the current count, writes nothing, and the last confirmed
	// write stays what it was.
	if d := runRPMReconcile(t, r, req, path); d.Action != "at_target" {
		t.Fatalf("second reconcile: %+v", d)
	}
	wantOnly(t, "decision", seriesOf(t, decisionActionGauge, ns, name), "action=at_target", 1)
	wantOnly(t, "requested", seriesOf(t, requestedReplicasGauge, ns, name), "", 5)
	wantOnly(t, "target", seriesOf(t, targetReplicasGauge, ns, name), "", 5)
	wantOnly(t, "applied", seriesOf(t, appliedReplicasGauge, ns, name), "", 5)
	wantWrites(t, seriesOf(t, scaleWritesTotal, ns, name), map[string]float64{writeApplied: 1})
}

func TestAWriteThatWasNotConfirmedIsNeverCountedAsApplied(t *testing.T) {
	scale := schema.GroupResource{Group: "apps", Resource: "deployments"}
	for _, c := range []struct {
		name   string
		err    error
		result string
	}{
		{"conflict", apierrors.NewConflict(scale, "x", fmt.Errorf("changed")), writeStale},
		{"forbidden", apierrors.NewForbidden(scale, "x", fmt.Errorf("no")), writeRejected},
		{"gateway timeout", apierrors.NewTimeoutError("no answer in time", 1), writeUnknown},
		{"internal error", apierrors.NewInternalError(fmt.Errorf("boom")), writeUnknown},
		{"deadline", context.DeadlineExceeded, writeUnknown},
	} {
		t.Run(c.name, func(t *testing.T) {
			r, req, _ := stageHarness(t, autoscalerv1alpha1.ModeActive, "3000")
			wrapClient(r, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if sub == "scale" {
						return c.err
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
			})
			delete(r.lastReconcileMap, req.NamespacedName.String())
			_, _ = r.Reconcile(context.Background(), req) // a failed write ends the reconcile with its error
			ns, name := req.Namespace, req.Name
			wantWrites(t, seriesOf(t, scaleWritesTotal, ns, name), map[string]float64{c.result: 1})
			wantOnly(t, "requested (the attempt's target)", seriesOf(t, requestedReplicasGauge, ns, name), "", 5)
			wantAbsent(t, "applied", seriesOf(t, appliedReplicasGauge, ns, name))
			wantAbsent(t, "applied timestamp", seriesOf(t, appliedTimestampGauge, ns, name))
		})
	}
}

func TestWriteFailureKeepsAnUnknownOutcomeUnknown(t *testing.T) {
	gr := schema.GroupResource{Group: "apps", Resource: "deployments"}
	for _, c := range []struct {
		err  error
		want string
	}{
		{apierrors.NewConflict(gr, "x", fmt.Errorf("rv")), writeStale},
		{fmt.Errorf("%w: not listed", errWriteNotAllowed), writeNotAllowed},
		{apierrors.NewForbidden(gr, "x", fmt.Errorf("no")), writeRejected},
		{apierrors.NewNotFound(gr, "x"), writeRejected},
		{apierrors.NewTooManyRequests("slow down", 1), writeRejected},
		{fmt.Errorf("update: %w", apierrors.NewForbidden(gr, "x", fmt.Errorf("no"))), writeRejected},
		{apierrors.NewTimeoutError("no answer", 1), writeUnknown},
		{apierrors.NewServerTimeout(gr, "update", 1), writeUnknown},
		{apierrors.NewInternalError(fmt.Errorf("boom")), writeUnknown},
		{apierrors.NewServiceUnavailable("down"), writeUnknown},
		{context.DeadlineExceeded, writeUnknown},
		{fmt.Errorf("connection reset by peer"), writeUnknown},
	} {
		if got := writeFailure(c.err); got != c.want {
			t.Errorf("%v: got %s, want %s", c.err, got, c.want)
		}
	}
}

func TestARefusedTargetRequestsNothingAndAttemptsNoWrite(t *testing.T) {
	r, req, path := stageHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	r.WriteAllowlist = allowlistFor(t, req.Namespace, "another-deployment", "abc")
	if d := runRPMReconcile(t, r, req, path); d.Action != "target_not_allowed" {
		t.Fatalf("setup: %+v", d)
	}
	ns, name := req.Namespace, req.Name
	wantOnly(t, "decision", seriesOf(t, decisionActionGauge, ns, name), "action=target_not_allowed", 1)
	wantOnly(t, "calculated", seriesOf(t, calculatedReplicasGauge, ns, name), "", 5)
	wantAbsent(t, "requested", seriesOf(t, requestedReplicasGauge, ns, name))
	wantWrites(t, seriesOf(t, scaleWritesTotal, ns, name), nil)
	if got := seriesOf(t, conditionReasonGauge, ns, name); got["reason=TargetNotAllowed,type=ScalingActive"] != 1 {
		t.Fatalf("the hold reason must be exported: %v", got)
	}
}

func TestTheCapacityIsPublishedInRequestsPerSecondWithItsSource(t *testing.T) {
	t.Cleanup(func() { forgetStageMetrics("ns-c", "pa-c") })
	q := resource.MustParse("17500m")
	spec := autoscalerv1alpha1.PredictiveAutoscalerSpec{Metrics: autoscalerv1alpha1.MetricsConfig{
		Requests: &autoscalerv1alpha1.RequestsMetric{CapacityPerReplica: &q}}}
	rpm, source, ok := perReplicaCapacityRPM(spec)
	recordCapacity("ns-c", "pa-c", rpm, source, ok)
	wantOnly(t, "capacity", seriesOf(t, capacityPerReplicaGauge, "ns-c", "pa-c"), "source=capacityPerReplica", 17.5)
	spec.Metrics.Requests = &autoscalerv1alpha1.RequestsMetric{TargetRPS: 10} // the source changes: one series
	rpm, source, ok = perReplicaCapacityRPM(spec)
	recordCapacity("ns-c", "pa-c", rpm, source, ok)
	wantOnly(t, "capacity", seriesOf(t, capacityPerReplicaGauge, "ns-c", "pa-c"), "source=targetRPS", 10)
	spec.Metrics.Requests = nil
	rpm, source, ok = perReplicaCapacityRPM(spec)
	recordCapacity("ns-c", "pa-c", rpm, source, ok)
	wantAbsent(t, "no capacity resolves", seriesOf(t, capacityPerReplicaGauge, "ns-c", "pa-c"))
}

func TestTheForecastWindowIsPublishedAlsoWhenUntrustedAndTheUsedFamilyOnlyWhenAccepted(t *testing.T) {
	ns, name := "ns-w", "pa-w"
	t.Cleanup(func() { forgetStageMetrics(ns, name) })
	resp := &v2Response{SelectedCandidate: "seasonal_naive", ModelFamily: "seasonal_naive", TrustState: "Shadow",
		LeadSeconds: 600, IssuedAtMs: 1_794_000_030_500, WindowStart: 1_794_000_600, WindowEnd: 1_794_000_660,
		ValidUntil: 1_794_000_540}
	recordPredictionWindow(ns, name, resp, false) // a shadow candidate: shown, not used
	wantOnly(t, "selected", seriesOf(t, selectedFamilyGauge, ns, name), "family=seasonal_naive", 1)
	wantAbsent(t, "used", seriesOf(t, usedFamilyGauge, ns, name))
	wantOnly(t, "lead", seriesOf(t, predictionLeadGauge, ns, name), "", 600)
	wantOnly(t, "issued", seriesOf(t, predictionIssuedGauge, ns, name), "", 1_794_000_030.5)
	wantOnly(t, "window start", seriesOf(t, predictionWindowStartGauge, ns, name), "", 1_794_000_600)
	wantOnly(t, "window end", seriesOf(t, predictionWindowEndGauge, ns, name), "", 1_794_000_660)
	wantOnly(t, "valid until", seriesOf(t, predictionValidUntilGauge, ns, name), "", 1_794_000_540)
	resp.SelectedCandidate, resp.ModelFamily = "statistical", "statistical"
	recordPredictionWindow(ns, name, resp, true) // accepted: the obsolete selection is gone, not left at 1 or 0
	wantOnly(t, "selected", seriesOf(t, selectedFamilyGauge, ns, name), "family=statistical", 1)
	wantOnly(t, "used", seriesOf(t, usedFamilyGauge, ns, name), "family=statistical", 1)
	recordPredictionWindow(ns, name, resp, false)
	wantAbsent(t, "used (no longer accepted)", seriesOf(t, usedFamilyGauge, ns, name))
	resp.SelectedCandidate = "something new"
	recordPredictionWindow(ns, name, resp, false)
	wantOnly(t, "an unknown family is not a label", seriesOf(t, selectedFamilyGauge, ns, name), "family=Other", 1)
	recordPredictionWindow(ns, name, nil, false) // no verified forecast: nothing is shown
	for what, c := range map[string]prometheus.Collector{"selected": selectedFamilyGauge, "lead": predictionLeadGauge,
		"issued": predictionIssuedGauge, "start": predictionWindowStartGauge, "end": predictionWindowEndGauge,
		"valid": predictionValidUntilGauge} {
		wantAbsent(t, what, seriesOf(t, c, ns, name))
	}
}

func TestLabelsNeverCarryFreeText(t *testing.T) {
	ns, name := "ns-l", "pa-l"
	t.Cleanup(func() { forgetConditionMetrics(ns, name) })
	for _, text := range []string{`Get "https://10.0.0.1": dial tcp: i/o timeout`, "UserSuppliedValue123", "", "ready"} {
		recordConditionMetrics(ns, name, "Active", true, []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse, Reason: text}})
		wantOnly(t, "reason "+text, seriesOf(t, conditionReasonGauge, ns, name), "reason=Other,type=Ready", 1)
	}
	recordConditionMetrics(ns, name, "Active", true, []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "ReconcileCompleted"}})
	wantOnly(t, "the previous reason is removed", seriesOf(t, conditionReasonGauge, ns, name), "reason=ReconcileCompleted,type=Ready", 1)
	recordDecisionAction(ns, name, "rm -rf / ; made up")
	wantOnly(t, "action", seriesOf(t, decisionActionGauge, ns, name), "action=Other", 1)
	recordWrite(ns, name, "made up", 3, 1)
	if got := seriesOf(t, scaleWritesTotal, ns, name); len(got) != 1 || got["result=Other"] != 1 {
		t.Fatalf("write result: %v", got)
	}
	wantAbsent(t, "an unknown result is not a confirmed write", seriesOf(t, appliedReplicasGauge, ns, name))
}

func TestADeletedAutoscalerLeavesNoStageSeries(t *testing.T) {
	r, req, path := stageHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	runRPMReconcile(t, r, req, path)
	recordPredictionWindow(req.Namespace, req.Name, &v2Response{SelectedCandidate: "statistical", ModelFamily: "statistical"}, true)
	collectors := map[string]prometheus.Collector{"mode": modeGauge, "reason": conditionReasonGauge, "action": decisionActionGauge,
		"calculated": calculatedReplicasGauge, "requested": requestedReplicasGauge, "applied": appliedReplicasGauge,
		"applied at": appliedTimestampGauge, "target": targetReplicasGauge, "ready": targetReadyReplicasGauge,
		"writes": scaleWritesTotal, "capacity": capacityPerReplicaGauge, "selected": selectedFamilyGauge, "used": usedFamilyGauge,
		"lead": predictionLeadGauge, "issued": predictionIssuedGauge, "start": predictionWindowStartGauge,
		"end": predictionWindowEndGauge, "valid": predictionValidUntilGauge}
	for what, c := range collectors {
		if len(seriesOf(t, c, req.Namespace, req.Name)) == 0 {
			t.Fatalf("setup: %s has no series", what)
		}
	}
	a := getPA(t, r, req)
	if err := r.Delete(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	delete(r.lastReconcileMap, req.NamespacedName.String())
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for what, c := range collectors {
		wantAbsent(t, what+" after the autoscaler was deleted", seriesOf(t, c, req.Namespace, req.Name))
	}
}

// The reasons are an explicit list, so the list must keep up with the code: every reason literal the operator's
// sources pass to a condition is in it. (A reason that is missing would be exported as Other, never as free text.)
func TestEveryReasonTheOperatorSetsIsExportedByName(t *testing.T) {
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`\bset(?:Status)?\("[A-Za-z]+", [A-Za-z.]+, "([A-Za-z0-9]+)"`), // set("Type", ok, "Reason"
		regexp.MustCompile(`updateStatusWithError\(ctx, [^,]+, "([A-Za-z0-9]+)"`),         // a failed reconcile
		regexp.MustCompile(`v2Verdict\{(?:Eligible: true, )?Reason: "([A-Za-z0-9]+)"`),    // the operator's verdicts
		regexp.MustCompile(`return (?:true|false), "([A-Za-z0-9]+)"`),                     // readiness, capacityCondition
		regexp.MustCompile(`\bBlocked(?:, obs\.BlockedMsg)? = "([A-Za-z0-9]+)"`),          // ScaleOutBlocked
		regexp.MustCompile(`"[a-z_]+":\s+"([A-Z][A-Za-z0-9]+)"`),                          // forecastReasons
		regexp.MustCompile(`\breason = "([A-Z][A-Za-z0-9]+)"`),                            // a reason set in a variable
		regexp.MustCompile(`(?m)^\s+return "([A-Z][A-Za-z0-9]+)"$`),                       // v2FailureReason
		regexp.MustCompile(`const v2[A-Za-z]+ = "([A-Z][A-Za-z0-9]+)"`),                   // a reason held in a constant
	}
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	found := map[string]string{}
	for _, f := range files {
		if strings.HasSuffix(f, "_test.go") {
			continue
		}
		raw, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, p := range patterns {
			for _, m := range p.FindAllStringSubmatch(string(raw), -1) {
				found[m[1]] = f
			}
		}
	}
	if len(found) < 40 {
		t.Fatalf("the scan found only %d reasons: its patterns no longer match the sources", len(found))
	}
	for _, r := range []string{"NotEligible", "ForecastRejected", "RequestRefused", "DataUnavailable", v2AutoscalerLimitReached} {
		if found[r] == "" {
			t.Errorf("the scan no longer finds %s: a reason set this way would be missed", r)
		}
	}
	for _, r := range append(append([]string{}, v2Reasons...), "Unavailable") {
		found[r] = "the forecast contract"
	}
	// The documentation is the second witness: every reason the configuration reference names for a condition is
	// exported by name. (Its row also names the condition types and the statuses, which are not reasons.)
	doc, err := os.ReadFile(filepath.Join("..", "..", "docs", "configuration.md"))
	if err != nil {
		t.Fatal(err)
	}
	var row string
	for _, line := range strings.Split(string(doc), "\n") {
		if strings.HasPrefix(line, "| `conditions` |") {
			row = line
		}
	}
	notReasons := map[string]bool{"Unknown": true, "Off": true, "True": true, "False": true}
	for _, typ := range conditionMetricTypes {
		notReasons[typ] = true
	}
	documented := regexp.MustCompile("`([A-Z][A-Za-z0-9]+)`").FindAllStringSubmatch(row, -1)
	if len(documented) < 40 {
		t.Fatalf("the conditions row of docs/configuration.md names only %d terms: the row was not found", len(documented))
	}
	for _, m := range documented {
		if !notReasons[m[1]] {
			found[m[1]] = "docs/configuration.md"
		}
	}
	for reason, where := range found {
		if oneOf(reason, conditionReasons) != reason {
			t.Errorf("reason %s (%s) is not in conditionReasons: it would be exported as Other", reason, where)
		}
	}
	seen := map[string]bool{}
	for _, r := range conditionReasons {
		if seen[r] || r == otherLabel {
			t.Errorf("conditionReasons lists %s twice or reserves the fallback label", r)
		}
		seen[r] = true
	}
}

// A forecast that was accepted into the calculation but expired before the write was rejected by the final check:
// it is not the used family.
func TestAForecastRejectedByTheFinalCheckIsNotTheUsedFamily(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
	f.stub.set("eligible", 1000)
	f.at(284)
	t.Cleanup(func() { forgetConditionMetrics(f.req.Namespace, f.req.Name) })
	wrapClient(f.r, interceptor.Funcs{
		SubResourceGet: func(ctx context.Context, c client.Client, sub string, obj client.Object, out client.Object, opts ...client.SubResourceGetOption) error {
			if sub == "scale" {
				f.at(286) // the forecast's validity ends while the fresh Scale is read
			}
			return c.SubResource(sub).Get(ctx, obj, out, opts...)
		},
	})
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.V2 == nil || d.V2.Accepted {
		t.Fatalf("setup: the forecast must have expired before the write: %+v", d.V2)
	}
	ns, name := f.req.Namespace, f.req.Name
	wantAbsent(t, "used family after the final rejection", seriesOf(t, usedFamilyGauge, ns, name))
	if got := seriesOf(t, selectedFamilyGauge, ns, name); len(got) != 1 {
		t.Fatalf("the forecast in view is still shown: %v", got)
	}
	wantWrites(t, seriesOf(t, scaleWritesTotal, ns, name), map[string]float64{writeGuardAbort: 1})
}

// The last confirmed write belongs to the objects it was made for.
func TestTheAppliedStageDoesNotOutliveItsTargetOrItsAutoscaler(t *testing.T) {
	r, req, path := stageHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	ns, name := req.Namespace, req.Name
	runRPMReconcile(t, r, req, path)
	wantOnly(t, "applied", seriesOf(t, appliedReplicasGauge, ns, name), "", 5)
	// A switch to Recommend keeps it, with its time: it is still the last write to this target.
	a := getPA(t, r, req)
	a.Spec.Mode = autoscalerv1alpha1.ModeRecommend
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	at := seriesOf(t, appliedTimestampGauge, ns, name)[""]
	runRPMReconcile(t, r, req, path)
	wantOnly(t, "applied after a switch to Recommend", seriesOf(t, appliedReplicasGauge, ns, name), "", 5)
	wantOnly(t, "its time is unchanged", seriesOf(t, appliedTimestampGauge, ns, name), "", at)
	// The Deployment is replaced under its name: nothing was written to the new object.
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	dep.UID = types.UID("replacement")
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	runRPMReconcile(t, r, req, path)
	wantAbsent(t, "applied after the target was replaced", seriesOf(t, appliedReplicasGauge, ns, name))
	wantAbsent(t, "applied timestamp after the target was replaced", seriesOf(t, appliedTimestampGauge, ns, name))
	// The autoscaler is deleted and created again between two reconciles (no reconcile saw it missing).
	recordWrite(ns, name, writeApplied, 7, 1)
	recordIdentity(ns, name, "autoscaler-1", "replacement")
	recordWrite(ns, name, writeApplied, 7, 1)
	recordIdentity(ns, name, "autoscaler-1", "replacement")
	wantOnly(t, "the same objects keep it", seriesOf(t, appliedReplicasGauge, ns, name), "", 7)
	recordIdentity(ns, name, "autoscaler-2", "replacement")
	wantAbsent(t, "applied after the autoscaler was recreated", seriesOf(t, appliedReplicasGauge, ns, name))
}

// A reconcile that ends before its decision leaves nothing standing that only a completed reconcile can say.
func TestAReconcileThatEndsEarlyRemovesTheStagesOfThePreviousOne(t *testing.T) {
	r, req, path := stageHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	ns, name := req.Namespace, req.Name
	runRPMReconcile(t, r, req, path)
	wantOnly(t, "applied", seriesOf(t, appliedReplicasGauge, ns, name), "", 5)
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	delete(r.lastReconcileMap, req.NamespacedName.String())
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	for what, c := range map[string]prometheus.Collector{"target": targetReplicasGauge, "ready": targetReadyReplicasGauge,
		"calculated": calculatedReplicasGauge, "requested": requestedReplicasGauge, "decision": decisionActionGauge,
		"capacity": capacityPerReplicaGauge, "applied (the target is gone)": appliedReplicasGauge,
		"applied timestamp": appliedTimestampGauge} {
		wantAbsent(t, what+" after a reconcile without a target", seriesOf(t, c, ns, name))
	}
	// What is still true stays: the mode, why the reconcile failed, and the count of past writes.
	if got := seriesOf(t, modeGauge, ns, name); got["mode=Active"] != 1 {
		t.Fatalf("mode: %v", got)
	}
	if got := seriesOf(t, conditionReasonGauge, ns, name); got["reason=DeploymentNotFound,type=Ready"] != 1 {
		t.Fatalf("the failure reason must be exported: %v", got)
	}
	wantWrites(t, seriesOf(t, scaleWritesTotal, ns, name), map[string]float64{writeApplied: 1})
}

// The registration limit is told apart from the transient causes, and only by the service's exact code on a 503.
func TestTheRegistrationLimitHasItsOwnReason(t *testing.T) {
	limit := `{"detail": {"error": "AutoscalerLimitReached", "message": "the service's registration limit is reached: 8 autoscalers are already registered"}}`
	for _, c := range []struct {
		err  error
		want string
	}{
		{&forecastServerError{status: 503, body: limit}, "AutoscalerLimitReached"},
		{fmt.Errorf("forecast: %w", &forecastServerError{status: 503, body: limit}), "AutoscalerLimitReached"},
		{&forecastServerError{status: 503, body: `{"detail": {"error": "StateUnavailable", "message": "x"}}`}, "DataUnavailable"},
		{&forecastServerError{status: 503, body: `AutoscalerLimitReached`}, "DataUnavailable"}, // not the service's body
		{&forecastServerError{status: 503, body: `{"detail": "AutoscalerLimitReached"}`}, "DataUnavailable"},
		{&forecastServerError{status: 503, body: ``}, "DataUnavailable"},
		{&forecastServerError{status: 500, body: limit}, "DataUnavailable"}, // only on a 503
		{&forecastRefusedError{status: 422, body: limit}, "RequestRefused"},
		{fmt.Errorf("%w: bad window", errV2Refused), "ForecastRejected"},
		{context.DeadlineExceeded, "DataUnavailable"},
	} {
		got := v2FailureReason(c.err)
		if got != c.want {
			t.Errorf("%v: got %s, want %s", c.err, got, c.want)
		}
		if oneOf(got, conditionReasons) != got {
			t.Errorf("%s would be exported as Other", got)
		}
	}
}

// End to end: the service refuses the autoscaler at its limit. The autoscaler stays reactive, says why in a reason of
// its own (also as a metric label), warns once, and recovers when the service answers again.
func TestAnAutoscalerRefusedAtTheServiceLimitSaysSoAndStaysReactive(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
	t.Cleanup(func() { forgetConditionMetrics(f.req.Namespace, f.req.Name) })
	f.stub.set("limit", 0)
	d := f.reconcile(t)
	if d.AppliedReplicas != 5 || d.Action != "scale_up" {
		t.Fatalf("the reactive need is still served: %+v", d)
	}
	c := f.cond(t, "PredictionReady")
	if c.Status != "False" || c.Reason != "AutoscalerLimitReached" || !strings.Contains(c.Message, "registration limit") {
		t.Fatalf("PredictionReady: %+v", c)
	}
	if ev := strings.Join(f.events(), "\n"); !strings.Contains(ev, "Warning PredictionNotReady AutoscalerLimitReached") {
		t.Fatalf("the limit must warn: %s", ev)
	}
	reasons := seriesOf(t, conditionReasonGauge, f.req.Namespace, f.req.Name)
	if reasons["reason=AutoscalerLimitReached,type=PredictionReady"] != 1 {
		t.Fatalf("the reason must be a metric label of its own: %v", reasons)
	}
	f.stub.set("status:503", 0) // another 503 is the transient kind
	f.at(310)
	f.reconcile(t)
	if c := f.cond(t, "PredictionReady"); c.Reason != "DataUnavailable" {
		t.Fatalf("an ordinary 503 stays DataUnavailable: %+v", c)
	}
}

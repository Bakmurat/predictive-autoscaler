package controllers

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	recordConditionMetrics(ns, name, "Active", true, []metav1.Condition{
		{Type: "Ready", Status: metav1.ConditionFalse, Reason: `Get "https://10.0.0.1": dial tcp: i/o timeout`}})
	wantOnly(t, "reason", seriesOf(t, conditionReasonGauge, ns, name), "reason=Other,type=Ready", 1)
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

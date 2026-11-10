package controllers

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// R3-02. A target with 0 desired replicas is paused: nothing is written in any mode. A positive count outside
// [minReplicas, maxReplicas] is still brought inside the bounds when no need can be computed, and the ledger and the
// status then say that the bounds were enforced, never that the count was held.

func setBounds(t *testing.T, r *PredictiveAutoscalerReconciler, req ctrl.Request, min, max int32) {
	t.Helper()
	a := getPA(t, r, req)
	a.Spec.MinReplicas, a.Spec.MaxReplicas = min, max
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
}

// The review's test: a Deployment scaled to 0 by hand (maintenance). It used to be scaled back to minReplicas, also
// during a telemetry "hold", with a record that said keep_current.
func TestATargetWithZeroReplicasIsPausedAndNeverWritten(t *testing.T) {
	for _, mode := range []string{autoscalerv1alpha1.ModeActive, autoscalerv1alpha1.ModeRecommend} {
		for _, rpm := range []string{"", "3000"} { // the rate cannot be measured / a rate that needs 5 replicas
			t.Run(mode+"/rate="+rpm, func(t *testing.T) {
				r, req, path, rec := recommendHarness(t, mode, rpm)
				writes := countScaleWrites(r)
				setReplicas(t, r, req, 0) // kubectl scale --replicas=0
				for i := 0; i < 3; i++ {
					d := runRPMReconcile(t, r, req, path) // also checks that the Deployment is still at applied_replicas
					if d.Action != "paused" || d.AppliedReplicas != 0 || d.DesiredReplicas != 0 || d.DesiredSource != "paused" ||
						d.CurrentReplicas != 0 || !hasString(d.Safeguards, "target_paused") ||
						hasString(d.Safeguards, "telemetry_unavailable_hold") || hasString(d.Safeguards, "bounds_enforced") {
						t.Fatalf("decision %d: %+v", i, d)
					}
					if (rpm == "") != (d.TelemetryStatus == "unavailable") {
						t.Fatalf("the telemetry is still recorded as it was: %+v", d)
					}
				}
				if len(writes) != 0 {
					t.Fatalf("a paused target was written: %v", writes)
				}
				a := getPA(t, r, req)
				c := condition(t, r, req.NamespacedName, "ScalingActive")
				if c.Status != "False" || c.Reason != "TargetPaused" || !strings.Contains(c.Message, "0 desired replicas") {
					t.Fatalf("ScalingActive: %+v", c)
				}
				if s := a.Status; s.CurrentReplicas != 0 || s.CalculatedReplicas != 0 || s.StabilizedReplicas != 0 ||
					s.AppliedReplicas != 0 || s.LastScaleTime != nil || s.Mode != mode {
					t.Fatalf("status: %+v", s)
				}
				if c := condition(t, r, req.NamespacedName, "Ready"); c.Status != "True" || !strings.Contains(c.Message, "calculated=paused") {
					t.Fatalf("Ready: %+v", c)
				}
				ev := strings.Join(drain(rec), "\n")
				if !strings.Contains(ev, "Normal TargetPaused") || strings.Contains(ev, "Warning ScalingInactive") || strings.Contains(ev, "Scaled") {
					t.Fatalf("pausing is a configured state, not a fault: %s", ev)
				}
				// The user restores a positive count: normal behaviour returns at the next reconcile.
				setReplicas(t, r, req, 2)
				d := runRPMReconcile(t, r, req, path)
				want := map[string]string{autoscalerv1alpha1.ModeActive + "3000": "scale_up", autoscalerv1alpha1.ModeActive: "keep_current",
					autoscalerv1alpha1.ModeRecommend + "3000": "recommend", autoscalerv1alpha1.ModeRecommend: "recommend"}[mode+rpm]
				if d.Action != want || d.DesiredSource == "paused" || hasString(d.Safeguards, "target_paused") {
					t.Fatalf("after the resume: %+v", d)
				}
				if c := condition(t, r, req.NamespacedName, "ScalingActive"); c.Reason == "TargetPaused" {
					t.Fatalf("ScalingActive after the resume: %+v", c)
				}
			})
		}
	}
}

// A known zero is published: status.currentReplicas (and readyReplicas) must be in the serialized status at 0, as the
// documentation says ("a zero is reported"). With omitempty the field dropped out exactly when the target was paused.
func TestAZeroReplicaCountIsSerialized(t *testing.T) {
	raw, err := json.Marshal(autoscalerv1alpha1.PredictiveAutoscalerStatus{})
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]interface{}
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"currentReplicas", "readyReplicas"} {
		if v, ok := got[field]; !ok || v != float64(0) {
			t.Fatalf("%s must be serialized as 0, got %v in %s", field, v, raw)
		}
	}
	// And through the reconcile of a paused target: the object the API client stores carries the zero.
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	setReplicas(t, r, req, 0)
	runRPMReconcile(t, r, req, path)
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(autoscalerv1alpha1.GroupVersion.WithKind("PredictiveAutoscaler"))
	if err := r.Get(context.Background(), req.NamespacedName, u); err != nil {
		t.Fatal(err)
	}
	if v, found, err := unstructured.NestedInt64(u.Object, "status", "currentReplicas"); err != nil || !found || v != 0 {
		t.Fatalf("status.currentReplicas of a paused target: %v found=%v err=%v", v, found, err)
	}
}

// A resume is a fresh start for the scale-down holds, like a switch to Active or a re-created target: whatever the
// history before the pause, a scale-down waits both windows again. Scale-up is not delayed (the test above).
func TestAResumedTargetStartsWithFreshScaleDownHolds(t *testing.T) {
	r, req, path := headroomHarness(t, 6, "600", nil) // 600 rpm at 600 per replica: 1
	st := agedState(t, r, req, path)
	if d := runRPMReconcile(t, r, req, path); d.Action != "scale_down" || d.AppliedReplicas != 4 {
		t.Fatalf("precondition: an aged state scales down: %+v", d)
	}
	old := time.Now().Add(-time.Hour)
	st.lastScaleUp, st.lastScaleDown, st.belowCurrentSince = old, old, old // every hold is over again
	setReplicas(t, r, req, 0)
	if d := runRPMReconcile(t, r, req, path); d.Action != "paused" {
		t.Fatalf("paused: %+v", d)
	}
	setReplicas(t, r, req, 6)
	before := time.Now()
	if d := runRPMReconcile(t, r, req, path); d.Action != "hold_stabilizing" || d.AppliedReplicas != 6 {
		t.Fatalf("a resumed target must not be scaled down at once: %+v", d)
	}
	st = r.scaleStates[req.NamespacedName.String()]
	if st.lastScaleUp.Before(before) || st.lastScaleDown.Before(before) || st.paused {
		t.Fatalf("the resume must start the holds now: %+v", st)
	}
}

// The engine statistical path pauses the same way, also with an eligible forecast.
func TestV2ATargetWithZeroReplicasIsPaused(t *testing.T) {
	f := v2Harness(t, autoscalerv1alpha1.ModeActive, "3000")
	f.stub.set("eligible", 1000)
	writes := countScaleWrites(f.r)
	setReplicas(t, f.r, f.req, 0)
	d := f.reconcile(t)
	if d.Action != "paused" || d.AppliedReplicas != 0 || d.DesiredSource != "paused" || len(writes) != 0 {
		t.Fatalf("decision: %+v, writes %v", d, writes)
	}
	if c := f.cond(t, "ScalingActive"); c.Reason != "TargetPaused" {
		t.Fatalf("ScalingActive: %+v", c)
	}
}

// A positive count outside the bounds while no need can be computed: the bounds are enforced as documented, and the
// records say so.
func TestBoundsEnforcedWithoutAMeasuredNeedIsRecordedAsThat(t *testing.T) {
	for _, why := range []string{"telemetry", "capacity"} {
		harness := func(t *testing.T, mode string) (*PredictiveAutoscalerReconciler, ctrl.Request, string, func() []string) {
			if why == "telemetry" {
				r, req, path, rec := recommendHarness(t, mode, "") // the rate cannot be measured
				return r, req, path, func() []string { return drain(rec) }
			}
			return capacityHarness(t, mode, "3000", nil) // no per-replica capacity
		}
		heldBy, holdReason, whyText := "telemetry_unavailable_hold", "TelemetryHold", "the current request rate is unavailable"
		if why == "capacity" {
			heldBy, holdReason, whyText = "capacity_missing_hold", "CapacityMissing", "no per-replica capacity is configured"
		}
		honest := func(t *testing.T, d decisionRecord, source string) {
			t.Helper()
			if d.DesiredSource != source || !hasString(d.Safeguards, "bounds_enforced") || hasString(d.Safeguards, heldBy) {
				t.Fatalf("the record must name the bound, not a hold: %+v", d)
			}
			if (why == "telemetry") != (d.TelemetryStatus == "unavailable") {
				t.Fatalf("telemetry: %+v", d)
			}
		}
		t.Run(why+"/below minReplicas: written at once", func(t *testing.T) {
			r, req, path, _ := harness(t, autoscalerv1alpha1.ModeActive)
			setBounds(t, r, req, 3, 12)
			writes := countScaleWrites(r)
			d := runRPMReconcile(t, r, req, path) // 1 → 3
			honest(t, d, "min_replicas")
			if d.Action != "scale_up" || d.AppliedReplicas != 3 || d.DesiredReplicas != 3 || writes[req.Name] != 1 {
				t.Fatalf("decision: %+v, writes %v", d, writes)
			}
			c := condition(t, r, req.NamespacedName, "ScalingActive")
			if c.Status != "False" || c.Reason != "BoundsEnforced" || !strings.Contains(c.Message, whyText) ||
				!strings.Contains(c.Message, "was set from 1 to 3") || strings.Contains(c.Message, "holding") {
				t.Fatalf("ScalingActive: %+v", c)
			}
			if s := getPA(t, r, req).Status; s.AppliedReplicas != 3 || s.LastScaleTime == nil {
				t.Fatalf("status: %+v", s)
			}
			// Inside the bounds again: now it is a hold, and says so.
			d = runRPMReconcile(t, r, req, path)
			if d.Action != "keep_current" || d.DesiredSource != "keep_current" || !hasString(d.Safeguards, heldBy) ||
				hasString(d.Safeguards, "bounds_enforced") || writes[req.Name] != 1 {
				t.Fatalf("a count inside the bounds is held: %+v", d)
			}
			if c := condition(t, r, req.NamespacedName, "ScalingActive"); c.Reason != holdReason || !strings.Contains(c.Message, "holding") {
				t.Fatalf("ScalingActive: %+v", c)
			}
		})
		t.Run(why+"/above maxReplicas: lowered through the scale-down holds", func(t *testing.T) {
			r, req, path, _ := harness(t, autoscalerv1alpha1.ModeActive)
			setReplicas(t, r, req, 15)
			writes := countScaleWrites(r)
			d := runRPMReconcile(t, r, req, path) // a fresh Active state: the holds apply to this too
			honest(t, d, "max_replicas")
			if d.Action != "hold_stabilizing" || d.AppliedReplicas != 15 || d.DesiredReplicas != 12 || len(writes) != 0 {
				t.Fatalf("decision: %+v, writes %v", d, writes)
			}
			c := condition(t, r, req.NamespacedName, "ScalingActive")
			if c.Reason != "BoundsEnforced" || !strings.Contains(c.Message, "nothing was written") || !strings.Contains(c.Message, "above maxReplicas 12") {
				t.Fatalf("ScalingActive: %+v", c)
			}
			st := r.scaleStates[req.NamespacedName.String()]
			old := time.Now().Add(-time.Hour)
			st.lastScaleUp, st.lastScaleDown, st.belowCurrentSince = old, old, old
			d = runRPMReconcile(t, r, req, path) // max(2, 10 %) per step: 15 → 13
			honest(t, d, "max_replicas")
			if d.Action != "scale_down" || d.AppliedReplicas != 13 || writes[req.Name] != 1 {
				t.Fatalf("decision: %+v, writes %v", d, writes)
			}
			if c := condition(t, r, req.NamespacedName, "ScalingActive"); c.Reason != "BoundsEnforced" || !strings.Contains(c.Message, "was set from 15 to 13") {
				t.Fatalf("ScalingActive: %+v", c)
			}
		})
		t.Run(why+"/Recommend: recorded, not written", func(t *testing.T) {
			r, req, path, _ := harness(t, autoscalerv1alpha1.ModeRecommend)
			setBounds(t, r, req, 3, 12)
			writes := countScaleWrites(r)
			d := runRPMReconcile(t, r, req, path)
			honest(t, d, "min_replicas")
			if d.Action != "recommend" || d.AppliedReplicas != 1 || len(writes) != 0 {
				t.Fatalf("decision: %+v, writes %v", d, writes)
			}
			if c := condition(t, r, req.NamespacedName, "ScalingActive"); c.Reason != "RecommendMode" {
				t.Fatalf("ScalingActive: %+v", c)
			}
		})
		t.Run(why+"/the write is refused", func(t *testing.T) {
			r, req, path, _ := harness(t, autoscalerv1alpha1.ModeActive)
			setBounds(t, r, req, 3, 12)
			wrapClient(r, interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if sub == "scale" {
						return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments/scale"}, obj.GetName(), nil)
					}
					return c.SubResource(sub).Update(ctx, obj, opts...)
				},
			})
			d := runRPMReconcile(t, r, req, path)
			honest(t, d, "min_replicas")
			if d.Action != "scale_error" || d.AppliedReplicas != 1 {
				t.Fatalf("decision: %+v", d)
			}
			c := condition(t, r, req.NamespacedName, "ScalingActive")
			if c.Status != "False" || c.Reason != "ScalingError" || !strings.Contains(c.Message, "forbidden") {
				t.Fatalf("ScalingActive: %+v", c)
			}
		})
	}
}

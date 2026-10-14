package controllers

import (
	"context"
	"math"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	ctrl "sigs.k8s.io/controller-runtime"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Scale-down headroom (soak observation O1, Codex partner b6b): a scale-down must leave
// spec.behavior.scaleDown.headroomPercent of spare request capacity in the resulting fleet.

func TestHeadroomFloor(t *testing.T) {
	for _, tc := range []struct {
		name          string
		rpm, perPod   float64
		pct, max, out int32
	}{
		{"O1: 798 rpm at 120/pod holds 8, not 7", 798, 120, 10, 12, 8},
		{"exactly 7 x 108 rpm fits 7", 756, 120, 10, 12, 7},
		{"just above the threshold needs 8", 756.01, 120, 10, 12, 8},
		{"just below the threshold fits 7", 755.99, 120, 10, 12, 7},
		{"headroom off", 798, 120, 0, 12, 0},
		{"measured zero", 0, 120, 10, 12, 0},
		{"huge finite demand is capped at max", 1e30, 120, 10, 12, 12},
		{"NaN", math.NaN(), 120, 10, 12, 0},
		{"Inf", math.Inf(1), 120, 10, 12, 0},
		{"negative", -1, 120, 10, 12, 0},
		{"no capacity", 798, 0, 10, 12, 0},
		{"99 % headroom", 120, 120, 99, 1000, 100},
		{"100 % is invalid (API rejects it), treated as off", 798, 120, 100, 12, 0},
	} {
		if got := headroomFloor(tc.rpm, tc.perPod, tc.pct, tc.max); got != tc.out {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.out)
		}
	}
}

func TestScaleDownHeadroomPercentDefaults(t *testing.T) {
	ten, zero := int32(10), int32(0)
	for _, tc := range []struct {
		spec autoscalerv1alpha1.PredictiveAutoscalerSpec
		want int32
	}{
		{autoscalerv1alpha1.PredictiveAutoscalerSpec{}, ten},
		{autoscalerv1alpha1.PredictiveAutoscalerSpec{Behavior: &autoscalerv1alpha1.BehaviorConfig{}}, ten},
		{autoscalerv1alpha1.PredictiveAutoscalerSpec{Behavior: &autoscalerv1alpha1.BehaviorConfig{
			ScaleDown: &autoscalerv1alpha1.ScaleDownBehavior{}}}, ten},
		{autoscalerv1alpha1.PredictiveAutoscalerSpec{Behavior: &autoscalerv1alpha1.BehaviorConfig{
			ScaleDown: &autoscalerv1alpha1.ScaleDownBehavior{HeadroomPercent: &zero}}}, 0},
	} {
		if got := tc.spec.ScaleDownHeadroomPercent(); got != tc.want {
			t.Errorf("%+v: got %d, want %d", tc.spec.Behavior, got, tc.want)
		}
	}
}

// headroomHarness: Active, 600 rpm per pod (the harness's targetRPS), the target at `replicas`, the rate `rpm`,
// and the scale history aged past every window so that only the headroom can hold a scale-down.
func headroomHarness(t *testing.T, replicas int32, rpm string, headroom *int32) (*PredictiveAutoscalerReconciler, ctrl.Request, string) {
	t.Helper()
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, rpm)
	setReplicas(t, r, req, replicas)
	if headroom != nil {
		a := getPA(t, r, req)
		a.Spec.Behavior = &autoscalerv1alpha1.BehaviorConfig{ScaleDown: &autoscalerv1alpha1.ScaleDownBehavior{HeadroomPercent: headroom}}
		if err := r.Update(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	return r, req, path
}

func setReplicas(t *testing.T, r *PredictiveAutoscalerReconciler, req ctrl.Request, n int32) {
	t.Helper()
	var d appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &d); err != nil {
		t.Fatal(err)
	}
	d.Spec.Replicas = &n
	if err := r.Update(context.Background(), &d); err != nil {
		t.Fatal(err)
	}
}

// agedState runs one reconcile (creating the state), then ages every hold so that a scale-down is otherwise due.
func agedState(t *testing.T, r *PredictiveAutoscalerReconciler, req ctrl.Request, path string) *scaleState {
	t.Helper()
	runRPMReconcile(t, r, req, path)
	st := r.scaleStates[req.NamespacedName.String()]
	old := time.Now().Add(-time.Hour)
	st.lastScaleUp, st.lastScaleDown, st.belowCurrentSince = old, old, old
	return st
}

func TestO1AScaleDownThatWouldLeaveTooLittleHeadroomIsHeld(t *testing.T) {
	r, req, path := headroomHarness(t, 6, "2900", nil) // needs 4.83 → 5, but 5 x 540 = 2700 < 2900: floor 6
	st := agedState(t, r, req, path)
	rec := runRPMReconcile(t, r, req, path)
	if rec.Action != "hold_headroom" || rec.AppliedReplicas != 6 || rec.ScaleDownHeadroomFloor == nil || *rec.ScaleDownHeadroomFloor != 6 {
		t.Fatalf("decision %s applied %d floor %v", rec.Action, rec.AppliedReplicas, rec.ScaleDownHeadroomFloor)
	}
	if !st.belowCurrentSince.IsZero() {
		t.Fatal("losing eligibility must reset the below-current timer")
	}
	c := meta.FindStatusCondition(getPA(t, r, req).Status.Conditions, "ScaleDownHeadroom")
	if c == nil || c.Status != "True" || c.Reason != "Holding" {
		t.Fatalf("condition %+v", c)
	}
	if rc := meta.FindStatusCondition(getPA(t, r, req).Status.Conditions, "Ready"); rc == nil || rc.Status != "True" {
		t.Fatalf("a headroom hold must not make the autoscaler unready: %+v", rc)
	}
}

func TestHeadroomZeroRestoresThePlainScaleDown(t *testing.T) {
	zero := int32(0)
	r, req, path := headroomHarness(t, 6, "2900", &zero)
	agedState(t, r, req, path)
	if rec := runRPMReconcile(t, r, req, path); rec.Action != "scale_down" || rec.AppliedReplicas != 5 {
		t.Fatalf("with headroom 0 the scale-down to 5 proceeds: %s %d", rec.Action, rec.AppliedReplicas)
	}
}

func TestHeadroomBoundsTheGradualTarget(t *testing.T) {
	// desired ceil(5.83)=6, floor ceil(6.48)=7; the gradual step from 8 removes 2, which would reach 6 without the floor
	r, req, path := headroomHarness(t, 8, "3500", nil)
	agedState(t, r, req, path)
	rec := runRPMReconcile(t, r, req, path)
	if rec.Action != "scale_down" || rec.AppliedReplicas != 7 {
		t.Fatalf("scale-down below the headroom floor: %s %d", rec.Action, rec.AppliedReplicas)
	}
}

func TestHeadroomNeverDelaysScaleUp(t *testing.T) {
	r, req, path := headroomHarness(t, 4, "2900", nil) // needs 5
	if rec := runRPMReconcile(t, r, req, path); rec.Action != "scale_up" || rec.AppliedReplicas != 5 {
		t.Fatalf("scale-up must be unchanged: %s %d", rec.Action, rec.AppliedReplicas)
	}
}

func TestTheOverrideAndTheRestartFloorDoNotBypassTheHeadroom(t *testing.T) {
	r, req, path := headroomHarness(t, 6, "2900", nil)
	st := agedState(t, r, req, path)
	st.overrideActive = true
	if rec := runRPMReconcile(t, r, req, path); rec.Action != "hold_headroom" {
		t.Fatalf("the overestimate override bypassed the headroom: %s", rec.Action)
	}
	r.scaleStates = map[string]*scaleState{} // a restart: the startup floor applies as well
	if rec := runRPMReconcile(t, r, req, path); rec.Action != "hold_headroom" || rec.AppliedReplicas != 6 {
		t.Fatalf("after a restart: %s %d", rec.Action, rec.AppliedReplicas)
	}
}

func TestALoweredMaximumIsStillApplied(t *testing.T) {
	r, req, path := headroomHarness(t, 9, "6000", nil) // floor ceil(11.1)=12, but max is lowered to 6
	a := getPA(t, r, req)
	a.Spec.MaxReplicas = 6
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	agedState(t, r, req, path)
	rec := runRPMReconcile(t, r, req, path)
	if rec.Action != "scale_down" || rec.AppliedReplicas >= 9 || rec.ScaleDownHeadroomFloor == nil || *rec.ScaleDownHeadroomFloor != 6 {
		t.Fatalf("the floor must be capped at max: %s %d floor %v", rec.Action, rec.AppliedReplicas, rec.ScaleDownHeadroomFloor)
	}
}

func TestMissingTelemetryHoldsWithoutAFloor(t *testing.T) {
	r, req, path := headroomHarness(t, 6, "", nil)
	agedState(t, r, req, path)
	rec := runRPMReconcile(t, r, req, path)
	if rec.AppliedReplicas != 6 || rec.ScaleDownHeadroomFloor != nil {
		t.Fatalf("missing telemetry must keep the count and record no floor: %s %d %v", rec.Action, rec.AppliedReplicas, rec.ScaleDownHeadroomFloor)
	}
}

func TestEligibilityRegainedStartsAFreshWindow(t *testing.T) {
	r, req, path := headroomHarness(t, 6, "2900", nil)
	st := agedState(t, r, req, path)
	runRPMReconcile(t, r, req, path) // held for headroom: timer reset
	vmServer(t, rateBody(t, "2400"), 200)
	// 2400 needs 4, floor ceil(4.44)=5 < 6: eligible again, but the below-current window starts now
	if rec := runRPMReconcile(t, r, req, path); rec.Action != "hold_stabilizing" || st.belowCurrentSince.IsZero() {
		t.Fatalf("regained eligibility must start a fresh window: %s timer %v", rec.Action, st.belowCurrentSince)
	}
	st.belowCurrentSince = time.Now().Add(-scaleDownStabilization - time.Second)
	if rec := runRPMReconcile(t, r, req, path); rec.Action != "scale_down" || rec.AppliedReplicas != 5 {
		t.Fatalf("after the window: %s %d", rec.Action, rec.AppliedReplicas)
	}
}

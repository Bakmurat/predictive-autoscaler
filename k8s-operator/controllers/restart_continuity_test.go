package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Restart and leader-change continuity of the scale-down holds (plan PLAN-NEXT-20261014 item 3).

func setTargetUID(t *testing.T, r *PredictiveAutoscalerReconciler, nn types.NamespacedName, uid string) {
	t.Helper()
	var dep appsv1.Deployment // the fake client assigns no UID; real objects always have one
	if err := r.Get(context.Background(), nn, &dep); err != nil {
		t.Fatal(err)
	}
	dep.UID = types.UID(uid)
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
}

func TestPersistedWriteTime(t *testing.T) {
	now := time.Date(2026, 10, 14, 12, 0, 0, 0, time.UTC)
	at := func(d time.Duration) *metav1.Time { m := metav1.NewTime(now.Add(d)); return &m }
	for _, tc := range []struct {
		name      string
		statusUID string
		last      *metav1.Time
		want      time.Time
	}{
		{"same target", "dep-1", at(-30 * time.Second), now.Add(-30 * time.Second)},
		{"other target", "dep-0", at(-30 * time.Second), time.Time{}},
		{"no target recorded", "", at(-30 * time.Second), time.Time{}},
		{"no write recorded", "dep-1", nil, time.Time{}},
		{"clock skew: capped at now", "dep-1", at(10 * time.Minute), now},
	} {
		a := &autoscalerv1alpha1.PredictiveAutoscaler{}
		a.Status.TargetUID, a.Status.LastScaleTime = tc.statusUID, tc.last
		if got := persistedWriteTime(a, "dep-1", now); !got.Equal(tc.want) {
			t.Errorf("%s: got %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestRestartKeepsTheScaleDownHolds(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	setTargetUID(t, r, req.NamespacedName, "dep-1")
	runRPMReconcile(t, r, req, path) // 3000 rpm at 600 per pod: 1 → 5
	before := getPA(t, r, req).Status
	if before.AppliedReplicas != 5 || before.LastScaleTime == nil || before.TargetUID != "dep-1" {
		t.Fatalf("precondition: %+v", before)
	}

	r.scaleStates = map[string]*scaleState{} // an operator restart or a new leader: only the persisted status remains
	vmServer(t, rateBody(t, "600"), 200)    // demand falls to one replica
	if got := runRPMReconcile(t, r, req, path); got.AppliedReplicas != 5 {
		t.Fatalf("the first reconcile after a restart scaled down to %d", got.AppliedReplicas)
	}
	st := r.scaleStates[req.NamespacedName.String()]
	if !st.lastScaleDown.Equal(before.LastScaleTime.Time) {
		t.Fatalf("cooldown seed %v, want the persisted write %v", st.lastScaleDown, before.LastScaleTime.Time)
	}
	if st.belowCurrentSince.IsZero() {
		t.Fatal("the stabilization timer must restart at the first reconcile after the restart")
	}

	// The overestimate override bypasses the stabilization holds but never the cooldown, which now spans the restart.
	st.overrideActive = true
	if got := r.calculateScaleDownTarget(logr.Discard(), st, 5, 1); got != 5 {
		t.Fatalf("override inside the cooldown of the persisted write: scaled to %d", got)
	}
	st.lastScaleDown = time.Now().Add(-scaleDownCooldown - time.Second)
	if got := r.calculateScaleDownTarget(logr.Discard(), st, 5, 1); got >= 5 {
		t.Fatalf("after the cooldown the override may reduce; got %d", got)
	}
}

func TestRestartSeedsNothingForAnotherTargetOrRecommendMode(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	setTargetUID(t, r, req.NamespacedName, "dep-1")
	runRPMReconcile(t, r, req, path)
	key := req.NamespacedName.String()

	setTargetUID(t, r, req.NamespacedName, "dep-2") // a re-created target: the predecessor's write does not apply
	r.scaleStates = map[string]*scaleState{}
	runRPMReconcile(t, r, req, path)
	if st := r.scaleStates[key]; !st.lastScaleDown.IsZero() {
		t.Fatalf("a replaced target inherited the cooldown seed %v", st.lastScaleDown)
	}

	a := getPA(t, r, req)
	a.Spec.Mode = autoscalerv1alpha1.ModeRecommend
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	r.scaleStates = map[string]*scaleState{}
	runRPMReconcile(t, r, req, path)
	if st := r.scaleStates[key]; !st.lastScaleDown.IsZero() {
		t.Fatalf("Recommend mode keeps no scale history; got a seed %v", st.lastScaleDown)
	}
}

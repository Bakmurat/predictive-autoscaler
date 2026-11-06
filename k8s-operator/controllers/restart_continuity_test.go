package controllers

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/types"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Restart and leader-change continuity of the scale-down holds (plan PLAN-NEXT-20261014 item 3; Codex partner review
// b6a-restart-holds): a fresh Active state is treated as if a write had just happened.

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

// restartAfterScaleUp scales 1 → 5, then simulates an operator restart (only the persisted status survives) and a fall
// in demand to one replica, and returns the fresh state after the first reconcile of the new process.
func restartAfterScaleUp(t *testing.T, clearStatus bool) (*PredictiveAutoscalerReconciler, *scaleState, time.Time) {
	t.Helper()
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	setTargetUID(t, r, req.NamespacedName, "dep-1")
	runRPMReconcile(t, r, req, path) // 3000 rpm at 600 per pod: 1 → 5
	if s := getPA(t, r, req).Status; s.AppliedReplicas != 5 || s.LastScaleTime == nil {
		t.Fatalf("precondition: %+v", s)
	}
	if clearStatus { // the status update after the write failed, or the cached status lags the write
		a := getPA(t, r, req)
		a.Status.LastScaleTime, a.Status.AppliedReplicas = nil, 0
		if err := r.Status().Update(context.Background(), a); err != nil {
			t.Fatal(err)
		}
	}
	r.scaleStates = map[string]*scaleState{}
	vmServer(t, rateBody(t, "600"), 200)
	restart := time.Now()
	if got := runRPMReconcile(t, r, req, path); got.AppliedReplicas != 5 {
		t.Fatalf("the first reconcile after a restart scaled down to %d", got.AppliedReplicas)
	}
	return r, r.scaleStates[req.NamespacedName.String()], restart
}

func TestRestartHoldsBothStabilizationWindows(t *testing.T) {
	// Without a restart, a scale-up at minute 0 blocks scale-down until minute 5 (rule 1), and only then does the
	// below-current timer start (rule 2): no scale-down before minute 10. A restart at minute 1 must not shorten that.
	r, st, restart := restartAfterScaleUp(t, false)
	if st.lastScaleUp.Before(restart) || st.lastScaleDown.Before(restart) {
		t.Fatalf("a fresh Active state must count as a write at the restart: up=%v down=%v restart=%v",
			st.lastScaleUp, st.lastScaleDown, restart)
	}
	if !st.belowCurrentSince.IsZero() {
		t.Fatal("the below-current timer must not start while the scale-up hold is active")
	}
	// Minute 6 of the new process: the scale-up hold has ended, the below-current timer only starts now.
	st.lastScaleUp = time.Now().Add(-scaleDownStabilization - time.Second)
	st.lastScaleDown = st.lastScaleUp
	if got := r.calculateScaleDownTarget(logr.Discard(), st, 5, 1); got != 5 || st.belowCurrentSince.IsZero() {
		t.Fatalf("the below-current window must start after the scale-up hold: got %d, timer %v", got, st.belowCurrentSince)
	}
	// After the full below-current window as well, scale-down proceeds (gradually).
	st.belowCurrentSince = time.Now().Add(-scaleDownStabilization - time.Second)
	if got := r.calculateScaleDownTarget(logr.Discard(), st, 5, 1); got >= 5 {
		t.Fatalf("after both windows a scale-down is due; got %d", got)
	}
}

func TestRestartCooldownDoesNotDependOnThePersistedStatus(t *testing.T) {
	// The write succeeded but its status update did not: a fresh process sees no lastScaleTime. The overestimate
	// override skips the stabilization holds, so only the cooldown protects the target; it must start at the restart.
	r, st, _ := restartAfterScaleUp(t, true)
	st.overrideActive = true
	if got := r.calculateScaleDownTarget(logr.Discard(), st, 5, 1); got != 5 {
		t.Fatalf("override inside the startup cooldown scaled to %d", got)
	}
	st.lastScaleDown = time.Now().Add(-scaleDownCooldown - time.Second)
	if got := r.calculateScaleDownTarget(logr.Discard(), st, 5, 1); got >= 5 {
		t.Fatalf("after the cooldown the override may reduce; got %d", got)
	}
}

func TestRecommendModeKeepsNoScaleHistoryAfterARestart(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	setTargetUID(t, r, req.NamespacedName, "dep-1")
	runRPMReconcile(t, r, req, path)
	r.scaleStates = map[string]*scaleState{}
	runRPMReconcile(t, r, req, path)
	if st := r.scaleStates[req.NamespacedName.String()]; !st.lastScaleUp.IsZero() || !st.lastScaleDown.IsZero() {
		t.Fatalf("Recommend mode keeps no hypothetical write history: up=%v down=%v", st.lastScaleUp, st.lastScaleDown)
	}
}

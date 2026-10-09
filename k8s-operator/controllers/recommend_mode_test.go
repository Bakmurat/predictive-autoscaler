package controllers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Recommend mode (the default, also when spec.mode is absent), status, conditions and events (plan items #2/#3, B1).

func recommendHarness(t *testing.T, mode string, rpm string) (*PredictiveAutoscalerReconciler, ctrl.Request, string, *events.FakeRecorder) {
	t.Helper()
	r, a, req, path := rpmGaugeReconciler(t)
	a.Spec.Mode = mode
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	if rpm == "" {
		vmServer(t, `{"status":"success","data":{"resultType":"vector","result":[]}}`, 200)
	} else {
		vmServer(t, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"`+rpm+`"]}]}}`, 200)
	}
	ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	t.Cleanup(ml.Close)
	t.Setenv("ML_API_URL", ml.URL)
	rec := events.NewFakeRecorder(100)
	r.Recorder = rec
	t.Cleanup(func() {
		currentRpmGauge.DeleteLabelValues(req.Name, req.Namespace)
		actualNeededReplicasGauge.DeleteLabelValues(req.Name, req.Namespace)
		predictionErrorPercentGauge.DeleteLabelValues(req.Name, req.Namespace)
	})
	return r, req, path, rec
}

func getPA(t *testing.T, r *PredictiveAutoscalerReconciler, req ctrl.Request) *autoscalerv1alpha1.PredictiveAutoscaler {
	t.Helper()
	var a autoscalerv1alpha1.PredictiveAutoscaler
	if err := r.Get(context.Background(), req.NamespacedName, &a); err != nil {
		t.Fatal(err)
	}
	return &a
}

func drain(rec *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-rec.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func TestRecommendIsTheDefaultAndWritesNothing(t *testing.T) {
	for _, mode := range []string{"", autoscalerv1alpha1.ModeRecommend} {
		t.Run("mode="+mode, func(t *testing.T) {
			r, req, path, _ := recommendHarness(t, mode, "3000") // 3000 rpm / 600 per pod → 5 replicas needed
			d := runRPMReconcile(t, r, req, path)
			if d.Action != "recommend" || d.AppliedReplicas != 1 || d.DesiredReplicas != 5 {
				t.Fatalf("Recommend must compute 5 and write nothing: %+v", d)
			}
			var dep appsv1.Deployment
			if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil || *dep.Spec.Replicas != 1 {
				t.Fatalf("the target was written: %v %v", dep.Spec.Replicas, err)
			}
			st := r.scaleStates[req.NamespacedName.String()]
			if !st.lastScaleUp.IsZero() || !st.lastScaleDown.IsZero() || !st.belowCurrentSince.IsZero() {
				t.Fatalf("Recommend advanced the scale history: %+v", st)
			}
			a := getPA(t, r, req)
			s := a.Status
			if s.Mode != autoscalerv1alpha1.ModeRecommend || s.CalculatedReplicas != 5 || s.StabilizedReplicas != 0 ||
				s.AppliedReplicas != 0 || s.CurrentReplicas != 1 || s.ObservedGeneration != a.Generation {
				t.Fatalf("status: %+v", s)
			}
			c := meta.FindStatusCondition(s.Conditions, "ScalingActive")
			if c == nil || c.Status != "False" || c.Reason != "RecommendMode" {
				t.Fatalf("ScalingActive: %+v", c)
			}
		})
	}
}

func TestActiveScalesAndRecordsTheWrite(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "scale_up" || d.AppliedReplicas != 5 {
		t.Fatalf("Active must scale to 5: %+v", d)
	}
	s := getPA(t, r, req).Status
	if s.AppliedReplicas != 5 || s.StabilizedReplicas != 5 || s.LastScaleTime == nil || s.Mode != "Active" {
		t.Fatalf("status: %+v", s)
	}
	if ev := strings.Join(drain(rec), "\n"); !strings.Contains(ev, "ScaledUp") {
		t.Fatalf("no ScaledUp event: %s", ev)
	}
}

func TestModeChangeAndTargetRecreationResetTheScaleState(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeRecommend, "300") // 1 replica needed, at target
	runRPMReconcile(t, r, req, path)
	key := req.NamespacedName.String()
	stale := time.Now().Add(-time.Minute)
	r.scaleStates[key].lastScaleUp, r.scaleStates[key].belowCurrentSince, r.scaleStates[key].overrideActive = stale, stale, true
	a := getPA(t, r, req)
	a.Spec.Mode = autoscalerv1alpha1.ModeActive
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	runRPMReconcile(t, r, req, path) // at target: no new timestamps
	st := r.scaleStates[key]
	if st.mode != "Active" || !st.lastScaleUp.IsZero() || st.overrideActive {
		t.Fatalf("switching to Active must start from a clean state: %+v", st)
	}
	// a re-created target (new UID) resets it too
	st.lastScaleUp = stale
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	dep.UID = types.UID("re-created")
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	runRPMReconcile(t, r, req, path)
	if st := r.scaleStates[key]; st.targetUID != "re-created" || !st.lastScaleUp.IsZero() {
		t.Fatalf("a re-created target must reset the state: %+v", st)
	}
}

func TestConditionsPreserveTransitionsAndUnchangedStatusIsNotWritten(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "") // metrics unavailable → hold
	runRPMReconcile(t, r, req, path)
	a1 := getPA(t, r, req)
	tel := meta.FindStatusCondition(a1.Status.Conditions, "TelemetryAvailable")
	if tel == nil || tel.Status != "False" || tel.Reason != "MetricsUnavailable" {
		t.Fatalf("TelemetryAvailable: %+v", tel)
	}
	if c := meta.FindStatusCondition(a1.Status.Conditions, "ScalingActive"); c == nil || c.Reason != "TelemetryHold" {
		t.Fatalf("ScalingActive: %+v", c)
	}
	first := strings.Join(drain(rec), "\n")
	if !strings.Contains(first, "TelemetryUnavailable") {
		t.Fatalf("missing TelemetryUnavailable event: %s", first)
	}
	runRPMReconcile(t, r, req, path) // same inputs
	a2 := getPA(t, r, req)
	if a2.ResourceVersion != a1.ResourceVersion {
		t.Fatalf("an unchanged status was written (%s → %s)", a1.ResourceVersion, a2.ResourceVersion)
	}
	if again := drain(rec); len(again) != 0 {
		t.Fatalf("events repeated without a transition: %v", again)
	}
	vmServer(t, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"300"]}]}}`, 200)
	runRPMReconcile(t, r, req, path)
	a3 := getPA(t, r, req)
	tel3 := meta.FindStatusCondition(a3.Status.Conditions, "TelemetryAvailable")
	if tel3 == nil || tel3.Status != "True" || !tel3.LastTransitionTime.After(tel.LastTransitionTime.Time.Add(-time.Second)) {
		t.Fatalf("TelemetryAvailable after restore: %+v", tel3)
	}
	ready1, ready3 := meta.FindStatusCondition(a1.Status.Conditions, "Ready"), meta.FindStatusCondition(a3.Status.Conditions, "Ready")
	if ready1 == nil || ready3 == nil || !ready1.LastTransitionTime.Equal(&ready3.LastTransitionTime) {
		t.Fatalf("Ready stayed True: its transition time must be preserved (%v vs %v)", ready1, ready3)
	}
	if ev := strings.Join(drain(rec), "\n"); !strings.Contains(ev, "TelemetryRestored") {
		t.Fatalf("missing TelemetryRestored event: %s", ev)
	}
}

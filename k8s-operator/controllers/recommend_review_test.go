package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Codex task-08 r04 regressions for B1.

func TestReplacedTargetClearsThePersistedWriteHistory(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	var orig appsv1.Deployment // the fake client assigns no UID; real objects always have one
	if err := r.Get(context.Background(), req.NamespacedName, &orig); err != nil {
		t.Fatal(err)
	}
	orig.UID = types.UID("original")
	if err := r.Update(context.Background(), &orig); err != nil {
		t.Fatal(err)
	}
	runRPMReconcile(t, r, req, path) // scales 1 → 5
	if s := getPA(t, r, req).Status; s.AppliedReplicas != 5 || s.LastScaleTime == nil || s.TargetUID != "original" {
		t.Fatalf("precondition: %+v", s)
	}
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	dep.UID = types.UID("replacement")
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	r.scaleStates = map[string]*scaleState{} // as after an operator restart: only the persisted status remains
	runRPMReconcile(t, r, req, path)         // at target (5): no new write
	s := getPA(t, r, req).Status
	if s.TargetUID != "replacement" || s.AppliedReplicas != 0 || s.LastScaleTime != nil {
		t.Fatalf("the predecessor's write history must be cleared: %+v", s)
	}
}

func TestMissingTargetReportsTheEffectiveModeAndInactiveScaling(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	runRPMReconcile(t, r, req, path)
	a := getPA(t, r, req)
	a.Spec.Mode = autoscalerv1alpha1.ModeRecommend
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
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
	s := getPA(t, r, req).Status
	c := meta.FindStatusCondition(s.Conditions, "ScalingActive")
	if s.Mode != autoscalerv1alpha1.ModeRecommend || c == nil || c.Status != "False" || c.Reason != "DeploymentNotFound" {
		t.Fatalf("status after a failed reconcile: mode=%s ScalingActive=%+v", s.Mode, c)
	}
}

func TestNoEventsWhenTheStatusWriteFails(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "")
	fail := true
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "status" && fail {
				return errors.New("status write refused")
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	runRPMReconcile(t, r, req, path)
	if ev := drain(rec); len(ev) != 0 {
		t.Fatalf("events emitted although the status was not persisted: %v", ev)
	}
	fail = false
	runRPMReconcile(t, r, req, path)
	if ev := strings.Join(drain(rec), "\n"); !strings.Contains(ev, "TelemetryUnavailable") {
		t.Fatalf("the transition must be reported once the status is persisted: %s", ev)
	}
}

func TestExpectedStatesAreNormalEvents(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeRecommend, "300")
	no := false
	a := getPA(t, r, req)
	a.Spec.Prediction.Enabled = &no
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	runRPMReconcile(t, r, req, path)
	for _, e := range drain(rec) {
		if strings.HasPrefix(e, "Warning") && (strings.Contains(e, "RecommendMode") || strings.Contains(e, "Disabled")) {
			t.Fatalf("a configured state was reported as a warning: %s", e)
		}
	}
}

func TestReadyReplicasZeroIsSerialized(t *testing.T) {
	raw, err := json.Marshal(autoscalerv1alpha1.PredictiveAutoscalerStatus{})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), `"readyReplicas":0`) {
		t.Fatalf("a zero readyReplicas must be reported: %s", raw)
	}
}

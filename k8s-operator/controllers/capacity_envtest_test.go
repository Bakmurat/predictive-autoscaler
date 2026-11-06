package controllers

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The capacity contract's CRD rules on the real API server (CEL with the Kubernetes quantity library), and the operator's
// hold without a capacity through real /scale and status writes.

func TestEnvtestCapacityPerReplicaValidation(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeRecommend)
	ctx := context.Background()
	create := func(name string, requests map[string]interface{}) (*unstructured.Unstructured, error) {
		spec := map[string]interface{}{"targetDeployment": map[string]interface{}{"name": "web", "namespace": f.req.Namespace},
			"minReplicas": int64(1), "maxReplicas": int64(3)}
		if requests != nil {
			spec["metrics"] = map[string]interface{}{"requests": requests}
		}
		u := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": autoscalerv1alpha1.GroupVersion.String(), "kind": "PredictiveAutoscaler",
			"metadata": map[string]interface{}{"name": name, "namespace": f.req.Namespace}, "spec": spec,
		}}
		return u, f.c.Create(ctx, u)
	}
	for name, requests := range map[string]map[string]interface{}{
		"both":         {"capacityPerReplica": "12.5", "targetRPS": int64(10)},
		"zero":         {"capacityPerReplica": "0"},
		"zero-int":     {"capacityPerReplica": int64(0)},
		"negative":     {"capacityPerReplica": "-1"},
		"above-bound":  {"capacityPerReplica": "1000001"},
		"above-bound2": {"capacityPerReplica": "2M"},
		"not-quantity": {"capacityPerReplica": "fast"},
		"zero-rps":     {"targetRPS": int64(0)},
	} {
		_, err := create(name, requests)
		if err == nil {
			t.Errorf("%s: must be rejected", name)
			continue
		}
		if name == "both" && !strings.Contains(err.Error(), "not both") {
			t.Errorf("both: the message must say why: %v", err)
		}
		if strings.HasPrefix(name, "above") && !strings.Contains(err.Error(), "at most 1000000") {
			t.Errorf("%s: the message must say why: %v", name, err)
		}
	}
	for name, requests := range map[string]map[string]interface{}{
		"fractional":  {"capacityPerReplica": "12.5"},
		"integer":     {"capacityPerReplica": int64(40)},
		"milli":       {"capacityPerReplica": "500m"},
		"upper-bound": {"capacityPerReplica": "1000000"},
		"legacy":      {"targetRPS": int64(10)},
		"neither":     {"enabled": true},
		"no-requests": nil,
	} {
		if _, err := create(name, requests); err != nil {
			t.Errorf("%s: must be accepted: %v", name, err)
		}
	}
	var pa autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(ctx, types.NamespacedName{Namespace: f.req.Namespace, Name: "fractional"}, &pa); err != nil {
		t.Fatal(err)
	}
	if q := pa.Spec.Metrics.Requests.CapacityPerReplica; q == nil || q.String() != "12500m" || q.AsApproximateFloat64() != 12.5 {
		t.Fatalf("the fractional capacity must survive the round trip: %v", q)
	}
	// Admission does not yet require a capacity: a legacy object with neither field stays updatable.
	for _, name := range []string{"neither", "no-requests"} {
		var legacy autoscalerv1alpha1.PredictiveAutoscaler
		if err := f.c.Get(ctx, types.NamespacedName{Namespace: f.req.Namespace, Name: name}, &legacy); err != nil {
			t.Fatal(err)
		}
		legacy.Spec.MaxReplicas, legacy.Spec.Mode = 5, autoscalerv1alpha1.ModeActive
		if err := f.c.Update(ctx, &legacy); err != nil {
			t.Fatalf("%s: an update without a capacity must be accepted: %v", name, err)
		}
	}
	// Adding capacityPerReplica to a legacy targetRPS object without removing targetRPS is rejected on update too.
	var legacy autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(ctx, types.NamespacedName{Namespace: f.req.Namespace, Name: "legacy"}, &legacy); err != nil {
		t.Fatal(err)
	}
	legacy.Spec.Metrics.Requests.CapacityPerReplica = quantity("10")
	if err := f.c.Update(ctx, &legacy); err == nil {
		t.Fatal("an update setting both fields must be rejected")
	}
	legacy.Spec.Metrics.Requests.TargetRPS = 0
	if err := f.c.Update(ctx, &legacy); err != nil {
		t.Fatalf("the migration targetRPS → capacityPerReplica must be accepted: %v", err)
	}
}

// Active without a capacity through the real API server: no write, the hold and the condition in the persisted status.
func TestEnvtestNoCapacityHoldsWithoutWriting(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive) // 3000 rpm, 1 replica: any capacity up to 50 rps would scale up
	ctx := context.Background()
	var pa autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(ctx, f.req.NamespacedName, &pa); err != nil {
		t.Fatal(err)
	}
	pa.Spec.Metrics.Requests = nil
	if err := f.c.Update(ctx, &pa); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "keep_current" || d.CapacitySource != capacitySourceMissing ||
		*f.deployment(t).Spec.Replicas != 1 {
		t.Fatalf("decision %+v, replicas %d", d, *f.deployment(t).Spec.Replicas)
	}
	if err := f.c.Get(ctx, f.req.NamespacedName, &pa); err != nil {
		t.Fatal(err)
	}
	c := meta.FindStatusCondition(pa.Status.Conditions, "CapacityConfigured")
	sa := meta.FindStatusCondition(pa.Status.Conditions, "ScalingActive")
	if c == nil || c.Status != "False" || c.Reason != "CapacityMissing" || sa == nil || sa.Reason != "CapacityMissing" ||
		pa.Status.CalculatedReplicas != 0 {
		t.Fatalf("status: %+v / %+v / calculated %d", c, sa, pa.Status.CalculatedReplicas)
	}
}

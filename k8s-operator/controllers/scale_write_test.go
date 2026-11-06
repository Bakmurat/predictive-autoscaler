package controllers

import (
	"context"
	"encoding/json"
	stderrors "errors"
	"os"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// B3 — replicas are written only through /scale, and only while the count the decision was computed from is current.

func lastDecision(t *testing.T, path string) decisionRecord {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var last decisionRecord
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		var d decisionRecord
		if err := json.Unmarshal([]byte(line), &d); err != nil {
			t.Fatal(err)
		}
		if d.Event == "decision" {
			last = d
		}
	}
	return last
}

func reconcileOnce(t *testing.T, r *PredictiveAutoscalerReconciler, req ctrl.Request) ctrl.Result {
	t.Helper()
	delete(r.lastReconcileMap, req.NamespacedName.String())
	res, err := r.Reconcile(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// wrapClient puts an interceptor around the reconciler's client (the cached client in production); the coexistence
// reader falls back to it as well.
func wrapClient(r *PredictiveAutoscalerReconciler, f interceptor.Funcs) {
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), f)
}

func TestScalingWritesOnlyTheScaleSubresource(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000") // 1 → 5
	var scaleWrites int
	wrapClient(r, interceptor.Funcs{
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				t.Errorf("full Deployment update of %s: replicas must go through /scale", obj.GetName())
			}
			return c.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, p client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				t.Errorf("Deployment patch of %s", obj.GetName())
			}
			return c.Patch(ctx, obj, p, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				scaleWrites++
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	if d := runRPMReconcile(t, r, req, path); d.Action != "scale_up" || d.AppliedReplicas != 5 {
		t.Fatalf("scale up: %+v", d)
	}
	if scaleWrites != 1 {
		t.Fatalf("scale subresource writes = %d, want 1", scaleWrites)
	}
}

// Another writer changed the count after the operator read the Deployment (cache) and decided: the write is aborted,
// not applied over the newer count, and a fresh reconcile follows within seconds.
func TestAStaleDecisionIsNotWrittenAndIsRetriedSoon(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	var scaleWrites int
	wrapClient(r, interceptor.Funcs{
		SubResourceGet: func(ctx context.Context, c client.Client, sub string, obj client.Object, out client.Object, opts ...client.SubResourceGetOption) error {
			if err := c.SubResource(sub).Get(ctx, obj, out, opts...); err != nil {
				return err
			}
			if s, ok := out.(*autoscalingv1.Scale); ok && sub == "scale" {
				s.Spec.Replicas = 3 // set by someone else after our read
			}
			return nil
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				scaleWrites++
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	res := reconcileOnce(t, r, req)
	d := lastDecision(t, path)
	if d.Action != "guard_abort" || d.AppliedReplicas != 1 || scaleWrites != 0 {
		t.Fatalf("a stale decision must not be written: %+v (writes %d)", d, scaleWrites)
	}
	if res.RequeueAfter != staleDecisionRetry {
		t.Fatalf("requeue after %v, want %v", res.RequeueAfter, staleDecisionRetry)
	}
	if _, throttled := r.lastReconcileMap[req.NamespacedName.String()]; throttled {
		t.Fatal("the retry must not be throttled to the reconcile interval")
	}
	if c := condition(t, r, req.NamespacedName, "ScalingActive"); c.Reason != "GuardAborted" || !strings.Contains(c.Message, "decided from 1, now 3") {
		t.Fatalf("ScalingActive: %+v", c)
	}
}

func TestAWriteConflictIsNotRetriedWithTheOldDecision(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	attempts := 0
	wrapClient(r, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				attempts++
				return apierrors.NewConflict(schema.GroupResource{Group: "apps", Resource: "deployments"}, obj.GetName(), nil)
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	res := reconcileOnce(t, r, req)
	if d := lastDecision(t, path); d.Action != "guard_abort" || attempts != 1 {
		t.Fatalf("a conflict must abort after one attempt: %+v (attempts %d)", d, attempts)
	}
	if res.RequeueAfter != staleDecisionRetry {
		t.Fatalf("requeue after %v", res.RequeueAfter)
	}
	if n := replicasOf(t, r, req.NamespacedName); n != 1 {
		t.Fatalf("replicas %d", n)
	}
}

// Other write errors stay scale_error (a real failure, not a stale decision).
func TestAForbiddenScaleWriteIsAScalingError(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	wrapClient(r, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments/scale"}, obj.GetName(), nil)
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	reconcileOnce(t, r, req)
	if d := lastDecision(t, path); d.Action != "scale_error" {
		t.Fatalf("decision: %+v", d)
	}
	if c := condition(t, r, req.NamespacedName, "Ready"); c.Reason != "ScalingError" {
		t.Fatalf("Ready: %+v", c)
	}
}

// freshScale accepts the Scale only for the same object at the decided count, with a resourceVersion (Codex r07).
func TestFreshScaleAcceptsOnlyTheDecidedObjectAndCount(t *testing.T) {
	r, req, _, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	stored := &appsv1.Deployment{}
	if err := r.Get(context.Background(), req.NamespacedName, stored); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		uid    string
		from   int32
		mutate func(*autoscalingv1.Scale)
		ok     bool
	}{
		{"same_object_same_count", string(stored.UID), 1, nil, true},
		{"replaced_object", "the-old-uid", 1, nil, false},
		{"decided_without_uid", "", 1, nil, false},
		{"count_changed", string(stored.UID), 2, nil, false},
		{"scale_without_uid", string(stored.UID), 1, func(s *autoscalingv1.Scale) { s.UID = "" }, false},
		{"scale_without_resource_version", string(stored.UID), 1, func(s *autoscalingv1.Scale) { s.ResourceVersion = "" }, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := &PredictiveAutoscalerReconciler{Client: interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
				SubResourceGet: func(ctx context.Context, c client.Client, sub string, obj client.Object, out client.Object, opts ...client.SubResourceGetOption) error {
					if err := c.SubResource(sub).Get(ctx, obj, out, opts...); err != nil {
						return err
					}
					if s, ok := out.(*autoscalingv1.Scale); ok && tc.mutate != nil {
						tc.mutate(s)
					}
					return nil
				},
			})}
			dep := stored.DeepCopy()
			dep.UID = types.UID(tc.uid)
			_, err := rr.freshScale(context.Background(), dep, tc.from)
			if tc.ok != (err == nil) || (err != nil && !stderrors.Is(err, errStaleDecision)) {
				t.Fatalf("err = %v, want ok=%v", err, tc.ok)
			}
		})
	}
}

// A stalled /scale read or update ends at scaleWriteTimeout: nothing is written and the reconcile returns (Codex r08).
func TestAStalledScaleRequestEndsAtTheDeadline(t *testing.T) {
	old := scaleWriteTimeout
	scaleWriteTimeout = 50 * time.Millisecond
	t.Cleanup(func() { scaleWriteTimeout = old })
	for _, stall := range []string{"get", "update"} {
		t.Run(stall, func(t *testing.T) {
			r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
			wrapClient(r, interceptor.Funcs{
				SubResourceGet: func(ctx context.Context, c client.Client, sub string, obj client.Object, out client.Object, opts ...client.SubResourceGetOption) error {
					if sub == "scale" && stall == "get" {
						<-ctx.Done()
						return ctx.Err()
					}
					return c.SubResource(sub).Get(ctx, obj, out, opts...)
				},
				SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if sub == "scale" && stall == "update" {
						<-ctx.Done()
						return ctx.Err()
					}
					return c.SubResource(sub).Update(ctx, obj, opts...)
				},
			})
			start := time.Now()
			reconcileOnce(t, r, req)
			if el := time.Since(start); el > 5*time.Second {
				t.Fatalf("the reconcile took %v", el)
			}
			if d := lastDecision(t, path); d.Action != "scale_error" || replicasOf(t, r, req.NamespacedName) != 1 {
				t.Fatalf("a stalled %s must end without a write: %+v", stall, d)
			}
		})
	}
}

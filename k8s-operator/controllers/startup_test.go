package controllers

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

var startupNow = time.Date(2026, 11, 6, 12, 0, 0, 0, time.UTC)

type podSpec struct {
	name, hash    string
	createdAgo    time.Duration
	readyAfter    time.Duration // 0: not Ready
	restarts      int32
	terminating   bool
	unschedulable bool
}

func startupPod(p podSpec) *corev1.Pod {
	created := startupNow.Add(-p.createdAgo)
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: p.name, Namespace: "shop", UID: types.UID("uid-" + p.name),
		Labels: map[string]string{"app": "web", podTemplateHashLabel: p.hash}, CreationTimestamp: metav1.NewTime(created)},
		Status: corev1.PodStatus{ContainerStatuses: []corev1.ContainerStatus{{Name: "web", RestartCount: p.restarts}}}}
	if p.terminating {
		now := metav1.NewTime(startupNow)
		pod.DeletionTimestamp = &now
		pod.Finalizers = []string{"test/keep"} // the fake client refuses a deletionTimestamp without a finalizer
	}
	if p.readyAfter > 0 {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady,
			Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(created.Add(p.readyAfter))})
	} else {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodReady, Status: corev1.ConditionFalse})
	}
	if p.unschedulable {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodScheduled,
			Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "0/3 nodes are available: insufficient cpu"})
	}
	return pod
}

func startupDeployment() *appsv1.Deployment {
	one := int32(1)
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", UID: "dep-uid"},
		Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "nginx:1"}}}}}}
}

func startupReconciler(t *testing.T, pods ...*corev1.Pod) *PredictiveAutoscalerReconciler {
	t.Helper()
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	objs := make([]client.Object, 0, len(pods))
	for _, p := range pods {
		objs = append(objs, p)
	}
	other := startupPod(podSpec{name: "other-app", hash: "h1", createdAgo: time.Hour, readyAfter: 999 * time.Second})
	other.Labels["app"] = "other"
	objs = append(objs, other)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &PredictiveAutoscalerReconciler{Client: c, Scheme: s}
}

func TestStartupSamplesCountOnlyCompletedStartsOfTheCurrentTemplate(t *testing.T) {
	var pods []*corev1.Pod
	for i := 0; i < 20; i++ { // ready after 10..29 s
		pods = append(pods, startupPod(podSpec{name: fmt.Sprintf("p%02d", i), hash: "h2", createdAgo: time.Hour - time.Duration(i)*time.Second,
			readyAfter: time.Duration(10+i) * time.Second}))
	}
	pods = append(pods,
		startupPod(podSpec{name: "old-template", hash: "h1", createdAgo: 2 * time.Hour, readyAfter: 900 * time.Second}),
		startupPod(podSpec{name: "restarted", hash: "h2", createdAgo: time.Hour, readyAfter: 800 * time.Second, restarts: 1}),
		startupPod(podSpec{name: "terminating", hash: "h2", createdAgo: time.Hour, readyAfter: 700 * time.Second, terminating: true}),
		startupPod(podSpec{name: "eight-days-old", hash: "h2", createdAgo: 8 * 24 * time.Hour, readyAfter: 600 * time.Second}))
	r := startupReconciler(t, pods...)
	obs := r.observeStartup(context.Background(), "shop/web-pa", startupDeployment(), startupNow)
	if !obs.Observable || obs.SampleCount != 20 || obs.P90 == nil || *obs.P90 != 27 || obs.Pending != nil || obs.Blocked != "" {
		t.Fatalf("observation: %+v (p90 %v)", obs, obs.P90)
	}
	d, method, ok := startupBudget(obs, startupConfig{Allowance: 15})
	if !ok || method != "measured" || d != 34 { // ceil(27 x 1.25)
		t.Fatalf("budget %d %s %v", d, method, ok)
	}
	// The samples outlive their pods (until 7 days), and a pod is sampled once.
	r2 := startupReconciler(t)
	r2.startupTrackers = r.startupTrackers
	if obs := r2.observeStartup(context.Background(), "shop/web-pa", startupDeployment(), startupNow.Add(time.Minute)); obs.SampleCount != 20 {
		t.Fatalf("samples after the pods left: %d", obs.SampleCount)
	}
	if obs := r2.observeStartup(context.Background(), "shop/web-pa", startupDeployment(), startupNow.Add(7*24*time.Hour)); obs.SampleCount != 0 {
		t.Fatalf("samples older than 7 days: %d", obs.SampleCount)
	}
}

func TestStartupBudgetFallsBackAndNeverGuesses(t *testing.T) {
	r := startupReconciler(t, startupPod(podSpec{name: "a", hash: "h", createdAgo: time.Hour, readyAfter: 20 * time.Second}))
	obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	if obs.SampleCount != 1 || obs.P90 != nil {
		t.Fatalf("one sample is not a measurement: %+v", obs)
	}
	if _, method, ok := startupBudget(obs, startupConfig{Allowance: 15}); ok || method != "none" {
		t.Fatal("without enough samples and without a fallback there is no budget")
	}
	fb := int64(90)
	if d, method, ok := startupBudget(obs, startupConfig{Fallback: &fb, Minimum: 120}); !ok || method != "configured" || d != 120 {
		t.Fatalf("configured fallback raised to the minimum: %d %s", d, method)
	}
}

// A start still pending is censored: older than the budget, it becomes the budget, so the lead never looks shorter.
func TestStartupPendingScaleOutRaisesTheBudget(t *testing.T) {
	pods := []*corev1.Pod{startupPod(podSpec{name: "slow", hash: "h", createdAgo: 400 * time.Second})}
	r := startupReconciler(t, pods...)
	obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	fb := int64(120)
	if obs.Pending == nil || *obs.Pending != 400 {
		t.Fatalf("pending: %+v", obs)
	}
	if d, _, _ := startupBudget(obs, startupConfig{Fallback: &fb}); d != 400 {
		t.Fatalf("a pending start older than the budget must raise it: %d", d)
	}
	young := startupReconciler(t, startupPod(podSpec{name: "young", hash: "h", createdAgo: 30 * time.Second}))
	obs = young.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	if d, _, _ := startupBudget(obs, startupConfig{Fallback: &fb}); d != 120 {
		t.Fatalf("a younger pending start leaves the budget: %d", d)
	}
}

func TestStartupSurfacesBlockedScaleOuts(t *testing.T) {
	r := startupReconciler(t, startupPod(podSpec{name: "stuck", hash: "h", createdAgo: time.Minute, unschedulable: true}))
	if obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.Blocked != "Unschedulable" ||
		!strings.Contains(obs.BlockedMsg, "insufficient cpu") {
		t.Fatalf("unschedulable: %+v", obs)
	}
	dep := startupDeployment()
	dep.Status.Conditions = []appsv1.DeploymentCondition{{Type: appsv1.DeploymentReplicaFailure, Status: corev1.ConditionTrue,
		Reason: "FailedCreate", Message: `pods "web-x" is forbidden: exceeded quota: compute, requested: cpu=1`}}
	if obs := startupReconciler(t).observeStartup(context.Background(), "k", dep, startupNow); obs.Blocked != "QuotaExceeded" {
		t.Fatalf("quota: %+v", obs)
	}
	dep.Status.Conditions[0].Message = "some other failure"
	if obs := startupReconciler(t).observeStartup(context.Background(), "k", dep, startupNow); obs.Blocked != "FailedCreate" {
		t.Fatalf("failed create: %+v", obs)
	}
}

func TestStartupTemplateChangeDiscardsTheSamples(t *testing.T) {
	var pods []*corev1.Pod
	for i := 0; i < 20; i++ {
		pods = append(pods, startupPod(podSpec{name: fmt.Sprintf("p%02d", i), hash: "h", createdAgo: time.Hour, readyAfter: 30 * time.Second}))
	}
	r := startupReconciler(t, pods...)
	dep := startupDeployment()
	if obs := r.observeStartup(context.Background(), "k", dep, startupNow); obs.P90 == nil {
		t.Fatal("precondition: measured")
	}
	dep.Spec.Template.Spec.Containers[0].Image = "nginx:2" // a rollout: per-replica startup may have changed
	obs := r.observeStartup(context.Background(), "k", dep, startupNow.Add(time.Minute))
	if obs.SampleCount != 0 || obs.P90 != nil {
		t.Fatalf("a template change must discard the samples: %+v", obs)
	}
}

func TestStartupUnreadablePodsAreReportedNotGuessed(t *testing.T) {
	r := startupReconciler(t)
	r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			return fmt.Errorf("pods is forbidden")
		},
	})
	obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	if obs.Observable || !strings.Contains(obs.ReadError, "forbidden") || obs.SampleCount != 0 {
		t.Fatalf("observation: %+v", obs)
	}
}

// ScaleOutBlocked through Reconcile: a Warning when a scale-out cannot complete, a Normal event when it clears.
func TestScaleOutBlockedConditionAndEvents(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	stuck := []corev1.Pod{*startupPod(podSpec{name: "stuck", hash: "h", createdAgo: time.Minute, unschedulable: true})}
	stuck[0].Namespace, stuck[0].Labels = req.Namespace, nil
	var serve []corev1.Pod
	wrapClient(r, interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		if pl, ok := list.(*corev1.PodList); ok {
			pl.Items = serve
			return nil
		}
		return c.List(ctx, list, opts...)
	}})
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	stuck[0].Labels = map[string]string{"app": "web", podTemplateHashLabel: "h"}
	serve = stuck
	runRPMReconcile(t, r, req, path)
	if c := condition(t, r, req.NamespacedName, "ScaleOutBlocked"); c.Status != "True" || c.Reason != "Unschedulable" {
		t.Fatalf("ScaleOutBlocked: %+v", c)
	}
	if ev := strings.Join(drain(rec), "\n"); !strings.Contains(ev, "Warning ScaleOutBlocked Unschedulable") {
		t.Fatalf("no warning: %s", ev)
	}
	serve = nil
	runRPMReconcile(t, r, req, path)
	if c := condition(t, r, req.NamespacedName, "ScaleOutBlocked"); c.Status != "False" || c.Reason != "None" {
		t.Fatalf("ScaleOutBlocked after: %+v", c)
	}
	if ev := strings.Join(drain(rec), "\n"); !strings.Contains(ev, "Normal ScaleOutUnblocked") {
		t.Fatalf("no recovery event: %s", ev)
	}
}

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
	name, hash    string // hash: the ReplicaSet ("h1" old, "h2" current, other values as given), "" = a foreign owner
	createdAgo    time.Duration
	readyAfter    time.Duration // 0: not Ready
	restarts      int32
	terminating   bool
	unschedulable bool
	// The kubelet's part of the status, as its status manager writes it: status.startTime and the first Ready=False
	// carry the time of its first report (startedAfter after creation; 0 = at creation), and Ready's lastTransitionTime
	// moves only when the status changes.
	startedAfter time.Duration
	lostReadyAgo time.Duration   // not 0: the pod was Ready and stopped being Ready this long before startupNow
	phase        corev1.PodPhase // "" = Running
	unreported   bool            // no kubelet has reported the pod: no startTime, no Ready condition
	bareNotReady bool            // Ready=False without a transition time or a startTime (not a kubelet's status)
}

func startupPod(p podSpec) *corev1.Pod {
	created := startupNow.Add(-p.createdAgo)
	owner := metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "web-" + p.hash, UID: types.UID("rs-" + p.hash), Controller: &yes}
	if p.hash == "" {
		owner = metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "impostor", UID: "rs-foreign", Controller: &yes}
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: p.name, Namespace: "shop", UID: types.UID("uid-" + p.name),
		Labels: map[string]string{"app": "web", podTemplateHashLabel: p.hash}, CreationTimestamp: metav1.NewTime(created),
		OwnerReferences: []metav1.OwnerReference{owner}},
		Status: corev1.PodStatus{Phase: p.phase, ContainerStatuses: []corev1.ContainerStatus{{Name: "web", RestartCount: p.restarts}}}}
	if p.terminating {
		now := metav1.NewTime(startupNow)
		pod.DeletionTimestamp = &now
		pod.Finalizers = []string{"test/keep"} // the fake client refuses a deletionTimestamp without a finalizer
	}
	started := metav1.NewTime(created.Add(p.startedAfter))
	cond := func(t corev1.PodConditionType, status corev1.ConditionStatus, at metav1.Time) {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: t, Status: status, LastTransitionTime: at})
	}
	switch {
	case p.unreported || p.unschedulable:
	case p.bareNotReady:
		cond(corev1.PodReady, corev1.ConditionFalse, metav1.Time{})
	case p.readyAfter > 0 && p.lostReadyAgo != 0:
		lost := metav1.NewTime(startupNow.Add(-p.lostReadyAgo))
		pod.Status.StartTime = &started
		cond(corev1.ContainersReady, corev1.ConditionFalse, lost)
		cond(corev1.PodReady, corev1.ConditionFalse, lost)
	case p.readyAfter > 0:
		ready := metav1.NewTime(created.Add(p.readyAfter))
		pod.Status.StartTime = &started
		cond(corev1.ContainersReady, corev1.ConditionTrue, ready)
		cond(corev1.PodReady, corev1.ConditionTrue, ready)
	default:
		pod.Status.StartTime = &started
		cond(corev1.ContainersReady, corev1.ConditionFalse, started)
		cond(corev1.PodReady, corev1.ConditionFalse, started)
	}
	if p.unschedulable {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: corev1.PodScheduled,
			Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, Message: "0/3 nodes are available: insufficient cpu"})
	}
	return pod
}

var yes = true

func startupDeployment() *appsv1.Deployment {
	one := int32(1)
	return &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop", UID: "dep-uid",
		Annotations: map[string]string{revisionAnnotation: "2"}},
		Spec: appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "web"}},
				Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "nginx:1"}}}}}}
}

// startupReplicaSet is the Deployment's ReplicaSet "web-<hash>" at a revision, with the given template and owner.
func startupReplicaSet(hash, revision string, template corev1.PodTemplateSpec, owner types.UID) *appsv1.ReplicaSet {
	t := *template.DeepCopy()
	t.Labels = map[string]string{"app": "web", podTemplateHashLabel: hash}
	return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: "web-" + hash, Namespace: "shop", UID: types.UID("rs-" + hash),
		Labels: map[string]string{"app": "web", podTemplateHashLabel: hash}, Annotations: map[string]string{revisionAnnotation: revision},
		OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "Deployment", Name: "web", UID: owner, Controller: &yes}}},
		Spec: appsv1.ReplicaSetSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}, Template: t}}
}

// startupReconciler: the Deployment's current ReplicaSet web-h2 (revision 2) and an old one web-h1 (revision 1),
// plus the given pods and a pod of another app.
func startupReconciler(t *testing.T, pods ...*corev1.Pod) *PredictiveAutoscalerReconciler {
	t.Helper()
	dep := startupDeployment()
	old := dep.Spec.Template.DeepCopy()
	old.Spec.Containers[0].Image = "nginx:0"
	return startupReconcilerWith(t, []client.Object{startupReplicaSet("h2", "2", dep.Spec.Template, dep.UID),
		startupReplicaSet("h1", "1", *old, dep.UID)}, pods...)
}

func startupReconcilerWith(t *testing.T, sets []client.Object, pods ...*corev1.Pod) *PredictiveAutoscalerReconciler {
	t.Helper()
	s := runtime.NewScheme()
	_ = corev1.AddToScheme(s)
	_ = appsv1.AddToScheme(s)
	objs := append([]client.Object{}, sets...)
	for _, p := range pods {
		objs = append(objs, p)
	}
	other := startupPod(podSpec{name: "other-app", hash: "h1", createdAgo: time.Hour, readyAfter: 999 * time.Second})
	other.Labels["app"] = "other"
	objs = append(objs, other)
	c := fake.NewClientBuilder().WithScheme(s).WithObjects(objs...).Build()
	return &PredictiveAutoscalerReconciler{Client: c, Scheme: s}
}

// watchedStarts observes the pods twice with one tracker, the way the operator sees a start it may sample: first
// while each pod has not been Ready yet (its kubelet has reported it), then as given. It returns the second reconciler.
func watchedStarts(t *testing.T, key string, pods ...*corev1.Pod) *PredictiveAutoscalerReconciler {
	t.Helper()
	var starting []*corev1.Pod
	for _, p := range pods {
		q := p.DeepCopy()
		started := metav1.NewTime(q.CreationTimestamp.Time)
		q.Status.StartTime = &started
		q.Status.Conditions = []corev1.PodCondition{{Type: corev1.ContainersReady, Status: corev1.ConditionFalse, LastTransitionTime: started},
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: started}}
		starting = append(starting, q)
	}
	first := startupReconciler(t, starting...)
	if obs := first.observeStartup(context.Background(), key, startupDeployment(), startupNow.Add(-time.Minute)); !obs.Observable {
		t.Fatalf("precondition: %+v", obs)
	}
	second := startupReconciler(t, pods...)
	second.startupTrackers = first.startupTrackers
	return second
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
	r := watchedStarts(t, "shop/web-pa", pods...)
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
	r2.startupTrackers = r.startupTrackers // the same ReplicaSet: its samples stay
	if obs := r2.observeStartup(context.Background(), "shop/web-pa", startupDeployment(), startupNow.Add(time.Minute)); obs.SampleCount != 20 {
		t.Fatalf("samples after the pods left: %d", obs.SampleCount)
	}
	if obs := r2.observeStartup(context.Background(), "shop/web-pa", startupDeployment(), startupNow.Add(7*24*time.Hour)); obs.SampleCount != 0 {
		t.Fatalf("samples older than 7 days: %d", obs.SampleCount)
	}
}

func TestStartupBudgetFallsBackAndNeverGuesses(t *testing.T) {
	r := watchedStarts(t, "k", startupPod(podSpec{name: "a", hash: "h2", createdAgo: time.Hour, readyAfter: 20 * time.Second}))
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
	pods := []*corev1.Pod{startupPod(podSpec{name: "slow", hash: "h2", createdAgo: 400 * time.Second})}
	r := startupReconciler(t, pods...)
	obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	fb := int64(120)
	if obs.Pending == nil || *obs.Pending != 400 {
		t.Fatalf("pending: %+v", obs)
	}
	if d, _, _ := startupBudget(obs, startupConfig{Fallback: &fb}); d != 400 {
		t.Fatalf("a pending start older than the budget must raise it: %d", d)
	}
	young := startupReconciler(t, startupPod(podSpec{name: "young", hash: "h2", createdAgo: 30 * time.Second}))
	obs = young.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	if d, _, _ := startupBudget(obs, startupConfig{Fallback: &fb}); d != 120 {
		t.Fatalf("a younger pending start leaves the budget: %d", d)
	}
}

// A pod that is not Ready is a pending start only when it has never been Ready (R3-03). Terminal and terminating pods
// are no starts at all; a pod that served and lost readiness is a readiness loss. Each case is checked twice: with the
// tracker that saw the pod earlier, and after an operator restart (an empty tracker), where only the pod object speaks.
func TestStartupPendingCountsOnlyPodsThatWereNeverReady(t *testing.T) {
	fb := int64(120)
	cfg := startupConfig{Fallback: &fb, Allowance: 15}
	lead := func(obs startupObservation) (int64, int64) {
		d, _, _ := startupBudget(obs, cfg)
		return d, v2AutomaticLead(d, v2LeadDecisionInterval(300*time.Second, 300), cfg.Allowance, 300)
	}
	served := podSpec{name: "web-a", hash: "h2", createdAgo: 25 * time.Minute, startedAfter: 2 * time.Second, readyAfter: 20 * time.Second}
	with := func(base podSpec, change func(*podSpec)) podSpec { change(&base); return base }
	for _, tc := range []struct {
		name     string
		before   *podSpec // what the same tracker saw one reconcile earlier (nil: the pod is new to it)
		pod      podSpec
		pending  int64 // 0: not a pending start
		evidence string
	}{
		{name: "a long-Ready pod fails one readiness probe", before: &served,
			pod: with(served, func(p *podSpec) { p.lostReadyAgo = 10 * time.Second })},
		{name: "a pod that served and whose container then restarted", before: &served,
			pod: with(served, func(p *podSpec) { p.lostReadyAgo = 30 * time.Second; p.restarts = 3 })},
		{name: "an evicted pod still listed", before: &served,
			pod: with(served, func(p *podSpec) { p.lostReadyAgo = 5 * time.Minute; p.phase = corev1.PodFailed })},
		{name: "a pod that failed before it was ever Ready",
			pod: podSpec{name: "web-f", hash: "h2", createdAgo: 20 * time.Minute, phase: corev1.PodFailed}},
		{name: "a completed pod",
			pod: podSpec{name: "web-s", hash: "h2", createdAgo: 20 * time.Minute, phase: corev1.PodSucceeded}},
		{name: "a terminating pod that was never Ready",
			pod: podSpec{name: "web-t", hash: "h2", createdAgo: 20 * time.Minute, terminating: true}},
		{name: "a new pod that has never been Ready",
			pod: podSpec{name: "web-n", hash: "h2", createdAgo: 400 * time.Second, startedAfter: 3 * time.Second}, pending: 400, evidence: "neverReady"},
		{name: "a crash-looping pod that has never been Ready", // a restart does not prove that the pod served
			pod: podSpec{name: "web-c", hash: "h2", createdAgo: 400 * time.Second, restarts: 5}, pending: 400, evidence: "neverReady"},
		{name: "a pod that waited for a node and has not been Ready since", // judged from the first report, not from creation
			pod: podSpec{name: "web-l", hash: "h2", createdAgo: 20 * time.Minute, startedAfter: 19 * time.Minute}, pending: 1200, evidence: "neverReady"},
		{name: "a pod no kubelet has reported",
			pod: podSpec{name: "web-u", hash: "h2", createdAgo: 400 * time.Second, unreported: true}, pending: 400, evidence: "neverReady"},
		{name: "a not-Ready pod whose status does not say since when",
			pod: podSpec{name: "web-x", hash: "h2", createdAgo: 400 * time.Second, bareNotReady: true}, pending: 400, evidence: "uncertain"},
	} {
		for _, restart := range []bool{false, true} {
			name := tc.name
			if restart {
				name += " (after an operator restart)"
			}
			t.Run(name, func(t *testing.T) {
				trackers := map[string]*startupTracker{}
				if tc.before != nil && !restart {
					r := startupReconciler(t, startupPod(*tc.before))
					r.startupTrackers = trackers
					if obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow.Add(-time.Minute)); obs.Pending != nil {
						t.Fatalf("precondition: %+v", obs)
					}
				}
				r := startupReconciler(t, startupPod(tc.pod))
				r.startupTrackers = trackers
				obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
				d, l := lead(obs)
				if tc.pending == 0 {
					if !obs.Observable || obs.Pending != nil || obs.PendingEvidence != "" || d != 120 || l != 900 {
						t.Fatalf("not a pending start, budget 120 s and lead 900 s: %+v (pending %v) budget %d lead %d", obs, obs.Pending, d, l)
					}
					return
				}
				if obs.Pending == nil || *obs.Pending != tc.pending || obs.PendingEvidence != tc.evidence || d != tc.pending {
					t.Fatalf("a pending start of %d s (%s) that raises the budget: %+v (pending %v) budget %d", tc.pending,
						tc.evidence, obs, obs.Pending, d)
				}
			})
		}
	}
}

// What the pod object cannot prove is decided by what the operator itself saw, and otherwise the safe way: a pending
// start, marked uncertain. A status that contradicts itself proves nothing.
func TestStartupUncertainHistoryIsAPendingStartUnlessTheOperatorSawThePodReady(t *testing.T) {
	served := startupPod(podSpec{name: "web-a", hash: "h2", createdAgo: 25 * time.Minute, readyAfter: 20 * time.Second})
	bare := startupPod(podSpec{name: "web-a", hash: "h2", createdAgo: 25 * time.Minute, bareNotReady: true})
	r := startupReconciler(t, served)
	if obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow.Add(-time.Minute)); obs.Pending != nil {
		t.Fatalf("precondition: %+v", obs)
	}
	seen := startupReconciler(t, bare)
	seen.startupTrackers = r.startupTrackers
	if obs := seen.observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.Pending != nil {
		t.Fatalf("the operator saw this pod Ready: it is a readiness loss, not a start: pending %d", *obs.Pending)
	}
	// It stays known while the pod is listed, also over several reconciles.
	if obs := seen.observeStartup(context.Background(), "k", startupDeployment(), startupNow.Add(time.Minute)); obs.Pending != nil {
		t.Fatalf("still the same pod: pending %d", *obs.Pending)
	}
	restarted := startupReconciler(t, bare)
	if obs := restarted.observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.Pending == nil ||
		*obs.Pending != 1500 || obs.PendingEvidence != "uncertain" {
		t.Fatalf("without proof either way the pod is a pending start, marked uncertain: %+v", obs)
	}
	// Ready=False "since 10 s ago", while ContainersReady has been False since the first report: Ready cannot have
	// been True, so the transition time is not a kubelet's and proves nothing.
	odd := startupPod(podSpec{name: "web-o", hash: "h2", createdAgo: 25 * time.Minute, readyAfter: 20 * time.Second, lostReadyAgo: 10 * time.Second})
	for i := range odd.Status.Conditions {
		if odd.Status.Conditions[i].Type == corev1.ContainersReady {
			odd.Status.Conditions[i].LastTransitionTime = *odd.Status.StartTime
		}
	}
	if obs := startupReconciler(t, odd).observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.Pending == nil ||
		obs.PendingEvidence != "uncertain" {
		t.Fatalf("a contradictory status is uncertain: %+v", obs)
	}
	// The oldest pending start decides the budget, and its evidence is the one reported.
	both := startupReconciler(t, startupPod(podSpec{name: "web-x", hash: "h2", createdAgo: 300 * time.Second, bareNotReady: true}),
		startupPod(podSpec{name: "web-n", hash: "h2", createdAgo: 400 * time.Second}))
	if obs := both.observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.Pending == nil ||
		*obs.Pending != 400 || obs.PendingEvidence != "neverReady" {
		t.Fatalf("the oldest pending start: %+v", obs)
	}
	st := startupObservation{Observable: true, Pending: func() *int64 { v := int64(1500); return &v }(), PendingEvidence: "uncertain"}.status(1500, "configured", true)
	if st.PendingSeconds == nil || *st.PendingSeconds != 1500 || st.PendingEvidence != "uncertain" {
		t.Fatalf("status: %+v", st)
	}
}

// A completed start is sampled only when this process watched it: it saw the pod while it had not been Ready, and
// then Ready. A pod's Ready condition keeps only its latest transition, so a pod that is already Ready when it is
// first seen (an operator restart, or a start between two reconciles) says nothing about its first readiness, and a
// pod known to have lost readiness comes back as a recovery, not as a start (the reviewer's two repros on PR #60).
func TestStartupSamplesOnlyStartsItWatchedNeverARecovery(t *testing.T) {
	cfg := func() startupConfig { fb := int64(120); return startupConfig{Fallback: &fb, Allowance: 15} }()
	// Repro 1: after an operator restart a 20-minute-old pod is not Ready (a proven readiness loss); it recovers.
	lost := podSpec{name: "web-a", hash: "h2", createdAgo: 20 * time.Minute, readyAfter: 20 * time.Second, lostReadyAgo: time.Minute}
	r := startupReconciler(t, startupPod(lost))
	if obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.Pending != nil || obs.SampleCount != 0 {
		t.Fatalf("a readiness loss: %+v", obs)
	}
	recovered := startupPod(podSpec{name: "web-a", hash: "h2", createdAgo: 20 * time.Minute, readyAfter: 20 * time.Minute}) // Ready again now
	r2 := startupReconciler(t, recovered)
	r2.startupTrackers = r.startupTrackers
	obs := r2.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	if d, method, _ := startupBudget(obs, cfg); obs.SampleCount != 0 || obs.Pending != nil || d != 120 || method != "configured" {
		t.Fatalf("a recovery must not be sampled as a 1200 s start: %+v budget %d %s", obs, d, method)
	}
	// Repro 2: the operator starts after the recovery. 20 pods first became Ready after 20 s, lost readiness and
	// recovered a minute ago: their status holds only that last transition, and no container restarted.
	var pods []*corev1.Pod
	for i := 0; i < 20; i++ {
		p := startupPod(podSpec{name: fmt.Sprintf("web-%d", i), hash: "h2", createdAgo: 20 * time.Minute, readyAfter: 20 * time.Second})
		for j := range p.Status.Conditions {
			p.Status.Conditions[j].LastTransitionTime = metav1.NewTime(startupNow.Add(-time.Minute))
		}
		pods = append(pods, p)
	}
	fresh := startupReconciler(t, pods...)
	for i := 0; i < 2; i++ { // also on the following reconciles: still the same already-Ready pods
		obs = fresh.observeStartup(context.Background(), "k", startupDeployment(), startupNow.Add(time.Duration(i)*time.Minute))
		if d, method, _ := startupBudget(obs, cfg); obs.SampleCount != 0 || obs.P90 != nil || d != 120 || method != "configured" {
			t.Fatalf("already-Ready pods give no sample (the reviewer measured P90 1140 s): %+v budget %d %s", obs, d, method)
		}
	}
	// The same holds for pods that really started 20 s after creation and were never seen starting: no evidence, no
	// sample, the configured budget.
	var unseen []*corev1.Pod
	for i := 0; i < 20; i++ {
		unseen = append(unseen, startupPod(podSpec{name: fmt.Sprintf("new-%d", i), hash: "h2", createdAgo: time.Minute, readyAfter: 20 * time.Second}))
	}
	if obs := startupReconciler(t, unseen...).observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.SampleCount != 0 {
		t.Fatalf("a pod first seen Ready is not a watched start: %+v", obs)
	}
	// A pod whose history was uncertain while it was not Ready is no watched start either when it turns Ready.
	bare := startupReconciler(t, startupPod(podSpec{name: "web-x", hash: "h2", createdAgo: 400 * time.Second, bareNotReady: true}))
	if obs := bare.observeStartup(context.Background(), "k", startupDeployment(), startupNow.Add(-time.Minute)); obs.PendingEvidence != "uncertain" {
		t.Fatalf("precondition: %+v", obs)
	}
	later := startupReconciler(t, startupPod(podSpec{name: "web-x", hash: "h2", createdAgo: 400 * time.Second, readyAfter: 390 * time.Second}))
	later.startupTrackers = bare.startupTrackers
	if obs := later.observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.SampleCount != 0 {
		t.Fatalf("an uncertain pod turning Ready is not sampled: %+v", obs)
	}
	// What it did watch is sampled once, at the pod's Ready transition, and stays when the pod flaps afterwards.
	slow := podSpec{name: "web-s", hash: "h2", createdAgo: 10 * time.Minute, readyAfter: 300 * time.Second}
	w := watchedStarts(t, "k", startupPod(slow))
	if obs := w.observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.SampleCount != 1 ||
		w.startupTrackers["k"].samples["uid-web-s"].seconds != 300 {
		t.Fatalf("a watched start: %+v %+v", obs, w.startupTrackers["k"].samples)
	}
	slow.lostReadyAgo = 2 * time.Minute
	flap := startupReconciler(t, startupPod(slow))
	flap.startupTrackers = w.startupTrackers
	flap.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	again := startupReconciler(t, startupPod(podSpec{name: "web-s", hash: "h2", createdAgo: 10 * time.Minute, readyAfter: 590 * time.Second}))
	again.startupTrackers = w.startupTrackers
	if obs := again.observeStartup(context.Background(), "k", startupDeployment(), startupNow); obs.SampleCount != 1 ||
		again.startupTrackers["k"].samples["uid-web-s"].seconds != 300 {
		t.Fatalf("the sample of a watched start must survive a later flap unchanged: %+v", again.startupTrackers["k"].samples)
	}
}

func TestStartupSurfacesBlockedScaleOuts(t *testing.T) {
	r := startupReconciler(t, startupPod(podSpec{name: "stuck", hash: "h2", createdAgo: time.Minute, unschedulable: true}))
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
		pods = append(pods, startupPod(podSpec{name: fmt.Sprintf("p%02d", i), hash: "h2", createdAgo: time.Hour, readyAfter: 30 * time.Second}))
	}
	r := watchedStarts(t, "k", pods...)
	dep := startupDeployment()
	if obs := r.observeStartup(context.Background(), "k", dep, startupNow); obs.P90 == nil {
		t.Fatal("precondition: measured")
	}
	// A rollout: the Deployment's new template, revision and ReplicaSet (per-replica startup may have changed).
	dep.Spec.Template.Spec.Containers[0].Image = "nginx:3"
	dep.Annotations[revisionAnnotation] = "3"
	if err := r.Client.Create(context.Background(), startupReplicaSet("h3", "3", dep.Spec.Template, dep.UID)); err != nil {
		t.Fatal(err)
	}
	obs := r.observeStartup(context.Background(), "k", dep, startupNow.Add(time.Minute))
	if !obs.Observable || obs.SampleCount != 0 || obs.P90 != nil {
		t.Fatalf("a new ReplicaSet must discard the samples: %+v", obs)
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

// Pods that share the selector but are not the current ReplicaSet's (another owner, the old ReplicaSet) are neither
// samples nor blockers, and they cannot hide the target's own unschedulable pod (Codex #26 repro: 20 newer foreign pods
// gave p90 = 1 s).
func TestStartupCountsOnlyTheCurrentReplicaSetsPods(t *testing.T) {
	var pods []*corev1.Pod
	for i := 0; i < 20; i++ {
		pods = append(pods, startupPod(podSpec{name: fmt.Sprintf("foreign%02d", i), hash: "", createdAgo: time.Minute, readyAfter: time.Second}))
		pods = append(pods, startupPod(podSpec{name: fmt.Sprintf("old%02d", i), hash: "h1", createdAgo: time.Minute, readyAfter: time.Second}))
	}
	pods = append(pods, startupPod(podSpec{name: "foreign-stuck", hash: "", createdAgo: time.Hour, unschedulable: true}),
		startupPod(podSpec{name: "target-stuck", hash: "h2", createdAgo: 2 * time.Hour, unschedulable: true}))
	r := startupReconciler(t, pods...)
	obs := r.observeStartup(context.Background(), "k", startupDeployment(), startupNow)
	if !obs.Observable || obs.SampleCount != 0 || obs.P90 != nil || obs.Blocked != "Unschedulable" ||
		!strings.Contains(obs.BlockedMsg, "target-stuck") || obs.Pending == nil || *obs.Pending != 7200 {
		t.Fatalf("observation: %+v", obs)
	}
}

// The current ReplicaSet must be certain: owned by the Deployment, at its revision, with its template, exactly one.
func TestStartupWithoutACertainReplicaSetAttributesNothing(t *testing.T) {
	dep := startupDeployment()
	other := dep.Spec.Template.DeepCopy()
	other.Spec.Containers[0].Image = "nginx:9"
	pods := []*corev1.Pod{startupPod(podSpec{name: "stuck", hash: "h2", createdAgo: time.Hour, unschedulable: true})}
	for i := 0; i < 20; i++ {
		pods = append(pods, startupPod(podSpec{name: fmt.Sprintf("p%02d", i), hash: "h2", createdAgo: time.Hour, readyAfter: time.Second}))
	}
	for name, sets := range map[string][]client.Object{
		"none":           nil,
		"foreign owner":  {startupReplicaSet("h2", "2", dep.Spec.Template, "someone-else")},
		"other revision": {startupReplicaSet("h2", "1", dep.Spec.Template, dep.UID)},
		"other template": {startupReplicaSet("h2", "2", *other, dep.UID)},
		"two at the revision": {startupReplicaSet("h2", "2", dep.Spec.Template, dep.UID),
			startupReplicaSet("h9", "2", dep.Spec.Template, dep.UID)},
	} {
		r := startupReconcilerWith(t, sets, pods...)
		obs := r.observeStartup(context.Background(), "k", dep, startupNow)
		if obs.Observable || !obs.Unresolved || obs.SampleCount != 0 || obs.P90 != nil || obs.Pending != nil || obs.Blocked != "" {
			t.Errorf("%s: %+v", name, obs)
		}
		fb := int64(300)
		if d, method, ok := startupBudget(obs, startupConfig{Fallback: &fb}); !ok || method != "configured" || d != 300 {
			t.Errorf("%s: the configured budget must apply: %d %s", name, d, method)
		}
	}
}

// ScaleOutBlocked through Reconcile: a Warning when a scale-out cannot complete, a Normal event when it clears.
func TestScaleOutBlockedConditionAndEvents(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	var dep appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &dep); err != nil {
		t.Fatal(err)
	}
	dep.Spec.Selector = &metav1.LabelSelector{MatchLabels: map[string]string{"app": "web"}}
	if err := r.Update(context.Background(), &dep); err != nil {
		t.Fatal(err)
	}
	rs := startupReplicaSet("h2", "", dep.Spec.Template, dep.UID) // the harness Deployment records no revision
	rs.Namespace, rs.Spec.Template = req.Namespace, *dep.Spec.Template.DeepCopy()
	rs.Spec.Template.Labels = map[string]string{podTemplateHashLabel: "h2"}
	stuck := []corev1.Pod{*startupPod(podSpec{name: "stuck", hash: "h2", createdAgo: time.Minute, unschedulable: true})}
	stuck[0].Namespace = req.Namespace
	var serve []corev1.Pod
	wrapClient(r, interceptor.Funcs{List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
		switch l := list.(type) {
		case *corev1.PodList:
			l.Items = serve
			return nil
		case *appsv1.ReplicaSetList:
			l.Items = []appsv1.ReplicaSet{*rs}
			return nil
		}
		return c.List(ctx, list, opts...)
	}})
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

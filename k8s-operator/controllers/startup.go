package controllers

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Startup delay (plan item 4, minimum version; design C2 section 5): how long a scale-out takes to become usable, per
// target. The operator resolves the target's current ReplicaSet (owned by the Deployment, at the Deployment's revision,
// with the Deployment's pod template) and reads, uncached, only the pods that ReplicaSet controls. Once per pod it
// records pod creation to Ready for pods that became Ready without a restart; only Ready pods count as serving. This is
// not the whole startup: the time from the scale request to the pod's creation (the ReplicaSet controller's work) is not
// in it, and the actuation allowance covers it. With at least startupMinSamples such starts in the last 7 days the
// budget is their 90th percentile plus startupMarginPercent; otherwise the configured
// spec.behavior.startup.fallbackDelaySeconds, and without that there is no budget (and so no predictive lead). A pod
// that has never been Ready is a censored start: when it is already older than the budget, its age becomes the budget.
// A pod that was Ready and is not Ready now (a failed readiness probe, a crashed container), a terminal pod (Failed,
// Succeeded: an evicted pod stays listed until pod garbage collection) and a terminating pod are not starts and leave
// the budget alone (classifyNotReady). A new current ReplicaSet (a rollout, a rollback) discards the samples. When the
// current ReplicaSet cannot be identified with certainty, nothing is measured and nothing is attributed: the configured
// budget applies.
//
// The same observation surfaces scale-outs that cannot complete: pods of the current ReplicaSet the scheduler cannot
// place (Unschedulable) and pods the ReplicaSet cannot create (the Deployment's ReplicaFailure condition, for example an
// exceeded quota).

const (
	startupMinSamples    = 20
	startupWindow        = 7 * 24 * time.Hour
	startupMaxSamples    = 512
	startupPercentile    = 90
	startupMarginPercent = 25
	podTemplateHashLabel = "pod-template-hash"
	revisionAnnotation   = "deployment.kubernetes.io/revision"
)

// startupListTimeout bounds each read: a stalled API server must not stall the decision.
var startupListTimeout = 3 * time.Second

type startupSample struct {
	created time.Time
	seconds int64
}

// startupTracker holds one target's samples in memory (an operator restart starts again from the configured budget).
type startupTracker struct {
	targetUID types.UID
	rsUID     types.UID // the current ReplicaSet the samples belong to
	samples   map[types.UID]startupSample
	// seenReady holds the listed pods of the current ReplicaSet this process has seen Ready, restarted or not (samples
	// leave out restarted pods). It is proof that a pod served, never that another did not: it is empty after an
	// operator restart.
	seenReady map[types.UID]struct{}
}

// startupObservation is one reconcile's view of the target's startup and of blocked scale-outs.
type startupObservation struct {
	Observable  bool   // the current ReplicaSet was identified and its pods read
	ReadError   string // why not
	Unresolved  bool   // the reads worked, but the current ReplicaSet could not be identified with certainty
	SampleCount int
	P90         *int64 // seconds, with at least startupMinSamples samples
	Pending     *int64 // age of the oldest pod of the current ReplicaSet that has never been Ready, seconds
	// PendingEvidence says how that pod was classified: pendingNeverReady or pendingUncertain ("" without one).
	PendingEvidence string
	PendingPod      string // its name (for the log)
	Blocked         string // Unschedulable, QuotaExceeded, FailedCreate or ""
	BlockedMsg      string
}

func podCondition(p *corev1.Pod, t corev1.PodConditionType) *corev1.PodCondition {
	for i := range p.Status.Conditions {
		if p.Status.Conditions[i].Type == t {
			return &p.Status.Conditions[i]
		}
	}
	return nil
}

func podRestarts(p *corev1.Pod) int32 {
	n := int32(0)
	for _, c := range p.Status.ContainerStatuses {
		n += c.RestartCount
	}
	return n
}

// How a pod of the current ReplicaSet that is not Ready now is classified (classifyNotReady).
const (
	// pendingNeverReady and pendingUncertain are the values of status.prediction.startup.pendingEvidence.
	pendingNeverReady = "neverReady"
	pendingUncertain  = "uncertain"
	// readyFirstReportSlack is how far Ready's lastTransitionTime may be from status.startTime and still be the kubelet's
	// first report. The kubelet stamps both in one call, Ready first, and the API stores whole seconds, so they differ by
	// at most one second, either way. A pod that became Ready and lost readiness again inside the slack is taken for one
	// that never was Ready: a pending start, the safe side.
	readyFirstReportSlack = 2 * time.Second
)

type notReadyClass int

const (
	notReadyNeverReady notReadyClass = iota // proven: a start still pending (censored)
	notReadyWasReady                        // proven: a readiness loss after serving, not a start
	notReadyUncertain                       // not provable either way: counted as a pending start, marked uncertain
)

// classifyNotReady decides whether a pod that is not Ready now (and is neither terminal nor terminating) is still
// starting or served before. Only a pod that has never been Ready is a pending start. The evidence is bound to the pod
// (its UID): what this process saw of it, and what the kubelet recorded in the pod object, which also holds after an
// operator restart. The kubelet's status manager stamps status.startTime and every condition's lastTransitionTime at
// its first report of the pod, and afterwards moves a condition's lastTransitionTime only when its status changes; it
// writes Ready as True or False only (Kubernetes 1.35, pkg/kubelet/status/status_manager.go: updateStatusInternal and
// updateLastTransitionTime; the node lifecycle controller's MarkPodsNotReady keeps the time of an unchanged status too).
//
//	evidence                                                              classification       proven?
//	this process saw the pod Ready (seen)                                 was Ready            yes (lost by a restart)
//	no Ready condition and no startTime (no kubelet has reported it)      never Ready          yes
//	Ready=False since the first report (|lastTransitionTime - startTime|  never Ready          yes
//	  within readyFirstReportSlack)
//	Ready=False, lastTransitionTime later than the first report, and      was Ready            yes (the status changed
//	  ContainersReady does not contradict it                                                   after the first report,
//	                                                                                           so it was True before)
//	anything else: Ready without a lastTransitionTime or without a        uncertain: counted   no
//	  startTime, Ready=Unknown, a transition before the first report,     as a pending start
//	  or ContainersReady=False since the first report next to a later
//	  Ready transition (Ready needs ContainersReady, so that was not
//	  a kubelet's status)
//
// Not evidence: absence from seen or from the samples (both are empty after an operator restart), a restart count (a
// pod can crash-loop without ever serving, and a pod that served can restart), and the pod's creation time (a pod can
// wait for a node for long before the kubelet's first report). Uncertain counts as pending because that is the
// classification that cannot shorten the lead: at worst the lead is longer than needed, or there is none
// (LeadInsufficient) and the decision is reactive.
func classifyNotReady(p *corev1.Pod, seen bool) notReadyClass {
	if seen {
		return notReadyWasReady
	}
	ready, started := podCondition(p, corev1.PodReady), p.Status.StartTime
	if ready == nil {
		if started == nil {
			return notReadyNeverReady
		}
		return notReadyUncertain
	}
	if ready.Status != corev1.ConditionFalse || ready.LastTransitionTime.IsZero() || started == nil || started.IsZero() {
		return notReadyUncertain
	}
	sinceFirstReport := func(c *corev1.PodCondition) time.Duration { return c.LastTransitionTime.Sub(started.Time) }
	switch d := sinceFirstReport(ready); {
	case d < -readyFirstReportSlack:
		return notReadyUncertain
	case d <= readyFirstReportSlack:
		return notReadyNeverReady
	}
	if cr := podCondition(p, corev1.ContainersReady); cr != nil && cr.Status != corev1.ConditionTrue &&
		(cr.LastTransitionTime.IsZero() || sinceFirstReport(cr) <= readyFirstReportSlack) {
		return notReadyUncertain
	}
	return notReadyWasReady
}

// controlledBy reports whether obj's controller owner reference is the object with this UID.
func controlledBy(obj metav1.Object, uid types.UID) bool {
	ref := metav1.GetControllerOf(obj)
	return ref != nil && ref.UID == uid
}

// sameTemplate compares a ReplicaSet's pod template with the Deployment's, ignoring the pod-template-hash label the
// Deployment controller adds to the ReplicaSet's copy.
func sameTemplate(rs *appsv1.ReplicaSet, dep *appsv1.Deployment) bool {
	t := rs.Spec.Template.DeepCopy()
	delete(t.Labels, podTemplateHashLabel)
	if len(t.Labels) == 0 {
		t.Labels = nil
	}
	d := dep.Spec.Template.DeepCopy()
	if len(d.Labels) == 0 {
		d.Labels = nil
	}
	return apiequality.Semantic.DeepEqual(*t, *d)
}

// currentReplicaSet is the Deployment's ReplicaSet for its current template: controlled by the Deployment, at the
// Deployment's revision (when it records one) and with the Deployment's pod template. Exactly one must match; otherwise
// the answer is nil (uncertain).
func (r *PredictiveAutoscalerReconciler) currentReplicaSet(ctx context.Context, dep *appsv1.Deployment, sel labels.Selector) (*appsv1.ReplicaSet, error) {
	var list appsv1.ReplicaSetList
	lctx, cancel := context.WithTimeout(ctx, startupListTimeout)
	err := r.reader().List(lctx, &list, client.InNamespace(dep.Namespace), client.MatchingLabelsSelector{Selector: sel})
	cancel()
	if err != nil {
		return nil, err
	}
	revision := dep.Annotations[revisionAnnotation]
	var found *appsv1.ReplicaSet
	for i := range list.Items {
		rs := &list.Items[i]
		if rs.DeletionTimestamp != nil || !controlledBy(rs, dep.UID) || !sameTemplate(rs, dep) {
			continue
		}
		if revision != "" && rs.Annotations[revisionAnnotation] != revision {
			continue
		}
		if found != nil {
			return nil, nil // two candidates: uncertain
		}
		found = rs
	}
	return found, nil
}

// observeStartup updates the target's samples from its current ReplicaSet's pods and reports the startup budget inputs
// and blockers.
func (r *PredictiveAutoscalerReconciler) observeStartup(ctx context.Context, key string, dep *appsv1.Deployment, now time.Time) startupObservation {
	if r.startupTrackers == nil {
		r.startupTrackers = map[string]*startupTracker{}
	}
	tr := r.startupTrackers[key]
	if tr == nil || tr.targetUID != dep.UID {
		tr = &startupTracker{targetUID: dep.UID, samples: map[types.UID]startupSample{}}
		r.startupTrackers[key] = tr
	}
	var obs startupObservation
	for _, c := range dep.Status.Conditions {
		if c.Type == appsv1.DeploymentReplicaFailure && c.Status == corev1.ConditionTrue {
			obs.Blocked, obs.BlockedMsg = "FailedCreate", c.Message
			if strings.Contains(c.Message, "exceeded quota") || strings.Contains(strings.ToLower(c.Reason), "quota") {
				obs.Blocked = "QuotaExceeded"
			}
		}
	}
	sel, err := metav1.LabelSelectorAsSelector(dep.Spec.Selector)
	if err == nil && sel.Empty() {
		err = fmt.Errorf("the Deployment has an empty selector")
	}
	var rs *appsv1.ReplicaSet
	if err == nil {
		rs, err = r.currentReplicaSet(ctx, dep, sel)
	}
	var pods corev1.PodList
	if err == nil && rs != nil {
		lctx, cancel := context.WithTimeout(ctx, startupListTimeout)
		err = r.reader().List(lctx, &pods, client.InNamespace(dep.Namespace), client.MatchingLabelsSelector{Selector: sel})
		cancel()
	}
	switch {
	case err != nil:
		obs.ReadError = err.Error()
	case rs == nil:
		obs.Unresolved = true
		obs.ReadError = "the Deployment's current ReplicaSet could not be identified (none or several at its revision and template)"
	default:
		obs.Observable = true
		if tr.rsUID != rs.UID { // a rollout or rollback: per-replica startup may have changed with the template
			tr.rsUID, tr.samples, tr.seenReady = rs.UID, map[types.UID]startupSample{}, nil
		}
		r.recordStartups(tr, rs.UID, pods.Items, now, &obs)
	}
	for uid, s := range tr.samples {
		if now.Sub(s.created) > startupWindow {
			delete(tr.samples, uid)
		}
	}
	for len(tr.samples) > startupMaxSamples {
		var oldest types.UID
		for uid, s := range tr.samples {
			if oldest == "" || s.created.Before(tr.samples[oldest].created) {
				oldest = uid
			}
		}
		delete(tr.samples, oldest)
	}
	obs.SampleCount = len(tr.samples)
	if !obs.Observable {
		obs.SampleCount = 0 // nothing attributable this time: the configured budget applies
		return obs
	}
	if obs.SampleCount >= startupMinSamples {
		vals := make([]int64, 0, len(tr.samples))
		for _, s := range tr.samples {
			vals = append(vals, s.seconds)
		}
		sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
		p := vals[int(math.Ceil(float64(startupPercentile)/100*float64(len(vals))))-1] // nearest rank
		obs.P90 = &p
	}
	return obs
}

// recordStartups adds the completed starts of the current ReplicaSet's pods and finds its pending and unschedulable pods.
// Pods of other owners that share the selector, and of older ReplicaSets, are ignored, and so are terminating and
// terminal pods (Failed, Succeeded): they are no longer starting, whatever their Ready condition says.
func (r *PredictiveAutoscalerReconciler) recordStartups(tr *startupTracker, rsUID types.UID, pods []corev1.Pod, now time.Time, obs *startupObservation) {
	seenReady := map[types.UID]struct{}{} // rebuilt from the listed pods, so it never outgrows the ReplicaSet
	for i := range pods {
		p := &pods[i]
		created := p.CreationTimestamp.Time
		if p.DeletionTimestamp != nil || !controlledBy(p, rsUID) || created.IsZero() || now.Sub(created) > startupWindow ||
			p.Status.Phase == corev1.PodFailed || p.Status.Phase == corev1.PodSucceeded {
			continue
		}
		ready := podCondition(p, corev1.PodReady)
		if ready != nil && ready.Status == corev1.ConditionTrue {
			seenReady[p.UID] = struct{}{}
			if _, seen := tr.samples[p.UID]; !seen && podRestarts(p) == 0 && !ready.LastTransitionTime.IsZero() {
				if d := ready.LastTransitionTime.Sub(created); d >= 0 {
					tr.samples[p.UID] = startupSample{created: created, seconds: int64(math.Ceil(d.Seconds()))}
				}
			}
			continue
		}
		_, seen := tr.seenReady[p.UID]
		class := classifyNotReady(p, seen)
		if class == notReadyWasReady {
			seenReady[p.UID] = struct{}{} // a readiness loss: not a start, so neither pending nor a blocked scale-out
			continue
		}
		age := int64(math.Ceil(now.Sub(created).Seconds()))
		if age < 0 {
			age = 0
		}
		if obs.Pending == nil || age > *obs.Pending {
			obs.Pending, obs.PendingEvidence, obs.PendingPod = &age, pendingNeverReady, p.Name
			if class == notReadyUncertain {
				obs.PendingEvidence = pendingUncertain
			}
		}
		if sc := podCondition(p, corev1.PodScheduled); sc != nil && sc.Status == corev1.ConditionFalse &&
			sc.Reason == corev1.PodReasonUnschedulable {
			obs.Blocked, obs.BlockedMsg = "Unschedulable", fmt.Sprintf("pod %s: %s", p.Name, sc.Message)
		}
	}
	tr.seenReady = seenReady
}

// startupConfig is spec.behavior.startup with its defaults.
type startupConfig struct {
	Fallback  *int64
	Minimum   int64
	Allowance int64
}

func startupConfigOf(spec autoscalerv1alpha1.PredictiveAutoscalerSpec) startupConfig {
	c := startupConfig{Allowance: v2DefaultActuationSecs}
	if spec.Behavior == nil || spec.Behavior.Startup == nil {
		return c
	}
	s := spec.Behavior.Startup
	if s.FallbackDelaySeconds != nil {
		v := int64(*s.FallbackDelaySeconds)
		c.Fallback = &v
	}
	if s.MinimumDelaySeconds != nil {
		c.Minimum = int64(*s.MinimumDelaySeconds)
	}
	if s.ActuationAllowanceSeconds != nil {
		c.Allowance = int64(*s.ActuationAllowanceSeconds)
	}
	return c
}

// startupBudget is the budget D and how it was chosen: measured (p90 + margin of enough samples), configured (the
// fallback), or none. Either is raised to the configured minimum and to a censored pending start older than it. The
// pending start is never cut to fit the horizon: a budget whose lead exceeds it leaves no lead (prepareV2).
func startupBudget(obs startupObservation, cfg startupConfig) (int64, string, bool) {
	var d int64
	method := "configured"
	switch {
	case obs.P90 != nil:
		d = (*obs.P90*(100+startupMarginPercent) + 99) / 100
		method = "measured"
	case cfg.Fallback != nil:
		d = *cfg.Fallback
	default:
		return 0, "none", false
	}
	if cfg.Minimum > d {
		d = cfg.Minimum
	}
	if obs.Pending != nil && *obs.Pending > d {
		d = *obs.Pending
	}
	return d, method, true
}

func int32Ptr(v int64) *int32 {
	if v > math.MaxInt32 {
		v = math.MaxInt32
	}
	x := int32(v)
	return &x
}

// status is the startup part of status.prediction.
func (obs startupObservation) status(delay int64, method string, ok bool) *autoscalerv1alpha1.StartupStatus {
	st := &autoscalerv1alpha1.StartupStatus{Method: method, SampleCount: int32(obs.SampleCount), Observable: obs.Observable,
		Measures: "podCreationToReady"}
	if ok {
		st.DelaySeconds = int32Ptr(delay)
	}
	if obs.P90 != nil {
		st.MeasuredP90Seconds = int32Ptr(*obs.P90)
	}
	if obs.Pending != nil {
		st.PendingSeconds, st.PendingEvidence = int32Ptr(*obs.Pending), obs.PendingEvidence
	}
	return st
}

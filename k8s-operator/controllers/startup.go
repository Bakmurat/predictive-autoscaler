package controllers

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Startup delay (plan item 4, minimum version; design C2 section 5): how long a scale-out takes to become usable, per
// target. The operator reads the target's pods (uncached) and records, once per pod, creation to Ready for pods of the
// current pod template that became Ready without a restart; only Ready pods count as serving. With at least
// startupMinSamples such starts in the last 7 days the budget is their 90th percentile plus startupMarginPercent;
// otherwise the configured spec.behavior.startup.fallbackDelaySeconds, and without that there is no budget (and so no
// predictive lead). A pod still not Ready is a censored start: when it is already older than the budget, its age becomes
// the budget, so a slow scale-out can never make the lead look shorter than it is. A pod-template change discards the
// samples (per-replica startup may have changed with it).
//
// The same observation surfaces scale-outs that cannot complete: pods the scheduler cannot place (Unschedulable) and
// pods the ReplicaSet cannot create (the Deployment's ReplicaFailure condition, for example an exceeded quota).

const (
	startupMinSamples    = 20
	startupWindow        = 7 * 24 * time.Hour
	startupMaxSamples    = 512
	startupPercentile    = 90
	startupMarginPercent = 25
	podTemplateHashLabel = "pod-template-hash"
)

// startupListTimeout bounds the pod read: a stalled API server must not stall the decision.
var startupListTimeout = 3 * time.Second

type startupSample struct {
	created time.Time
	seconds int64
}

// startupTracker holds one target's samples in memory (an operator restart starts again from the configured budget).
type startupTracker struct {
	targetUID     types.UID
	templateSHA   string
	templateSince time.Time // when a template change was seen; zero when the operator started with the template
	samples       map[types.UID]startupSample
}

// startupObservation is one reconcile's view of the target's startup and of blocked scale-outs.
type startupObservation struct {
	Observable  bool   // the pods could be read
	ReadError   string // why not
	SampleCount int
	P90         *int64 // seconds, with at least startupMinSamples samples
	Pending     *int64 // age of the oldest current-template pod not yet Ready, seconds
	Blocked     string // Unschedulable, QuotaExceeded, FailedCreate or ""
	BlockedMsg  string
}

func templateSHA(dep *appsv1.Deployment) string {
	raw, _ := json.Marshal(dep.Spec.Template)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
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

// observeStartup updates the target's samples from its pods and reports the startup budget inputs and blockers.
func (r *PredictiveAutoscalerReconciler) observeStartup(ctx context.Context, key string, dep *appsv1.Deployment, now time.Time) startupObservation {
	if r.startupTrackers == nil {
		r.startupTrackers = map[string]*startupTracker{}
	}
	tr := r.startupTrackers[key]
	sha := templateSHA(dep)
	switch {
	case tr == nil || tr.targetUID != dep.UID:
		tr = &startupTracker{targetUID: dep.UID, templateSHA: sha, samples: map[types.UID]startupSample{}}
		r.startupTrackers[key] = tr
	case tr.templateSHA != sha:
		tr.templateSHA, tr.templateSince, tr.samples = sha, now, map[types.UID]startupSample{}
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
	var pods corev1.PodList
	if err == nil {
		lctx, cancel := context.WithTimeout(ctx, startupListTimeout)
		err = r.reader().List(lctx, &pods, client.InNamespace(dep.Namespace), client.MatchingLabelsSelector{Selector: sel})
		cancel()
	}
	if err != nil {
		obs.ReadError = err.Error()
	} else {
		obs.Observable = true
		r.recordStartups(tr, pods.Items, now, &obs)
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

// recordStartups adds the completed starts of the current pod template and finds pending pods and unschedulable ones.
func (r *PredictiveAutoscalerReconciler) recordStartups(tr *startupTracker, pods []corev1.Pod, now time.Time, obs *startupObservation) {
	// The current template is the newest live pod's; pods of an older template are leaving and say nothing about it.
	var newest *corev1.Pod
	for i := range pods {
		p := &pods[i]
		if p.DeletionTimestamp != nil || p.Labels[podTemplateHashLabel] == "" {
			continue
		}
		if newest == nil || newest.CreationTimestamp.Before(&p.CreationTimestamp) ||
			(newest.CreationTimestamp.Equal(&p.CreationTimestamp) && p.UID > newest.UID) {
			newest = p
		}
	}
	if newest == nil {
		return
	}
	hash := newest.Labels[podTemplateHashLabel]
	for i := range pods {
		p := &pods[i]
		created := p.CreationTimestamp.Time
		if p.DeletionTimestamp != nil || p.Labels[podTemplateHashLabel] != hash || created.IsZero() ||
			(!tr.templateSince.IsZero() && created.Before(tr.templateSince)) || now.Sub(created) > startupWindow {
			continue
		}
		ready := podCondition(p, corev1.PodReady)
		if ready != nil && ready.Status == corev1.ConditionTrue {
			if _, seen := tr.samples[p.UID]; !seen && podRestarts(p) == 0 && !ready.LastTransitionTime.IsZero() {
				if d := ready.LastTransitionTime.Sub(created); d >= 0 {
					tr.samples[p.UID] = startupSample{created: created, seconds: int64(math.Ceil(d.Seconds()))}
				}
			}
			continue
		}
		age := int64(math.Ceil(now.Sub(created).Seconds()))
		if age < 0 {
			age = 0
		}
		if obs.Pending == nil || age > *obs.Pending {
			obs.Pending = &age
		}
		if sc := podCondition(p, corev1.PodScheduled); sc != nil && sc.Status == corev1.ConditionFalse &&
			sc.Reason == corev1.PodReasonUnschedulable {
			obs.Blocked, obs.BlockedMsg = "Unschedulable", fmt.Sprintf("pod %s: %s", p.Name, sc.Message)
		}
	}
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
// fallback), or none. Either is raised to the configured minimum and to a censored pending start older than it.
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
	st := &autoscalerv1alpha1.StartupStatus{Method: method, SampleCount: int32(obs.SampleCount), Observable: obs.Observable}
	if ok {
		st.DelaySeconds = int32Ptr(delay)
	}
	if obs.P90 != nil {
		st.MeasuredP90Seconds = int32Ptr(*obs.P90)
	}
	if obs.Pending != nil {
		st.PendingSeconds = int32Ptr(*obs.Pending)
	}
	return st
}

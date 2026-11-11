package controllers

import (
	stderrors "errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/metrics"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// What a dashboard needs to follow one autoscaler through a reconcile: its mode, why each condition holds, what the
// last decision did, the replica count at every stage from the calculation to the Ready pods, the outcome of every
// write attempt, the configured capacity, and which forecast the operator was looking at.
//
// Every label besides namespace and autoscaler is an enum defined in this package or checked against one: never an
// error text, a condition message or anything a user writes. A value that does not exist has no series: nothing is
// published as 0 that was not measured, decided or confirmed.

var (
	stageLabels = []string{"namespace", "autoscaler"}

	modeGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_mode",
		Help: "Effective mode of the autoscaler: 1 for the current mode (Recommend or Active), 0 for the other",
	}, []string{"namespace", "autoscaler", "mode"})
	conditionReasonGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_condition_reason",
		Help: "Reason of a status condition: one series with value 1 per condition type, labelled with its current reason",
	}, []string{"namespace", "autoscaler", "type", "reason"})
	decisionActionGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_decision_action",
		Help: "What the last decision did (scale_up, a hold, recommend, target_not_allowed, ...): one series with value 1",
	}, []string{"namespace", "autoscaler", "action"})

	calculatedReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_calculated_replicas",
		Help: "Stage 1: the replica count the last reconcile calculated, in any mode, before the scale-down safeguards",
	}, stageLabels)
	requestedReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_requested_replicas",
		Help: "Stage 2: the count the write path intended in the last reconcile: the target of a write attempt (final guards may still have prevented its submission) or the held count (Active mode only; absent when nothing was intended)",
	}, stageLabels)
	appliedReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_applied_replicas",
		Help: "Stage 3: the replica count of the last write the API server confirmed to this operator process for this autoscaler and target object (absent before its first write and after either was replaced)",
	}, stageLabels)
	appliedTimestampGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_applied_timestamp_seconds",
		Help: "When the API server confirmed the last write of this operator process (Unix seconds)",
	}, stageLabels)
	targetReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_target_replicas",
		Help: "Stage 4: the target Deployment's desired replica count as the last reconcile read it",
	}, stageLabels)
	targetReadyReplicasGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_target_ready_replicas",
		Help: "Stage 5: the target Deployment's Ready replicas as the last reconcile read them",
	}, stageLabels)
	scaleWritesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "predictive_autoscaler_scale_writes_total",
		Help: "Write attempts by outcome: applied (confirmed), stale (the target changed since the decision), guard_abort, " +
			"not_allowed, read_failed (nothing was sent), rejected (the API server refused it), unknown (no answer: it may or may not have been applied)",
	}, []string{"namespace", "autoscaler", "result"})
	capacityPerReplicaGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_capacity_per_replica_rps",
		Help: "Configured request capacity of one replica in requests per second, and the field it comes from (absent when none resolves)",
	}, []string{"namespace", "autoscaler", "source"})

	selectedFamilyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_prediction_selected_family",
		Help: "Forecast family the service's selection chose in the last verified forecast, trusted or not: one series with value 1",
	}, []string{"namespace", "autoscaler", "family"})
	usedFamilyGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_prediction_used_family",
		Help: "Family of the forecast the operator accepted into the last decision: one series with value 1, absent when no forecast was accepted",
	}, []string{"namespace", "autoscaler", "family"})
	predictionLeadGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_prediction_lead_seconds",
		Help: "Lead of the last verified forecast, trusted or not (seconds)",
	}, stageLabels)
	predictionIssuedGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_prediction_issued_timestamp_seconds",
		Help: "When the last verified forecast was issued (Unix seconds)",
	}, stageLabels)
	predictionWindowStartGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_prediction_window_start_timestamp_seconds",
		Help: "Start of the future window the last verified forecast covers (Unix seconds); its values are about this window, not about the time of issuance",
	}, stageLabels)
	predictionWindowEndGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_prediction_window_end_timestamp_seconds",
		Help: "End of the future window the last verified forecast covers (Unix seconds)",
	}, stageLabels)
	predictionValidUntilGauge = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "predictive_autoscaler_prediction_valid_until_timestamp_seconds",
		Help: "Until when the last verified forecast may authorize a write (Unix seconds)",
	}, stageLabels)
)

// Outcomes of a write attempt (scaleWritesTotal's result label).
const (
	writeApplied    = "applied"
	writeStale      = "stale"
	writeGuardAbort = "guard_abort"
	writeNotAllowed = "not_allowed"
	writeReadFailed = "read_failed"
	writeRejected   = "rejected"
	writeUnknown    = "unknown"
)

var (
	writeResults = []string{writeApplied, writeStale, writeGuardAbort, writeNotAllowed, writeReadFailed, writeRejected, writeUnknown}
	modes        = []string{autoscalerv1alpha1.ModeRecommend, autoscalerv1alpha1.ModeActive}
	// decisionActions are the actions recordDecision is called with (decision_record.go).
	decisionActions = []string{"scale_up", "scale_down", "hold_stabilizing", "hold_cooldown", "hold_headroom", "at_target",
		"keep_current", "scale_error", "recommend", "conflict_hold", "target_not_allowed", "guard_abort", "paused"}
	// conditionReasons is every reason the operator sets on an exported condition; any other value is exported as
	// Other. TestEveryReasonTheOperatorSetsIsExportedByName scans the sources for reasons missing here.
	conditionReasons = []string{
		// Ready and the failure reasons it shares with ScalingActive
		"ReconcileCompleted", "CrossNamespaceTarget", "DeploymentNotFound", "InvalidMetricQuery", "ScalingError",
		// ScalingActive
		"Active", "RecommendMode", "TargetPaused", "TargetNotAllowed", "CheckFailed", "Conflict", "GuardAborted",
		"BoundsEnforced", "CapacityMissing", "TelemetryHold",
		// CapacityConfigured
		"CapacityPerReplica", "DeprecatedTargetRPS", "CapacityInvalid",
		// TelemetryAvailable
		"Measured", "MetricsUnavailable",
		// ForecastAvailable
		"Used", "Disabled", "Unavailable", "HorizonElapsed", "SanityRejected", "Unauthorized", "Forbidden",
		"AuthUnavailable", "AuthMisconfigured", "NotTrusted", "Rejected", "LeadInsufficient", "SettingsInvalid",
		// ConflictDetected, VPAInterference, ScaleOutBlocked, ScaleDownHeadroom
		"ReplicaWriter", "NoConflict", "VPAUpdatesPods", "None", "FailedCreate", "QuotaExceeded", "Unschedulable",
		"OwnershipUnresolved", "PodsUnreadable", "Holding", "NotHolding",
		// PredictionReady: the service's reasons, the operator's own verdicts and request failures
		"HistoryInsufficient", "CalibrationInsufficient", "ShadowInsufficient", "CoverageLow", "DataUnavailable",
		"IdentityMismatch", "StateUnavailable", "ConfigurationNotValidated", "ReplayGateMissing", "Ready",
		"NotEligible", "EvidenceInsufficient", "EvidenceStale", "IssuedInFuture", "DeadlinePassed", "ForecastRejected",
		"RequestRefused", "AutoscalerLimitReached",
	}
	// appliedOwner remembers, per autoscaler, the autoscaler and target objects (UIDs) its applied stage belongs to.
	appliedOwner sync.Map
)

const otherLabel = "Other"

func init() {
	metrics.Registry.MustRegister(modeGauge, conditionReasonGauge, decisionActionGauge, calculatedReplicasGauge,
		requestedReplicasGauge, appliedReplicasGauge, appliedTimestampGauge, targetReplicasGauge, targetReadyReplicasGauge,
		scaleWritesTotal, capacityPerReplicaGauge, selectedFamilyGauge, usedFamilyGauge, predictionLeadGauge,
		predictionIssuedGauge, predictionWindowStartGauge, predictionWindowEndGauge, predictionValidUntilGauge)
}

func oneOf(v string, allowed []string) string {
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	return otherLabel
}

// setOnly leaves exactly one series of the autoscaler in a one-series-per-value gauge (more with extra fixed labels,
// e.g. the condition type): the previous value's series is removed, never left at 1 or set to 0.
func setOnly(g *prometheus.GaugeVec, fixed prometheus.Labels, key, value string) {
	g.DeletePartialMatch(fixed)
	labels := prometheus.Labels{key: value}
	for k, v := range fixed {
		labels[k] = v
	}
	g.With(labels).Set(1)
}

func setOrDelete(g *prometheus.GaugeVec, ns, name string, value float64, present bool) {
	if present {
		g.WithLabelValues(ns, name).Set(value)
		return
	}
	g.DeleteLabelValues(ns, name)
}

func recordMode(ns, name, mode string) {
	for _, m := range modes {
		modeGauge.WithLabelValues(ns, name, m).Set(map[bool]float64{true: 1, false: 0}[m == mode])
	}
}

func recordConditionReason(ns, name, typ string, reason *string) {
	fixed := prometheus.Labels{"namespace": ns, "autoscaler": name, "type": typ}
	if reason == nil {
		conditionReasonGauge.DeletePartialMatch(fixed)
		return
	}
	setOnly(conditionReasonGauge, fixed, "reason", oneOf(*reason, conditionReasons))
}

func recordDecisionAction(ns, name, action string) {
	setOnly(decisionActionGauge, prometheus.Labels{"namespace": ns, "autoscaler": name}, "action", oneOf(action, decisionActions))
}

// recordIdentity binds the applied stage to the autoscaler and target objects it was confirmed for. When either is
// another object now (a replaced Deployment, an autoscaler deleted and created again under its name, a target that
// cannot be read: targetUID ""), the last confirmed write says nothing about it and is removed. It is called before
// any write of the reconcile.
func recordIdentity(ns, name, autoscalerUID, targetUID string) {
	owner := autoscalerUID + "/" + targetUID
	if prev, ok := appliedOwner.Swap(ns+"/"+name, owner); !ok || prev != owner {
		appliedReplicasGauge.DeleteLabelValues(ns, name)
		appliedTimestampGauge.DeleteLabelValues(ns, name)
	}
}

// clearReconcileStages removes what only a completed reconcile can say: a reconcile that ended before its decision
// (no target, an invalid query) must not leave the previous one's counts, action, capacity or forecast standing as
// if they were current. The mode, the condition reasons, the write counters and a still valid applied stage stay.
func clearReconcileStages(ns, name string) {
	fixed := prometheus.Labels{"namespace": ns, "autoscaler": name}
	for _, g := range []*prometheus.GaugeVec{calculatedReplicasGauge, requestedReplicasGauge, targetReplicasGauge,
		targetReadyReplicasGauge, decisionActionGauge, capacityPerReplicaGauge} {
		g.DeletePartialMatch(fixed)
	}
	forgetPredictionWindow(ns, name)
}

// stageReplicas is one reconcile's replica count at each stage. requested is nil when the write path intended
// nothing (Recommend mode, a paused target, a refused or blocked write).
type stageReplicas struct {
	calculated, target, ready int32
	requested                 *int32
}

func recordStages(ns, name string, s stageReplicas) {
	calculatedReplicasGauge.WithLabelValues(ns, name).Set(float64(s.calculated))
	targetReplicasGauge.WithLabelValues(ns, name).Set(float64(s.target))
	targetReadyReplicasGauge.WithLabelValues(ns, name).Set(float64(s.ready))
	if s.requested != nil {
		requestedReplicasGauge.WithLabelValues(ns, name).Set(float64(*s.requested))
	} else {
		requestedReplicasGauge.DeleteLabelValues(ns, name)
	}
	for _, r := range writeResults { // every outcome exists from the first reconcile on, so a rate over it is defined
		scaleWritesTotal.WithLabelValues(ns, name, r).Add(0)
	}
}

// recordWrite counts one write attempt's outcome; a confirmed write also becomes the applied stage.
func recordWrite(ns, name, result string, replicas int32, at float64) {
	scaleWritesTotal.WithLabelValues(ns, name, oneOf(result, writeResults)).Inc()
	if result == writeApplied {
		appliedReplicasGauge.WithLabelValues(ns, name).Set(float64(replicas))
		appliedTimestampGauge.WithLabelValues(ns, name).Set(at)
	}
}

// writeFailure classifies an error of the /scale update itself. Only an answer from the API server that refuses the
// request (a 4xx status) is a rejection; a timeout, a cut connection or a server-side failure leaves the outcome
// unknown, because the update may have been applied before the answer was lost.
func writeFailure(err error) string {
	switch {
	case apierrors.IsConflict(err):
		return writeStale
	case stderrors.Is(err, errWriteNotAllowed):
		return writeNotAllowed
	}
	var status apierrors.APIStatus
	if stderrors.As(err, &status) {
		if code := status.Status().Code; code >= 400 && code < 500 {
			return writeRejected
		}
	}
	return writeUnknown
}

func recordCapacity(ns, name string, rpm float64, source string, ok bool) {
	capacityPerReplicaGauge.DeletePartialMatch(prometheus.Labels{"namespace": ns, "autoscaler": name})
	if ok {
		capacityPerReplicaGauge.WithLabelValues(ns, name, oneOf(source, []string{capacitySourcePerReplica, capacitySourceTargetRPS})).Set(rpm / 60)
	}
}

// recordPredictionWindow publishes the forecast the operator last verified, also when it is not trusted (a shadow
// candidate is shown as what it is), and the family of the forecast it accepted, if it accepted one.
func recordPredictionWindow(ns, name string, resp *v2Response, accepted bool) {
	fixed := prometheus.Labels{"namespace": ns, "autoscaler": name}
	if resp == nil {
		forgetPredictionWindow(ns, name)
		return
	}
	setOnly(selectedFamilyGauge, fixed, "family", oneOf(resp.SelectedCandidate, v2ModelFamilies))
	if accepted {
		setOnly(usedFamilyGauge, fixed, "family", oneOf(resp.ModelFamily, v2ModelFamilies))
	} else {
		usedFamilyGauge.DeletePartialMatch(fixed)
	}
	predictionLeadGauge.WithLabelValues(ns, name).Set(float64(resp.LeadSeconds))
	predictionIssuedGauge.WithLabelValues(ns, name).Set(float64(resp.IssuedAtMs) / 1000)
	predictionWindowStartGauge.WithLabelValues(ns, name).Set(float64(resp.WindowStart))
	predictionWindowEndGauge.WithLabelValues(ns, name).Set(float64(resp.WindowEnd))
	predictionValidUntilGauge.WithLabelValues(ns, name).Set(float64(resp.ValidUntil))
}

func forgetPredictionWindow(ns, name string) {
	fixed := prometheus.Labels{"namespace": ns, "autoscaler": name}
	selectedFamilyGauge.DeletePartialMatch(fixed)
	usedFamilyGauge.DeletePartialMatch(fixed)
	for _, g := range []*prometheus.GaugeVec{predictionLeadGauge, predictionIssuedGauge, predictionWindowStartGauge,
		predictionWindowEndGauge, predictionValidUntilGauge} {
		g.DeleteLabelValues(ns, name)
	}
}

// forgetStageMetrics removes every series of a deleted autoscaler.
func forgetStageMetrics(ns, name string) {
	fixed := prometheus.Labels{"namespace": ns, "autoscaler": name}
	for _, g := range []*prometheus.GaugeVec{modeGauge, conditionReasonGauge, decisionActionGauge, calculatedReplicasGauge,
		requestedReplicasGauge, appliedReplicasGauge, appliedTimestampGauge, targetReplicasGauge, targetReadyReplicasGauge,
		capacityPerReplicaGauge} {
		g.DeletePartialMatch(fixed)
	}
	scaleWritesTotal.DeletePartialMatch(fixed)
	forgetPredictionWindow(ns, name)
	appliedOwner.Delete(ns + "/" + name)
}

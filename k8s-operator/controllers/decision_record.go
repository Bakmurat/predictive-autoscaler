package controllers

import (
	"time"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// decisionRecord is the companion line appended to FORECAST_LOG on EVERY reconcile (Codex D-140).
// It records how the predictive component participated in the decision -- the raw lead-window
// replicas, the confidence-adjusted value, each safeguard that changed it, the reactive floor, and
// the action actually applied -- so a scorer can measure participation instead of inferring it.
// It is observation only: nothing here feeds back into the decision. Lines carry "event":"decision",
// so readers that treat event lines as non-issuance records keep working.
type decisionRecord struct {
	Event            string   `json:"event"`
	At               string   `json:"at"`
	Application      string   `json:"application"`
	Namespace        string   `json:"namespace"`
	Forecasting      bool     `json:"forecasting"`
	ForecastStatus   string   `json:"forecast_status"` // used|unavailable|horizon_elapsed|sanity_rejected|disabled
	ForecastIssuedAt *string  `json:"forecast_issued_at"`
	ArtifactSHA256   *string  `json:"artifact_sha256"`
	ModelVersion     *string  `json:"model_version"`
	LeadWindowPeak   *float64 `json:"lead_window_peak_rpm"`
	Confidence       *float64 `json:"confidence"`
	RawPredicted     *int32   `json:"raw_predicted_replicas"`
	ConfidenceAdj    *int32   `json:"confidence_adjusted_replicas"`
	PredictedClamped *int32   `json:"predicted_after_clamp"`
	// PredictedReplicas is the value that entered max(predicted, reactive, min): after the sanity
	// and overestimate safeguards; 0 when no usable forecast participated.
	PredictedReplicas int32    `json:"predicted_replicas"`
	Safeguards        []string `json:"safeguards"`
	ReactiveReplicas  int32    `json:"reactive_replicas"`
	CurrentRPM        float64  `json:"current_rpm"`
	MinReplicas       int32    `json:"min_replicas"`
	MaxReplicas       int32    `json:"max_replicas"`
	DesiredReplicas   int32    `json:"desired_replicas"`
	DesiredSource     string   `json:"desired_source"` // prediction|reactive|tie|min_replicas|max_replicas|keep_current
	CurrentReplicas   int32    `json:"current_replicas"`
	AppliedReplicas   int32    `json:"applied_replicas"`
	Action            string   `json:"action"` // scale_up|scale_down|hold_stabilizing|hold_cooldown|at_target|keep_current|scale_error
}

func newDecisionRecord(a *autoscalerv1alpha1.PredictiveAutoscaler, forecasting bool, current int32) *decisionRecord {
	d := &decisionRecord{
		Event: "decision", Application: a.Spec.TargetDeployment.Name, Namespace: a.Spec.TargetDeployment.Namespace,
		Forecasting: forecasting, ForecastStatus: "unavailable", Safeguards: []string{},
		MinReplicas: a.Spec.MinReplicas, MaxReplicas: a.Spec.MaxReplicas, CurrentReplicas: current,
	}
	if d.Namespace == "" {
		d.Namespace = a.Namespace
	}
	if !forecasting {
		d.ForecastStatus = "disabled"
	}
	return d
}

// setForecast records the forecast that was evaluated this reconcile and its intermediate values.
func (d *decisionRecord) setForecast(p *MLPredictionResponse, det predictedDetail, usable bool) {
	if p == nil {
		return
	}
	if !p.issuedAt.IsZero() {
		s := p.issuedAt.UTC().Format(time.RFC3339)
		d.ForecastIssuedAt = &s
	}
	if p.ArtifactSHA256 != "" {
		s := p.ArtifactSHA256
		d.ArtifactSHA256 = &s
	}
	if p.ModelVersion != "" {
		s := p.ModelVersion
		d.ModelVersion = &s
	}
	c := det.Confidence
	d.Confidence = &c
	if !usable {
		d.ForecastStatus = "horizon_elapsed"
		return
	}
	d.ForecastStatus = "used"
	peak, raw, adj, clamped := det.PeakRPM, det.Raw, det.Damped, det.Clamped
	d.LeadWindowPeak, d.RawPredicted, d.ConfidenceAdj, d.PredictedClamped = &peak, &raw, &adj, &clamped
	d.Safeguards = append(d.Safeguards, det.Safeguards...)
}

// setDecision records the unified decision and which component set it. The prediction "sets" the
// decision only when a USED forecast's value strictly exceeds the reactive floor and minReplicas and
// was not cut to maxReplicas; equality with the reactive value is a tie, never a prediction win.
func (d *decisionRecord) setDecision(predicted, desired int32, keepCurrent bool) {
	d.PredictedReplicas, d.DesiredReplicas = predicted, desired
	used := d.ForecastStatus == "used"
	p := int32(0)
	if used {
		p = predicted
	}
	top := p
	if d.ReactiveReplicas > top {
		top = d.ReactiveReplicas
	}
	switch {
	case keepCurrent:
		d.DesiredSource = "keep_current"
	case top > d.MaxReplicas, used && p > d.ReactiveReplicas && p == d.MaxReplicas && d.hasSafeguard("max_clamp"):
		d.DesiredSource = "max_replicas"
	case top < d.MinReplicas || top == 0:
		d.DesiredSource = "min_replicas"
	case used && p > d.ReactiveReplicas && p > d.MinReplicas:
		d.DesiredSource = "prediction"
	case used && p > d.ReactiveReplicas: // p == minReplicas: the floor, not the forecast, set it
		d.DesiredSource = "min_replicas"
	case used && p == d.ReactiveReplicas:
		d.DesiredSource = "tie"
	default:
		d.DesiredSource = "reactive"
	}
}

func (d *decisionRecord) hasSafeguard(name string) bool {
	for _, s := range d.Safeguards {
		if s == name {
			return true
		}
	}
	return false
}

// recordDecision stamps the applied action and appends the record (no-op when FORECAST_LOG is unset).
func (r *PredictiveAutoscalerReconciler) recordDecision(d *decisionRecord, action string, applied int32) {
	if d == nil {
		return
	}
	d.At = time.Now().UTC().Format(time.RFC3339)
	d.Action, d.AppliedReplicas = action, applied
	r.appendForecastLog(d)
}

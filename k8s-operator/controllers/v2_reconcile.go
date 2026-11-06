package controllers

import (
	"context"
	stderrors "errors"
	"fmt"
	"math"
	"net/http"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The v2 path inside Reconcile (engine statistical): prepareV2 resolves the settings, the startup budget and the lead
// and publishes the lead; runV2 asks for and verifies a forecast; the decision then uses v2PredictiveReplicas.

func (r *PredictiveAutoscalerReconciler) now() time.Time {
	if r.Clock != nil {
		return r.Clock()
	}
	return time.Now()
}

const (
	// v2ScheduleOffset: a v2 decision starts this long after its grid origin (tolerates clock skew with the service).
	v2ScheduleOffset = time.Second
	// v2ScheduleSlackSeconds is added to C in the lead: the start offset plus queueing jitter.
	v2ScheduleSlackSeconds = 5
	// v2MinWait is the shortest requeue (a decision that overran its interval is followed at once).
	v2MinWait = 100 * time.Millisecond
)

// v2DecisionInterval is the decision period on the grid: the reconcile interval rounded up to the cadence, and at least
// one cadence. Decisions are anchored to grid origins (Reconcile), so two decisions are C apart.
func v2DecisionInterval(reconcileInterval time.Duration, dt int64) int64 {
	c := int64(math.Ceil(reconcileInterval.Seconds()))
	c = (c + dt - 1) / dt * dt
	if c < dt {
		c = dt
	}
	return c
}

// v2LeadDecisionInterval is the C used in the lead: the decision period plus the start offset and queueing jitter, the
// longest gap between two decisions when the operator keeps up.
func v2LeadDecisionInterval(reconcileInterval time.Duration, dt int64) int64 {
	return v2DecisionInterval(reconcileInterval, dt) + v2ScheduleSlackSeconds
}

// v2ScheduleOf is the grid origin of a decision started at now, and the decision period, for an autoscaler on the v2
// path with valid settings (ok false otherwise: the ordinary throttle applies).
func v2ScheduleOf(a *autoscalerv1alpha1.PredictiveAutoscaler, reconcileInterval time.Duration, now time.Time) (origin, c int64, ok bool) {
	if !v2Selected(a) {
		return 0, 0, false
	}
	s, err := resolveV2Settings(a.Spec)
	if err != nil {
		return 0, 0, false
	}
	dt := s.SampleIntervalSeconds
	return now.Unix() / dt * dt, v2DecisionInterval(reconcileInterval, dt), true
}

// v2Wait is the requeue delay to the next decision: v2ScheduleOffset after the grid origin next, net of now.
func v2Wait(now time.Time, next int64) time.Duration {
	if d := time.Unix(next, 0).Add(v2ScheduleOffset).Sub(now); d > v2MinWait {
		return d
	}
	return v2MinWait
}

// prepareV2 resolves what a v2 request needs and publishes status.prediction.{engine, settingsSHA256, lead, leadBasis,
// frozenLeadSeconds} before any request (the forecasting service reads the lead from the live object). An error means
// the publication failed; the caller then makes no request.
func (r *PredictiveAutoscalerReconciler) prepareV2(ctx context.Context, a *autoscalerv1alpha1.PredictiveAutoscaler,
	obs startupObservation, reconcileInterval time.Duration) (*v2Outcome, error) {
	out := &v2Outcome{}
	out.Settings, out.SettingsErr = resolveV2Settings(a.Spec)
	cfg := startupConfigOf(a.Spec)
	d, method, ok := startupBudget(obs, cfg)
	out.startup = obs.status(d, method, ok)
	want := autoscalerv1alpha1.PredictionStatus{Engine: autoscalerv1alpha1.EngineStatistical, Startup: out.startup}
	if out.SettingsErr == nil {
		s := out.Settings
		want.SettingsSHA256 = s.fingerprint()
		c := v2LeadDecisionInterval(reconcileInterval, s.SampleIntervalSeconds)
		fallback := int64(-1)
		if cfg.Fallback != nil {
			fallback = *cfg.Fallback
		}
		want.LeadBasis = v2Hash(map[string]interface{}{"settings_sha256": want.SettingsSHA256, "fallback": fallback,
			"minimum": cfg.Minimum, "allowance": cfg.Allowance, "decision_interval": c})
		frozen := int64(0)
		if prev := a.Status.Prediction; prev != nil && prev.LeadBasis == want.LeadBasis {
			frozen = int64(prev.FrozenLeadSeconds)
		}
		want.FrozenLeadSeconds = int32(frozen)
		switch {
		case !ok:
			out.leadProblem = "no startup budget: set spec.behavior.startup.fallbackDelaySeconds until enough " +
				fmt.Sprintf("starts are measured (%d completed starts of the current pod template)", startupMinSamples)
		default:
			lead := v2AutomaticLead(d, c, cfg.Allowance, s.SampleIntervalSeconds)
			if frozen > lead {
				lead = frozen // never shorten automatically within one basis
			}
			if lead > s.HorizonSeconds {
				out.leadProblem = fmt.Sprintf("the lead %d s (startup %d s + decision interval %d s + allowance %d s, on "+
					"the %d s grid) exceeds the %d s horizon", lead, d, c, cfg.Allowance, s.SampleIntervalSeconds, s.HorizonSeconds)
				break
			}
			sample := int64(0)
			if method == "measured" {
				sample = int64(obs.SampleCount)
			}
			l := &autoscalerv1alpha1.PredictionLead{Seconds: int32(lead), StartupDelaySeconds: int32(d),
				ActuationAllowanceSeconds: int32(cfg.Allowance), Method: method, SampleCount: int32(sample),
				ObservedGeneration: a.Generation}
			if p := v2LeadProblem(l, s, a.Generation); p != "" {
				out.leadProblem = p
				break
			}
			want.Lead, want.FrozenLeadSeconds = l, int32(lead)
		}
	}
	out.published = want
	prev := a.Status.Prediction
	if prev == nil || prev.Engine != want.Engine || prev.SettingsSHA256 != want.SettingsSHA256 ||
		prev.LeadBasis != want.LeadBasis || prev.FrozenLeadSeconds != want.FrozenLeadSeconds ||
		!leadEqual(prev.Lead, want.Lead) {
		next := want.DeepCopy()
		a.Status.Prediction = next
		if err := r.Status().Update(ctx, a); err != nil {
			return nil, err
		}
	}
	return out, nil
}

func leadEqual(a, b *autoscalerv1alpha1.PredictionLead) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// runV2 asks for the forecast of the current grid origin (or reuses the verified answer for it) and records the
// operator's verdict. Nothing here writes.
func (r *PredictiveAutoscalerReconciler) runV2(ctx context.Context, key string, a *autoscalerv1alpha1.PredictiveAutoscaler,
	dep *appsv1.Deployment, out *v2Outcome, now time.Time) {
	lead := out.published.Lead
	ms := a.Status.MetricSource
	if out.SettingsErr != nil || lead == nil || ms == nil {
		delete(r.v2Cache, key)
		return
	}
	s := out.Settings
	dt := s.SampleIntervalSeconds
	b := v2Binding{AutoscalerNamespace: a.Namespace, AutoscalerName: a.Name, AutoscalerUID: string(a.UID),
		Generation: a.Generation, TargetNamespace: dep.Namespace, TargetName: dep.Name, TargetUID: ms.TargetUID,
		QuerySHA: ms.SHA256, Settings: s, SettingsSHA: out.published.SettingsSHA256, Origin: now.Unix() / dt * dt,
		Lead: *lead}
	b.EvidenceKey = v2EvidenceKey(b.AutoscalerUID, b.TargetUID, b.QuerySHA, s, int64(lead.Seconds))
	out.Binding = &b
	out.Response, out.Cached, out.Err = r.fetchV2(ctx, key, b, now)
	if out.Err != nil {
		var refused *forecastRefusedError
		if stderrors.Is(out.Err, errV2Refused) || (stderrors.As(out.Err, &refused) &&
			(refused.status == http.StatusUnauthorized || refused.status == http.StatusForbidden)) {
			delete(r.v2Cache, key)
		}
		return
	}
	// The verdict uses the clock after the request, not the reconcile's start: the request takes time.
	out.Verdict = out.Response.verdict(s, r.now())
}

// forecastStatus is the decision record's forecast status (and the ForecastAvailable reason) for the v2 path.
func (out *v2Outcome) forecastStatus() string {
	switch {
	case out.SettingsErr != nil:
		return "settings_invalid"
	case out.published.Lead == nil:
		return "lead_insufficient"
	case out.Err != nil && stderrors.Is(out.Err, errV2Refused):
		return "rejected"
	case out.Err != nil:
		return forecastFailureStatus(out.Err)
	case out.Response == nil:
		return "unavailable"
	case !out.Verdict.Eligible:
		return "not_eligible"
	}
	return "used"
}

// readiness is the PredictionReady condition: True only when the operator verified a forecast as eligible now. It is
// about evidence, not authorization: Recommend still writes nothing.
func (out *v2Outcome) readiness(capacityOK bool, mode string) (bool, string, string) {
	switch {
	case out.SettingsErr != nil:
		return false, "SettingsInvalid", out.SettingsErr.Error()
	case !capacityOK:
		return false, "CapacityMissing", "no per-replica capacity: no forecast is requested"
	case out.published.Lead == nil:
		return false, "LeadInsufficient", out.leadProblem
	case out.Err != nil:
		return false, v2FailureReason(out.Err), out.Err.Error()
	case out.Response == nil:
		return false, "DataUnavailable", "no forecast"
	case !out.Verdict.Eligible:
		return false, out.Verdict.Reason, out.Verdict.Detail
	case mode != autoscalerv1alpha1.ModeActive:
		return true, "Ready", "ready to activate: the forecast passed every check; Recommend mode still makes no writes"
	}
	return true, "Ready", "the verified forecast may raise the decision"
}

// predictionStatus is status.prediction after the decision.
func (out *v2Outcome) predictionStatus(pol *v2Policy, capacityOK bool, mode string) *autoscalerv1alpha1.PredictionStatus {
	st := out.published.DeepCopy()
	_, st.Reason, _ = out.readiness(capacityOK, mode)
	if resp := out.Response; resp != nil {
		st.Phase, st.Model, st.IssuanceID = resp.TrustState, resp.SelectedCandidate, resp.IssuanceID
		origin, until := metav1.NewTime(time.Unix(resp.DecisionOrigin, 0).UTC()), metav1.NewTime(time.Unix(resp.ValidUntil, 0).UTC())
		st.DecisionOrigin, st.ValidUntil = &origin, &until
		st.BaseWindowMaxRPS = v2FormatRPS(resp.BaseWindowMaxRPS)
		if resp.UpperWindowMaxRPS != nil {
			st.UpperWindowMaxRPS = v2FormatRPS(*resp.UpperWindowMaxRPS)
		}
		st.CalibrationWindows, st.ProspectiveWindows = int32Clamp(resp.CalibrationWindows), int32Clamp(resp.ProspectiveWindows)
		if resp.GateCoverageRatio != nil {
			st.GateCoverageBps = int32Ptr(int64(math.Round(*resp.GateCoverageRatio * v2BPS)))
		}
	}
	st.Eligible = out.Verdict.Eligible && capacityOK
	if pol != nil {
		st.PredictiveReplicas, st.CappedReplicas, st.CapBinding = &pol.Uncapped, &pol.Capped, pol.Binding
	}
	return st
}

func int32Clamp(v int64) int32 {
	if v > math.MaxInt32 {
		return math.MaxInt32
	}
	if v < math.MinInt32 {
		return math.MinInt32
	}
	return int32(v)
}

// v2DecisionDetail is the v2 part of the decision ledger line: the forecast it used or rejected, by issuance ID, and
// how the predictive term was formed.
type v2DecisionDetail struct {
	IssuanceID          *string  `json:"issuance_id"`
	DecisionOrigin      *string  `json:"decision_origin"`
	ValidUntil          *string  `json:"valid_until"`
	Cached              bool     `json:"cached"`
	TrustState          *string  `json:"trust_state"`
	ServiceReason       *string  `json:"service_reason"`
	ServiceEligible     bool     `json:"service_eligible"`
	Accepted            bool     `json:"accepted"`
	Verdict             string   `json:"verdict"` // Ready, or why the forecast was not used
	VerdictDetail       string   `json:"verdict_detail,omitempty"`
	BaseWindowMaxRPS    *float64 `json:"base_window_max_rps"`
	UpperWindowMaxRPS   *float64 `json:"upper_window_max_rps"`
	CapacityRPM         *float64 `json:"capacity_rpm_per_replica"`
	PredictiveReplicas  *int32   `json:"predictive_replicas_uncapped"`
	PredictiveAllowance *int32   `json:"predictive_extra_allowance"`
	CappedReplicas      *int32   `json:"predictive_replicas"`
	CapBinding          bool     `json:"predictive_cap_binding"`
	LeadSeconds         *int32   `json:"lead_seconds"`
	StartupDelaySeconds *int32   `json:"startup_delay_seconds"`
	StartupMethod       string   `json:"startup_method"`
	SettingsSHA256      string   `json:"settings_sha256,omitempty"`
}

func (out *v2Outcome) ledger(pol *v2Policy, capacityRPM float64, capacityOK bool, mode string) *v2DecisionDetail {
	d := &v2DecisionDetail{Cached: out.Cached, SettingsSHA256: out.published.SettingsSHA256}
	_, d.Verdict, d.VerdictDetail = out.readiness(capacityOK, mode)
	if out.startup != nil {
		d.StartupMethod = out.startup.Method
	}
	if l := out.published.Lead; l != nil {
		sec, sd := l.Seconds, l.StartupDelaySeconds
		d.LeadSeconds, d.StartupDelaySeconds = &sec, &sd
	}
	if capacityOK {
		c := capacityRPM
		d.CapacityRPM = &c
	}
	if resp := out.Response; resp != nil {
		id, origin, until := resp.IssuanceID, v2FormatTS(resp.DecisionOrigin), v2FormatTS(resp.ValidUntil)
		ts, reason, base := resp.TrustState, resp.Reason, resp.BaseWindowMaxRPS
		d.IssuanceID, d.DecisionOrigin, d.ValidUntil, d.TrustState, d.ServiceReason = &id, &origin, &until, &ts, &reason
		d.ServiceEligible, d.BaseWindowMaxRPS, d.UpperWindowMaxRPS = resp.PredictiveEligible, &base, resp.UpperWindowMaxRPS
	}
	if pol != nil {
		d.Accepted = true
		d.PredictiveReplicas, d.PredictiveAllowance, d.CappedReplicas, d.CapBinding = &pol.Uncapped, &pol.Allowance, &pol.Capped, pol.Binding
	}
	return d
}

// recordV2Metrics publishes the predictive extra, whether the cap bound, and the trust phase (one-hot), or removes
// them for an autoscaler that is not on the v2 path.
func recordV2Metrics(ns, name string, out *v2Outcome, pol *v2Policy, reactive int32) {
	if out == nil {
		forgetV2Metrics(ns, name)
		return
	}
	if pol != nil {
		extra := pol.Capped - reactive
		if extra < 0 {
			extra = 0
		}
		predictiveExtraReplicasGauge.WithLabelValues(ns, name).Set(float64(extra))
		predictiveCappedGauge.WithLabelValues(ns, name).Set(map[bool]float64{true: 1, false: 0}[pol.Binding])
	} else {
		predictiveExtraReplicasGauge.DeleteLabelValues(ns, name)
		predictiveCappedGauge.DeleteLabelValues(ns, name)
	}
	phase := ""
	if out.Response != nil {
		phase = out.Response.TrustState
	}
	for _, p := range v2Phases {
		if phase == "" {
			predictionGateGauge.DeleteLabelValues(ns, name, p)
			continue
		}
		predictionGateGauge.WithLabelValues(ns, name, p).Set(map[bool]float64{true: 1, false: 0}[p == phase])
	}
}

func forgetV2Metrics(ns, name string) {
	predictiveExtraReplicasGauge.DeleteLabelValues(ns, name)
	predictiveCappedGauge.DeleteLabelValues(ns, name)
	for _, p := range v2Phases {
		predictionGateGauge.DeleteLabelValues(ns, name, p)
	}
}

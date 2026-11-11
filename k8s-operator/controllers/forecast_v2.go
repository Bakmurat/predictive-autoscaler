package controllers

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	stderrors "errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"time"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Forecast contract v2 (window-max-rps/v1, design C2 sections 5 and 7), the operator's side. The statistical
// forecasting service answers POST /v2/predict with a calibrated upper bound U of the peak request rate (RPS) over an
// immutable future window W_t = {t+Δ, ..., t+L}. The operator does not take the service's word for anything that
// authorizes a write: it decodes the answer strictly, checks it against the request it sent (identity, settings
// fingerprint, origin, lead, evidence epoch), checks the evidence and the remaining window on its own clock, and only
// then lets ceil(U / capacity), capped by spec.prediction.predictiveExtra, raise the decision. The v1 heuristics
// (confidence damping, the overestimate cap, the divergence sanity check) never run on this path.
//
// The settings fingerprint and the evidence key are computed exactly as ml-engine/forecasting/contracts.py does; the
// shared fixtures in testdata/v2-contract-fixtures.json (k8s-operator/hack/gen_v2_fixtures.py) keep the two in step.

const (
	forecastContractV2     = "window-max-rps/v1"
	v2SettingsSchema       = "forecast-settings/v1"
	v2EvidenceSchema       = "forecast-evidence-key/v1"
	v2Unit                 = "requests/second"
	v2MaxHorizonSeconds    = 3600
	v2MaxSteps             = 60
	v2MaxHistoryDays       = 28
	v2LegacyLSTMInterval   = 600
	v2BPS                  = 10000
	v2MaxResponseBytes     = 1 << 20
	v2IssueClockSkew       = 2 * time.Second // an issuance may be this far ahead of the operator's clock
	v2DefaultActuationSecs = 15
)

// v2CalibrationPolicyRevision is contracts.CALIBRATION_POLICY_REVISION: the calibration method revision. It is part of
// the settings fingerprint and (through the calibration settings) of the evidence key, so an operator and a forecaster
// that disagree about the score's meaning can never exchange an accepted answer.
const v2CalibrationPolicyRevision = "normalized-window-score/two-part-scale/v1"

// v2TrustPolicyRevision is contracts.TRUST_POLICY_REVISION: what the forecaster's coverage rules mean. It is part of
// the settings fingerprint (with the policy's four false-alarm budgets and the dependence of the model its thresholds
// are computed under) and of the evidence key. The thresholds themselves are the forecaster's (forecasting/trust.py);
// the operator never receives miss history.
const v2TrustPolicyRevision = "budgeted-coverage-tests/v2"

var (
	v2AllowedIntervals = []int64{60, 120, 180, 240, 300, 360, 480, 600}
	v2Phases           = []string{"Collecting", "Calibrating", "Shadow", "Trusted", "Degraded"}
	v2Reasons          = []string{"HistoryInsufficient", "CalibrationInsufficient", "ShadowInsufficient", "CoverageLow",
		"DataUnavailable", "IdentityMismatch", "StateUnavailable", "ConfigurationNotValidated", "LeadInsufficient",
		"ReplayGateMissing", "Ready"}
	v2ModelFamilies = []string{"seasonal_naive", "statistical", "lstm"}
	v2SHA256        = regexp.MustCompile(`^[0-9a-f]{64}$`)
	v2GridTime      = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})Z$`)
	v2MilliTime     = regexp.MustCompile(`^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})\.(\d{3})Z$`)
	v2Integer       = regexp.MustCompile(`^-?(0|[1-9][0-9]*)$`)
)

func v2In(v string, set []string) bool {
	for _, s := range set {
		if s == v {
			return true
		}
	}
	return false
}

// v2Selected reports whether an autoscaler uses forecast contract v2: forecasting on and engine statistical. Every other
// autoscaler (no engine, legacyLstm) keeps the v1 path unchanged.
func v2Selected(a *autoscalerv1alpha1.PredictiveAutoscaler) bool {
	return predictionEnabled(a) && a.Spec.Prediction.Engine == autoscalerv1alpha1.EngineStatistical
}

// --- settings -------------------------------------------------------------------------------------------------

// v2Settings are the resolved v2 settings, the Go mirror of contracts.ForecastSettings (same names, same defaults).
type v2Settings struct {
	Engine                string
	SampleIntervalSeconds int64
	Seasonality           string
	HistoryDays           int64
	HorizonSeconds        int64
	MinLeadSeconds        int64 // the lead is never shorter than this, rounded up to the grid (0: no minimum)
	UpdateIntervalSeconds int64 // the reconcile period as applied (v2EffectiveUpdateInterval): part of the identity
	SettlementSeconds     int64
	OutcomeGraceSeconds   int64
	MiscoverageBps        int64
	Calibration           [9]int64 // v2CalibrationKeys; the last three are the method's scale-fit constants (not in the spec)
	Trust                 [20]int64
	ExtraMaxReplicas      int64
	ExtraMaxPercent       int64
	Families              []string // sorted; seasonalNaive always present
}

// v2Families are the forecast families spec.prediction.families may list.
var v2Families = []string{"seasonalNaive", "statistical"}

var v2CalibrationKeys = [9]string{"min_windows", "max_scores", "retention_days", "eta_bps", "min_miscoverage_bps",
	"max_miscoverage_bps", "scale_bins", "scale_floor_bps", "scale_min_season_bps"}

// The last five are the trust policy's four false-alarm budgets and the dependence of the model its thresholds are
// computed under: resolved, never read from the spec.
var v2TrustKeys = [20]string{"min_shadow_hours", "min_prospective_windows", "coverage_tolerance_bps",
	"worst_day_min_outcomes", "min_availability_bps", "acute_consecutive_misses", "acute_recent_windows",
	"repromotion_hours", "repromotion_windows", "challenger_min_pairs", "challenger_min_hours",
	"challenger_loss_ratio_bps", "challenger_exit_loss_ratio_bps", "challenger_cost_ratio_bps", "max_fit_age_hours",
	"rolling_alarm_ppm", "consecutive_alarm_ppm", "day_alarm_ppm", "seven_day_alarm_ppm", "miss_dependence_bps"}

const (
	v2CalMinWindows = iota
	v2CalMaxScores
	v2CalRetentionDays
	v2CalEtaBps
	v2CalMinMiscoverage
	v2CalMaxMiscoverage
)

const (
	v2TrustMinShadowHours = iota
	v2TrustMinProspectiveWindows
	_
	_
	_
	_
	v2TrustAcuteRecentWindows
	_
	_
	_
	_
	v2TrustChallengerLossRatio
	v2TrustChallengerExitLossRatio
)

func v2Default() v2Settings {
	return v2Settings{Engine: autoscalerv1alpha1.EngineStatistical, SampleIntervalSeconds: 300, Seasonality: "daily",
		HistoryDays: 14, HorizonSeconds: 3600, UpdateIntervalSeconds: defaultReconcileSeconds,
		SettlementSeconds: 120, OutcomeGraceSeconds: 600, MiscoverageBps: 1000,
		Calibration: [9]int64{128, 512, 7, 50, 100, 2500, 8, 500, 8000},
		Trust: [20]int64{48, 64, 500, 24, 9500, 5, 64, 24, 32, 64, 48, 9500, 10000, 10200, 12, 33000, 7000, 8000, 2000,
			1750},
		ExtraMaxReplicas: 2, ExtraMaxPercent: 50, Families: []string{"seasonalNaive", "statistical"}}
}

type v2Bound struct {
	key    string
	value  *int32
	lo, hi int64
}

// v2EffectiveUpdateInterval is contracts.effective_update_interval: spec.prediction.updateIntervalSeconds as the
// reconciler applies it (absent or 0: defaultReconcileSeconds; never below minReconcileSeconds). It decides how often
// a bound is issued, so it is part of the settings fingerprint and of the evidence key.
func v2EffectiveUpdateInterval(configured int32) int64 {
	switch {
	case configured == 0:
		return defaultReconcileSeconds
	case configured < minReconcileSeconds:
		return minReconcileSeconds
	}
	return int64(configured)
}

func v2Set(dst *int64, b v2Bound) error {
	if b.value == nil {
		return nil
	}
	v := int64(*b.value)
	if v < b.lo || v > b.hi {
		return fmt.Errorf("spec.prediction.%s must be an integer in %d..%d, got %d", b.key, b.lo, b.hi, v)
	}
	*dst = v
	return nil
}

// resolveV2Settings resolves spec.prediction like contracts.resolve_settings: field bounds, then the cross-field
// feasibility checks. The CRD enforces the per-field bounds too; they are checked again here.
func resolveV2Settings(spec autoscalerv1alpha1.PredictiveAutoscalerSpec) (v2Settings, error) {
	p := spec.Prediction
	s := v2Default()
	s.UpdateIntervalSeconds = v2EffectiveUpdateInterval(p.UpdateIntervalSeconds)
	if p.Enabled != nil && !*p.Enabled {
		return s, fmt.Errorf("forecasting is disabled for this autoscaler (spec.prediction.enabled)")
	}
	if p.Engine != "" {
		if p.Engine != autoscalerv1alpha1.EngineStatistical && p.Engine != autoscalerv1alpha1.EngineLegacyLSTM {
			return s, fmt.Errorf("spec.prediction.engine %q is unknown", p.Engine)
		}
		s.Engine = p.Engine
	}
	if p.Seasonality != "" {
		if p.Seasonality != "daily" {
			return s, fmt.Errorf("spec.prediction.seasonality %q is not supported (admitted: daily)", p.Seasonality)
		}
		s.Seasonality = p.Seasonality
	}
	if p.SampleIntervalSeconds != nil {
		v := int64(*p.SampleIntervalSeconds)
		ok := false
		for _, a := range v2AllowedIntervals {
			ok = ok || a == v
		}
		if !ok {
			return s, fmt.Errorf("spec.prediction.sampleIntervalSeconds must be one of %v, got %d", v2AllowedIntervals, v)
		}
		s.SampleIntervalSeconds = v
	}
	if s.Engine == autoscalerv1alpha1.EngineLegacyLSTM && s.SampleIntervalSeconds != v2LegacyLSTMInterval {
		return s, fmt.Errorf("engine legacyLstm supports only the %d s grid", v2LegacyLSTMInterval)
	}
	horizonMin := int64(p.HorizonMinutes)
	if horizonMin == 0 {
		horizonMin = s.HorizonSeconds / 60
	}
	if horizonMin <= 0 {
		return s, fmt.Errorf("spec.prediction.horizonMinutes must be a positive integer, got %d", horizonMin)
	}
	s.HorizonSeconds = horizonMin * 60
	dt := s.SampleIntervalSeconds
	if s.HorizonSeconds > v2MaxHorizonSeconds || s.HorizonSeconds%dt != 0 || s.HorizonSeconds/dt > v2MaxSteps {
		return s, fmt.Errorf("horizon %d min must be at most %d min, at most %d steps and a multiple of the %d s cadence",
			horizonMin, v2MaxHorizonSeconds/60, v2MaxSteps, dt)
	}
	for _, b := range []struct {
		dst *int64
		b   v2Bound
	}{
		{&s.HistoryDays, v2Bound{"historyDays", p.HistoryDays, 1, v2MaxHistoryDays}},
		{&s.MinLeadSeconds, v2Bound{"minLeadSeconds", p.MinLeadSeconds, 0, v2MaxHorizonSeconds}},
		{&s.SettlementSeconds, v2Bound{"settlementSeconds", p.SettlementSeconds, 0, 900}},
		{&s.OutcomeGraceSeconds, v2Bound{"outcomeGraceSeconds", p.OutcomeGraceSeconds, 0, 3600}},
	} {
		if err := v2Set(b.dst, b.b); err != nil {
			return s, err
		}
	}
	if r := p.Risk; r != nil {
		if err := v2Set(&s.MiscoverageBps, v2Bound{"risk.miscoverageBps", r.MiscoverageBps, 100, 2500}); err != nil {
			return s, err
		}
	}
	if c := p.Calibration; c != nil {
		for i, b := range []v2Bound{{"calibration.minWindows", c.MinWindows, 16, 4096},
			{"calibration.maxScores", c.MaxScores, 16, 4096}, {"calibration.retentionDays", c.RetentionDays, 1, 28},
			{"calibration.etaBps", c.EtaBps, 1, 1000}, {"calibration.minMiscoverageBps", c.MinMiscoverageBps, 1, 5000},
			{"calibration.maxMiscoverageBps", c.MaxMiscoverageBps, 1, 5000}} {
			if err := v2Set(&s.Calibration[i], b); err != nil {
				return s, err
			}
		}
	}
	if t := p.Trust; t != nil {
		for i, b := range []v2Bound{{"trust.minShadowHours", t.MinShadowHours, 1, 24 * 28},
			{"trust.minProspectiveWindows", t.MinProspectiveWindows, 1, 4096},
			{"trust.coverageToleranceBps", t.CoverageToleranceBps, 0, 2500},
			{"trust.worstDayMinOutcomes", t.WorstDayMinOutcomes, 1, 1440},
			{"trust.minAvailabilityBps", t.MinAvailabilityBps, 0, v2BPS},
			{"trust.acuteConsecutiveMisses", t.AcuteConsecutiveMisses, 1, 1000},
			{"trust.acuteRecentWindows", t.AcuteRecentWindows, 1, 4096},
			{"trust.repromotionHours", t.RepromotionHours, 0, 24 * 28},
			{"trust.repromotionWindows", t.RepromotionWindows, 1, 4096},
			{"trust.challengerMinPairs", t.ChallengerMinPairs, 1, 4096},
			{"trust.challengerMinHours", t.ChallengerMinHours, 1, 24 * 28},
			{"trust.challengerLossRatioBps", t.ChallengerLossRatioBps, 1, v2BPS},
			{"trust.challengerExitLossRatioBps", t.ChallengerExitLossRatioBps, 1, 2 * v2BPS},
			{"trust.challengerCostRatioBps", t.ChallengerCostRatioBps, v2BPS, 2 * v2BPS},
			{"trust.maxFitAgeHours", t.MaxFitAgeHours, 1, 24}} {
			if err := v2Set(&s.Trust[i], b); err != nil {
				return s, err
			}
		}
	}
	if e := p.PredictiveExtra; e != nil {
		if err := v2Set(&s.ExtraMaxReplicas, v2Bound{"predictiveExtra.maxReplicas", e.MaxReplicas, 0, 1000}); err != nil {
			return s, err
		}
		if err := v2Set(&s.ExtraMaxPercent, v2Bound{"predictiveExtra.maxPercent", e.MaxPercent, 0, 1000}); err != nil {
			return s, err
		}
	}
	if p.Families != nil { // [] is refused (the CRD needs one item); absent keeps both families
		if len(p.Families) == 0 {
			return s, fmt.Errorf("spec.prediction.families must not be empty")
		}
		seen := map[string]bool{}
		for _, f := range p.Families {
			if !v2In(f, v2Families) || seen[f] {
				return s, fmt.Errorf("spec.prediction.families must list distinct families of %v, got %v", v2Families, p.Families)
			}
			seen[f] = true
		}
		if !seen["seasonalNaive"] {
			return s, fmt.Errorf("spec.prediction.families must include seasonalNaive (the statistical family is selected " +
				"only over a trusted seasonal-naive forecast)")
		}
		s.Families = append([]string(nil), p.Families...)
		sort.Strings(s.Families)
	}
	return s, s.feasible()
}

// v2Rank is k = ceil((n+1)(1-a)) in exact integer arithmetic.
func v2Rank(n, miscoverageBps int64) int64 {
	num := (n + 1) * (v2BPS - miscoverageBps)
	return (num + v2BPS - 1) / v2BPS
}

func (s v2Settings) feasible() error {
	c := s.Calibration
	switch {
	case s.MiscoverageBps < c[v2CalMinMiscoverage] || s.MiscoverageBps > c[v2CalMaxMiscoverage]:
		return fmt.Errorf("the nominal miscoverage must lie within the adaptive bounds")
	case c[v2CalMinMiscoverage] >= c[v2CalMaxMiscoverage]:
		return fmt.Errorf("calibration.minMiscoverageBps must be below maxMiscoverageBps")
	case c[v2CalMaxScores] < c[v2CalMinWindows]:
		return fmt.Errorf("calibration.maxScores must hold at least minWindows scores")
	case v2Rank(c[v2CalMinWindows], c[v2CalMinMiscoverage]) > c[v2CalMinWindows]:
		return fmt.Errorf("calibration.minMiscoverageBps is infeasible: with minWindows scores the order-statistic rank " +
			"exceeds the sample count")
	case c[v2CalRetentionDays]*86400/s.HorizonSeconds < c[v2CalMinWindows]:
		return fmt.Errorf("calibration.retentionDays cannot hold minWindows nonoverlapping windows at this horizon")
	case s.Engine == autoscalerv1alpha1.EngineStatistical && s.HistoryDays < 2:
		// The legacy engine fits no scale and keeps its one-day minimum (the CRD's rule on spec.prediction agrees).
		return fmt.Errorf("historyDays must be at least 2 with engine statistical: the calibration scale is fitted " +
			"from residuals one day apart")
	case s.Trust[v2TrustAcuteRecentWindows] > c[v2CalMaxScores] || s.Trust[v2TrustMinProspectiveWindows] > c[v2CalMaxScores]:
		return fmt.Errorf("trust window counts exceed the calibration buffer")
	case s.Trust[v2TrustChallengerExitLossRatio] < s.Trust[v2TrustChallengerLossRatio]:
		return fmt.Errorf("trust.challengerExitLossRatioBps must not be below challengerLossRatioBps")
	case s.Trust[v2TrustMinShadowHours] > c[v2CalRetentionDays]*24:
		return fmt.Errorf("trust.minShadowHours exceeds the retention")
	case s.Trust[v2TrustMinProspectiveWindows] < s.Trust[v2TrustAcuteRecentWindows]:
		// The forecaster's rolling threshold is computed for a full window.
		return fmt.Errorf("trust.minProspectiveWindows must be at least acuteRecentWindows")
	case v2MinimumLead(s) > s.HorizonSeconds:
		return fmt.Errorf("minLeadSeconds, rounded up to the cadence, exceeds the horizon")
	}
	return nil
}

func (s v2Settings) group(keys []string, vals []int64) map[string]interface{} {
	m := make(map[string]interface{}, len(keys))
	for i, k := range keys {
		m[k] = vals[i]
	}
	return m
}

func (s v2Settings) riskMap() map[string]interface{} {
	return map[string]interface{}{"miscoverage_bps": s.MiscoverageBps}
}

func (s v2Settings) calibrationMap() map[string]interface{} {
	m := s.group(v2CalibrationKeys[:], s.Calibration[:])
	m["method"] = v2CalibrationPolicyRevision
	return m
}

func (s v2Settings) trustMap() map[string]interface{} {
	m := s.group(v2TrustKeys[:], s.Trust[:])
	m["policy"] = v2TrustPolicyRevision
	return m
}

// canonical is contracts.ForecastSettings.canonical(): {"schema": ..., "settings": asdict(settings)}.
func (s v2Settings) canonical() map[string]interface{} {
	return map[string]interface{}{"schema": v2SettingsSchema, "settings": map[string]interface{}{
		"engine": s.Engine, "sample_interval_seconds": s.SampleIntervalSeconds, "seasonality": s.Seasonality,
		"history_days": s.HistoryDays, "horizon_seconds": s.HorizonSeconds, "min_lead_seconds": s.MinLeadSeconds,
		"update_interval_seconds": s.UpdateIntervalSeconds,
		"settlement_seconds":      s.SettlementSeconds,
		"outcome_grace_seconds": s.OutcomeGraceSeconds, "risk": s.riskMap(), "calibration": s.calibrationMap(),
		"trust":            s.trustMap(),
		"predictive_extra": map[string]interface{}{"max_replicas": s.ExtraMaxReplicas, "max_percent": s.ExtraMaxPercent},
		"families":         s.Families,
	}}
}

// v2CanonicalJSON is contracts.canonical_json: sorted keys, no whitespace, ASCII only.
func v2CanonicalJSON(v interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	out := bytes.TrimRight(buf.Bytes(), "\n")
	for _, b := range out {
		if b > 0x7e {
			return nil, fmt.Errorf("canonical JSON must be ASCII")
		}
	}
	return out, nil
}

func v2Hash(v interface{}) string {
	raw, err := v2CanonicalJSON(v)
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func (s v2Settings) fingerprint() string { return v2Hash(s.canonical()) }

// v2EvidenceKey is contracts.evidence_key: the identity forecast evidence belongs to (the calibration epoch).
func v2EvidenceKey(paUID, targetUID, querySHA string, s v2Settings, leadSeconds int64) string {
	return v2Hash(map[string]interface{}{"schema": v2EvidenceSchema, "autoscaler_uid": paUID, "target_uid": targetUID,
		"query_sha256": querySHA, "signal_contract": metricContract, "forecast_contract": forecastContractV2,
		"interval_seconds": s.SampleIntervalSeconds, "lead_seconds": leadSeconds,
		"min_lead_seconds": s.MinLeadSeconds, "update_interval_seconds": s.UpdateIntervalSeconds, "engine": s.Engine,
		"seasonality": s.Seasonality, "history_days": s.HistoryDays, "settlement_seconds": s.SettlementSeconds,
		"outcome_grace_seconds": s.OutcomeGraceSeconds, "risk": s.riskMap(), "calibration": s.calibrationMap(),
		"trust_policy": v2TrustPolicyRevision})
}

// --- lead and window ------------------------------------------------------------------------------------------

// v2AutomaticLead is contracts.automatic_lead_seconds: L = max(ceil((D + C + A + Δ) / Δ), ceil(M / Δ)) * Δ, with M the
// configured minimum lead (spec.prediction.minLeadSeconds). The startup budget D is an input only: a minimum lead
// lengthens the forecast window and never changes the measured startup delay.
func v2AutomaticLead(startup, decisionInterval, allowance, interval, minLead int64) int64 {
	total := startup + decisionInterval + allowance + interval
	steps := (total + interval - 1) / interval
	if m := (minLead + interval - 1) / interval; m > steps {
		steps = m
	}
	return steps * interval
}

// v2MinimumLead is contracts.minimum_lead_seconds: the configured minimum lead rounded up to the grid.
func v2MinimumLead(s v2Settings) int64 {
	dt := s.SampleIntervalSeconds
	return (s.MinLeadSeconds + dt - 1) / dt * dt
}

// v2LeadProblem mirrors contracts.resolve_lead for a lead the operator is about to publish: "" when the service will
// accept it for this generation and these settings.
func v2LeadProblem(l *autoscalerv1alpha1.PredictionLead, s v2Settings, generation int64) string {
	if l == nil {
		return "no lead"
	}
	if l.Method != "configured" && l.Method != "measured" {
		return fmt.Sprintf("lead method %q is unknown", l.Method)
	}
	if l.Seconds < 0 || l.StartupDelaySeconds < 0 || l.ActuationAllowanceSeconds < 0 || l.SampleCount < 0 || l.ObservedGeneration < 0 {
		return "lead fields must be non-negative integers"
	}
	if l.ObservedGeneration != generation {
		return fmt.Sprintf("the lead was selected for generation %d, the object is at %d", l.ObservedGeneration, generation)
	}
	dt, L := s.SampleIntervalSeconds, int64(l.Seconds)
	if L < dt || L%dt != 0 || L > s.HorizonSeconds {
		return "the lead must be a positive multiple of the cadence within the horizon"
	}
	if L < int64(l.StartupDelaySeconds)+dt+int64(l.ActuationAllowanceSeconds) {
		return "the lead does not cover startup delay + decision interval + actuation allowance"
	}
	if L < v2MinimumLead(s) {
		return "the lead is below the configured minimum lead (spec.prediction.minLeadSeconds)"
	}
	return ""
}

// v2Window is W_t = {t+Δ, ..., t+L} for origin t (epoch seconds).
type v2Window struct {
	Origin, Interval, Lead int64
}

func (w v2Window) targets() []int64 {
	out := make([]int64, 0, w.Lead/w.Interval)
	for k := int64(1); k <= w.Lead/w.Interval; k++ {
		out = append(out, w.Origin+k*w.Interval)
	}
	return out
}

// validUntil is min(t + Δ - A, t + L - D - C - A) with C = Δ: the last instant a write may act on the window.
func (w v2Window) validUntil(startup, allowance int64) int64 {
	a := w.Origin + w.Interval - allowance
	b := w.Origin + w.Lead - startup - w.Interval - allowance
	if b < a {
		return b
	}
	return a
}

// actionableAt: now + A precedes the first target and the deadline has not passed (contracts.actionable_at).
func (w v2Window) actionableAt(nowMs, startup, allowance int64) bool {
	return nowMs+allowance*1000 < (w.Origin+w.Interval)*1000 && nowMs < w.validUntil(startup, allowance)*1000
}

// --- timestamps -----------------------------------------------------------------------------------------------

func v2FormatTS(epoch int64) string { return time.Unix(epoch, 0).UTC().Format("2006-01-02T15:04:05Z") }

func v2ParseTS(text, what string) (int64, error) {
	m := v2GridTime.FindStringSubmatch(text)
	if m == nil {
		return 0, fmt.Errorf("%s %q is not an RFC 3339 UTC grid time (whole seconds, Z)", what, text)
	}
	return v2Civil(m[1:7], what, text)
}

func v2ParseMS(text, what string) (int64, error) {
	m := v2MilliTime.FindStringSubmatch(text)
	if m == nil {
		return 0, fmt.Errorf("%s %q is not an RFC 3339 UTC time with milliseconds", what, text)
	}
	s, err := v2Civil(m[1:7], what, text)
	if err != nil {
		return 0, err
	}
	ms, _ := strconv.ParseInt(m[7], 10, 64)
	return s*1000 + ms, nil
}

func v2Civil(f []string, what, text string) (int64, error) {
	n := make([]int, 6)
	for i, x := range f {
		n[i], _ = strconv.Atoi(x)
	}
	t := time.Date(n[0], time.Month(n[1]), n[2], n[3], n[4], n[5], 0, time.UTC)
	if t.Year() != n[0] || int(t.Month()) != n[1] || t.Day() != n[2] || t.Hour() != n[3] || t.Minute() != n[4] || t.Second() != n[5] {
		return 0, fmt.Errorf("%s %q is not a valid date and time", what, text)
	}
	return t.Unix(), nil
}

// --- request --------------------------------------------------------------------------------------------------

// v2Request is the POST /v2/predict body: the v1 reference fields plus the forecast contract, the decision origin and
// the settings fingerprint. Nothing else (no query, history, horizon or risk) is ever sent.
type v2Request struct {
	AutoscalerNamespace  string `json:"autoscaler_namespace"`
	AutoscalerName       string `json:"autoscaler_name"`
	AutoscalerUID        string `json:"autoscaler_uid"`
	AutoscalerGeneration int64  `json:"autoscaler_generation"`
	TargetUID            string `json:"target_uid"`
	MetricQuerySHA256    string `json:"metric_query_sha256"`
	Contract             string `json:"contract"`
	Application          string `json:"application"`
	Namespace            string `json:"namespace"`
	MetricType           string `json:"metric_type"`
	ForecastContract     string `json:"forecast_contract"`
	DecisionOrigin       string `json:"decision_origin"`
	SettingsSHA256       string `json:"settings_sha256"`
}

// v2Binding is everything a v2 answer must echo, fixed when the request is made.
type v2Binding struct {
	AutoscalerNamespace, AutoscalerName, AutoscalerUID string
	Generation                                         int64
	TargetNamespace, TargetName, TargetUID, QuerySHA   string
	Settings                                           v2Settings
	SettingsSHA                                        string
	Origin                                             int64
	Lead                                               autoscalerv1alpha1.PredictionLead
	EvidenceKey                                        string
}

func (b v2Binding) request() v2Request {
	return v2Request{AutoscalerNamespace: b.AutoscalerNamespace, AutoscalerName: b.AutoscalerName,
		AutoscalerUID: b.AutoscalerUID, AutoscalerGeneration: b.Generation, TargetUID: b.TargetUID,
		MetricQuerySHA256: b.QuerySHA, Contract: metricContract, Application: b.TargetName, Namespace: b.TargetNamespace,
		MetricType: "requests", ForecastContract: forecastContractV2, DecisionOrigin: v2FormatTS(b.Origin),
		SettingsSHA256: b.SettingsSHA}
}

// key identifies the binding for the cache: a cached answer is reused only for exactly the same request.
func (b v2Binding) key() string {
	return fmt.Sprintf("%s|%d|%s|%s|%s|%d|%d/%d/%d|%s", b.AutoscalerUID, b.Generation, b.TargetUID, b.QuerySHA,
		b.SettingsSHA, b.Origin, b.Lead.Seconds, b.Lead.StartupDelaySeconds, b.Lead.ActuationAllowanceSeconds, b.EvidenceKey)
}

// --- response -------------------------------------------------------------------------------------------------

// v2Response is a decoded and internally coherent /v2/predict answer (contracts.V2Response). Times are epoch seconds
// (grid) or milliseconds (issue, receipt).
type v2Response struct {
	AutoscalerNamespace, AutoscalerName, AutoscalerUID string
	AutoscalerGeneration                               int64
	TargetNamespace, TargetName, TargetUID             string
	MetricQuerySHA256, Contract, SettingsSHA256        string
	ForecastContract, Unit                             string
	IssuanceID                                         string
	IssuedAtMs, InputEnd, InputReceivedThroughMs       int64
	FitCutoff                                          *int64
	ModelFamily, ModelRecipe                           string
	ArtifactSHA256                                     *string
	CalibrationEpoch                                   string
	CheckpointSHA256                                   *string
	DecisionOrigin, WindowStart, WindowEnd             int64
	TargetTimestamps                                   []int64
	SampleIntervalSeconds, LeadSeconds                 int64
	StartupDelaySeconds, ActuationAllowanceSeconds     int64
	ValidUntil                                         int64
	PointRPS                                           []float64
	BaseWindowMaxRPS                                   float64
	UpperWindowMaxRPS                                  *float64
	NominalMiscoverageBps                              int64
	WorkingMiscoverageRatio                            *float64
	CalibrationWindows, ProspectiveWindows             int64
	GateCoverageRatio, MissingOutcomeRatio             *float64
	SelectedCandidate, TrustState, Reason              string
	PredictiveEligible                                 bool
	EvidenceCutoff                                     *int64
}

// v2ResponseFields is the exact wire key set: every field present, no other.
var v2ResponseFields = []string{"autoscaler_namespace", "autoscaler_name", "autoscaler_uid", "autoscaler_generation",
	"target_namespace", "target_name", "target_uid", "metric_query_sha256", "contract", "settings_sha256",
	"forecast_contract", "unit", "issuance_id", "issued_at", "input_end", "input_received_through", "fit_cutoff",
	"model_family", "model_recipe", "artifact_sha256", "calibration_epoch", "checkpoint_sha256", "decision_origin",
	"window_start", "window_end", "target_timestamps", "sample_interval_seconds", "lead_seconds",
	"startup_delay_seconds", "actuation_allowance_seconds", "valid_until", "point_rps", "base_window_max_rps",
	"upper_window_max_rps", "nominal_miscoverage_bps", "working_miscoverage_ratio", "calibration_windows",
	"prospective_windows", "gate_coverage_ratio", "missing_outcome_ratio", "selected_candidate", "trust_state",
	"reason", "predictive_eligible", "evidence_cutoff"}

// errV2Refused marks every answer the operator refuses to use (malformed, incoherent or bound to another request).
var errV2Refused = stderrors.New("forecast v2 answer refused")

type v2Decoder struct {
	raw map[string]json.RawMessage
	err error
}

func (d *v2Decoder) fail(format string, args ...interface{}) {
	if d.err == nil {
		d.err = fmt.Errorf("%w: %s", errV2Refused, fmt.Sprintf(format, args...))
	}
}

func (d *v2Decoder) isNull(k string) bool {
	return bytes.Equal(bytes.TrimSpace(d.raw[k]), []byte("null"))
}

func (d *v2Decoder) str(k string) string {
	var v string
	if err := json.Unmarshal(d.raw[k], &v); err != nil {
		d.fail("%s is not a string", k)
	}
	return v
}

func (d *v2Decoder) optStr(k string) *string {
	if d.isNull(k) {
		return nil
	}
	v := d.str(k)
	return &v
}

func (d *v2Decoder) integer(k string) int64 {
	t := string(bytes.TrimSpace(d.raw[k]))
	if !v2Integer.MatchString(t) {
		d.fail("%s is not an integer", k)
		return 0
	}
	v, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		d.fail("%s is out of range", k)
	}
	return v
}

func v2Number(raw json.RawMessage) (float64, bool) {
	t := bytes.TrimSpace(raw)
	if len(t) == 0 || t[0] == '"' || t[0] == 't' || t[0] == 'f' || t[0] == 'n' || t[0] == '[' || t[0] == '{' {
		return 0, false
	}
	var v float64
	if err := json.Unmarshal(t, &v); err != nil || math.IsNaN(v) || math.IsInf(v, 0) {
		return 0, false
	}
	return v, true
}

func (d *v2Decoder) number(k string) float64 {
	v, ok := v2Number(d.raw[k])
	if !ok {
		d.fail("%s is not a finite number", k)
	}
	return v
}

func (d *v2Decoder) optNumber(k string) *float64 {
	if d.isNull(k) {
		return nil
	}
	v := d.number(k)
	return &v
}

func (d *v2Decoder) gridTime(k string) int64 {
	v, err := v2ParseTS(d.str(k), k)
	if err != nil {
		d.fail("%v", err)
	}
	return v
}

func (d *v2Decoder) optGridTime(k string) *int64 {
	if d.isNull(k) {
		return nil
	}
	v := d.gridTime(k)
	return &v
}

func (d *v2Decoder) milliTime(k string) int64 {
	v, err := v2ParseMS(d.str(k), k)
	if err != nil {
		d.fail("%v", err)
	}
	return v
}

// decodeV2Response decodes an answer strictly (contracts.response_from_wire) and checks its internal coherence.
func decodeV2Response(body []byte) (*v2Response, error) {
	d := &v2Decoder{}
	if err := json.Unmarshal(body, &d.raw); err != nil || d.raw == nil {
		return nil, fmt.Errorf("%w: the answer is not a JSON object", errV2Refused)
	}
	want := map[string]bool{}
	for _, k := range v2ResponseFields {
		want[k] = true
		if _, ok := d.raw[k]; !ok {
			return nil, fmt.Errorf("%w: field %s is missing", errV2Refused, k)
		}
	}
	for k := range d.raw {
		if !want[k] {
			return nil, fmt.Errorf("%w: unknown field %s", errV2Refused, k)
		}
	}
	r := &v2Response{
		AutoscalerNamespace: d.str("autoscaler_namespace"), AutoscalerName: d.str("autoscaler_name"),
		AutoscalerUID: d.str("autoscaler_uid"), AutoscalerGeneration: d.integer("autoscaler_generation"),
		TargetNamespace: d.str("target_namespace"), TargetName: d.str("target_name"), TargetUID: d.str("target_uid"),
		MetricQuerySHA256: d.str("metric_query_sha256"), Contract: d.str("contract"),
		SettingsSHA256: d.str("settings_sha256"), ForecastContract: d.str("forecast_contract"), Unit: d.str("unit"),
		IssuanceID: d.str("issuance_id"), IssuedAtMs: d.milliTime("issued_at"), InputEnd: d.gridTime("input_end"),
		InputReceivedThroughMs: d.milliTime("input_received_through"), FitCutoff: d.optGridTime("fit_cutoff"),
		ModelFamily: d.str("model_family"), ModelRecipe: d.str("model_recipe"),
		ArtifactSHA256: d.optStr("artifact_sha256"), CalibrationEpoch: d.str("calibration_epoch"),
		CheckpointSHA256: d.optStr("checkpoint_sha256"), DecisionOrigin: d.gridTime("decision_origin"),
		WindowStart: d.gridTime("window_start"), WindowEnd: d.gridTime("window_end"),
		SampleIntervalSeconds: d.integer("sample_interval_seconds"), LeadSeconds: d.integer("lead_seconds"),
		StartupDelaySeconds:       d.integer("startup_delay_seconds"),
		ActuationAllowanceSeconds: d.integer("actuation_allowance_seconds"), ValidUntil: d.gridTime("valid_until"),
		BaseWindowMaxRPS: d.number("base_window_max_rps"), UpperWindowMaxRPS: d.optNumber("upper_window_max_rps"),
		NominalMiscoverageBps:   d.integer("nominal_miscoverage_bps"),
		WorkingMiscoverageRatio: d.optNumber("working_miscoverage_ratio"),
		CalibrationWindows:      d.integer("calibration_windows"), ProspectiveWindows: d.integer("prospective_windows"),
		GateCoverageRatio: d.optNumber("gate_coverage_ratio"), MissingOutcomeRatio: d.optNumber("missing_outcome_ratio"),
		SelectedCandidate: d.str("selected_candidate"), TrustState: d.str("trust_state"), Reason: d.str("reason"),
		EvidenceCutoff: d.optGridTime("evidence_cutoff"),
	}
	if err := json.Unmarshal(d.raw["predictive_eligible"], &r.PredictiveEligible); err != nil {
		d.fail("predictive_eligible is not a boolean")
	}
	var targets []string
	if err := json.Unmarshal(d.raw["target_timestamps"], &targets); err != nil || targets == nil {
		d.fail("target_timestamps is not an array of times")
	}
	if len(targets) > v2MaxSteps {
		d.fail("target_timestamps holds more than %d steps", v2MaxSteps)
		targets = nil
	}
	for _, t := range targets {
		v, err := v2ParseTS(t, "target timestamp")
		if err != nil {
			d.fail("%v", err)
		}
		r.TargetTimestamps = append(r.TargetTimestamps, v)
	}
	var points []json.RawMessage
	if err := json.Unmarshal(d.raw["point_rps"], &points); err != nil || points == nil {
		d.fail("point_rps is not an array")
	}
	if len(points) > v2MaxSteps {
		d.fail("point_rps holds more than %d steps", v2MaxSteps)
		points = nil
	}
	for _, p := range points {
		v, ok := v2Number(p)
		if !ok {
			d.fail("point_rps holds a value that is not a finite number")
		}
		r.PointRPS = append(r.PointRPS, v)
	}
	if d.err != nil {
		return nil, d.err
	}
	if err := r.coherent(); err != nil {
		return nil, fmt.Errorf("%w: %v", errV2Refused, err)
	}
	return r, nil
}

// coherent is contracts.check_response without a request: the answer agrees with itself.
func (r *v2Response) coherent() error {
	if r.ForecastContract != forecastContractV2 || r.Contract != metricContract || r.Unit != v2Unit {
		return fmt.Errorf("unknown forecast contract, signal contract or unit")
	}
	for name, v := range map[string]int64{"autoscaler_generation": r.AutoscalerGeneration,
		"sample_interval_seconds": r.SampleIntervalSeconds, "lead_seconds": r.LeadSeconds,
		"startup_delay_seconds": r.StartupDelaySeconds, "actuation_allowance_seconds": r.ActuationAllowanceSeconds,
		"nominal_miscoverage_bps": r.NominalMiscoverageBps, "calibration_windows": r.CalibrationWindows,
		"prospective_windows": r.ProspectiveWindows, "issued_at": r.IssuedAtMs,
		"input_received_through": r.InputReceivedThroughMs, "input_end": r.InputEnd, "decision_origin": r.DecisionOrigin,
		"window_start": r.WindowStart, "window_end": r.WindowEnd, "valid_until": r.ValidUntil} {
		if v < 0 {
			return fmt.Errorf("%s must be non-negative", name)
		}
	}
	if !v2In(r.TrustState, v2Phases) || !v2In(r.Reason, v2Reasons) {
		return fmt.Errorf("unknown trust state or reason")
	}
	if !v2In(r.ModelFamily, v2ModelFamilies) || !v2In(r.SelectedCandidate, v2ModelFamilies) {
		return fmt.Errorf("unknown model family")
	}
	if !v2SHA256.MatchString(r.MetricQuerySHA256) || !v2SHA256.MatchString(r.SettingsSHA256) {
		return fmt.Errorf("a hash is not a sha256")
	}
	dt := r.SampleIntervalSeconds
	allowed := false
	for _, a := range v2AllowedIntervals {
		allowed = allowed || a == dt
	}
	if !allowed || r.DecisionOrigin%dt != 0 || r.LeadSeconds%dt != 0 || r.LeadSeconds < dt {
		return fmt.Errorf("the window is not on an admitted grid")
	}
	// Bounded before any allocation or timestamp arithmetic: the v2 contract allows at most a 60-minute, 60-step window.
	if r.LeadSeconds > v2MaxHorizonSeconds || r.LeadSeconds/dt > v2MaxSteps || r.StartupDelaySeconds > v2MaxHorizonSeconds ||
		r.ActuationAllowanceSeconds > v2MaxHorizonSeconds || len(r.TargetTimestamps) > v2MaxSteps || len(r.PointRPS) > v2MaxSteps {
		return fmt.Errorf("the window exceeds the contract's %d s / %d steps", v2MaxHorizonSeconds, v2MaxSteps)
	}
	w := v2Window{r.DecisionOrigin, dt, r.LeadSeconds}
	want := w.targets()
	if r.WindowStart != r.DecisionOrigin || r.WindowEnd != r.DecisionOrigin+r.LeadSeconds || len(want) != len(r.TargetTimestamps) {
		return fmt.Errorf("window bounds or target timestamps do not match the origin and lead")
	}
	for i := range want {
		if want[i] != r.TargetTimestamps[i] {
			return fmt.Errorf("window bounds or target timestamps do not match the origin and lead")
		}
	}
	if r.ValidUntil != w.validUntil(r.StartupDelaySeconds, r.ActuationAllowanceSeconds) {
		return fmt.Errorf("valid_until does not follow from the window and the lead")
	}
	if len(r.PointRPS) != len(r.TargetTimestamps) {
		return fmt.Errorf("point_rps must hold one rate per target")
	}
	peak := math.Inf(-1)
	for _, p := range r.PointRPS {
		if p < 0 {
			return fmt.Errorf("point_rps must be non-negative")
		}
		peak = math.Max(peak, p)
	}
	if r.BaseWindowMaxRPS < 0 || r.BaseWindowMaxRPS != peak {
		return fmt.Errorf("base_window_max_rps must be the maximum of point_rps")
	}
	if r.UpperWindowMaxRPS != nil && *r.UpperWindowMaxRPS < 0 {
		return fmt.Errorf("upper_window_max_rps must be null or non-negative")
	}
	for name, x := range map[string]*float64{"gate_coverage_ratio": r.GateCoverageRatio,
		"missing_outcome_ratio": r.MissingOutcomeRatio, "working_miscoverage_ratio": r.WorkingMiscoverageRatio} {
		if x != nil && (*x < 0 || *x > 1) {
			return fmt.Errorf("%s must be null or within 0..1", name)
		}
	}
	if r.GateCoverageRatio != nil && r.CalibrationWindows == 0 && r.ProspectiveWindows == 0 {
		return fmt.Errorf("coverage without any scored window")
	}
	if r.InputEnd > r.DecisionOrigin || (r.FitCutoff != nil && *r.FitCutoff > r.InputEnd) {
		return fmt.Errorf("input or fit cutoff after the decision origin")
	}
	if r.InputReceivedThroughMs > r.IssuedAtMs || r.IssuedAtMs < r.DecisionOrigin*1000 {
		return fmt.Errorf("issue/receipt times are out of order")
	}
	if r.EvidenceCutoff != nil && *r.EvidenceCutoff*1000 > r.IssuedAtMs {
		return fmt.Errorf("evidence cutoff after the issue time")
	}
	if r.TargetNamespace != r.AutoscalerNamespace {
		return fmt.Errorf("the target must be in the autoscaler's namespace")
	}
	if r.IssuedAtMs >= r.TargetTimestamps[0]*1000 {
		return fmt.Errorf("the issuance is not before the first target")
	}
	if r.PredictiveEligible && (r.UpperWindowMaxRPS == nil || r.TrustState != "Trusted" || r.Reason != "Ready") {
		return fmt.Errorf("an eligible forecast needs an upper bound, trust state Trusted and reason Ready")
	}
	if r.PredictiveEligible && !w.actionableAt(r.IssuedAtMs, r.StartupDelaySeconds, r.ActuationAllowanceSeconds) {
		return fmt.Errorf("an eligible forecast must be issued while its window is still actionable")
	}
	return nil
}

// bound is contracts.check_response with a request: the answer is for exactly the request sent.
func (r *v2Response) bound(b v2Binding) error {
	if r.AutoscalerNamespace != b.AutoscalerNamespace || r.AutoscalerName != b.AutoscalerName ||
		r.AutoscalerUID != b.AutoscalerUID || r.AutoscalerGeneration != b.Generation ||
		r.TargetNamespace != b.TargetNamespace || r.TargetName != b.TargetName || r.TargetUID != b.TargetUID ||
		r.MetricQuerySHA256 != b.QuerySHA || r.SettingsSHA256 != b.SettingsSHA || r.DecisionOrigin != b.Origin ||
		r.SampleIntervalSeconds != b.Settings.SampleIntervalSeconds || r.NominalMiscoverageBps != b.Settings.MiscoverageBps {
		return fmt.Errorf("%w: the answer does not echo the request's identity, settings and origin", errV2Refused)
	}
	if r.LeadSeconds > b.Settings.HorizonSeconds {
		return fmt.Errorf("%w: the answer's lead exceeds the autoscaler's horizon", errV2Refused)
	}
	if r.LeadSeconds != int64(b.Lead.Seconds) || r.StartupDelaySeconds != int64(b.Lead.StartupDelaySeconds) ||
		r.ActuationAllowanceSeconds != int64(b.Lead.ActuationAllowanceSeconds) {
		return fmt.Errorf("%w: the answer's lead differs from the autoscaler's", errV2Refused)
	}
	if r.CalibrationEpoch != b.EvidenceKey {
		return fmt.Errorf("%w: the answer's calibration epoch is not the request's evidence key", errV2Refused)
	}
	return nil
}

// v2Verdict is the operator's own decision on a bound answer: eligible only when every check passes now. Reason is
// "Ready", the service's reason when it did not offer the forecast, or the operator's when it overruled it.
type v2Verdict struct {
	Eligible bool
	Reason   string
	Detail   string
}

// verdict checks write eligibility independently of the service's boolean: trust state and reason, an upper bound,
// evidence counts against this autoscaler's settings, evidence age, an issue time not ahead of the operator's clock,
// and the remaining window at now (never extended: a cached answer keeps its original window and deadline).
func (r *v2Response) verdict(s v2Settings, now time.Time) v2Verdict {
	if !r.PredictiveEligible || r.TrustState != "Trusted" || r.Reason != "Ready" || r.UpperWindowMaxRPS == nil {
		reason := r.Reason
		if reason == "Ready" {
			reason = "NotEligible"
		}
		return v2Verdict{Reason: reason, Detail: fmt.Sprintf("trust state %s, reason %s, service eligibility %v",
			r.TrustState, r.Reason, r.PredictiveEligible)}
	}
	if r.CalibrationWindows < s.Calibration[v2CalMinWindows] || r.ProspectiveWindows < s.Trust[v2TrustMinProspectiveWindows] {
		return v2Verdict{Reason: "EvidenceInsufficient", Detail: fmt.Sprintf(
			"%d calibration / %d prospective windows, need %d / %d", r.CalibrationWindows, r.ProspectiveWindows,
			s.Calibration[v2CalMinWindows], s.Trust[v2TrustMinProspectiveWindows])}
	}
	nowMs := now.UnixMilli()
	maxAge := r.LeadSeconds + s.SettlementSeconds + s.OutcomeGraceSeconds + 2*s.SampleIntervalSeconds
	if r.EvidenceCutoff == nil || nowMs-*r.EvidenceCutoff*1000 > maxAge*1000 {
		return v2Verdict{Reason: "EvidenceStale", Detail: fmt.Sprintf("no qualifying outcome within %d s", maxAge)}
	}
	if r.IssuedAtMs > nowMs+v2IssueClockSkew.Milliseconds() {
		return v2Verdict{Reason: "IssuedInFuture", Detail: "the issuance is ahead of the operator's clock"}
	}
	w := v2Window{r.DecisionOrigin, r.SampleIntervalSeconds, r.LeadSeconds}
	if !w.actionableAt(nowMs, r.StartupDelaySeconds, r.ActuationAllowanceSeconds) {
		return v2Verdict{Reason: "DeadlinePassed", Detail: "valid until " + v2FormatTS(r.ValidUntil)}
	}
	return v2Verdict{Eligible: true, Reason: "Ready"}
}

// actionable reports whether the answer's own window can still be acted on at now (never extended or slid).
func (r *v2Response) actionable(now time.Time) bool {
	w := v2Window{r.DecisionOrigin, r.SampleIntervalSeconds, r.LeadSeconds}
	return w.actionableAt(now.UnixMilli(), r.StartupDelaySeconds, r.ActuationAllowanceSeconds)
}

// --- decision -------------------------------------------------------------------------------------------------

// v2Policy is the predictive term of a v2 decision (design C2, section 5):
//
//	R = ceil(rate / c)                         (the reactive replicas, computed by the caller)
//	P_uncapped = ceil(U / c)                   (U the verified upper bound, c the shared C1 capacity)
//	E = min(maxReplicas, ceil(maxPercent x max(R, minReplicas) / 100))
//	P = min(P_uncapped, R + E)
//
// The caller then takes clamp(max(R, P, minReplicas, schedule floor), min, max) with unifiedDesired; there is no
// schedule floor yet, so it is 0. The cap is a budget, not the quantile: both P_uncapped and P are recorded.
type v2Policy struct {
	Uncapped, Allowance, Capped int32
	Binding                     bool
}

func v2PredictiveReplicas(upperRPS, capacityRPM float64, reactive, minReplicas int32, maxExtra, maxPercent int64) v2Policy {
	p := v2Policy{Uncapped: boundedReplicas(math.Ceil(upperRPS * 60 / capacityRPM))}
	base := reactive
	if minReplicas > base {
		base = minReplicas
	}
	byPercent := (maxPercent*int64(base) + 99) / 100
	allowance := maxExtra
	if byPercent < allowance {
		allowance = byPercent
	}
	if allowance > math.MaxInt32 {
		allowance = math.MaxInt32
	}
	p.Allowance = int32(allowance)
	limit := int64(reactive) + allowance
	p.Capped = p.Uncapped
	if int64(p.Uncapped) > limit {
		p.Capped, p.Binding = int32(limit), true
	}
	return p
}

// --- client ---------------------------------------------------------------------------------------------------

// v2CachedForecast is one verified answer kept for reuse within its own window and deadline only.
type v2CachedForecast struct {
	resp    *v2Response
	binding string
}

// v2Outcome is what one reconcile learned from the v2 path.
type v2Outcome struct {
	Settings    v2Settings
	SettingsErr error
	Binding     *v2Binding
	Response    *v2Response
	Err         error // transport, refusal or verification failure (Response is nil)
	Verdict     v2Verdict
	Cached      bool
	published   autoscalerv1alpha1.PredictionStatus // what prepareV2 published (lead and its basis)
	startup     *autoscalerv1alpha1.StartupStatus
	leadProblem string // why there is no lead
}

func mlAPIBaseURL() string {
	if u := os.Getenv("ML_API_URL"); u != "" {
		return u
	}
	return "http://ml-api-service.ml-engine.svc.cluster.local:8000"
}

// fetchV2 returns a verified answer for binding b: the cached one when it was made for exactly this request and its
// window is still actionable, otherwise a fresh POST /v2/predict, bounded by the forecast wait allowance
// (forecast_wait.go). A refused, unauthorized or incoherent answer drops the cache. There is no stale reuse: an answer
// past its deadline is never used, and a request that ran out of its deadline is cancelled, so no late answer exists
// here (the service may still have journalled an issuance for it: that is not delivery, and never use).
func (r *PredictiveAutoscalerReconciler) fetchV2(ctx context.Context, key string, b v2Binding, now time.Time) (*v2Response, bool, error) {
	if r.v2Cache == nil {
		r.v2Cache = map[string]*v2CachedForecast{}
	}
	if c, ok := r.v2Cache[key]; ok {
		w := v2Window{c.resp.DecisionOrigin, c.resp.SampleIntervalSeconds, c.resp.LeadSeconds}
		if c.binding == b.key() && w.actionableAt(now.UnixMilli(), c.resp.StartupDelaySeconds, c.resp.ActuationAllowanceSeconds) {
			return c.resp, true, nil
		}
		delete(r.v2Cache, key)
	}
	body, err := json.Marshal(b.request())
	if err != nil {
		return nil, false, err
	}
	fctx, answered, err := r.forecastContext(ctx, key)
	if err != nil {
		return nil, false, err // not sent
	}
	out, err := r.requestV2(fctx, b, body)
	if err = answered(err); err != nil {
		return nil, false, err // also a verified answer that arrived after the deadline: v2.request is then "timeout"
	}
	r.v2Cache[key] = &v2CachedForecast{resp: out, binding: b.key()}
	return out, false, nil
}

// requestV2 is one POST /v2/predict: the answer read within its size limit, decoded strictly and bound to b.
func (r *PredictiveAutoscalerReconciler) requestV2(ctx context.Context, b v2Binding, body []byte) (*v2Response, error) {
	resp, err := r.mlAPI().post(ctx, mlAPIBaseURL()+"/v2/predict", body)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, v2MaxResponseBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read the v2 answer: %w", err)
	}
	switch {
	case resp.StatusCode >= 400 && resp.StatusCode < 500:
		return nil, &forecastRefusedError{status: resp.StatusCode, body: string(raw)}
	case resp.StatusCode != http.StatusOK:
		return nil, &forecastServerError{status: resp.StatusCode, body: string(raw)}
	case len(raw) > v2MaxResponseBytes:
		return nil, fmt.Errorf("%w: the answer exceeds %d bytes", errV2Refused, v2MaxResponseBytes)
	}
	out, err := decodeV2Response(raw)
	if err == nil {
		err = out.bound(b)
	}
	if err != nil {
		return nil, err
	}
	return out, nil
}

// v2FailureReason is the PredictionReady reason for a failed request.
func v2FailureReason(err error) string {
	var refused *forecastRefusedError
	switch {
	case stderrors.Is(err, errV2Refused):
		return "ForecastRejected"
	case stderrors.As(err, &refused) && refused.status == http.StatusUnprocessableEntity:
		return "RequestRefused"
	}
	return "DataUnavailable"
}

func v2FormatRPS(v float64) string { return strconv.FormatFloat(v, 'f', -1, 64) }

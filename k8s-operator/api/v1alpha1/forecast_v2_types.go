package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// Forecast contract v2 (window-max-rps/v1, design C2): the settings the statistical forecasting service resolves from
// spec.prediction, the startup budget the operator chooses its lead from, and the status both sides read. The keys and
// bounds mirror ml-engine/forecasting/contracts.py (resolve_settings); absent fields take that module's defaults, and
// the operator hashes the resolved values exactly as the service does (controllers/forecast_v2.go).

const (
	// EngineStatistical selects forecast contract v2 (the statistical forecasting service, POST /v2/predict).
	EngineStatistical = "statistical"
	// EngineLegacyLSTM keeps the v1 forecast path (POST /predict). An absent engine also keeps it.
	EngineLegacyLSTM = "legacyLstm"
)

// ForecastV2Settings are the v2 keys of spec.prediction. The forecasting service refuses any other key there.
type ForecastV2Settings struct {
	// Engine selects the forecast path. statistical uses forecast contract v2: a calibrated upper bound of the peak
	// request rate in the lead window, used only when the operator verifies it as trusted. legacyLstm, or no engine,
	// keeps the v1 path.
	// +kubebuilder:validation:Enum=statistical;legacyLstm
	// +optional
	Engine string `json:"engine,omitempty"`

	// SampleIntervalSeconds is the forecast grid (default 300): a minute multiple that divides a day.
	// +kubebuilder:validation:Enum=60;120;180;240;300;360;480;600
	// +optional
	SampleIntervalSeconds *int32 `json:"sampleIntervalSeconds,omitempty"`

	// Seasonality of the statistical models (default daily; only daily is supported so far).
	// +kubebuilder:validation:Enum=daily
	// +optional
	Seasonality string `json:"seasonality,omitempty"`

	// HistoryDays is the history the models learn from (default 14).
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=28
	// +optional
	HistoryDays *int32 `json:"historyDays,omitempty"`

	// SettlementSeconds is how long after a grid time its sample counts as final (default 120).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=900
	// +optional
	SettlementSeconds *int32 `json:"settlementSeconds,omitempty"`

	// OutcomeGraceSeconds is how long a forecast window's outcome may stay incomplete before it is unscorable
	// (default 600).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=3600
	// +optional
	OutcomeGraceSeconds *int32 `json:"outcomeGraceSeconds,omitempty"`

	// +optional
	Risk *ForecastRisk `json:"risk,omitempty"`
	// +optional
	Calibration *ForecastCalibration `json:"calibration,omitempty"`
	// +optional
	Trust *ForecastTrust `json:"trust,omitempty"`
	// +optional
	PredictiveExtra *PredictiveExtra `json:"predictiveExtra,omitempty"`
}

// ForecastRisk is the nominal risk of the upper bound.
type ForecastRisk struct {
	// MiscoverageBps is the share of windows whose peak may exceed the upper bound, in basis points (default 1000,
	// 90 % coverage).
	// +kubebuilder:validation:Minimum=100
	// +kubebuilder:validation:Maximum=2500
	// +optional
	MiscoverageBps *int32 `json:"miscoverageBps,omitempty"`
}

// ForecastCalibration tunes the window calibration (defaults in ml-engine/forecasting/contracts.py).
type ForecastCalibration struct {
	// +kubebuilder:validation:Minimum=16
	// +kubebuilder:validation:Maximum=4096
	// +optional
	MinWindows *int32 `json:"minWindows,omitempty"`
	// +kubebuilder:validation:Minimum=16
	// +kubebuilder:validation:Maximum=4096
	// +optional
	MaxScores *int32 `json:"maxScores,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=28
	// +optional
	RetentionDays *int32 `json:"retentionDays,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	// +optional
	EtaBps *int32 `json:"etaBps,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5000
	// +optional
	MinMiscoverageBps *int32 `json:"minMiscoverageBps,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=5000
	// +optional
	MaxMiscoverageBps *int32 `json:"maxMiscoverageBps,omitempty"`
}

// ForecastTrust tunes the evidence gates a model must pass before its forecast may scale (defaults in
// ml-engine/forecasting/contracts.py).
type ForecastTrust struct {
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=672
	// +optional
	MinShadowHours *int32 `json:"minShadowHours,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	// +optional
	MinProspectiveWindows *int32 `json:"minProspectiveWindows,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=2500
	// +optional
	CoverageToleranceBps *int32 `json:"coverageToleranceBps,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1440
	// +optional
	WorstDayMinOutcomes *int32 `json:"worstDayMinOutcomes,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=10000
	// +optional
	MinAvailabilityBps *int32 `json:"minAvailabilityBps,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=1000
	// +optional
	AcuteConsecutiveMisses *int32 `json:"acuteConsecutiveMisses,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	// +optional
	AcuteRecentWindows *int32 `json:"acuteRecentWindows,omitempty"`
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=672
	// +optional
	RepromotionHours *int32 `json:"repromotionHours,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	// +optional
	RepromotionWindows *int32 `json:"repromotionWindows,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=4096
	// +optional
	ChallengerMinPairs *int32 `json:"challengerMinPairs,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=672
	// +optional
	ChallengerMinHours *int32 `json:"challengerMinHours,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=10000
	// +optional
	ChallengerLossRatioBps *int32 `json:"challengerLossRatioBps,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=20000
	// +optional
	ChallengerExitLossRatioBps *int32 `json:"challengerExitLossRatioBps,omitempty"`
	// +kubebuilder:validation:Minimum=10000
	// +kubebuilder:validation:Maximum=20000
	// +optional
	ChallengerCostRatioBps *int32 `json:"challengerCostRatioBps,omitempty"`
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=24
	// +optional
	MaxFitAgeHours *int32 `json:"maxFitAgeHours,omitempty"`
}

// PredictiveExtra caps how many replicas a forecast may add above the reactive need. The stricter of the two caps
// applies; 0 in either disables predictive excess.
type PredictiveExtra struct {
	// MaxReplicas is the most replicas a forecast may add (default 2).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	// +optional
	MaxReplicas *int32 `json:"maxReplicas,omitempty"`
	// MaxPercent is the most a forecast may add, in percent of max(reactive need, minReplicas) (default 50).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=1000
	// +optional
	MaxPercent *int32 `json:"maxPercent,omitempty"`
}

// StartupBehavior is the budget for a scale-out to become usable: scale request to Ready. The operator measures it
// per target (pod creation to Ready, current pod template only) and uses the measurement once it has enough samples;
// until then it uses FallbackDelaySeconds. Without either it chooses no predictive lead, and the autoscaler stays
// reactive.
type StartupBehavior struct {
	// FallbackDelaySeconds is the conservative scale-out-to-Ready budget used until enough measurements exist (20
	// completed starts of the current pod template in the last 7 days). Choose it from observed starts, including
	// scheduling, node provisioning, image pull and readiness.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=3600
	// +optional
	FallbackDelaySeconds *int32 `json:"fallbackDelaySeconds,omitempty"`

	// MinimumDelaySeconds is a floor kept even when measurements are lower (default 0).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=3600
	// +optional
	MinimumDelaySeconds *int32 `json:"minimumDelaySeconds,omitempty"`

	// ActuationAllowanceSeconds covers the request to the forecasting service and the write to /scale (default 15).
	// +kubebuilder:validation:Minimum=0
	// +kubebuilder:validation:Maximum=300
	// +optional
	ActuationAllowanceSeconds *int32 `json:"actuationAllowanceSeconds,omitempty"`
}

// PredictionLead is the lead the operator selected; the forecasting service reads it (status.prediction.lead) and
// accepts it only for the observed generation. Every field is always present.
type PredictionLead struct {
	// Seconds is the window length L, a multiple of the cadence covering startupDelay + cadence + allowance.
	Seconds int32 `json:"seconds"`
	// StartupDelaySeconds is the startup budget D the lead was chosen for.
	StartupDelaySeconds int32 `json:"startupDelaySeconds"`
	// ActuationAllowanceSeconds is the allowance A.
	ActuationAllowanceSeconds int32 `json:"actuationAllowanceSeconds"`
	// Method is configured (spec.behavior.startup.fallbackDelaySeconds) or measured.
	// +kubebuilder:validation:Enum=configured;measured
	Method string `json:"method"`
	// SampleCount is the number of completed starts behind a measured budget (0 when configured).
	SampleCount int32 `json:"sampleCount"`
	// ObservedGeneration is the autoscaler generation the lead was selected for.
	ObservedGeneration int64 `json:"observedGeneration"`
}

// StartupStatus is the operator's startup-delay measurement for the target.
type StartupStatus struct {
	// DelaySeconds is the startup budget in use (absent when there is none).
	// +optional
	DelaySeconds *int32 `json:"delaySeconds,omitempty"`
	// Method is configured, measured or none.
	// +optional
	Method string `json:"method,omitempty"`
	// SampleCount is the number of completed starts of the current pod template in the last 7 days.
	SampleCount int32 `json:"sampleCount"`
	// MeasuredP90Seconds is the 90th percentile of those starts (absent with fewer than 20).
	// +optional
	MeasuredP90Seconds *int32 `json:"measuredP90Seconds,omitempty"`
	// PendingSeconds is the age of the oldest not-yet-Ready pod of the current template (absent when none); a pending
	// start longer than the budget raises the budget.
	// +optional
	PendingSeconds *int32 `json:"pendingSeconds,omitempty"`
	// Observable is false when the operator could not read the target's pods (the configured budget then applies).
	Observable bool `json:"observable"`
}

// PredictionStatus is the forecast contract v2 state of an autoscaler.
type PredictionStatus struct {
	// Engine is the forecast engine applied in the last reconcile.
	// +optional
	Engine string `json:"engine,omitempty"`
	// SettingsSHA256 is the fingerprint of the resolved v2 settings (the same hash the forecasting service computes).
	// +optional
	SettingsSHA256 string `json:"settingsSHA256,omitempty"`
	// Lead is the selected lead (absent when no startup budget is available, or it does not fit the horizon).
	// +optional
	Lead *PredictionLead `json:"lead,omitempty"`
	// LeadBasis fingerprints the settings and startup configuration the lead was frozen for: the lead never shrinks
	// automatically while it is unchanged.
	// +optional
	LeadBasis string `json:"leadBasis,omitempty"`
	// FrozenLeadSeconds is the longest lead selected for LeadBasis. A later selection never goes below it, also after
	// a period without a lead: a shorter lead needs a change of the settings or the startup configuration.
	// +optional
	FrozenLeadSeconds int32 `json:"frozenLeadSeconds,omitempty"`
	// Startup is the startup-delay measurement.
	// +optional
	Startup *StartupStatus `json:"startup,omitempty"`
	// Phase is the forecasting service's trust state for the last forecast (Collecting, Calibrating, Shadow, Trusted,
	// Degraded).
	// +optional
	Phase string `json:"phase,omitempty"`
	// Reason explains readiness: the service's reason, or the operator's own when it rejected the forecast.
	// +optional
	Reason string `json:"reason,omitempty"`
	// Model is the selected model family.
	// +optional
	Model string `json:"model,omitempty"`
	// IssuanceID is the last forecast's issuance (also in the decision ledger).
	// +optional
	IssuanceID string `json:"issuanceID,omitempty"`
	// DecisionOrigin and ValidUntil bound the last forecast's use.
	// +optional
	DecisionOrigin *metav1.Time `json:"decisionOrigin,omitempty"`
	// +optional
	ValidUntil *metav1.Time `json:"validUntil,omitempty"`
	// BaseWindowMaxRPS and UpperWindowMaxRPS are the forecast window's point peak and calibrated upper bound, in
	// requests per second, as decimal strings (UpperWindowMaxRPS absent while uncalibrated).
	// +optional
	BaseWindowMaxRPS string `json:"baseWindowMaxRPS,omitempty"`
	// +optional
	UpperWindowMaxRPS string `json:"upperWindowMaxRPS,omitempty"`
	// CalibrationWindows and ProspectiveWindows count the evidence behind the bound.
	CalibrationWindows int32 `json:"calibrationWindows"`
	ProspectiveWindows int32 `json:"prospectiveWindows"`
	// GateCoverageBps is the coverage of the trust cohort, in basis points (absent with no scored window).
	// +optional
	GateCoverageBps *int32 `json:"gateCoverageBps,omitempty"`
	// Eligible is the operator's verdict: the forecast passed every check and may set a predictive floor (it still
	// writes only in Active mode).
	Eligible bool `json:"eligible"`
	// PredictiveReplicas is ceil(upper bound / capacity) before the cap, CappedReplicas after it (both absent when the
	// forecast is not eligible); CapBinding says the predictive-extra cap lowered it.
	// +optional
	PredictiveReplicas *int32 `json:"predictiveReplicas,omitempty"`
	// +optional
	CappedReplicas *int32 `json:"cappedReplicas,omitempty"`
	CapBinding     bool   `json:"capBinding"`
}

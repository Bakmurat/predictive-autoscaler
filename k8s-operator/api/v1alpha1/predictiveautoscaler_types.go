package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PredictiveAutoscalerSpec defines the desired state of PredictiveAutoscaler
type PredictiveAutoscalerSpec struct {
	// TargetDeployment specifies the deployment to scale
	TargetDeployment TargetDeployment `json:"targetDeployment"`

	// MinReplicas is the minimum number of replicas
	MinReplicas int32 `json:"minReplicas"`

	// MaxReplicas is the maximum number of replicas
	MaxReplicas int32 `json:"maxReplicas"`

	// Metrics configuration for predictions
	Metrics MetricsConfig `json:"metrics,omitempty"`

	// Prediction configuration
	Prediction PredictionConfig `json:"prediction,omitempty"`

	// Resources configuration for replica calculation
	Resources ResourcesConfig `json:"resources,omitempty"`

	// Mode selects what the operator does with its decision. Recommend (the default, also when the field is absent)
	// computes and publishes the recommended replica count in status, metrics and the decision ledger and writes nothing
	// to the target. Active scales the target. Only an explicit Active authorizes writes.
	// +kubebuilder:validation:Enum=Recommend;Active
	// +kubebuilder:default=Recommend
	// +optional
	Mode string `json:"mode,omitempty"`
}

const (
	// ModeRecommend computes and publishes decisions without writing to the target.
	ModeRecommend = "Recommend"
	// ModeActive scales the target.
	ModeActive = "Active"
)

// EffectiveMode is the mode the operator applies: Active only when explicitly set, Recommend otherwise.
func (s PredictiveAutoscalerSpec) EffectiveMode() string {
	if s.Mode == ModeActive {
		return ModeActive
	}
	return ModeRecommend
}

// TargetDeployment specifies the target deployment
type TargetDeployment struct {
	// Name of the deployment
	Name string `json:"name"`

	// Namespace of the deployment
	Namespace string `json:"namespace"`

	// Container name (optional, defaults to first container)
	Container string `json:"container,omitempty"`
}

// MetricsConfig defines metrics to monitor
type MetricsConfig struct {
	// CPU metrics configuration
	CPU *CPUMetric `json:"cpu,omitempty"`

	// Memory metrics configuration
	Memory *MemoryMetric `json:"memory,omitempty"`

	// Request rate metrics configuration
	Requests *RequestsMetric `json:"requests,omitempty"`
}

// CPUMetric defines CPU metric configuration
type CPUMetric struct {
	// Enabled indicates if CPU metrics should be used
	Enabled bool `json:"enabled"`

	// TargetPercent is the target CPU utilization percentage
	TargetPercent int32 `json:"targetPercent,omitempty"`
}

// MemoryMetric defines memory metric configuration
type MemoryMetric struct {
	// Enabled indicates if memory metrics should be used
	Enabled bool `json:"enabled"`

	// TargetPercent is the target memory utilization percentage
	TargetPercent int32 `json:"targetPercent,omitempty"`
}

// RequestsMetric defines request rate metric configuration
type RequestsMetric struct {
	// Enabled indicates if request metrics should be used
	Enabled bool `json:"enabled"`

	// TargetRPS is the target requests per second per pod
	// Matches Grafana/KEDA: sum(rate(istio_requests_total{...}[1m]))
	TargetRPS int32 `json:"targetRPS,omitempty"`
}

// PredictionConfig defines prediction settings
type PredictionConfig struct {
	// HorizonMinutes is how far ahead to predict (in minutes)
	// Enabled turns the forecasting component on or off. When false the operator scales the target from
	// the reactive request-rate rule only, which makes it a matched reactive-only control for benchmarks.
	// Defaults to true.
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	HorizonMinutes int32 `json:"horizonMinutes,omitempty"`

	// LeadTimeMinutes is how early to scale before predicted load
	LeadTimeMinutes int32 `json:"leadTimeMinutes,omitempty"`

	// UpdateIntervalSeconds is how often to update predictions
	UpdateIntervalSeconds int32 `json:"updateIntervalSeconds,omitempty"`
}

// ResourcesConfig defines resource configuration for calculations
type ResourcesConfig struct {
	// CPURequestMillicores is the CPU request per pod in millicores
	CPURequestMillicores int32 `json:"cpuRequestMillicores,omitempty"`

	// MemoryRequestMB is the memory request per pod in MB
	MemoryRequestMB int32 `json:"memoryRequestMB,omitempty"`

	// BaselineRPM is the baseline requests per minute
	BaselineRPM int32 `json:"baselineRPM,omitempty"`
}

// PredictiveAutoscalerStatus defines the observed state of PredictiveAutoscaler
type PredictiveAutoscalerStatus struct {
	// CurrentReplicas is the current number of replicas
	CurrentReplicas int32 `json:"currentReplicas,omitempty"`

	// PredictedReplicas is the last calculated (desired) replica count. Deprecated: kept with its historical meaning
	// for compatibility until the API-group migration; use calculatedReplicas and forecastReplicas.
	PredictedReplicas int32 `json:"predictedReplicas,omitempty"`

	// ObservedGeneration is the spec generation the status describes.
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Mode is the mode applied in the last reconcile (Recommend or Active).
	Mode string `json:"mode,omitempty"`

	// ForecastReplicas is the replica count derived from the forecast alone (absent when no forecast was used).
	ForecastReplicas int32 `json:"forecastReplicas,omitempty"`

	// CalculatedReplicas is the decision rule's result: clamp(max(forecast, reactive), min, max), or the held count
	// when the current request rate is unavailable.
	CalculatedReplicas int32 `json:"calculatedReplicas,omitempty"`

	// StabilizedReplicas is what Active mode applies after the scale-down stabilization and cooldown (Active only:
	// Recommend mode keeps no hypothetical scale history, so it is absent there).
	StabilizedReplicas int32 `json:"stabilizedReplicas,omitempty"`

	// AppliedReplicas is the replica count of the last successful write to the target (Active only).
	AppliedReplicas int32 `json:"appliedReplicas,omitempty"`

	// ReadyReplicas is the target's ready replica count observed in the last reconcile (a zero is reported).
	ReadyReplicas int32 `json:"readyReplicas"`

	// TargetUID is the UID of the target Deployment the status describes. When the target is replaced or retargeted,
	// the target-specific history (appliedReplicas, lastScaleTime) is cleared.
	TargetUID string `json:"targetUID,omitempty"`

	// LastPrediction is the timestamp of the last prediction
	LastPrediction *metav1.Time `json:"lastPrediction,omitempty"`

	// LastScaleTime is the timestamp of the last scaling action
	LastScaleTime *metav1.Time `json:"lastScaleTime,omitempty"`

	// Conditions represent the latest available observations
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

//+kubebuilder:object:root=true
//+kubebuilder:subresource:status
//+kubebuilder:resource:shortName=pa
//+kubebuilder:printcolumn:name="Target",type="string",JSONPath=".spec.targetDeployment.name"
//+kubebuilder:printcolumn:name="Min",type="integer",JSONPath=".spec.minReplicas"
//+kubebuilder:printcolumn:name="Max",type="integer",JSONPath=".spec.maxReplicas"
//+kubebuilder:printcolumn:name="Mode",type="string",JSONPath=".status.mode"
//+kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.currentReplicas"
//+kubebuilder:printcolumn:name="Calculated",type="integer",JSONPath=".status.calculatedReplicas"
//+kubebuilder:printcolumn:name="Applied",type="integer",JSONPath=".status.appliedReplicas"
//+kubebuilder:printcolumn:name="Ready",type="integer",JSONPath=".status.readyReplicas"
//+kubebuilder:printcolumn:name="Age",type="date",JSONPath=".metadata.creationTimestamp"

// PredictiveAutoscaler is the Schema for the predictiveautoscalers API
type PredictiveAutoscaler struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   PredictiveAutoscalerSpec   `json:"spec,omitempty"`
	Status PredictiveAutoscalerStatus `json:"status,omitempty"`
}

//+kubebuilder:object:root=true

// PredictiveAutoscalerList contains a list of PredictiveAutoscaler
type PredictiveAutoscalerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []PredictiveAutoscaler `json:"items"`
}

func init() {
	SchemeBuilder.Register(&PredictiveAutoscaler{}, &PredictiveAutoscalerList{})
}

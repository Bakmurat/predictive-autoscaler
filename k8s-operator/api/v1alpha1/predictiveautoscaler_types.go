package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PredictiveAutoscalerSpec defines the desired state of PredictiveAutoscaler
// +kubebuilder:validation:XValidation:rule="self.minReplicas <= self.maxReplicas",message="minReplicas must not exceed maxReplicas"
type PredictiveAutoscalerSpec struct {
	// TargetDeployment specifies the deployment to scale
	TargetDeployment TargetDeployment `json:"targetDeployment"`

	// MinReplicas is the minimum number of replicas
	// +kubebuilder:validation:Minimum=1
	MinReplicas int32 `json:"minReplicas"`

	// MaxReplicas is the maximum number of replicas
	// +kubebuilder:validation:Minimum=1
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

	// PresetIstio measures the target's requests with Istio's destination-reported istio_requests_total (the default).
	PresetIstio = "istio"
	// PresetPrometheus measures the target's requests with the user's PromQL template (source.query).
	PresetPrometheus = "prometheus"
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
	// +kubebuilder:validation:MinLength=1
	Name string `json:"name"`

	// Namespace of the deployment; must equal the PredictiveAutoscaler's namespace
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
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled"`

	// TargetPercent is the target CPU utilization percentage
	// +kubebuilder:default=70
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	TargetPercent int32 `json:"targetPercent,omitempty"`
}

// MemoryMetric defines memory metric configuration
type MemoryMetric struct {
	// Enabled indicates if memory metrics should be used
	// +kubebuilder:default=true
	// +optional
	Enabled bool `json:"enabled"`

	// TargetPercent is the target memory utilization percentage
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:validation:Maximum=100
	TargetPercent int32 `json:"targetPercent,omitempty"`
}

// RequestsMetric defines request rate metric configuration
type RequestsMetric struct {
	// Enabled indicates if request metrics should be used
	// +kubebuilder:default=false
	// +optional
	Enabled bool `json:"enabled"`

	// TargetRPS is the target requests per second per pod
	// +kubebuilder:validation:Minimum=1
	TargetRPS int32 `json:"targetRPS,omitempty"`

	// Source names the request-rate signal of the target (default: the istio preset).
	// +optional
	Source *MetricSource `json:"source,omitempty"`
}

// MetricSource is the PromQL that measures the target's requests per second: an instant expression returning exactly
// one series. A template may use only {{ .Namespace }} and {{ .Name }} (the target), which are escaped as PromQL string
// content.
// +kubebuilder:validation:XValidation:rule="self.preset == 'prometheus' ? (has(self.query) && size(self.query) > 0) : !has(self.query)",message="query is required with preset prometheus and not allowed otherwise"
type MetricSource struct {
	// +kubebuilder:validation:Enum=istio;prometheus
	// +kubebuilder:default=istio
	Preset string `json:"preset,omitempty"`
	// +kubebuilder:validation:MaxLength=2048
	// +optional
	Query string `json:"query,omitempty"`
}

// ReplicaWriter identifies another object that writes the target's replica count.
type ReplicaWriter struct {
	// Group is the writer's API group (empty for the core group).
	// +optional
	Group string `json:"group,omitempty"`
	Kind  string `json:"kind"`
	// Namespace is the writer's own namespace (a legacy-group autoscaler may live in another namespace than the target).
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	// Reason says why the object counts as a writer when its kind alone does not: PausedReplicas (a paused KEDA
	// ScaledObject holding a fixed count) or LegacyAPIGroup (any autoscaler of the legacy group, whatever its spec).
	// +optional
	Reason string `json:"reason,omitempty"`
}

const (
	// WriterReasonPausedReplicas marks a KEDA ScaledObject paused with a fixed replica count.
	WriterReasonPausedReplicas = "PausedReplicas"
	// WriterReasonLegacyAPIGroup marks a PredictiveAutoscaler of the legacy API group.
	WriterReasonLegacyAPIGroup = "LegacyAPIGroup"
)

// String is the writer as conditions, events and the decision ledger name it.
func (w ReplicaWriter) String() string {
	switch w.Reason {
	case WriterReasonLegacyAPIGroup:
		return w.Kind + "." + w.Group + "/" + w.Namespace + "/" + w.Name
	case WriterReasonPausedReplicas:
		return w.Kind + "/" + w.Name + " (paused-replicas)"
	}
	return w.Kind + "/" + w.Name
}

// MetricSourceStatus is the compiled request-rate query the forecasting service and the trainer use: they accept it
// only when ObservedGeneration equals the object's generation, TargetUID the target's UID and SHA256 the query's hash.
type MetricSourceStatus struct {
	Query              string `json:"query,omitempty"`
	SHA256             string `json:"sha256,omitempty"`
	ObservedGeneration int64  `json:"observedGeneration,omitempty"`
	TargetUID          string `json:"targetUID,omitempty"`
	Contract           string `json:"contract,omitempty"`
}

// PredictionConfig defines prediction settings
type PredictionConfig struct {
	// Enabled turns the forecasting component on or off. When false the operator scales the target from
	// the reactive request-rate rule only, which makes it a matched reactive-only control for benchmarks.
	// Defaults to true.
	// +kubebuilder:default=true
	Enabled *bool `json:"enabled,omitempty"`

	// HorizonMinutes is how far ahead to predict (in minutes)
	// +kubebuilder:default=60
	// +kubebuilder:validation:Minimum=5
	HorizonMinutes int32 `json:"horizonMinutes,omitempty"`

	// LeadTimeMinutes is how early to scale before predicted load
	// +kubebuilder:default=15
	// +kubebuilder:validation:Minimum=1
	LeadTimeMinutes int32 `json:"leadTimeMinutes,omitempty"`

	// UpdateIntervalSeconds is how often to update predictions
	// +kubebuilder:default=300
	// +kubebuilder:validation:Minimum=60
	UpdateIntervalSeconds int32 `json:"updateIntervalSeconds,omitempty"`
}

// ResourcesConfig defines resource configuration for calculations
type ResourcesConfig struct {
	// CPURequestMillicores is the CPU request per pod in millicores
	// +kubebuilder:default=100
	// +kubebuilder:validation:Minimum=1
	CPURequestMillicores int32 `json:"cpuRequestMillicores,omitempty"`

	// MemoryRequestMB is the memory request per pod in MB
	// +kubebuilder:default=128
	// +kubebuilder:validation:Minimum=1
	MemoryRequestMB int32 `json:"memoryRequestMB,omitempty"`

	// BaselineRPM is the baseline requests per minute
	// +kubebuilder:default=10000
	// +kubebuilder:validation:Minimum=1
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
	// +optional
	ReadyReplicas int32 `json:"readyReplicas"`

	// TargetUID is the UID of the target Deployment the status describes. When the target is replaced or retargeted,
	// the target-specific history (appliedReplicas, lastScaleTime) is cleared.
	TargetUID string `json:"targetUID,omitempty"`

	// Conflicts lists the other replica writers the last coexistence check found on the target. With a failed check
	// (ConflictDetected=Unknown) it holds those found before the failure.
	// +optional
	// +listType=atomic
	Conflicts []ReplicaWriter `json:"conflicts,omitempty"`

	// MetricSource is the compiled request-rate query (absent while the configuration is invalid).
	MetricSource *MetricSourceStatus `json:"metricSource,omitempty"`

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
//+kubebuilder:metadata:labels="autoscaling.devkuban.com/crd-revision=1"
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

	// +required
	Spec   PredictiveAutoscalerSpec   `json:"spec"`
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

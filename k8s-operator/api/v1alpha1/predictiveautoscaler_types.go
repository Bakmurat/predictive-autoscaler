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

	// PredictedReplicas is the predicted number of replicas needed
	PredictedReplicas int32 `json:"predictedReplicas,omitempty"`

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
//+kubebuilder:printcolumn:name="Current",type="integer",JSONPath=".status.currentReplicas"
//+kubebuilder:printcolumn:name="Predicted",type="integer",JSONPath=".status.predictedReplicas"
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

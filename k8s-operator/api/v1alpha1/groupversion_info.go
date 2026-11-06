// Package v1alpha1 contains the PredictiveAutoscaler API, group autoscaling.devkuban.com.
// +kubebuilder:object:generate=true
// +groupName=autoscaling.devkuban.com
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// GroupVersion is group version used to register these objects
var GroupVersion = schema.GroupVersion{Group: "autoscaling.devkuban.com", Version: "v1alpha1"}

// LegacyGroup is the API group before v0.1.0. Its PredictiveAutoscalers are other replica writers (see the coexistence
// check) and the input of the migration; nothing else uses it.
const LegacyGroup = "autoscaler.example.com"

// SchemeBuilder is used to add go types to the GroupVersionKind scheme
var (
	SchemeBuilder = &scheme.Builder{GroupVersion: GroupVersion}
	AddToScheme   = SchemeBuilder.AddToScheme
)

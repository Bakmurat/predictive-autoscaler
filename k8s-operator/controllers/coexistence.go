package controllers

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// Coexistence (plan item #3, user decision 2026-10-09: another scaler on the same target → REFUSE).
//
// Exactly one controller may own a Deployment's replica count. Before every write the operator re-reads, through an
// uncached reader, every other replica writer that can target the Deployment and refuses to write while one exists:
//   - a HorizontalPodAutoscaler whose scaleTargetRef is the Deployment (KEDA's generated HPA included, whatever its name);
//   - a KEDA ScaledObject on the Deployment, unless it is paused with autoscaling.keda.sh/paused: "true" (a
//     paused-replicas annotation makes KEDA set that count: a writer). The generated HPA of a paused ScaledObject is
//     removed by KEDA; while it still exists the HPA check above reports it;
//   - another PredictiveAutoscaler in Active mode on the same Deployment (both refuse; no ownership policy is implied).
// A VerticalPodAutoscaler in any mode except Off is reported as a warning: it changes the pods' resource requests but
// does not own the replica count.
//
// KEDA and the VPA are optional APIs. One counts as absent only when a fresh discovery succeeds and shows it is not
// served (the group is not served, or its version is served without the resource). A served group without the expected
// version, a failed discovery, or any error reading a served API makes the check fail, which blocks writes like a
// conflict (Codex task-08 r05: a 404 alone is no proof of absence). The checks are not atomic across resources; they
// run again before every write, within coexistenceTimeout.

// coexistenceTimeout bounds the whole check (and identityGuardTimeout the guard): a stalled API request becomes a hold.
var (
	coexistenceTimeout   = 10 * time.Second
	identityGuardTimeout = 5 * time.Second
)

type optionalAPI struct {
	gv       schema.GroupVersion
	resource string
	listKind string
}

var (
	kedaScaledObjects = optionalAPI{schema.GroupVersion{Group: "keda.sh", Version: "v1alpha1"}, "scaledobjects", "ScaledObjectList"}
	vpaObjects        = optionalAPI{schema.GroupVersion{Group: "autoscaling.k8s.io", Version: "v1"}, "verticalpodautoscalers", "VerticalPodAutoscalerList"}
)

// APIDiscovery is the part of API discovery the check needs, bounded by the check's context (client-go's
// DiscoveryClient methods of this version use context.TODO() internally, so the check's deadline would not apply).
type APIDiscovery interface {
	ServerGroups(ctx context.Context) (*metav1.APIGroupList, error)
	ServerResourcesForGroupVersion(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error)
}

// RESTDiscovery reads /apis and /apis/<group>/<version> uncached through a REST client configured for discovery
// (discovery.DiscoveryClient.RESTClient()).
type RESTDiscovery struct{ Client rest.Interface }

func (d RESTDiscovery) ServerGroups(ctx context.Context) (*metav1.APIGroupList, error) {
	var l metav1.APIGroupList
	if err := d.Client.Get().AbsPath("/apis").Do(ctx).Into(&l); err != nil {
		return nil, err
	}
	return &l, nil
}

func (d RESTDiscovery) ServerResourcesForGroupVersion(ctx context.Context, groupVersion string) (*metav1.APIResourceList, error) {
	var l metav1.APIResourceList
	if err := d.Client.Get().AbsPath("/apis", groupVersion).Do(ctx).Into(&l); err != nil {
		return nil, err
	}
	return &l, nil
}

type writerCheck struct {
	Conflicts  []string // "Kind/name" of every other replica writer on the target
	Warnings   []string // "Kind/name (mode)" of VerticalPodAutoscalers that change the target's pods
	VPAChecked bool     // the VPA part completed (Warnings is meaningful)
}

// servedAPIs returns, from one fresh discovery, whether each optional API is served. d == nil (unit tests with a fake
// client) treats every optional API as served, so it is listed and any error fails the check.
func servedAPIs(ctx context.Context, d APIDiscovery, apis ...optionalAPI) (map[optionalAPI]bool, error) {
	out := map[optionalAPI]bool{}
	if d == nil {
		for _, a := range apis {
			out[a] = true
		}
		return out, nil
	}
	groups, err := d.ServerGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("API discovery: %w", err)
	}
	for _, a := range apis {
		served, err := apiServed(ctx, d, groups, a)
		if err != nil {
			return nil, err
		}
		out[a] = served
	}
	return out, nil
}

func apiServed(ctx context.Context, d APIDiscovery, groups *metav1.APIGroupList, a optionalAPI) (bool, error) {
	var group *metav1.APIGroup
	for i := range groups.Groups {
		if groups.Groups[i].Name == a.gv.Group {
			group = &groups.Groups[i]
		}
	}
	if group == nil {
		return false, nil // a successful discovery without the group: not installed
	}
	hasVersion := false
	for _, v := range group.Versions {
		hasVersion = hasVersion || v.Version == a.gv.Version
	}
	if !hasVersion {
		return false, fmt.Errorf("API group %s is served without version %s, which this operator reads", a.gv.Group, a.gv.Version)
	}
	resources, err := d.ServerResourcesForGroupVersion(ctx, a.gv.String())
	if err != nil {
		return false, fmt.Errorf("API discovery of %s: %w", a.gv, err)
	}
	for _, r := range resources.APIResources {
		if r.Name == a.resource {
			return true, nil
		}
	}
	return false, nil // the group/version is served without this resource
}

func targetsDeployment(kind, apiVersion, name string, dep *appsv1.Deployment) bool {
	if name != dep.Name {
		return false
	}
	if kind != "" && kind != "Deployment" {
		return false
	}
	if apiVersion == "" {
		return true
	}
	gv, err := schema.ParseGroupVersion(apiVersion)
	return err == nil && gv.Group == "apps"
}

func listOptional(ctx context.Context, rd client.Reader, a optionalAPI, ns string) (*unstructured.UnstructuredList, error) {
	l := &unstructured.UnstructuredList{}
	l.SetGroupVersionKind(a.gv.WithKind(a.listKind))
	if err := rd.List(ctx, l, client.InNamespace(ns)); err != nil {
		return nil, fmt.Errorf("list %s.%s: %w", a.resource, a.gv.Group, err)
	}
	return l, nil
}

// checkReplicaWriters lists the replica writers on dep through rd (an uncached reader in production) after a fresh
// discovery of the optional APIs through d.
func checkReplicaWriters(ctx context.Context, rd client.Reader, d APIDiscovery, pa *autoscalerv1alpha1.PredictiveAutoscaler, dep *appsv1.Deployment) (writerCheck, error) {
	var out writerCheck
	ns := client.InNamespace(dep.Namespace)

	served, err := servedAPIs(ctx, d, kedaScaledObjects, vpaObjects)
	if err != nil {
		return out, err
	}

	var hpas autoscalingv2.HorizontalPodAutoscalerList
	if err := rd.List(ctx, &hpas, ns); err != nil {
		return out, fmt.Errorf("list HorizontalPodAutoscalers: %w", err)
	}
	for _, h := range hpas.Items {
		ref := h.Spec.ScaleTargetRef
		if targetsDeployment(ref.Kind, ref.APIVersion, ref.Name, dep) {
			out.Conflicts = append(out.Conflicts, "HorizontalPodAutoscaler/"+h.Name)
		}
	}

	if served[kedaScaledObjects] {
		sos, err := listOptional(ctx, rd, kedaScaledObjects, dep.Namespace)
		if err != nil {
			return out, err
		}
		for _, so := range sos.Items {
			kind, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "kind")
			apiVersion, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "apiVersion")
			name, _, _ := unstructured.NestedString(so.Object, "spec", "scaleTargetRef", "name")
			if !targetsDeployment(kind, apiVersion, name, dep) {
				continue
			}
			ann := so.GetAnnotations()
			if _, fixed := ann["autoscaling.keda.sh/paused-replicas"]; fixed {
				out.Conflicts = append(out.Conflicts, "ScaledObject/"+so.GetName()+" (paused-replicas)")
			} else if ann["autoscaling.keda.sh/paused"] != "true" {
				out.Conflicts = append(out.Conflicts, "ScaledObject/"+so.GetName())
			}
		}
	}

	var pas autoscalerv1alpha1.PredictiveAutoscalerList
	if err := rd.List(ctx, &pas, client.InNamespace(pa.Namespace)); err != nil {
		return out, fmt.Errorf("list PredictiveAutoscalers: %w", err)
	}
	for _, o := range pas.Items {
		if o.UID == pa.UID && o.Name == pa.Name {
			continue
		}
		tns := o.Spec.TargetDeployment.Namespace
		if tns == "" {
			tns = o.Namespace
		}
		if o.Spec.TargetDeployment.Name == dep.Name && tns == dep.Namespace && o.Spec.EffectiveMode() == autoscalerv1alpha1.ModeActive {
			out.Conflicts = append(out.Conflicts, "PredictiveAutoscaler/"+o.Name)
		}
	}

	if served[vpaObjects] {
		vpas, err := listOptional(ctx, rd, vpaObjects, dep.Namespace)
		if err != nil {
			return out, err
		}
		for _, v := range vpas.Items {
			kind, _, _ := unstructured.NestedString(v.Object, "spec", "targetRef", "kind")
			apiVersion, _, _ := unstructured.NestedString(v.Object, "spec", "targetRef", "apiVersion")
			name, _, _ := unstructured.NestedString(v.Object, "spec", "targetRef", "name")
			mode, _, _ := unstructured.NestedString(v.Object, "spec", "updatePolicy", "updateMode")
			if mode == "" {
				mode = "Auto"
			}
			if targetsDeployment(kind, apiVersion, name, dep) && mode != "Off" {
				out.Warnings = append(out.Warnings, "VerticalPodAutoscaler/"+v.GetName()+" ("+mode+")")
			}
		}
	}
	out.VPAChecked = true
	sort.Strings(out.Conflicts)
	sort.Strings(out.Warnings)
	return out, nil
}

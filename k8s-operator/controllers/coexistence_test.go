package controllers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// B2 — coexistence (plan item #3; user decision 2026-10-09: another scaler on the target → refuse).

func coexScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{appsv1.AddToScheme, autoscalingv2.AddToScheme, autoscalerv1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

func hpaObj(name, ns, kind, apiVersion, target string) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: kind, APIVersion: apiVersion, Name: target},
			MaxReplicas:    10,
		},
	}
}

func scaledObj(name, ns, target string, ann map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "keda.sh/v1alpha1", "kind": "ScaledObject",
		"metadata": map[string]interface{}{"name": name, "namespace": ns},
		"spec":     map[string]interface{}{"scaleTargetRef": map[string]interface{}{"name": target}}, // kind defaults to Deployment
	}}
	if ann != nil {
		u.SetAnnotations(ann)
	}
	return u
}

func vpaObj(name, ns, target, mode string) *unstructured.Unstructured {
	spec := map[string]interface{}{"targetRef": map[string]interface{}{"apiVersion": "apps/v1", "kind": "Deployment", "name": target}}
	if mode != "" {
		spec["updatePolicy"] = map[string]interface{}{"updateMode": mode}
	}
	return &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "autoscaling.k8s.io/v1", "kind": "VerticalPodAutoscaler",
		"metadata": map[string]interface{}{"name": name, "namespace": ns}, "spec": spec,
	}}
}

func paObj(name, ns, target, mode string) *autoscalerv1alpha1.PredictiveAutoscaler {
	return &autoscalerv1alpha1.PredictiveAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, UID: types.UID("uid-" + name)},
		Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{
			TargetDeployment: autoscalerv1alpha1.TargetDeployment{Name: target, Namespace: ns}, Mode: mode, MinReplicas: 1, MaxReplicas: 5,
		},
	}
}

func TestCheckReplicaWritersFindsEveryOtherWriter(t *testing.T) {
	const ns = "shop"
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns, UID: "dep-uid"}}
	self := paObj("self", ns, "web", autoscalerv1alpha1.ModeActive)
	paused := map[string]string{"autoscaling.keda.sh/paused": "true"}
	for _, tc := range []struct {
		name                string
		objs                []client.Object
		conflicts, warnings []string
	}{
		{"nothing_else", nil, nil, nil},
		{"hpa_on_target", []client.Object{hpaObj("h", ns, "Deployment", "apps/v1", "web")}, []string{"HorizontalPodAutoscaler/h"}, nil},
		{"hpa_without_api_version", []client.Object{hpaObj("h", ns, "Deployment", "", "web")}, []string{"HorizontalPodAutoscaler/h"}, nil},
		{"hpa_other_deployment", []client.Object{hpaObj("h", ns, "Deployment", "apps/v1", "api")}, nil, nil},
		{"hpa_statefulset_same_name", []client.Object{hpaObj("h", ns, "StatefulSet", "apps/v1", "web")}, nil, nil},
		{"hpa_other_namespace", []client.Object{hpaObj("h", "other", "Deployment", "apps/v1", "web")}, nil, nil},
		{"keda_active", []client.Object{scaledObj("so", ns, "web", nil)}, []string{"ScaledObject/so"}, nil},
		{"keda_paused", []client.Object{scaledObj("so", ns, "web", paused)}, nil, nil},
		{"keda_paused_false", []client.Object{scaledObj("so", ns, "web", map[string]string{"autoscaling.keda.sh/paused": "false"})}, []string{"ScaledObject/so"}, nil},
		{"keda_directional_pause_is_not_a_pause", []client.Object{scaledObj("so", ns, "web", map[string]string{"autoscaling.keda.sh/paused-scale-in": "true"})}, []string{"ScaledObject/so"}, nil},
		{"keda_paused_replicas", []client.Object{scaledObj("so", ns, "web", map[string]string{"autoscaling.keda.sh/paused-replicas": "3"})}, []string{"ScaledObject/so (paused-replicas)"}, nil},
		{"keda_paused_and_paused_replicas", []client.Object{scaledObj("so", ns, "web", map[string]string{"autoscaling.keda.sh/paused": "true", "autoscaling.keda.sh/paused-replicas": "3"})}, []string{"ScaledObject/so (paused-replicas)"}, nil},
		{"keda_paused_but_its_hpa_remains", []client.Object{scaledObj("so", ns, "web", paused), hpaObj("keda-hpa-so", ns, "Deployment", "apps/v1", "web")}, []string{"HorizontalPodAutoscaler/keda-hpa-so"}, nil},
		{"keda_other_target", []client.Object{scaledObj("so", ns, "api", nil)}, nil, nil},
		{"other_active_pa", []client.Object{paObj("other", ns, "web", autoscalerv1alpha1.ModeActive)}, []string{"PredictiveAutoscaler/other"}, nil},
		{"other_recommend_pa", []client.Object{paObj("other", ns, "web", autoscalerv1alpha1.ModeRecommend)}, nil, nil},
		{"other_pa_without_mode_is_recommend", []client.Object{paObj("other", ns, "web", "")}, nil, nil},
		{"other_active_pa_other_target", []client.Object{paObj("other", ns, "api", autoscalerv1alpha1.ModeActive)}, nil, nil},
		{"vpa_auto", []client.Object{vpaObj("v", ns, "web", "Auto")}, nil, []string{"VerticalPodAutoscaler/v (Auto)"}},
		{"vpa_mode_unset_is_auto", []client.Object{vpaObj("v", ns, "web", "")}, nil, []string{"VerticalPodAutoscaler/v (Auto)"}},
		{"vpa_recreate", []client.Object{vpaObj("v", ns, "web", "Recreate")}, nil, []string{"VerticalPodAutoscaler/v (Recreate)"}},
		{"vpa_in_place_or_recreate", []client.Object{vpaObj("v", ns, "web", "InPlaceOrRecreate")}, nil, []string{"VerticalPodAutoscaler/v (InPlaceOrRecreate)"}},
		{"vpa_initial", []client.Object{vpaObj("v", ns, "web", "Initial")}, nil, []string{"VerticalPodAutoscaler/v (Initial)"}},
		{"vpa_off", []client.Object{vpaObj("v", ns, "web", "Off")}, nil, nil},
		{"vpa_other_target", []client.Object{vpaObj("v", ns, "api", "Auto")}, nil, nil},
		{"all_sorted", []client.Object{paObj("zz", ns, "web", autoscalerv1alpha1.ModeActive), scaledObj("so", ns, "web", nil), hpaObj("h", ns, "Deployment", "apps/v1", "web")},
			[]string{"HorizontalPodAutoscaler/h", "PredictiveAutoscaler/zz", "ScaledObject/so"}, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := fake.NewClientBuilder().WithScheme(coexScheme(t)).WithObjects(append([]client.Object{self, dep}, tc.objs...)...).Build()
			got, err := checkReplicaWriters(context.Background(), c, nil, self, dep)
			if err != nil || !got.VPAChecked {
				t.Fatal(err, got)
			}
			if !reflect.DeepEqual(got.Conflicts, tc.conflicts) || !reflect.DeepEqual(got.Warnings, tc.warnings) {
				t.Fatalf("got conflicts %v warnings %v; want %v / %v", got.Conflicts, got.Warnings, tc.conflicts, tc.warnings)
			}
		})
	}
}

// stubDiscovery serves a fixed discovery document.
type stubDiscovery struct {
	groups    []metav1.APIGroup
	groupsErr error
	resources map[string][]string // group/version → resource names
	resErr    map[string]error
}

func (s *stubDiscovery) ServerGroups(context.Context) (*metav1.APIGroupList, error) {
	if s.groupsErr != nil {
		return nil, s.groupsErr
	}
	return &metav1.APIGroupList{Groups: s.groups}, nil
}

func (s *stubDiscovery) ServerResourcesForGroupVersion(_ context.Context, gv string) (*metav1.APIResourceList, error) {
	if err := s.resErr[gv]; err != nil {
		return nil, err
	}
	names, ok := s.resources[gv]
	if !ok {
		return nil, apierrors.NewNotFound(schema.GroupResource{}, gv)
	}
	l := &metav1.APIResourceList{GroupVersion: gv}
	for _, n := range names {
		l.APIResources = append(l.APIResources, metav1.APIResource{Name: n, Namespaced: true})
	}
	return l, nil
}

func apiGroup(name string, versions ...string) metav1.APIGroup {
	g := metav1.APIGroup{Name: name}
	for _, v := range versions {
		g.Versions = append(g.Versions, metav1.GroupVersionForDiscovery{GroupVersion: name + "/" + v, Version: v})
	}
	return g
}

// coreOnly is a cluster without KEDA and without the VPA.
func coreOnly() *stubDiscovery {
	return &stubDiscovery{groups: []metav1.APIGroup{apiGroup("apps", "v1"), apiGroup("autoscaling", "v2", "v1"),
		apiGroup("autoscaler.example.com", "v1alpha1")}, resources: map[string][]string{}}
}

func withKEDA(d *stubDiscovery, version string, resources ...string) *stubDiscovery {
	d.groups = append(d.groups, apiGroup("keda.sh", version))
	if resources != nil {
		d.resources["keda.sh/"+version] = resources
	}
	return d
}

func listKind(list client.ObjectList) string {
	if u, ok := list.(*unstructured.UnstructuredList); ok {
		return u.GroupVersionKind().Kind
	}
	switch list.(type) {
	case *autoscalingv2.HorizontalPodAutoscalerList:
		return "HorizontalPodAutoscalerList"
	case *autoscalerv1alpha1.PredictiveAutoscalerList:
		return "PredictiveAutoscalerList"
	}
	return ""
}

func failingLists(base client.WithWatch, fail map[string]error) client.WithWatch {
	return interceptor.NewClient(base, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err, ok := fail[listKind(list)]; ok {
				return err
			}
			return c.List(ctx, list, opts...)
		},
	})
}

// Absence of an optional API is accepted only from a successful fresh discovery (Codex task-08 r05 BLOCKER: a 404 is
// no proof of absence); everything else fails the check.
func TestCheckReplicaWritersAcceptsAbsenceOnlyFromDiscovery(t *testing.T) {
	const ns = "shop"
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns}}
	self := paObj("self", ns, "web", autoscalerv1alpha1.ModeActive)
	notFound := apierrors.NewNotFound(schema.GroupResource{Group: "keda.sh", Resource: "scaledobjects"}, "")
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "horizontalpodautoscalers"}, "", errors.New("RBAC"))
	noMatch := &meta.NoKindMatchError{GroupKind: schema.GroupKind{Group: "keda.sh", Kind: "ScaledObject"}, SearchedVersions: []string{"v1alpha1"}}
	mustNotList := errors.New("an absent API must not be listed")
	optionalLists := map[string]error{"ScaledObjectList": mustNotList, "VerticalPodAutoscalerList": mustNotList}
	for _, tc := range []struct {
		name    string
		disc    *stubDiscovery
		fail    map[string]error
		wantErr bool
	}{
		{"keda_and_vpa_not_served", coreOnly(), optionalLists, false},
		{"keda_version_served_without_scaledobjects", withKEDA(coreOnly(), "v1alpha1", "scaledjobs"), optionalLists, false},
		{"keda_served_and_readable", withKEDA(coreOnly(), "v1alpha1", "scaledobjects"), nil, false},
		{"keda_served_collection_404", withKEDA(coreOnly(), "v1alpha1", "scaledobjects"), map[string]error{"ScaledObjectList": notFound}, true},
		{"keda_served_rest_mapper_no_match", withKEDA(coreOnly(), "v1alpha1", "scaledobjects"), map[string]error{"ScaledObjectList": noMatch}, true},
		{"keda_served_forbidden", withKEDA(coreOnly(), "v1alpha1", "scaledobjects"), map[string]error{"ScaledObjectList": forbidden}, true},
		{"keda_group_without_v1alpha1", withKEDA(coreOnly(), "v1alpha2", "scaledobjects"), nil, true},
		{"keda_version_discovery_404", withKEDA(coreOnly(), "v1alpha1"), nil, true},
		{"keda_version_discovery_503", func() *stubDiscovery {
			d := withKEDA(coreOnly(), "v1alpha1", "scaledobjects")
			d.resErr = map[string]error{"keda.sh/v1alpha1": apierrors.NewServiceUnavailable("aggregated API down")}
			return d
		}(), nil, true},
		{"discovery_unavailable", &stubDiscovery{groupsErr: errors.New("connection refused")}, nil, true},
		{"vpa_served_timeout", func() *stubDiscovery {
			d := coreOnly()
			d.groups = append(d.groups, apiGroup("autoscaling.k8s.io", "v1"))
			d.resources["autoscaling.k8s.io/v1"] = []string{"verticalpodautoscalers"}
			return d
		}(), map[string]error{"VerticalPodAutoscalerList": apierrors.NewTimeoutError("slow", 1)}, true},
		{"hpa_forbidden", coreOnly(), map[string]error{"HorizontalPodAutoscalerList": forbidden}, true},
		{"pa_list_fails", coreOnly(), map[string]error{"PredictiveAutoscalerList": errors.New("boom")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := fake.NewClientBuilder().WithScheme(coexScheme(t)).WithObjects(self, dep).Build()
			got, err := checkReplicaWriters(context.Background(), failingLists(base, tc.fail), tc.disc, self, dep)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got.VPAChecked == tc.wantErr {
				t.Fatalf("VPAChecked = %v with err %v", got.VPAChecked, err)
			}
		})
	}
}

// The real client, REST mapper and discovery client (not fakes) against a stub API server.
func TestCheckReplicaWritersWithTheRealClient(t *testing.T) {
	var mu sync.Mutex
	keda := "absent"
	resources := func(gv, name, kind string) string {
		b, _ := json.Marshal(metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: gv,
			APIResources: []metav1.APIResource{{Name: name, Namespaced: true, Kind: kind, Verbs: metav1.Verbs{"get", "list", "watch"}}}})
		return string(b)
	}
	groups := func(withKEDA bool, kedaVersion string) string {
		gs := []metav1.APIGroup{apiGroup("apps", "v1"), apiGroup("autoscaling", "v2"), apiGroup("autoscaler.example.com", "v1alpha1")}
		if withKEDA {
			gs = append(gs, apiGroup("keda.sh", kedaVersion))
		}
		for i := range gs {
			gs[i].PreferredVersion = gs[i].Versions[0]
		}
		b, _ := json.Marshal(metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"}, Groups: gs})
		return string(b)
	}
	notFound := `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`
	unavailable := `{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"ServiceUnavailable","code":503}`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		state := keda
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		reply := func(code int, body string) { w.WriteHeader(code); fmt.Fprint(w, body) }
		switch r.URL.Path {
		case "/api":
			reply(200, `{"kind":"APIVersions","versions":["v1"],"serverAddressByClientCIDRs":[]}`)
		case "/apis":
			switch state {
			case "discovery_stalls":
				<-r.Context().Done() // never answers; only the check's deadline ends the request
				return
			case "discovery_down":
				reply(503, unavailable)
			case "absent":
				reply(200, groups(false, ""))
			case "other_version":
				reply(200, groups(true, "v1alpha2"))
			default:
				reply(200, groups(true, "v1alpha1"))
			}
		case "/apis/autoscaling/v2":
			reply(200, resources("autoscaling/v2", "horizontalpodautoscalers", "HorizontalPodAutoscaler"))
		case "/apis/autoscaling/v2/namespaces/shop/horizontalpodautoscalers":
			reply(200, `{"apiVersion":"autoscaling/v2","kind":"HorizontalPodAutoscalerList","metadata":{},"items":[]}`)
		case "/apis/autoscaler.example.com/v1alpha1":
			reply(200, resources("autoscaler.example.com/v1alpha1", "predictiveautoscalers", "PredictiveAutoscaler"))
		case "/apis/autoscaler.example.com/v1alpha1/namespaces/shop/predictiveautoscalers":
			reply(200, `{"apiVersion":"autoscaler.example.com/v1alpha1","kind":"PredictiveAutoscalerList","metadata":{},"items":[]}`)
		case "/apis/keda.sh/v1alpha1":
			if state == "version_404" {
				reply(404, notFound)
				return
			}
			reply(200, resources("keda.sh/v1alpha1", "scaledobjects", "ScaledObject"))
		case "/apis/keda.sh/v1alpha1/namespaces/shop/scaledobjects":
			if state == "collection_404" {
				reply(404, notFound)
				return
			}
			reply(200, `{"apiVersion":"keda.sh/v1alpha1","kind":"ScaledObjectList","metadata":{},"items":[`+
				`{"apiVersion":"keda.sh/v1alpha1","kind":"ScaledObject","metadata":{"name":"so","namespace":"shop"},"spec":{"scaleTargetRef":{"name":"web"}}}]}`)
		default:
			reply(404, notFound)
		}
	}))
	t.Cleanup(srv.Close)
	cfg := &rest.Config{Host: srv.URL}
	c, err := client.New(cfg, client.Options{Scheme: coexScheme(t)})
	if err != nil {
		t.Fatal(err)
	}
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	d := RESTDiscovery{Client: dc.RESTClient()}
	dep := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: "shop"}}
	self := paObj("self", "shop", "web", autoscalerv1alpha1.ModeActive)
	for _, step := range []struct {
		state     string
		wantErr   bool
		conflicts []string
	}{
		{"absent", false, nil},                            // KEDA and the VPA not installed: neither a conflict nor a failed check
		{"collection_404", true, nil},                     // advertised, but the collection answers 404
		{"version_404", true, nil},                        // group advertised, version discovery 404
		{"other_version", true, nil},                      // group served only in a version this operator does not read
		{"discovery_down", true, nil},                     // /apis unavailable
		{"discovery_stalls", true, nil},                   // bounded by the check's context, not by the request timeout
		{"installed", false, []string{"ScaledObject/so"}}, // installed after the operator started: noticed, no restart
	} {
		mu.Lock()
		keda = step.state
		mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		start := time.Now()
		got, err := checkReplicaWriters(ctx, c, d, self, dep)
		cancel()
		if el := time.Since(start); el > 2*time.Second {
			t.Fatalf("%s: the check took %v despite its deadline", step.state, el)
		}
		if (err != nil) != step.wantErr || !reflect.DeepEqual(got.Conflicts, step.conflicts) {
			t.Fatalf("%s: got %+v, err %v", step.state, got, err)
		}
	}
}

// --- reconcile level ---

func hasEvent(events []string, prefix string) bool {
	for _, e := range events {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}
	return false
}

func condition(t *testing.T, r *PredictiveAutoscalerReconciler, nn types.NamespacedName, typ string) metav1.Condition {
	t.Helper()
	var a autoscalerv1alpha1.PredictiveAutoscaler
	if err := r.Get(context.Background(), nn, &a); err != nil {
		t.Fatal(err)
	}
	c := meta.FindStatusCondition(a.Status.Conditions, typ)
	if c == nil {
		t.Fatalf("condition %s missing: %+v", typ, a.Status.Conditions)
	}
	return *c
}

func replicasOf(t *testing.T, r *PredictiveAutoscalerReconciler, nn types.NamespacedName) int32 {
	t.Helper()
	var d appsv1.Deployment
	if err := r.Get(context.Background(), nn, &d); err != nil {
		t.Fatal(err)
	}
	return *d.Spec.Replicas
}

func TestActiveRefusesWhileAnHPATargetsTheDeploymentAndResumesAfter(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000") // would scale 1 → 5
	h := hpaObj("web-hpa", req.Namespace, "Deployment", "apps/v1", req.Name)
	if err := r.Create(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "conflict_hold" || d.AppliedReplicas != 1 || !reflect.DeepEqual(d.Conflicts, []string{"HorizontalPodAutoscaler/web-hpa"}) {
		t.Fatalf("an HPA on the target must block the write: %+v", d)
	}
	if n := replicasOf(t, r, req.NamespacedName); n != 1 {
		t.Fatalf("replicas written despite the conflict: %d", n)
	}
	nn := req.NamespacedName
	if c := condition(t, r, nn, "ConflictDetected"); c.Status != metav1.ConditionTrue || c.Reason != "ReplicaWriter" || !strings.Contains(c.Message, "web-hpa") {
		t.Fatalf("ConflictDetected: %+v", c)
	}
	if c := condition(t, r, nn, "ScalingActive"); c.Status != metav1.ConditionFalse || c.Reason != "Conflict" {
		t.Fatalf("ScalingActive: %+v", c)
	}
	if s := getPA(t, r, req).Status; s.AppliedReplicas != 0 || s.LastScaleTime != nil || s.CalculatedReplicas != 5 {
		t.Fatalf("status must show the calculated count and no write: %+v", s)
	}
	if ev := drain(rec); !hasEvent(ev, "Warning ConflictDetected") {
		t.Fatalf("missing ConflictDetected warning: %v", ev)
	}
	runRPMReconcile(t, r, req, path) // still in conflict: no repeated event
	if ev := drain(rec); hasEvent(ev, "Warning ConflictDetected") {
		t.Fatalf("the conflict event must not repeat every reconcile: %v", ev)
	}

	if err := r.Delete(context.Background(), h); err != nil {
		t.Fatal(err)
	}
	d = runRPMReconcile(t, r, req, path)
	if d.Action != "scale_up" || d.AppliedReplicas != 5 || len(d.Conflicts) != 0 {
		t.Fatalf("after the HPA is gone the operator must scale: %+v", d)
	}
	if c := condition(t, r, nn, "ConflictDetected"); c.Status != metav1.ConditionFalse {
		t.Fatalf("ConflictDetected after resolution: %+v", c)
	}
	if ev := drain(rec); !hasEvent(ev, "Normal ConflictResolved") {
		t.Fatalf("missing ConflictResolved: %v", ev)
	}
}

func TestActiveRefusesAnUnpausedScaledObjectButNotAPausedOne(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	so := scaledObj("web-so", req.Namespace, req.Name, nil)
	if err := r.Create(context.Background(), so); err != nil {
		t.Fatal(err)
	}
	if d := runRPMReconcile(t, r, req, path); d.Action != "conflict_hold" {
		t.Fatalf("an active ScaledObject must block: %+v", d)
	}
	so.SetAnnotations(map[string]string{"autoscaling.keda.sh/paused": "true"})
	if err := r.Update(context.Background(), so); err != nil {
		t.Fatal(err)
	}
	if d := runRPMReconcile(t, r, req, path); d.Action != "scale_up" || d.AppliedReplicas != 5 {
		t.Fatalf("a paused ScaledObject without an HPA is the documented hand-over state: %+v", d)
	}
}

func TestTwoActiveAutoscalersOnOneTargetBothRefuse(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	other := getPA(t, r, req).DeepCopy()
	other.ObjectMeta = metav1.ObjectMeta{Name: req.Name + "-twin", Namespace: req.Namespace, UID: "twin"}
	other.Status = autoscalerv1alpha1.PredictiveAutoscalerStatus{}
	if err := r.Create(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if d := runRPMReconcile(t, r, req, path); d.Action != "conflict_hold" || !reflect.DeepEqual(d.Conflicts, []string{"PredictiveAutoscaler/" + other.Name}) {
		t.Fatalf("another Active autoscaler must block: %+v", d)
	}
	twinReq := req
	twinReq.Name = other.Name
	delete(r.lastReconcileMap, twinReq.NamespacedName.String())
	if _, err := r.Reconcile(context.Background(), twinReq); err != nil {
		t.Fatal(err)
	}
	if n := replicasOf(t, r, req.NamespacedName); n != 1 {
		t.Fatalf("the twin wrote %d replicas", n)
	}
}

func TestRecommendModeReportsConflictsAndChangesNothing(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	if err := r.Create(context.Background(), hpaObj("web-hpa", req.Namespace, "Deployment", "apps/v1", req.Name)); err != nil {
		t.Fatal(err)
	}
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "recommend" || len(d.Conflicts) != 1 {
		t.Fatalf("Recommend mode records the decision and the conflict: %+v", d)
	}
	nn := req.NamespacedName
	if c := condition(t, r, nn, "ConflictDetected"); c.Status != metav1.ConditionTrue {
		t.Fatalf("ConflictDetected: %+v", c)
	}
	if c := condition(t, r, nn, "ScalingActive"); c.Reason != "RecommendMode" {
		t.Fatalf("ScalingActive: %+v", c)
	}
}

func TestAFailedCoexistenceCheckBlocksWrites(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	r.APIReader = failingLists(r.Client.(client.WithWatch), map[string]error{
		"HorizontalPodAutoscalerList": apierrors.NewForbidden(schema.GroupResource{Group: "autoscaling", Resource: "horizontalpodautoscalers"}, "", errors.New("RBAC"))})
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "conflict_hold" || d.ConflictCheckError == nil || d.AppliedReplicas != 1 {
		t.Fatalf("a failed check must block like a conflict: %+v", d)
	}
	nn := req.NamespacedName
	if c := condition(t, r, nn, "ConflictDetected"); c.Status != metav1.ConditionUnknown || c.Reason != "CheckFailed" {
		t.Fatalf("ConflictDetected: %+v", c)
	}
	if c := condition(t, r, nn, "ScalingActive"); c.Status != metav1.ConditionFalse || c.Reason != "CheckFailed" {
		t.Fatalf("ScalingActive: %+v", c)
	}
	if ev := drain(rec); !hasEvent(ev, "Warning ConflictCheckFailed") {
		t.Fatalf("missing ConflictCheckFailed: %v", ev)
	}
}

func TestAbsentOptionalAPIsDoNotBlock(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	r.Discovery = coreOnly()
	mustNotList := errors.New("an absent API must not be listed")
	r.APIReader = failingLists(r.Client.(client.WithWatch), map[string]error{
		"ScaledObjectList": mustNotList, "VerticalPodAutoscalerList": mustNotList})
	if d := runRPMReconcile(t, r, req, path); d.Action != "scale_up" || d.ConflictCheckError != nil {
		t.Fatalf("a cluster without KEDA/VPA must scale normally: %+v", d)
	}
	if c := condition(t, r, req.NamespacedName, "ConflictDetected"); c.Status != metav1.ConditionFalse {
		t.Fatalf("ConflictDetected: %+v", c)
	}
}

func TestAServedButUnreadableOptionalAPIBlocks(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	r.Discovery = withKEDA(coreOnly(), "v1alpha1", "scaledobjects")
	r.APIReader = failingLists(r.Client.(client.WithWatch), map[string]error{
		"ScaledObjectList": apierrors.NewNotFound(schema.GroupResource{Group: "keda.sh", Resource: "scaledobjects"}, "")})
	if d := runRPMReconcile(t, r, req, path); d.Action != "conflict_hold" || d.ConflictCheckError == nil {
		t.Fatalf("an advertised KEDA that cannot be read must block: %+v", d)
	}
}

// A stalled API request ends at the deadline and becomes a hold (Codex r05).
func TestAStalledCheckBecomesAHold(t *testing.T) {
	old := coexistenceTimeout
	coexistenceTimeout = 50 * time.Millisecond
	t.Cleanup(func() { coexistenceTimeout = old })
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			<-ctx.Done() // the API server never answers
			return ctx.Err()
		},
	})
	start := time.Now()
	d := runRPMReconcile(t, r, req, path)
	if d.Action != "conflict_hold" || d.ConflictCheckError == nil || !strings.Contains(*d.ConflictCheckError, "deadline") {
		t.Fatalf("a stalled check must hold: %+v", d)
	}
	if el := time.Since(start); el > 5*time.Second {
		t.Fatalf("the reconcile took %v", el)
	}
}

func TestAStalledGuardAbortsTheWrite(t *testing.T) {
	old := identityGuardTimeout
	identityGuardTimeout = 50 * time.Millisecond
	t.Cleanup(func() { identityGuardTimeout = old })
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			<-ctx.Done()
			return ctx.Err()
		},
	})
	if d := runRPMReconcile(t, r, req, path); d.Action != "guard_abort" || d.AppliedReplicas != 1 {
		t.Fatalf("a stalled guard must abort the write: %+v", d)
	}
}

// VPAInterference is Unknown, not False, when its check did not complete, and only a completed check clears it.
func TestVPAInterferenceIsUnknownWhenTheCheckFails(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	if err := r.Create(context.Background(), vpaObj("web-vpa", req.Namespace, req.Name, "Auto")); err != nil {
		t.Fatal(err)
	}
	runRPMReconcile(t, r, req, path)
	if c := condition(t, r, req.NamespacedName, "VPAInterference"); c.Status != metav1.ConditionTrue {
		t.Fatalf("precondition: %+v", c)
	}
	drain(rec)
	healthy := r.APIReader
	r.APIReader = failingLists(r.Client.(client.WithWatch), map[string]error{"HorizontalPodAutoscalerList": errors.New("API down")})
	runRPMReconcile(t, r, req, path)
	if c := condition(t, r, req.NamespacedName, "VPAInterference"); c.Status != metav1.ConditionUnknown || c.Reason != "CheckFailed" {
		t.Fatalf("an incomplete check must not report False: %+v", c)
	}
	if ev := drain(rec); hasEvent(ev, "Normal VPAInterferenceCleared") {
		t.Fatalf("an incomplete check must not clear the warning: %v", ev)
	}
	r.APIReader = healthy
	var v unstructured.Unstructured
	v.SetGroupVersionKind(schema.GroupVersionKind{Group: "autoscaling.k8s.io", Version: "v1", Kind: "VerticalPodAutoscaler"})
	if err := r.Get(context.Background(), types.NamespacedName{Namespace: req.Namespace, Name: "web-vpa"}, &v); err != nil {
		t.Fatal(err)
	}
	if err := r.Delete(context.Background(), &v); err != nil {
		t.Fatal(err)
	}
	runRPMReconcile(t, r, req, path)
	if c := condition(t, r, req.NamespacedName, "VPAInterference"); c.Status != metav1.ConditionFalse {
		t.Fatalf("VPAInterference after the VPA is gone: %+v", c)
	}
}

func TestVPAIsAWarningNotAConflict(t *testing.T) {
	r, req, path, rec := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	if err := r.Create(context.Background(), vpaObj("web-vpa", req.Namespace, req.Name, "Auto")); err != nil {
		t.Fatal(err)
	}
	if d := runRPMReconcile(t, r, req, path); d.Action != "scale_up" {
		t.Fatalf("a VPA does not own the replica count: %+v", d)
	}
	if c := condition(t, r, req.NamespacedName, "VPAInterference"); c.Status != metav1.ConditionTrue {
		t.Fatalf("VPAInterference: %+v", c)
	}
	if ev := drain(rec); !hasEvent(ev, "Warning VPAInterference") {
		t.Fatalf("missing VPAInterference warning: %v", ev)
	}
}

// The write guard: the autoscaler or its target changed between the start of the reconcile and the write.
func TestWriteGuardAbortsWhenTheObjectsChangedDuringTheReconcile(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(obj client.Object) error
	}{
		{"mode_switched_to_recommend", func(o client.Object) error {
			if a, ok := o.(*autoscalerv1alpha1.PredictiveAutoscaler); ok {
				a.Spec.Mode = autoscalerv1alpha1.ModeRecommend
			}
			return nil
		}},
		{"spec_changed", func(o client.Object) error {
			if a, ok := o.(*autoscalerv1alpha1.PredictiveAutoscaler); ok {
				a.Generation++
			}
			return nil
		}},
		{"autoscaler_recreated", func(o client.Object) error {
			if a, ok := o.(*autoscalerv1alpha1.PredictiveAutoscaler); ok {
				a.UID = "recreated"
			}
			return nil
		}},
		{"autoscaler_deleted", func(o client.Object) error {
			if _, ok := o.(*autoscalerv1alpha1.PredictiveAutoscaler); ok {
				return apierrors.NewNotFound(schema.GroupResource{Resource: "predictiveautoscalers"}, o.GetName())
			}
			return nil
		}},
		{"target_replaced", func(o client.Object) error {
			if d, ok := o.(*appsv1.Deployment); ok {
				d.UID = "replacement"
			}
			return nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
			r.APIReader = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if err := c.Get(ctx, key, obj, opts...); err != nil {
						return err
					}
					return tc.mutate(obj)
				},
			})
			d := runRPMReconcile(t, r, req, path)
			if d.Action != "guard_abort" || d.AppliedReplicas != 1 {
				t.Fatalf("the write must be aborted: %+v", d)
			}
			if tc.name == "autoscaler_deleted" {
				return // the status write itself may fail for a deleted object
			}
			if c := condition(t, r, req.NamespacedName, "ScalingActive"); c.Status != metav1.ConditionFalse || c.Reason != "GuardAborted" {
				t.Fatalf("ScalingActive: %+v", c)
			}
		})
	}
}

func TestCrossNamespaceTargetIsRejected(t *testing.T) {
	r, req, _, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "3000")
	a := getPA(t, r, req)
	a.Spec.TargetDeployment.Namespace = "elsewhere"
	if err := r.Update(context.Background(), a); err != nil {
		t.Fatal(err)
	}
	delete(r.lastReconcileMap, req.NamespacedName.String())
	if _, err := r.Reconcile(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if c := condition(t, r, req.NamespacedName, "Ready"); c.Status != metav1.ConditionFalse || c.Reason != "CrossNamespaceTarget" {
		t.Fatalf("Ready: %+v", c)
	}
	if n := replicasOf(t, r, req.NamespacedName); n != 1 {
		t.Fatalf("replicas changed: %d", n)
	}
}

func TestAConflictHoldStopsTheScaleDownTimer(t *testing.T) {
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeActive, "300") // needs 1 replica
	var d appsv1.Deployment
	if err := r.Get(context.Background(), req.NamespacedName, &d); err != nil {
		t.Fatal(err)
	}
	five := int32(5)
	d.Spec.Replicas = &five
	if err := r.Update(context.Background(), &d); err != nil {
		t.Fatal(err)
	}
	if dec := runRPMReconcile(t, r, req, path); dec.Action != "hold_stabilizing" {
		t.Fatalf("precondition: %+v", dec)
	}
	st := r.scaleStates[req.NamespacedName.String()]
	if st.belowCurrentSince.IsZero() {
		t.Fatal("precondition: the scale-down timer runs")
	}
	st.belowCurrentSince = time.Now().Add(-time.Hour) // the stabilization window has long passed
	if err := r.Create(context.Background(), hpaObj("web-hpa", req.Namespace, "Deployment", "apps/v1", req.Name)); err != nil {
		t.Fatal(err)
	}
	if dec := runRPMReconcile(t, r, req, path); dec.Action != "conflict_hold" {
		t.Fatalf("conflict: %+v", dec)
	}
	if !r.scaleStates[req.NamespacedName.String()].belowCurrentSince.IsZero() {
		t.Fatal("a conflict hold must stop the scale-down timer")
	}
}

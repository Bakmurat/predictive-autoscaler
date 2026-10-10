package controllers

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// envtest: the write path against a real kube-apiserver + etcd (Codex task-08 r03: /scale resourceVersion handling,
// target re-creation and mode changes during a reconcile). The binaries come from setup-envtest
// (KUBEBUILDER_ASSETS); without them these tests skip, unless ENVTEST_REQUIRED=1 (CI), where they fail.

var (
	envOnce sync.Once
	envCfg  *rest.Config
	envErr  error
	env     *envtest.Environment
	envSeq  int
	envMu   sync.Mutex
)

func TestMain(m *testing.M) {
	code := m.Run()
	if env != nil {
		_ = env.Stop()
	}
	os.Exit(code)
}

func envScheme() *runtime.Scheme {
	s := runtime.NewScheme()
	_ = scheme.AddToScheme(s)
	_ = autoscalerv1alpha1.AddToScheme(s)
	return s
}

func startEnv(t *testing.T) *rest.Config {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("ENVTEST_REQUIRED") == "1" {
			t.Fatal("ENVTEST_REQUIRED=1 but KUBEBUILDER_ASSETS is not set")
		}
		t.Skip("envtest binaries not available (set KUBEBUILDER_ASSETS, see setup-envtest)")
	}
	envOnce.Do(func() {
		env = &envtest.Environment{
			// The generated CRD, and the legacy group's CRD as a fixture (legacy objects are other replica writers).
			CRDDirectoryPaths: []string{filepath.Join("..", "..", "k8s-manifests", "base", "01-crd.yaml"),
				filepath.Join("testdata", "legacy-crd.yaml")},
			ErrorIfCRDPathMissing: true,
		}
		envCfg, envErr = env.Start()
	})
	if envErr != nil {
		t.Fatal(envErr)
	}
	return envCfg
}

type envFixture struct {
	r    *PredictiveAutoscalerReconciler
	c    client.WithWatch // a direct client: the "other writer" and the test's own reads
	req  ctrl.Request
	path string
}

// envHarness creates a namespace, a Deployment at 1 replica and an Active PredictiveAutoscaler that wants 5
// (3000 rpm at 10 rps per pod), with the forecasting service down.
func envHarness(t *testing.T, mode string) *envFixture {
	t.Helper()
	cfg := startEnv(t)
	s := envScheme()
	c, err := client.NewWithWatch(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	envMu.Lock()
	envSeq++
	ns := fmt.Sprintf("envtest-%d", envSeq)
	envMu.Unlock()
	ctx := context.Background()
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	one := int32(1)
	labels := map[string]string{"app": "web"}
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns, Labels: map[string]string{"owner": "gitops"}},
		Spec: appsv1.DeploymentSpec{
			Replicas: &one,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "nginx"}}},
			},
		},
	}
	if err := c.Create(ctx, dep); err != nil {
		t.Fatal(err)
	}
	pa := &autoscalerv1alpha1.PredictiveAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: ns},
		Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{
			TargetDeployment: autoscalerv1alpha1.TargetDeployment{Name: "web", Namespace: ns},
			MinReplicas:      1, MaxReplicas: 12, Mode: mode,
			Metrics:    autoscalerv1alpha1.MetricsConfig{Requests: &autoscalerv1alpha1.RequestsMetric{TargetRPS: 10}},
			Prediction: autoscalerv1alpha1.PredictionConfig{HorizonMinutes: 60, LeadTimeMinutes: 20},
		},
	}
	if err := c.Create(ctx, pa); err != nil {
		t.Fatal(err)
	}
	vm := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"vector","result":[{"value":[1,"50"]}]}}`) // 3000 req/min
	}))
	t.Cleanup(vm.Close)
	t.Setenv("VICTORIAMETRICS_URL", vm.URL)
	ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }))
	t.Cleanup(ml.Close)
	t.Setenv("ML_API_URL", ml.URL)
	path := filepath.Join(t.TempDir(), "decisions.jsonl")
	t.Setenv("FORECAST_LOG", path)
	t.Cleanup(func() {
		for _, g := range []interface{ DeleteLabelValues(...string) bool }{currentRpmGauge, actualNeededReplicasGauge,
			predictionErrorPercentGauge, predictedRpmGauge} {
			g.DeleteLabelValues("web", ns)
		}
	})
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	r := &PredictiveAutoscalerReconciler{Client: c, Scheme: s, Log: logr.Discard(), APIReader: c,
		Discovery: RESTDiscovery{Client: dc.RESTClient()}, predictionCache: map[string]*cachedPrediction{}}
	return &envFixture{r: r, c: c, req: ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "web"}}, path: path}
}

func (f *envFixture) deployment(t *testing.T) *appsv1.Deployment {
	t.Helper()
	var d appsv1.Deployment
	if err := f.c.Get(context.Background(), f.req.NamespacedName, &d); err != nil {
		t.Fatal(err)
	}
	return &d
}

// replaceTarget deletes the Deployment and creates it again with the same spec (same replica count, new UID).
func replaceTarget(ctx context.Context, c client.Client, key client.ObjectKey) error {
	var old appsv1.Deployment
	if err := c.Get(ctx, key, &old); err != nil {
		return err
	}
	if err := c.Delete(ctx, &old); err != nil {
		return err
	}
	fresh := old.DeepCopy()
	fresh.ObjectMeta = metav1.ObjectMeta{Name: old.Name, Namespace: old.Namespace, Labels: old.Labels}
	fresh.Status = appsv1.DeploymentStatus{}
	return c.Create(ctx, fresh)
}

func TestEnvtestScalesThroughTheScaleSubresourceOnly(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	before := f.deployment(t)
	reconcileOnce(t, f.r, f.req)
	d := lastDecision(t, f.path)
	after := f.deployment(t)
	if d.Action != "scale_up" || *after.Spec.Replicas != 5 {
		t.Fatalf("decision %+v, replicas %d", d, *after.Spec.Replicas)
	}
	if after.Labels["owner"] != "gitops" || after.Spec.Template.Spec.Containers[0].Image != "nginx" {
		t.Fatal("the write touched more than the replica count")
	}
	var scaleWriter bool
	for _, m := range after.ManagedFields {
		if m.Subresource == "scale" {
			scaleWriter = true
		}
	}
	if !scaleWriter || after.Generation != before.Generation+1 {
		t.Fatalf("expected one write through /scale: managedFields %+v, generation %d → %d", after.ManagedFields, before.Generation, after.Generation)
	}
}

// The real API server rejects the /scale update when the Deployment changed after the operator read its Scale (here
// a label: the replica count is unchanged, only the resourceVersion moved).
func TestEnvtestAScaleWriteWithAnOldResourceVersionIsRejected(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	other := f.c
	f.r.Client = interceptor.NewClient(f.c, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				var d appsv1.Deployment
				if err := other.Get(ctx, client.ObjectKeyFromObject(obj), &d); err != nil {
					return err
				}
				d.Labels["touched"] = "yes"
				if err := other.Update(ctx, &d); err != nil {
					return err
				}
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	res := reconcileOnce(t, f.r, f.req)
	d := lastDecision(t, f.path)
	if d.Action != "guard_abort" || *f.deployment(t).Spec.Replicas != 1 || res.RequeueAfter != staleDecisionRetry {
		t.Fatalf("the API server's conflict must abort the write: %+v, replicas %d, requeue %v", d, *f.deployment(t).Spec.Replicas, res.RequeueAfter)
	}
	f.r.Client = f.c
	reconcileOnce(t, f.r, f.req) // the follow-up reconcile decides afresh and writes
	if n := *f.deployment(t).Spec.Replicas; n != 5 {
		t.Fatalf("follow-up reconcile: replicas %d", n)
	}
}

// Another writer sets the count after the decision: the fresh Scale shows it and nothing is written over it.
func TestEnvtestAnotherWritersCountIsNotOverwritten(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	other := f.c
	f.r.Client = interceptor.NewClient(f.c, interceptor.Funcs{
		SubResourceGet: func(ctx context.Context, c client.Client, sub string, obj client.Object, out client.Object, opts ...client.SubResourceGetOption) error {
			if sub == "scale" {
				d := &appsv1.Deployment{}
				if err := other.Get(ctx, client.ObjectKeyFromObject(obj), d); err != nil {
					return err
				}
				three := int32(3)
				d.Spec.Replicas = &three
				if err := other.Update(ctx, d); err != nil {
					return err
				}
			}
			return c.SubResource(sub).Get(ctx, obj, out, opts...)
		},
	})
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "guard_abort" || *f.deployment(t).Spec.Replicas != 3 {
		t.Fatalf("the other writer's 3 must stand: %+v, replicas %d", d, *f.deployment(t).Spec.Replicas)
	}
}

// The target is replaced after the Scale read, before the final authorization: the guard sees the new UID.
func TestEnvtestARecreatedTargetIsNotWritten(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	recreated := false
	f.r.APIReader = interceptor.NewClient(f.c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && !recreated {
				recreated = true
				if err := replaceTarget(ctx, c, key); err != nil {
					return err
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "guard_abort" || *f.deployment(t).Spec.Replicas != 1 {
		t.Fatalf("the re-created target must not be written: %+v", d)
	}
	if c := condition(t, f.r, f.req.NamespacedName, "ScalingActive"); !strings.Contains(c.Message, "replaced") {
		t.Fatalf("ScalingActive: %+v", c)
	}
}

// The user switches the autoscaler to Recommend while the reconcile runs: the real generation bump stops the write.
func TestEnvtestAModeChangeDuringTheReconcileStopsTheWrite(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	switched := false
	f.r.APIReader = interceptor.NewClient(f.c, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*autoscalerv1alpha1.PredictiveAutoscaler); ok && !switched {
				switched = true
				var pa autoscalerv1alpha1.PredictiveAutoscaler
				if err := c.Get(ctx, key, &pa); err != nil {
					return err
				}
				pa.Spec.Mode = autoscalerv1alpha1.ModeRecommend
				if err := c.Update(ctx, &pa); err != nil {
					return err
				}
			}
			return c.Get(ctx, key, obj, opts...)
		},
	})
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "guard_abort" || *f.deployment(t).Spec.Replicas != 1 {
		t.Fatalf("a mode change during the reconcile must stop the write: %+v", d)
	}
}

// The CRD's structural default: an autoscaler created without spec.mode is Recommend and never writes.
func TestEnvtestAnAutoscalerWithoutModeDefaultsToRecommend(t *testing.T) {
	f := envHarness(t, "")
	var pa autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(context.Background(), f.req.NamespacedName, &pa); err != nil {
		t.Fatal(err)
	}
	if pa.Spec.Mode != autoscalerv1alpha1.ModeRecommend {
		t.Fatalf("the API server must default spec.mode to Recommend, got %q", pa.Spec.Mode)
	}
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "recommend" || *f.deployment(t).Spec.Replicas != 1 {
		t.Fatalf("Recommend must not write: %+v", d)
	}
}

// Codex r07 BLOCKER: a replacement with the same replica count right before the Scale read must not receive the old
// target's decision (the Scale's UID is the replacement's).
func TestEnvtestAReplacementBeforeTheScaleReadIsNotWritten(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	other := f.c
	done := false
	f.r.Client = interceptor.NewClient(f.c, interceptor.Funcs{
		SubResourceGet: func(ctx context.Context, c client.Client, sub string, obj client.Object, out client.Object, opts ...client.SubResourceGetOption) error {
			if sub == "scale" && !done {
				done = true
				if err := replaceTarget(ctx, other, client.ObjectKeyFromObject(obj)); err != nil {
					return err
				}
			}
			return c.SubResource(sub).Get(ctx, obj, out, opts...)
		},
	})
	reconcileOnce(t, f.r, f.req)
	d := lastDecision(t, f.path)
	if d.Action != "guard_abort" || *f.deployment(t).Spec.Replicas != 1 {
		t.Fatalf("the replacement must not be scaled with the old decision: %+v, replicas %d", d, *f.deployment(t).Spec.Replicas)
	}
	if c := condition(t, f.r, f.req.NamespacedName, "ScalingActive"); !strings.Contains(c.Message, "replaced") {
		t.Fatalf("ScalingActive: %+v", c)
	}
}

// A replacement after the Scale read and before the update: the API server rejects the old UID (precondition).
func TestEnvtestAReplacementBeforeTheScaleUpdateIsRejectedByTheServer(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	other := f.c
	done := false
	f.r.Client = interceptor.NewClient(f.c, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" && !done {
				done = true
				if err := replaceTarget(ctx, other, client.ObjectKeyFromObject(obj)); err != nil {
					return err
				}
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	res := reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "guard_abort" || *f.deployment(t).Spec.Replicas != 1 || res.RequeueAfter != staleDecisionRetry {
		t.Fatalf("the server must reject the write to the replacement: %+v, replicas %d", d, *f.deployment(t).Spec.Replicas)
	}
}

// After an aborted write, an HPA created before the next reconcile is found by that reconcile's fresh check.
func TestEnvtestAnHPACreatedAfterAnAbortedWriteBlocksTheRetry(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	other := f.c
	f.r.Client = interceptor.NewClient(f.c, interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, c client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if sub == "scale" {
				var d appsv1.Deployment
				if err := other.Get(ctx, client.ObjectKeyFromObject(obj), &d); err != nil {
					return err
				}
				d.Labels["touched"] = "yes" // moves the resourceVersion: the update conflicts
				if err := other.Update(ctx, &d); err != nil {
					return err
				}
			}
			return c.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "guard_abort" {
		t.Fatalf("precondition: %+v", d)
	}
	f.r.Client = f.c
	if err := other.Create(context.Background(), hpaObj("web-hpa", f.req.Namespace, "Deployment", "apps/v1", "web")); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "conflict_hold" || *f.deployment(t).Spec.Replicas != 1 {
		t.Fatalf("the retry must see the new HPA and hold: %+v", d)
	}
}

// The CRD's validation of spec.metrics.requests.source on the real API server (CEL rule and default).
func TestEnvtestTheMetricSourceRulesAreEnforcedByTheAPIServer(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeRecommend)
	ctx := context.Background()
	try := func(name string, src *autoscalerv1alpha1.MetricSource) error {
		pa := &autoscalerv1alpha1.PredictiveAutoscaler{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: f.req.Namespace},
			Spec: autoscalerv1alpha1.PredictiveAutoscalerSpec{
				TargetDeployment: autoscalerv1alpha1.TargetDeployment{Name: "web", Namespace: f.req.Namespace},
				MinReplicas:      1, MaxReplicas: 3,
				Metrics: autoscalerv1alpha1.MetricsConfig{Requests: &autoscalerv1alpha1.RequestsMetric{TargetRPS: 10, Source: src}},
			},
		}
		return f.c.Create(ctx, pa)
	}
	if err := try("istio-with-query", &autoscalerv1alpha1.MetricSource{Preset: "istio", Query: "sum(x)"}); err == nil {
		t.Fatal("a query with the istio preset must be rejected")
	}
	if err := try("prometheus-without-query", &autoscalerv1alpha1.MetricSource{Preset: "prometheus"}); err == nil {
		t.Fatal("preset prometheus without a query must be rejected")
	}
	if err := try("unknown-preset", &autoscalerv1alpha1.MetricSource{Preset: "nginx-ingress"}); err == nil {
		t.Fatal("an unknown preset must be rejected")
	}
	if err := try("prometheus-ok", &autoscalerv1alpha1.MetricSource{Preset: "prometheus", Query: `sum(rate(x{d="{{ .Name }}"}[1m]))`}); err != nil {
		t.Fatalf("a prometheus query must be accepted: %v", err)
	}
	if err := try("defaulted", &autoscalerv1alpha1.MetricSource{}); err != nil {
		t.Fatal(err)
	}
	var pa autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(ctx, types.NamespacedName{Namespace: f.req.Namespace, Name: "defaulted"}, &pa); err != nil {
		t.Fatal(err)
	}
	if pa.Spec.Metrics.Requests.Source == nil || pa.Spec.Metrics.Requests.Source.Preset != "istio" {
		t.Fatalf("the preset must default to istio: %+v", pa.Spec.Metrics.Requests.Source)
	}
}

// The generated CRD (B5a) on the real API server: the bounds and defaults kept from the hand-written CRD, and the new
// minReplicas <= maxReplicas rule.
func TestEnvtestTheGeneratedSchemaValidatesAndDefaults(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeRecommend)
	ctx := context.Background()
	create := func(name string, spec map[string]interface{}) (*unstructured.Unstructured, error) {
		base := map[string]interface{}{"targetDeployment": map[string]interface{}{"name": "web", "namespace": f.req.Namespace},
			"minReplicas": int64(1), "maxReplicas": int64(3)}
		for k, v := range spec {
			base[k] = v
		}
		u := &unstructured.Unstructured{Object: map[string]interface{}{
			"apiVersion": autoscalerv1alpha1.GroupVersion.String(), "kind": "PredictiveAutoscaler",
			"metadata": map[string]interface{}{"name": name, "namespace": f.req.Namespace}, "spec": base,
		}}
		return u, f.c.Create(ctx, u)
	}
	for name, spec := range map[string]map[string]interface{}{
		"min-above-max":   {"minReplicas": int64(5), "maxReplicas": int64(3)},
		"zero-min":        {"minReplicas": int64(0)},
		"cpu-over-100":    {"metrics": map[string]interface{}{"cpu": map[string]interface{}{"targetPercent": int64(101)}}},
		"zero-target-rps": {"metrics": map[string]interface{}{"requests": map[string]interface{}{"targetRPS": int64(0)}}},
		"short-horizon":   {"prediction": map[string]interface{}{"horizonMinutes": int64(4)}},
		"fast-update":     {"prediction": map[string]interface{}{"updateIntervalSeconds": int64(59)}},
		"zero-baseline":   {"resources": map[string]interface{}{"baselineRPM": int64(0)}},
		"unknown-mode":    {"mode": "Auto"},
		"empty-target":    {"targetDeployment": map[string]interface{}{"name": "", "namespace": f.req.Namespace}},
	} {
		if _, err := create(name, spec); err == nil {
			t.Errorf("%s: must be rejected", name)
		}
	}
	noSpec := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": autoscalerv1alpha1.GroupVersion.String(), "kind": "PredictiveAutoscaler",
		"metadata": map[string]interface{}{"name": "no-spec", "namespace": f.req.Namespace},
	}}
	if err := f.c.Create(ctx, noSpec); err == nil {
		t.Error("an object without spec must be rejected")
	}
	off, err := create("explicit-off", map[string]interface{}{"metrics": map[string]interface{}{
		"cpu": map[string]interface{}{"enabled": false}, "requests": map[string]interface{}{"enabled": false}}})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"spec.metrics.cpu.enabled", "spec.metrics.requests.enabled"} {
		if got, _, _ := unstructured.NestedFieldNoCopy(off.Object, strings.Split(path, ".")...); got != false {
			t.Errorf("%s = %v: an explicit false must not be defaulted", path, got)
		}
	}
	u, err := create("defaults", map[string]interface{}{"metrics": map[string]interface{}{"cpu": map[string]interface{}{}},
		"prediction": map[string]interface{}{}, "resources": map[string]interface{}{}})
	if err != nil {
		t.Fatal(err)
	}
	for path, want := range map[string]interface{}{
		"spec.mode": "Recommend", "spec.metrics.cpu.enabled": true, "spec.metrics.cpu.targetPercent": int64(70),
		"spec.prediction.enabled": true, "spec.prediction.horizonMinutes": int64(60), "spec.prediction.leadTimeMinutes": int64(15),
		"spec.prediction.updateIntervalSeconds": int64(300), "spec.resources.baselineRPM": int64(10000),
		"spec.resources.cpuRequestMillicores": int64(100), "spec.resources.memoryRequestMB": int64(128),
	} {
		got, found, _ := unstructured.NestedFieldNoCopy(u.Object, strings.Split(path, ".")...)
		if !found || got != want {
			t.Errorf("%s = %v (found %v), want %v", path, got, found, want)
		}
	}
	var crd unstructured.Unstructured
	crd.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	if err := f.c.Get(ctx, types.NamespacedName{Name: "predictiveautoscalers." + autoscalerv1alpha1.GroupVersion.Group}, &crd); err != nil {
		t.Fatal(err)
	}
	if crd.GetLabels()["autoscaling.devkuban.com/crd-revision"] != "1" {
		t.Fatalf("the CRD revision label is missing: %v", crd.GetLabels())
	}
}

// A legacy-group PredictiveAutoscaler in another namespace that targets the Deployment blocks Active writes through the
// real API server's discovery and an all-namespaces list, and the block lifts once it is deleted (r23/r24).
func TestEnvtestALegacyAutoscalerElsewhereBlocksActiveWrites(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeActive)
	ctx := context.Background()
	legacyNS := f.req.Namespace + "-legacy"
	if err := f.c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: legacyNS}}); err != nil {
		t.Fatal(err)
	}
	old := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": autoscalerv1alpha1.LegacyGroup + "/v1alpha1", "kind": "PredictiveAutoscaler",
		"metadata": map[string]interface{}{"name": "old", "namespace": legacyNS},
		"spec":     map[string]interface{}{"targetDeployment": map[string]interface{}{"name": "web", "namespace": f.req.Namespace}},
	}}
	if err := f.c.Create(ctx, old); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.c.Delete(context.Background(), old) })
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "conflict_hold" || *f.deployment(t).Spec.Replicas != 1 {
		t.Fatalf("a legacy autoscaler on the target must block the write: %+v", d)
	}
	if c := condition(t, f.r, f.req.NamespacedName, "ConflictDetected"); c.Status != metav1.ConditionTrue ||
		!strings.Contains(c.Message, "PredictiveAutoscaler."+autoscalerv1alpha1.LegacyGroup+"/"+legacyNS+"/old") {
		t.Fatalf("ConflictDetected must name the legacy object: %+v", c)
	}
	var withConflict autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(ctx, f.req.NamespacedName, &withConflict); err != nil {
		t.Fatal(err)
	}
	if want := []autoscalerv1alpha1.ReplicaWriter{{Group: autoscalerv1alpha1.LegacyGroup, Kind: "PredictiveAutoscaler", Namespace: legacyNS,
		Name: "old", Reason: autoscalerv1alpha1.WriterReasonLegacyAPIGroup}}; !reflect.DeepEqual(withConflict.Status.Conflicts, want) {
		t.Fatalf("status.conflicts = %v, want %v", withConflict.Status.Conflicts, want)
	}
	if err := f.c.Delete(ctx, old); err != nil {
		t.Fatal(err)
	}
	reconcileOnce(t, f.r, f.req)
	if d := lastDecision(t, f.path); d.Action != "scale_up" || *f.deployment(t).Spec.Replicas != 5 {
		t.Fatalf("after the legacy object is gone the write proceeds: %+v", d)
	}
	var cleared autoscalerv1alpha1.PredictiveAutoscaler
	if err := f.c.Get(ctx, f.req.NamespacedName, &cleared); err != nil || len(cleared.Status.Conflicts) != 0 {
		t.Fatalf("status.conflicts must clear: %v %v", cleared.Status.Conflicts, err)
	}
}

// A condition message longer than the schema's 32768 bytes would make the API server reject the whole status update;
// the operator truncates it (conditionMessage) and the update is accepted.
func TestEnvtestALongConditionMessageDoesNotBreakTheStatusUpdate(t *testing.T) {
	f := envHarness(t, autoscalerv1alpha1.ModeRecommend)
	ctx := context.Background()
	long := strings.Repeat("conflict ", maxConditionMessage/8)
	update := func(msg string) error {
		var pa autoscalerv1alpha1.PredictiveAutoscaler
		if err := f.c.Get(ctx, f.req.NamespacedName, &pa); err != nil {
			return err
		}
		meta.SetStatusCondition(&pa.Status.Conditions, metav1.Condition{Type: "ConflictDetected", Status: metav1.ConditionTrue,
			Reason: "ReplicaWriter", Message: msg, ObservedGeneration: pa.Generation})
		return f.c.Status().Update(ctx, &pa)
	}
	if err := update(long); err == nil {
		t.Fatal("precondition: the API server must reject an over-long message")
	}
	if err := update(conditionMessage(long)); err != nil {
		t.Fatalf("a truncated message must be accepted: %v", err)
	}
}

// chunkedBody hides its length, so net/http sends it chunked, as kubectl's raw DELETE does.
type chunkedBody struct{ r io.Reader }

func (b chunkedBody) Read(p []byte) (int, error) { return b.r.Read(p) }

// The migration tool deletes legacy autoscalers with `kubectl delete --raw <path> -f <DeleteOptions>`: kubectl sends the
// file as a chunked body without a Content-Type. The real API server must enforce its preconditions (UID and
// resourceVersion) for exactly that request (hack/migrate-api-group.py, Codex task-08 r26/r27).
func TestEnvtestARawDeleteWithPreconditionsIsEnforcedByTheServer(t *testing.T) {
	cfg := startEnv(t)
	c, err := client.New(cfg, client.Options{Scheme: envScheme()})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	envMu.Lock()
	envSeq++
	ns := fmt.Sprintf("envtest-%d", envSeq)
	envMu.Unlock()
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	old := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": autoscalerv1alpha1.LegacyGroup + "/v1alpha1", "kind": "PredictiveAutoscaler",
		"metadata": map[string]interface{}{"name": "old", "namespace": ns},
		"spec":     map[string]interface{}{"targetDeployment": map[string]interface{}{"name": "web"}},
	}}
	if err := c.Create(ctx, old); err != nil {
		t.Fatal(err)
	}
	hc, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	del := func(uid, rv string) int {
		body := fmt.Sprintf(`{"kind":"DeleteOptions","apiVersion":"v1","preconditions":{"uid":%q,"resourceVersion":%q}}`, uid, rv)
		req, err := http.NewRequest(http.MethodDelete, strings.TrimRight(cfg.Host, "/")+
			"/apis/"+autoscalerv1alpha1.LegacyGroup+"/v1alpha1/namespaces/"+ns+"/predictiveautoscalers/old", chunkedBody{strings.NewReader(body)})
		if err != nil {
			t.Fatal(err)
		}
		resp, err := hc.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		_ = resp.Body.Close()
		return resp.StatusCode
	}
	uid, rv := string(old.GetUID()), old.GetResourceVersion()
	if code := del("someone-else", rv); code != http.StatusConflict {
		t.Fatalf("a wrong UID must be refused with 409, got %d", code)
	}
	if code := del(uid, "1"); code != http.StatusConflict {
		t.Fatalf("a stale resourceVersion must be refused with 409, got %d", code)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(old), old); err != nil {
		t.Fatalf("the object must still exist after refused deletes: %v", err)
	}
	if code := del(uid, rv); code != http.StatusOK {
		t.Fatalf("matching preconditions must delete, got %d", code)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(old), old); err == nil {
		t.Fatal("the object must be gone")
	}
}

// The CRD revision gate against the real API server and the generated CRD: a metadata-only read of the CRD, which the
// operator's RBAC allows by name only.
func TestEnvtestTheCRDRevisionGateReadsTheInstalledCRD(t *testing.T) {
	cfg := startEnv(t)
	c, err := client.New(cfg, client.Options{Scheme: envScheme()})
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckCRDRevision(context.Background(), c, RequiredCRDRevision); err != nil {
		t.Fatalf("the generated CRD must satisfy this operator: %v", err)
	}
	if err := CheckCRDRevision(context.Background(), c, RequiredCRDRevision+1); err == nil || !strings.Contains(err.Error(), "needs") {
		t.Fatalf("a newer operator must refuse this CRD: %v", err)
	}
}

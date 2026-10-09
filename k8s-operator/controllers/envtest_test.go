package controllers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
			CRDDirectoryPaths:     []string{filepath.Join("..", "..", "k8s-manifests", "base", "01-crd.yaml")},
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

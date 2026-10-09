package main

import (
	"flag"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/discovery"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
	"predictive-autoscaler/controllers"
)

const Version = "v4.2.0"

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(autoscalerv1alpha1.AddToScheme(scheme))
}

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var probeAddr string

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")

	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	setupLog.Info("Starting predictive-operator", "version", Version)

	// WATCH_NAMESPACES (comma-separated) restricts the manager's cache, and therefore the
	// reconciler, to those namespaces. Empty means cluster-wide (the historical behaviour).
	// Used by the benchmark so that a second operator instance can be exercised in an
	// isolated namespace without both instances reconciling the same objects.
	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr}, // plain HTTP, as before
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "predictive-autoscaler-leader",
	}
	if raw := strings.TrimSpace(os.Getenv("WATCH_NAMESPACES")); raw != "" {
		var namespaces []string
		watched := map[string]cache.Config{}
		for _, ns := range strings.Split(raw, ",") {
			if ns = strings.TrimSpace(ns); ns != "" {
				namespaces = append(namespaces, ns)
				watched[ns] = cache.Config{}
			}
		}
		mgrOpts.Cache = cache.Options{DefaultNamespaces: watched}
		setupLog.Info("Restricting watches to namespaces", "namespaces", namespaces)
	}
	restConfig := ctrl.GetConfigOrDie()
	mgr, err := ctrl.NewManager(restConfig, mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	// Uncached discovery for the coexistence check (optional KEDA/VPA APIs); the check's context bounds each request,
	// and the request timeout is a backstop.
	discoveryConfig := rest.CopyConfig(restConfig)
	discoveryConfig.Timeout = 10 * time.Second
	discoveryClient, err := discovery.NewDiscoveryClientForConfig(discoveryConfig)
	if err != nil {
		setupLog.Error(err, "unable to create the discovery client")
		os.Exit(1)
	}

	if err = (&controllers.PredictiveAutoscalerReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Log:       ctrl.Log.WithName("controllers").WithName("PredictiveAutoscaler"),
		Recorder:  mgr.GetEventRecorder("predictive-autoscaler"),
		APIReader: mgr.GetAPIReader(),
		Discovery: controllers.RESTDiscovery{Client: discoveryClient.RESTClient()},
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "PredictiveAutoscaler")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

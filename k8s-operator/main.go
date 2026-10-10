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
	"sigs.k8s.io/controller-runtime/pkg/client"
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

// leaderElectionID is the Lease the operator elects on. It is not the legacy operator's "predictive-autoscaler-leader",
// so both can run side by side during the API-group migration without one blocking the other (docs/upgrading.md).
const leaderElectionID = "predictive-autoscaler.autoscaling.devkuban.com"

func main() {
	var metricsAddr string
	var enableLeaderElection bool
	var leaderElectionNamespace string
	var probeAddr string

	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "The address the metric endpoint binds to.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", true,
		"Leader election: only the instance holding the Lease reconciles, so the pods of one installation (a rolling "+
			"update, two replicas) never both write. Installations whose watched namespaces overlap must share the same "+
			"Lease namespace. Set false only for a single local run.")
	flag.StringVar(&leaderElectionNamespace, "leader-election-namespace", "",
		"Namespace of the leader-election Lease (default: the pod's own namespace; required outside a cluster).")

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
		Scheme:                  scheme,
		Metrics:                 metricsserver.Options{BindAddress: metricsAddr}, // plain HTTP, as before
		HealthProbeBindAddress:  probeAddr,
		LeaderElection:          enableLeaderElection,
		LeaderElectionID:        leaderElectionID,
		LeaderElectionNamespace: leaderElectionNamespace,
		// The manager exits right after it stops, so releasing the Lease on shutdown is safe and hands over at once.
		LeaderElectionReleaseOnCancel: true,
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

	// The CRD revision gate runs BEFORE the manager exists: ctrl.NewManager already binds the probe and metrics ports
	// (so the gate's own probe server could not), and nothing may campaign for leadership or start a controller until
	// the installed CRD is one this operator works with. Meanwhile the probes report alive and not ready. A direct,
	// uncached client reads the CRD's metadata (found by the kind smoke test, 2026-10-10).
	ctx := ctrl.SetupSignalHandler()
	gateClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		setupLog.Error(err, "unable to create the client for the CRD gate")
		os.Exit(1)
	}
	if err := controllers.WaitForCRDRevision(ctx, gateClient, controllers.RequiredCRDRevision, probeAddr, setupLog); err != nil {
		setupLog.Error(err, "stopped while waiting for a compatible CRD")
		os.Exit(1)
	}

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
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

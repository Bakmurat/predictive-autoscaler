package controllers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// The CRD revision gate (DESIGN-B5, Codex task-08 r24): Helm installs crds/ once and never upgrades it, and Argo CD
// applies it on its own schedule, so a new operator can start against an older CRD whose schema lacks fields it writes.
// The installed CRD carries a revision label. Before the manager starts (before it campaigns for leadership or starts
// a controller), the operator waits until that revision is at least the one it needs.
const (
	// CRDRevisionLabel is the label on the CRD that numbers its schema revision (a kubebuilder marker on the type).
	CRDRevisionLabel = "autoscaling.devkuban.com/crd-revision"
	// RequiredCRDRevision is the lowest CRD revision this operator works with.
	RequiredCRDRevision = 1
)

// CRDName is the operator's own CRD.
var CRDName = "predictiveautoscalers." + autoscalerv1alpha1.GroupVersion.Group

// crdGateCheckTimeout bounds one read of the CRD.
var crdGateCheckTimeout = 10 * time.Second

// CheckCRDRevision reads the installed CRD's metadata through rd (uncached) and fails unless its revision label is a
// positive integer at least required. A missing CRD, a missing or malformed label and a failed read all fail.
func CheckCRDRevision(ctx context.Context, rd client.Reader, required int) error {
	crd := &metav1.PartialObjectMetadata{}
	crd.SetGroupVersionKind(schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"})
	if err := rd.Get(ctx, client.ObjectKey{Name: CRDName}, crd); err != nil {
		return fmt.Errorf("read CRD %s: %w", CRDName, err)
	}
	raw, ok := crd.GetLabels()[CRDRevisionLabel]
	if !ok {
		return fmt.Errorf("CRD %s has no %s label: apply this release's CRD (charts/predictive-autoscaler/crds/) first",
			CRDName, CRDRevisionLabel)
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 1 {
		return fmt.Errorf("CRD %s has a malformed %s label %q", CRDName, CRDRevisionLabel, raw)
	}
	if n < required {
		return fmt.Errorf("CRD %s is revision %d and this operator needs %d: apply this release's CRD "+
			"(kubectl apply --server-side -f charts/predictive-autoscaler/crds/) before upgrading", CRDName, n, required)
	}
	return nil
}

// WaitForCRDRevision retries CheckCRDRevision with backoff (1 s doubling to 30 s) until it passes or ctx ends. While it
// waits, a probe server on probeAddr answers /healthz 200 (the pod stays alive) and /readyz 503 with the reason (the
// pod stays unready). It closes that server before returning, so the manager can bind the same address. An empty
// probeAddr or "0" runs no probe server.
func WaitForCRDRevision(ctx context.Context, rd client.Reader, required int, probeAddr string, log logr.Logger) error {
	var reason atomic.Value
	reason.Store("checking the CRD revision")
	if probeAddr != "" && probeAddr != "0" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = fmt.Fprint(w, "ok") })
		mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = fmt.Fprint(w, reason.Load())
		})
		ln, err := net.Listen("tcp", probeAddr)
		if err != nil {
			return fmt.Errorf("probe server for the CRD gate: %w", err)
		}
		srv := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		done := make(chan struct{})
		go func() {
			defer close(done)
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error(err, "CRD gate probe server")
			}
		}()
		defer func() {
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = srv.Shutdown(shutdown)
			<-done
		}()
	}
	delay := time.Second
	for {
		checkCtx, cancel := context.WithTimeout(ctx, crdGateCheckTimeout)
		err := CheckCRDRevision(checkCtx, rd, required)
		cancel()
		if err == nil {
			return nil
		}
		reason.Store(err.Error())
		log.Info("Waiting for a compatible CRD before starting (not ready, no controller running)", "reason", err.Error())
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		if delay *= 2; delay > 30*time.Second {
			delay = 30 * time.Second
		}
	}
}

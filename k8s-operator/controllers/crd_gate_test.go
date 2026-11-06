package controllers

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func crdObj(labels map[string]string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]interface{}{"name": CRDName},
	}}
	if labels != nil {
		u.SetLabels(labels)
	}
	return u
}

func TestCheckCRDRevision(t *testing.T) {
	for _, tc := range []struct {
		name     string
		crd      client.Object
		required int
		wantErr  string
	}{
		{"current", crdObj(map[string]string{CRDRevisionLabel: "1"}), 1, ""},
		{"newer", crdObj(map[string]string{CRDRevisionLabel: "3"}), 1, ""},
		{"older", crdObj(map[string]string{CRDRevisionLabel: "1"}), 2, "is revision 1 and this operator needs 2"},
		{"no_label", crdObj(nil), 1, "has no " + CRDRevisionLabel},
		{"malformed", crdObj(map[string]string{CRDRevisionLabel: "one"}), 1, "malformed"},
		{"zero", crdObj(map[string]string{CRDRevisionLabel: "0"}), 1, "malformed"},
		{"missing", nil, 1, "read CRD"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b := fake.NewClientBuilder().WithScheme(runtime.NewScheme())
			if tc.crd != nil {
				b = b.WithObjects(tc.crd)
			}
			err := CheckCRDRevision(context.Background(), b.Build(), tc.required)
			if tc.wantErr == "" && err != nil || tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("err = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	return addr
}

func probe(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		return 0, err.Error()
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

// While the CRD is incompatible the operator is alive and unready and starts nothing; once the CRD is applied the gate
// returns and frees the probe address for the manager.
func TestWaitForCRDRevisionHoldsUnreadyUntilTheCRDIsCompatible(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).WithObjects(crdObj(map[string]string{CRDRevisionLabel: "1"})).Build()
	addr := freeAddr(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- WaitForCRDRevision(ctx, c, 2, addr, logr.Discard()) }()

	deadline := time.Now().Add(5 * time.Second)
	var code int
	var body string
	for time.Now().Before(deadline) {
		if code, body = probe(t, "http://"+addr+"/readyz"); code == http.StatusServiceUnavailable && strings.Contains(body, "needs 2") {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if code != http.StatusServiceUnavailable || !strings.Contains(body, "is revision 1 and this operator needs 2") {
		t.Fatalf("readyz while waiting: %d %q", code, body)
	}
	if code, _ := probe(t, "http://"+addr+"/healthz"); code != http.StatusOK {
		t.Fatalf("healthz while waiting: %d (the pod must stay alive)", code)
	}
	select {
	case err := <-done:
		t.Fatalf("the gate returned before the CRD was compatible: %v", err)
	default:
	}

	newer := crdObj(nil)
	if err := c.Get(ctx, client.ObjectKey{Name: CRDName}, newer); err != nil {
		t.Fatal(err)
	}
	newer.SetLabels(map[string]string{CRDRevisionLabel: "2"})
	if err := c.Update(ctx, newer); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the gate did not pass after the CRD was applied")
	}
	ln, err := net.Listen("tcp", addr) // the manager can bind the probe address now
	if err != nil {
		t.Fatalf("the probe address is still taken: %v", err)
	}
	_ = ln.Close()
}

func TestWaitForCRDRevisionStopsWithItsContext(t *testing.T) {
	c := fake.NewClientBuilder().WithScheme(runtime.NewScheme()).Build() // no CRD at all
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := WaitForCRDRevision(ctx, c, 1, "", logr.Discard()); err == nil {
		t.Fatal("a cancelled gate must return an error, not start the manager")
	}
}

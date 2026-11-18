package controllers

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	"predictive-autoscaler/internal/secretmeta"
)

// The metadata-only Secret reader against a real kube-apiserver: the server honours the metadata-only representation
// for Secrets, the reader gets the identity and the labels it named and nothing of the contents, and the server's own
// NotFound is what the reader takes for "absent".
func TestEnvtestSecretMetadataReaderAgainstARealAPIServer(t *testing.T) {
	cfg := startEnv(t)
	ctx := context.Background()
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	const ns, name, marker = "secretmeta-test", "sh.helm.release.v1.demo.v2", "SECRET-DATA-MARKER"
	if _, err := cs.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	created, err := cs.CoreV1().Secrets(ns).Create(ctx, &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns,
			Labels:      map[string]string{"owner": "helm", "name": "demo", "version": "2", "c28.devkuban.com/release-op": "0123456789abcdef0123"},
			Annotations: map[string]string{"note": marker}},
		Type: "helm.sh/release.v1",
		Data: map[string][]byte{"release": []byte(marker)},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	req := secretmeta.Request{Kubeconfig: kubeconfigFor(t, cfg), Context: "envtest", Namespace: ns, Name: name,
		LabelKeys: []string{"c28.devkuban.com/release-op", "version"}, Require: map[string]string{"owner": "helm", "name": "demo"},
		Timeout: 20 * time.Second}

	got, err := secretmeta.Get(ctx, req)
	if err != nil {
		t.Fatalf("the real API server's answer was refused: %v", err)
	}
	if !got.Found || got.UID != string(created.UID) || got.ResourceVersion != created.ResourceVersion ||
		got.Labels["c28.devkuban.com/release-op"] != "0123456789abcdef0123" || got.Labels["version"] != "2" || len(got.Labels) != 2 {
		t.Fatalf("got %+v, created uid %s rv %s", got, created.UID, created.ResourceVersion)
	}

	wrong := req
	wrong.Require = map[string]string{"owner": "helm", "name": "another-release"}
	if _, err := secretmeta.Get(ctx, wrong); err == nil || !strings.Contains(err.Error(), "required label") {
		t.Fatalf("a label mismatch was not refused: %v", err)
	}

	absent := req
	absent.Name = "sh.helm.release.v1.demo.v3"
	got, err = secretmeta.Get(ctx, absent)
	if err != nil || got == nil || got.Found {
		t.Fatalf("the server's NotFound was not read as absent: %+v, %v", got, err)
	}

	// The same Secret deleted and recreated is another object: the UID changes, which is what the reader exists for.
	if err := cs.CoreV1().Secrets(ns).Delete(ctx, name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	created.ResourceVersion, created.UID = "", ""
	again, err := cs.CoreV1().Secrets(ns).Create(ctx, created, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	second, err := secretmeta.Get(ctx, req)
	if err != nil || second.UID != string(again.UID) || second.UID == got.UID || second.UID == "" {
		t.Fatalf("after a delete and recreate: %+v, %v", second, err)
	}
}

func kubeconfigFor(t *testing.T, cfg *rest.Config) string {
	t.Helper()
	doc := clientcmdapi.NewConfig()
	doc.Clusters["c"] = &clientcmdapi.Cluster{Server: cfg.Host, CertificateAuthorityData: cfg.CAData}
	doc.AuthInfos["u"] = &clientcmdapi.AuthInfo{ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData}
	doc.Contexts["envtest"] = &clientcmdapi.Context{Cluster: "c", AuthInfo: "u"}
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := clientcmd.WriteToFile(*doc, path); err != nil {
		t.Fatal(err)
	}
	return path
}

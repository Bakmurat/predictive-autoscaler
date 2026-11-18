package secretmeta

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	token  = "TOKEN-MARKER-must-never-be-returned"
	secret = "SECRET-DATA-MARKER-must-never-be-returned"
	ns     = "pa-eval-system"
	name   = "sh.helm.release.v1.pa-eval.v2"
	path   = "/api/v1/namespaces/" + ns + "/secrets/" + name
)

type seen struct {
	Method, Path, Query, Auth string
	Accept                    []string
}

type api struct {
	t       *testing.T
	srv     *httptest.Server
	mu      sync.Mutex
	calls   []seen
	handler http.HandlerFunc
}

func newAPI(t *testing.T, handler http.HandlerFunc) *api {
	a := &api{t: t, handler: handler}
	a.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.mu.Lock()
		a.calls = append(a.calls, seen{r.Method, r.URL.Path, r.URL.RawQuery, r.Header.Get("Authorization"), r.Header.Values("Accept")})
		a.mu.Unlock()
		a.handler(w, r)
	}))
	t.Cleanup(a.srv.Close)
	return a
}

func (a *api) kubeconfig() string {
	ca := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.srv.Certificate().Raw})
	doc := fmt.Sprintf(`apiVersion: v1
kind: Config
current-context: not-this-one
clusters:
- name: c
  cluster:
    server: %s
    certificate-authority-data: %s
- name: elsewhere
  cluster:
    server: https://127.0.0.1:1
users:
- name: u
  user:
    token: %s
contexts:
- name: wanted
  context: {cluster: c, user: u}
- name: not-this-one
  context: {cluster: elsewhere, user: u}
`, a.srv.URL, base64.StdEncoding.EncodeToString(ca), token)
	f := filepath.Join(a.t.TempDir(), "kubeconfig")
	if err := os.WriteFile(f, []byte(doc), 0o600); err != nil {
		a.t.Fatal(err)
	}
	return f
}

func (a *api) request() Request {
	return Request{Kubeconfig: a.kubeconfig(), Context: "wanted", Namespace: ns, Name: name, Timeout: 5 * time.Second,
		LabelKeys: []string{"c28.devkuban.com/release-op", "version"},
		Require:   map[string]string{"owner": "helm", "name": "pa-eval"}}
}

func partial(over map[string]any) []byte {
	meta := map[string]any{"name": name, "namespace": ns, "uid": "11111111-2222-3333-4444-555555555555",
		"resourceVersion": "4711",
		"labels": map[string]string{"owner": "helm", "name": "pa-eval", "version": "2", "status": "deployed",
			"c28.devkuban.com/release-op": "0123456789abcdef0123"},
		"annotations":   map[string]string{"kubectl.kubernetes.io/last-applied-configuration": secret},
		"managedFields": []any{map[string]string{"manager": "helm"}}}
	doc := map[string]any{"kind": "PartialObjectMetadata", "apiVersion": "meta.k8s.io/v1", "metadata": meta}
	for k, v := range over {
		switch {
		case strings.HasPrefix(k, "metadata."):
			if v == nil {
				delete(meta, strings.TrimPrefix(k, "metadata."))
			} else {
				meta[strings.TrimPrefix(k, "metadata.")] = v
			}
		case v == nil:
			delete(doc, k)
		default:
			doc[k] = v
		}
	}
	b, _ := json.Marshal(doc)
	return b
}

func respond(status int, body []byte) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}
}

func clean(t *testing.T, parts ...string) {
	t.Helper()
	for _, p := range parts {
		for _, marker := range []string{token, secret, "last-applied", "managedFields"} {
			if strings.Contains(p, marker) {
				t.Fatalf("output carries %q", marker)
			}
		}
	}
}

func TestTheOneRequestIsMetadataOnlyWithNoFallbackAndReturnsOnlyWhatWasNamed(t *testing.T) {
	a := newAPI(t, respond(200, partial(nil)))
	got, err := Get(context.Background(), a.request())
	if err != nil {
		t.Fatal(err)
	}
	out, _ := json.Marshal(got)
	want := `{"schema":"c28-secret-metadata/v1","found":true,"namespace":"pa-eval-system",` +
		`"name":"sh.helm.release.v1.pa-eval.v2","uid":"11111111-2222-3333-4444-555555555555","resourceVersion":"4711",` +
		`"labels":{"c28.devkuban.com/release-op":"0123456789abcdef0123","version":"2"}}`
	if string(out) != want {
		t.Fatalf("got  %s\nwant %s", out, want)
	}
	clean(t, string(out))
	if len(a.calls) != 1 {
		t.Fatalf("%d requests, want exactly one", len(a.calls))
	}
	c := a.calls[0]
	if c.Method != "GET" || c.Path != path || c.Query != "" {
		t.Fatalf("request %s %s?%s", c.Method, c.Path, c.Query)
	}
	// the literal, not the package's constant: the test must not follow a change of the constant
	if len(c.Accept) != 1 || c.Accept[0] != "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1" {
		t.Fatalf("Accept %q: must be exactly the metadata-only type, with no alternative", c.Accept)
	}
	if c.Auth != "Bearer "+token {
		t.Fatal("the kubeconfig's credential did not reach the server through client-go")
	}
}

func TestAnythingButExactlyThatMetadataIsRefusedWithAFixedPhrase(t *testing.T) {
	full, _ := json.Marshal(map[string]any{"kind": "Secret", "apiVersion": "v1", "type": "helm.sh/release.v1",
		"metadata": map[string]any{"name": name, "namespace": ns, "uid": "u", "resourceVersion": "1"},
		"data":     map[string]string{"release": secret}})
	cases := []struct {
		what   string
		status int
		body   []byte
		phrase string
	}{
		{"the server ignores the negotiation and sends the Secret", 200, full, "beside kind, apiVersion and metadata"},
		{"metadata with a data field beside it", 200, partial(map[string]any{"data": map[string]string{"k": secret}}), "beside kind"},
		{"metadata with a stringData field", 200, partial(map[string]any{"stringData": map[string]string{"k": secret}}), "beside kind"},
		{"metadata with a type field", 200, partial(map[string]any{"type": "Opaque"}), "beside kind"},
		{"another kind", 200, partial(map[string]any{"kind": "Secret"}), "not a meta.k8s.io/v1 PartialObjectMetadata"},
		{"a Table", 200, partial(map[string]any{"kind": "Table"}), "not a meta.k8s.io/v1 PartialObjectMetadata"},
		{"another apiVersion", 200, partial(map[string]any{"apiVersion": "meta.k8s.io/v1beta1"}), "not a meta.k8s.io/v1"},
		{"no kind", 200, partial(map[string]any{"kind": nil}), "not a meta.k8s.io/v1"},
		{"another name", 200, partial(map[string]any{"metadata.name": "sh.helm.release.v1.pa-eval.v3"}), "another object"},
		{"another namespace", 200, partial(map[string]any{"metadata.namespace": "demo"}), "another object"},
		{"no UID", 200, partial(map[string]any{"metadata.uid": nil}), "no UID or resourceVersion"},
		{"no resourceVersion", 200, partial(map[string]any{"metadata.resourceVersion": nil}), "no UID or resourceVersion"},
		{"a required label with another value", 200, partial(map[string]any{"metadata.labels": map[string]string{"owner": "someone", "name": "pa-eval"}}), "required label"},
		{"a required label missing", 200, partial(map[string]any{"metadata.labels": map[string]string{"owner": "helm"}}), "required label"},
		{"no labels at all", 200, partial(map[string]any{"metadata.labels": nil}), "required label"},
		{"not JSON", 200, []byte(secret), "not a JSON object"},
		{"a JSON array", 200, []byte(`["` + secret + `"]`), "not a JSON object"},
		{"metadata that is not an object", 200, partial(map[string]any{"metadata": secret}), "metadata cannot be decoded"},
		{"negotiation not supported", 406, []byte(`{"kind":"Status","message":"` + secret + `"}`), "does not offer the metadata-only representation"},
		{"unauthorized", 401, []byte(secret), "HTTP 401"},
		{"forbidden", 403, []byte(`{"kind":"Status","message":"` + secret + `"}`), "HTTP 403"},
		{"server error", 500, []byte(secret), "HTTP 500"},
		{"a 404 that is not the API server's", 404, []byte("<html>" + secret + "</html>"), "HTTP 404 that is not"},
		{"a NotFound about another Secret", 404, []byte(`{"kind":"Status","reason":"NotFound","details":{"name":"other","kind":"secrets"}}`), "HTTP 404 that is not"},
		{"a NotFound about a resource of another API group", 404, []byte(`{"kind":"Status","reason":"NotFound","details":{"name":"` + name + `","kind":"secrets","group":"example.com"}}`), "HTTP 404 that is not"},
		{"a 404 Status with another reason", 404, []byte(`{"kind":"Status","reason":"Forbidden","details":{"name":"` + name + `","kind":"secrets"}}`), "HTTP 404 that is not"},
		{"a NotFound about another resource", 404, []byte(`{"kind":"Status","reason":"NotFound","details":{"name":"` + name + `","kind":"configmaps"}}`), "HTTP 404 that is not"},
		{"an oversized answer", 200, append(partial(nil), make([]byte, MaxBody+10)...), "exceeds"},
	}
	for _, c := range cases {
		a := newAPI(t, respond(c.status, c.body))
		got, err := Get(context.Background(), a.request())
		if err == nil || got != nil {
			t.Fatalf("%s: accepted (%+v)", c.what, got)
		}
		if _, ok := err.(*Error); !ok || !strings.Contains(err.Error(), c.phrase) {
			t.Fatalf("%s: error %q, want the phrase %q", c.what, err, c.phrase)
		}
		clean(t, err.Error())
		if len(a.calls) != 1 || a.calls[0].Method != "GET" {
			t.Fatalf("%s: %d requests", c.what, len(a.calls))
		}
	}
}

func TestAServerPathPrefixInTheKubeconfigIsKept(t *testing.T) {
	for _, prefix := range []string{"/k8s/clusters/test-cluster", "/k8s/clusters/test-cluster/", "/one"} {
		a := newAPI(t, respond(200, partial(nil)))
		r := a.request()
		doc, _ := os.ReadFile(r.Kubeconfig)
		_ = os.WriteFile(r.Kubeconfig, []byte(strings.Replace(string(doc), "server: "+a.srv.URL, "server: "+a.srv.URL+prefix, 1)), 0o600)
		got, err := Get(context.Background(), r)
		if err != nil || got == nil || !got.Found {
			t.Fatalf("prefix %q: %+v, %v", prefix, got, err)
		}
		want := strings.TrimSuffix(prefix, "/") + path
		if len(a.calls) != 1 || a.calls[0].Path != want || a.calls[0].Query != "" {
			t.Fatalf("prefix %q: requested %+v, want the path %s", prefix, a.calls, want)
		}
	}
}

func TestACredentialPluginIsRefusedAndNeverStarted(t *testing.T) {
	a := newAPI(t, respond(200, partial(nil)))
	ran := filepath.Join(t.TempDir(), "plugin-ran")
	plugin := filepath.Join(t.TempDir(), "plugin.sh")
	if err := os.WriteFile(plugin, []byte("#!/bin/sh\necho PLUGIN-MARKER >&2\ntouch "+ran+"\nsleep 1\nexit 1\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	users := map[string]string{
		"an exec plugin":         "    exec:\n      apiVersion: client.authentication.k8s.io/v1\n      command: " + plugin + "\n      interactiveMode: Never\n",
		"a legacy auth-provider": "    auth-provider:\n      name: oidc\n      config: {idp-issuer-url: https://issuer.invalid, client-id: x}\n",
	}
	for what, user := range users {
		r := a.request()
		doc, _ := os.ReadFile(r.Kubeconfig)
		changed := strings.Replace(string(doc), "    token: "+token+"\n", user, 1)
		if changed == string(doc) {
			t.Fatal("the kubeconfig's static token was not replaced")
		}
		_ = os.WriteFile(r.Kubeconfig, []byte(changed), 0o600)
		r.Timeout = 200 * time.Millisecond
		start := time.Now()
		got, err := Get(context.Background(), r)
		if err == nil || got != nil || !strings.Contains(err.Error(), "credential plugin") {
			t.Fatalf("%s: %+v, %v", what, got, err)
		}
		if time.Since(start) > 800*time.Millisecond {
			t.Fatalf("%s: the refusal took %s (a plugin ran?)", what, time.Since(start))
		}
	}
	if _, err := os.Stat(ran); err == nil {
		t.Fatal("the credential plugin was started")
	}
	if len(a.calls) != 0 {
		t.Fatalf("%d requests reached the server", len(a.calls))
	}
}

func TestTheDeclaredContentTypeMustBeJSON(t *testing.T) {
	for contentType, ok := range map[string]bool{"application/json": true, "application/json; charset=utf-8": true,
		"application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1": true, "text/plain": false, "": false,
		"application/vnd.kubernetes.protobuf": false, "application/yaml": false, "garbage;;": false} {
		a := newAPI(t, func(w http.ResponseWriter, _ *http.Request) {
			if contentType != "" {
				w.Header().Set("Content-Type", contentType)
			} else {
				w.Header()["Content-Type"] = nil
			}
			w.WriteHeader(200)
			_, _ = w.Write(partial(nil))
		})
		got, err := Get(context.Background(), a.request())
		if ok && (err != nil || got == nil || !got.Found) {
			t.Fatalf("content type %q refused: %v", contentType, err)
		}
		if !ok && (err == nil || !strings.Contains(err.Error(), "not declared as application/json")) {
			t.Fatalf("content type %q: %+v, %v", contentType, got, err)
		}
	}
}

func TestOnlyTheAPIServersOwnNotFoundForThisSecretMeansAbsent(t *testing.T) {
	a := newAPI(t, respond(404, []byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","details":{"name":"`+name+`","kind":"secrets"},"code":404}`)))
	got, err := Get(context.Background(), a.request())
	if err != nil || got == nil || got.Found || got.UID != "" || got.Name != name || got.Namespace != ns {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestARedirectIsNotFollowed(t *testing.T) {
	elsewhere := newAPI(t, respond(200, partial(nil)))
	a := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, elsewhere.srv.URL+path, http.StatusFound)
	})
	_, err := Get(context.Background(), a.request())
	if err == nil || !strings.Contains(err.Error(), "redirect") {
		t.Fatalf("error %v", err)
	}
	if len(elsewhere.calls) != 0 {
		t.Fatal("the redirect target was contacted (with the credential)")
	}
}

func TestTheRequestIsBoundedInTime(t *testing.T) {
	release := make(chan struct{})
	a := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	req := a.request()
	req.Timeout = 300 * time.Millisecond
	start := time.Now()
	_, err := Get(context.Background(), req)
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("error %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("the bound did not hold")
	}
	clean(t, err.Error())
}

func TestBadRequestsNeverReachTheServer(t *testing.T) {
	a := newAPI(t, respond(200, partial(nil)))
	base := a.request()
	cases := []struct {
		change func(r *Request)
		phrase string
	}{
		{func(r *Request) { r.Kubeconfig = "" }, "explicit kubeconfig and context are required"},
		{func(r *Request) { r.Context = "" }, "explicit kubeconfig and context are required"},
		{func(r *Request) { r.Context = "no-such-context" }, "kubeconfig or its context cannot be used"},
		{func(r *Request) { r.Kubeconfig = filepath.Join(t.TempDir(), "missing") }, "kubeconfig or its context cannot be used"},
		{func(r *Request) { r.Namespace = "" }, "namespace is not a valid name"},
		{func(r *Request) { r.Namespace = "demo/../kube-system" }, "namespace is not a valid name"},
		{func(r *Request) { r.Namespace = "Demo" }, "namespace is not a valid name"},
		{func(r *Request) { r.Namespace = strings.Repeat("a", 64) }, "namespace is not a valid name"},
		{func(r *Request) { r.Name = "" }, "Secret name is not a valid name"},
		{func(r *Request) { r.Name = "../configmaps/x" }, "Secret name is not a valid name"},
		{func(r *Request) { r.Name = "a?watch=true" }, "Secret name is not a valid name"},
		{func(r *Request) { r.Name = "a/b" }, "Secret name is not a valid name"},
		{func(r *Request) { r.Name = strings.Repeat("a", 254) }, "Secret name is not a valid name"},
		{func(r *Request) { r.Timeout = 0 }, "timeout must be positive"},
		{func(r *Request) { r.Timeout = -time.Second }, "timeout must be positive"},
		{func(r *Request) { r.Timeout = MaxTimeout + time.Second }, "timeout must be positive"},
	}
	for i, c := range cases {
		r := base
		c.change(&r)
		got, err := Get(context.Background(), r)
		if err == nil || got != nil {
			t.Fatalf("case %d accepted", i)
		}
		if _, ok := err.(*Error); !ok || !strings.Contains(err.Error(), c.phrase) {
			t.Fatalf("case %d: error %q, want the phrase %q", i, err, c.phrase)
		}
		clean(t, err.Error())
	}
	if len(a.calls) != 0 {
		t.Fatalf("%d requests reached the server", len(a.calls))
	}
}

func TestTheRequestsOwnDeadlineHoldsEvenForAClientWithoutATimeout(t *testing.T) {
	release := make(chan struct{})
	a := newAPI(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	})
	defer close(release)
	base, _ := url.Parse(a.srv.URL)
	req := a.request()
	req.Timeout = 300 * time.Millisecond
	client := a.srv.Client() // trusts the test certificate and has NO timeout of its own
	done := make(chan error, 1)
	go func() { _, err := get(context.Background(), client, base, req); done <- err }()
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "timed out") {
			t.Fatalf("error %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the request did not end by its own deadline")
	}
}

func TestAnUnreachableServerIsATransportFailureWithoutDetail(t *testing.T) {
	a := newAPI(t, respond(200, partial(nil)))
	r := a.request()
	a.srv.Close()
	_, err := Get(context.Background(), r)
	if err == nil || !(strings.Contains(err.Error(), "transport") || strings.Contains(err.Error(), "timed out")) {
		t.Fatalf("error %v", err)
	}
	if strings.Contains(err.Error(), "127.0.0.1") {
		t.Fatal("the error carries transport detail")
	}
}

func TestAnUntrustedServerCertificateIsRefused(t *testing.T) {
	// httptest servers share one built-in certificate, so "another server" would be trusted: the kubeconfig is given
	// a freshly generated authority instead, which did not sign the server's certificate.
	a := newAPI(t, respond(200, partial(nil)))
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "another authority"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true,
		KeyUsage: x509.KeyUsageCertSign, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	other := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}))
	own := base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: a.srv.Certificate().Raw}))
	r := a.request()
	doc, _ := os.ReadFile(r.Kubeconfig)
	if !strings.Contains(string(doc), own) {
		t.Fatal("the kubeconfig does not hold the server's certificate as its authority")
	}
	_ = os.WriteFile(r.Kubeconfig, []byte(strings.Replace(string(doc), own, other, 1)), 0o600)
	got, err := Get(context.Background(), r)
	if err == nil || got != nil || !strings.Contains(err.Error(), "transport") {
		t.Fatalf("got %+v, error %v", got, err)
	}
	if len(a.calls) != 0 {
		t.Fatalf("%d requests were accepted by a server whose certificate is not trusted", len(a.calls))
	}
	clean(t, err.Error())
}

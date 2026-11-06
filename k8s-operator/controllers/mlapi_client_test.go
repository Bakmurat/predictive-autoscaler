package controllers

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	stderrors "errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	autoscalerv1alpha1 "predictive-autoscaler/api/v1alpha1"
)

// B5e: the operator's client for the forecasting service (Codex task-08 r34).

type seenRequests struct {
	mu   sync.Mutex
	auth []string
}

func (s *seenRequests) add(r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.auth = append(s.auth, r.Header.Get("Authorization"))
}

func (s *seenRequests) list() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.auth...)
}

// tlsServer serves HTTPS with its own self-signed certificate (httptest's built-in certificate is shared by every
// test server, so it cannot tell one CA from another).
func tlsServer(t *testing.T, seen *seenRequests, status int) *httptest.Server {
	t.Helper()
	return ownCertServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.add(r)
		w.WriteHeader(status)
		_, _ = io.WriteString(w, `{}`)
	}))
}

func ownCertServer(t *testing.T, h http.Handler) *httptest.Server {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
	tmpl := &x509.Certificate{
		SerialNumber: serial, Subject: pkix.Name{CommonName: "forecaster-test"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage:    x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")}, DNSNames: []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(h)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func caFileOf(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "ca.crt")
	writeCA(t, path, srv)
	return path
}

func writeCA(t *testing.T, path string, srv *httptest.Server) {
	t.Helper()
	block := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw})
	if err := os.WriteFile(path, block, 0o600); err != nil {
		t.Fatal(err)
	}
}

func writeToken(t *testing.T, path, token string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(token+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
}

func postOnce(t *testing.T, c *mlAPIClient, url string) (int, error) {
	t.Helper()
	resp, err := c.post(context.Background(), url, []byte(`{}`))
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	return resp.StatusCode, nil
}

func TestTheTokenIsReadOnEveryRequestSoRotationIsPickedUp(t *testing.T) {
	seen := &seenRequests{}
	srv := tlsServer(t, seen, 200)
	token := filepath.Join(t.TempDir(), "token")
	c := newMLAPIClient(token, caFileOf(t, srv), 5*time.Second)
	writeToken(t, token, "first")
	if code, err := postOnce(t, c, srv.URL+"/predict"); err != nil || code != 200 {
		t.Fatal(code, err)
	}
	writeToken(t, token, "rotated")
	if code, err := postOnce(t, c, srv.URL+"/predict"); err != nil || code != 200 {
		t.Fatal(code, err)
	}
	if got := seen.list(); len(got) != 2 || got[0] != "Bearer first" || got[1] != "Bearer rotated" {
		t.Fatalf("Authorization headers %q", got)
	}
}

func TestNothingIsSentWithoutAUsableTokenOrOverPlainHTTP(t *testing.T) {
	seen := &seenRequests{}
	srv := tlsServer(t, seen, 200)
	plainSeen := &seenRequests{}
	plain := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { plainSeen.add(r) }))
	t.Cleanup(plain.Close)
	dir := t.TempDir()
	empty := filepath.Join(dir, "empty")
	writeToken(t, empty, "  ")
	present := filepath.Join(dir, "token")
	writeToken(t, present, "secret-token")
	for name, tc := range map[string]struct {
		c   *mlAPIClient
		url string
	}{
		"missing token file": {newMLAPIClient(filepath.Join(dir, "absent"), caFileOf(t, srv), time.Second), srv.URL},
		"empty token file":   {newMLAPIClient(empty, caFileOf(t, srv), time.Second), srv.URL},
		"token over http":    {newMLAPIClient(present, "", time.Second), plain.URL},
	} {
		_, err := postOnce(t, tc.c, tc.url+"/predict")
		var cfg *mlAPIConfigError
		if !stderrors.As(err, &cfg) {
			t.Errorf("%s: want a configuration error, got %v", name, err)
		}
	}
	if len(seen.list()) != 0 || len(plainSeen.list()) != 0 {
		t.Fatalf("a request was sent: tls %q, plain %q", seen.list(), plainSeen.list())
	}
}

func TestTheCAIsVerifiedFailsClosedAndIsReloadedWhenItChanges(t *testing.T) {
	seen := &seenRequests{}
	srv := tlsServer(t, seen, 200)
	other := tlsServer(t, &seenRequests{}, 200)
	dir := t.TempDir()

	wrong := newMLAPIClient("", caFileOf(t, other), time.Second)
	if _, err := postOnce(t, wrong, srv.URL); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("another CA must not be trusted: %v", err)
	}
	missing := newMLAPIClient("", filepath.Join(dir, "absent.crt"), time.Second)
	invalid := filepath.Join(dir, "invalid.crt")
	if err := os.WriteFile(invalid, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, c := range map[string]*mlAPIClient{"missing CA": missing, "invalid CA": newMLAPIClient("", invalid, time.Second)} {
		_, err := postOnce(t, c, srv.URL)
		var cfg *mlAPIConfigError
		if !stderrors.As(err, &cfg) {
			t.Errorf("%s: want a configuration error (fail closed), got %v", name, err)
		}
	}
	if len(seen.list()) != 0 {
		t.Fatalf("a request reached the server through an untrusted or missing CA: %d", len(seen.list()))
	}

	// The CA file is rotated: the client picks the new CA up at its next recheck.
	path := filepath.Join(dir, "ca.crt")
	writeCA(t, path, other)
	clock := time.Now()
	c := newMLAPIClient("", path, time.Second)
	c.now = func() time.Time { return clock }
	if _, err := postOnce(t, c, srv.URL); err == nil {
		t.Fatal("precondition: the old CA must not verify the server")
	}
	writeCA(t, path, srv)
	future := time.Now().Add(time.Hour) // a distinct mtime for the stamp
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	clock = clock.Add(caRecheck + time.Second)
	if code, err := postOnce(t, c, srv.URL); err != nil || code != 200 {
		t.Fatalf("the rotated CA must be used after the recheck: %d %v", code, err)
	}
}

func TestRedirectsAreNeverFollowed(t *testing.T) {
	targetSeen := &seenRequests{}
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { targetSeen.add(r) }))
	t.Cleanup(target.Close)
	redirect := ownCertServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/predict", http.StatusTemporaryRedirect)
	}))
	token := filepath.Join(t.TempDir(), "token")
	writeToken(t, token, "secret-token")
	c := newMLAPIClient(token, caFileOf(t, redirect), time.Second)
	if _, err := postOnce(t, c, redirect.URL+"/predict"); err == nil || !stderrors.Is(err, errRedirectRefused) {
		t.Fatalf("a redirect must fail the request: %v", err)
	}
	if len(targetSeen.list()) != 0 {
		t.Fatal("the redirect target received the request (and the token)")
	}
}

func TestForecastFailureStatuses(t *testing.T) {
	for err, want := range map[error]string{
		&mlAPIConfigError{"x"}:             "auth_misconfigured",
		&forecastRefusedError{status: 401}: "unauthorized",
		&forecastRefusedError{status: 403}: "forbidden",
		&forecastRefusedError{status: 422}: "unavailable",
		&forecastServerError{status: 503, body: `{"detail":{"error":"AuthUnavailable"}}`}: "auth_unavailable",
		&forecastServerError{status: 503, body: `{"detail":"busy"}`}:                      "unavailable",
		stderrors.New("transport"): "unavailable",
	} {
		if got := forecastFailureStatus(err); got != want {
			t.Errorf("%v: got %s, want %s", err, got, want)
		}
	}
}

// The reasons reach the autoscaler's ForecastAvailable condition, and an authentication refusal never reuses a cached
// forecast (the reactive rule applies).
func TestAuthenticationFailuresAreReportedAndReactive(t *testing.T) {
	for status, want := range map[int]string{401: "Unauthorized", 403: "Forbidden"} {
		r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeRecommend, "3000")
		ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(status) }))
		t.Cleanup(ml.Close)
		t.Setenv("ML_API_URL", ml.URL)
		d := runRPMReconcile(t, r, req, path)
		if d.ForecastStatus != strings.ToLower(want) || d.DesiredReplicas != 5 {
			t.Fatalf("%d: decision %+v", status, d)
		}
		c := condition(t, r, req.NamespacedName, "ForecastAvailable")
		if c.Status != metav1.ConditionFalse || c.Reason != want {
			t.Fatalf("%d: ForecastAvailable %+v", status, c)
		}
	}
	r, req, path, _ := recommendHarness(t, autoscalerv1alpha1.ModeRecommend, "3000")
	ml := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(503)
		_, _ = io.WriteString(w, `{"detail":{"error":"AuthUnavailable"}}`)
	}))
	t.Cleanup(ml.Close)
	t.Setenv("ML_API_URL", ml.URL)
	if d := runRPMReconcile(t, r, req, path); d.ForecastStatus != "auth_unavailable" {
		t.Fatalf("decision %+v", d)
	}
	if c := condition(t, r, req.NamespacedName, "ForecastAvailable"); c.Reason != "AuthUnavailable" {
		t.Fatalf("ForecastAvailable %+v", c)
	}
}

func TestAuthenticationFailureCachePolicy(t *testing.T) {
	for _, status := range []int{401, 403, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"detail":{"error":"AuthUnavailable"}}`)
			}))
			t.Cleanup(srv.Close)
			t.Setenv("ML_API_URL", srv.URL)
			a := &autoscalerv1alpha1.PredictiveAutoscaler{}
			response := &MLPredictionResponse{Predictions: []float64{100, 100}}
			r := &PredictiveAutoscalerReconciler{predictionCache: map[string]*cachedPrediction{}}
			seed := func(age time.Duration) {
				r.predictionCache["k"] = &cachedPrediction{response: response, fetchedAt: time.Now().Add(-age), binding: forecastBinding(a)}
			}
			seed(predictionCacheTTL + time.Minute)
			got, err := r.getCachedPrediction(context.Background(), a, "k")
			if status == 503 {
				if err != nil || got != response {
					t.Fatalf("authentication outage must reuse a recent forecast: got=%v err=%v", got, err)
				}
				seed(predictionStaleMax + time.Second)
				got, err = r.getCachedPrediction(context.Background(), a, "k")
			}
			if err == nil || got != nil || r.predictionCache["k"] != nil {
				t.Fatalf("refused or expired forecast must be dropped: got=%v err=%v", got, err)
			}
		})
	}

	t.Run("missing credentials", func(t *testing.T) {
		t.Setenv("ML_API_URL", "https://127.0.0.1:1")
		a := &autoscalerv1alpha1.PredictiveAutoscaler{}
		r := &PredictiveAutoscalerReconciler{
			MLAPI: newMLAPIClient(filepath.Join(t.TempDir(), "missing-token"), "", time.Second),
			predictionCache: map[string]*cachedPrediction{"k": {
				response:  &MLPredictionResponse{Predictions: []float64{100}},
				fetchedAt: time.Now().Add(-predictionCacheTTL - time.Minute), binding: forecastBinding(a),
			}},
		}
		got, err := r.getCachedPrediction(context.Background(), a, "k")
		var cfg *mlAPIConfigError
		if !stderrors.As(err, &cfg) || got != nil || r.predictionCache["k"] != nil {
			t.Fatalf("missing credentials must drop the cached forecast: got=%v err=%v", got, err)
		}
	})
}

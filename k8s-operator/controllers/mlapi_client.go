package controllers

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	stderrors "errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// How the operator reaches the forecasting service (B5e, DESIGN-B5; Codex task-08 r34):
//   - ML_API_TOKEN_FILE: a projected service account token (audience: the forecasting service). It is read from the file
//     on every request, because the kubelet rotates it, and sent as "Authorization: Bearer". With a token configured
//     only https URLs are used: the token never travels in clear text. A configured file that is missing or empty
//     sends no request at all.
//   - ML_API_CA_FILE: the forecasting service's CA bundle. The TLS client is rebuilt when the file changes (checked at
//     most every caRecheck). A configured CA that is missing or holds no certificate sends no request (fail closed).
//   - Redirects are never followed: the default client would follow a same-host redirect, possibly to plain HTTP, and
//     the forecasting service never redirects.

// mlAPIConfigError is a local configuration problem: no request was sent. The forecast is unavailable and the reactive
// rule applies; a cached forecast does not stand in for it.
type mlAPIConfigError struct{ reason string }

func (e *mlAPIConfigError) Error() string {
	return "forecasting service client not usable: " + e.reason
}

var errRedirectRefused = stderrors.New("the forecasting service answered with a redirect, which is never followed")

// caRecheck bounds how often the CA file is stat'ed for a change.
var caRecheck = 60 * time.Second

type mlAPIClient struct {
	tokenFile, caFile string
	timeout           time.Duration
	now               func() time.Time

	mu      sync.Mutex
	client  *http.Client
	caStamp string
	checked time.Time
}

func newMLAPIClient(tokenFile, caFile string, timeout time.Duration) *mlAPIClient {
	return &mlAPIClient{tokenFile: tokenFile, caFile: caFile, timeout: timeout, now: time.Now}
}

func newMLAPIClientFromEnv(timeout time.Duration) *mlAPIClient {
	return newMLAPIClient(os.Getenv("ML_API_TOKEN_FILE"), os.Getenv("ML_API_CA_FILE"), timeout)
}

// post sends body to url as JSON, authenticated when a token file is configured.
func (c *mlAPIClient) post(ctx context.Context, url string, body []byte) (*http.Response, error) {
	if c.tokenFile != "" && !strings.HasPrefix(strings.ToLower(url), "https://") {
		return nil, &mlAPIConfigError{"a service account token is configured but ML_API_URL is not https"}
	}
	client, err := c.httpClient()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if c.tokenFile != "" {
		raw, err := os.ReadFile(c.tokenFile)
		token := strings.TrimSpace(string(raw))
		if err != nil || token == "" {
			return nil, &mlAPIConfigError{"the service account token file is missing or empty"}
		}
		req.Header.Set("Authorization", "Bearer "+token)
	}
	return client.Do(req)
}

// httpClient returns the client, rebuilt when the CA file changed.
func (c *mlAPIClient) httpClient() (*http.Client, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client != nil && (c.caFile == "" || c.now().Sub(c.checked) < caRecheck) {
		return c.client, nil
	}
	c.checked = c.now()
	var tlsConfig *tls.Config
	stamp := ""
	if c.caFile != "" {
		fi, err := os.Stat(c.caFile)
		if err != nil {
			c.client = nil
			return nil, &mlAPIConfigError{"the CA file is missing"}
		}
		stamp = fmt.Sprintf("%d/%d", fi.ModTime().UnixNano(), fi.Size())
		if c.client != nil && stamp == c.caStamp {
			return c.client, nil
		}
		pem, err := os.ReadFile(c.caFile)
		pool := x509.NewCertPool()
		if err != nil || !pool.AppendCertsFromPEM(pem) {
			c.client = nil
			return nil, &mlAPIConfigError{"the CA file holds no valid certificate"}
		}
		tlsConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	}
	// Without a CA the client uses http.DefaultTransport at call time, as before; with one it gets its own transport.
	var transport http.RoundTripper
	if tlsConfig != nil {
		t := &http.Transport{}
		if base, ok := http.DefaultTransport.(*http.Transport); ok {
			t = base.Clone()
		}
		t.TLSClientConfig = tlsConfig
		transport = t
	}
	c.client = &http.Client{
		Timeout:       c.timeout,
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return errRedirectRefused },
	}
	c.caStamp = stamp
	return c.client, nil
}

// forecastServerError is a 5xx (or other non-2xx, non-4xx) answer from the forecasting service: an outage, so a recent
// cached forecast may stand in for it (up to predictionStaleMax).
type forecastServerError struct {
	status int
	body   string
}

func (e *forecastServerError) Error() string {
	return fmt.Sprintf("ML API returned status %d: %s", e.status, e.body)
}

// forecastFailureStatus is the decision's forecast status for a failed forecast: the authentication cases get their own
// (and their own ForecastAvailable reasons), everything else is "unavailable".
func forecastFailureStatus(err error) string {
	var cfg *mlAPIConfigError
	var refused *forecastRefusedError
	var server *forecastServerError
	switch {
	case stderrors.As(err, &cfg):
		return "auth_misconfigured"
	case stderrors.As(err, &refused) && refused.status == http.StatusUnauthorized:
		return "unauthorized"
	case stderrors.As(err, &refused) && refused.status == http.StatusForbidden:
		return "forbidden"
	case stderrors.As(err, &server) && server.status == http.StatusServiceUnavailable && strings.Contains(server.body, "AuthUnavailable"):
		return "auth_unavailable"
	}
	return "unavailable"
}

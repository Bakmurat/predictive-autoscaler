// Package secretmeta reads the METADATA of one named Secret, and nothing else.
//
// The evaluation tooling never reads Secrets. A few of its checks nevertheless need a Secret's identity: the UID and
// labels of a Helm release revision's storage object, or the UID and resourceVersion of a TLS Secret that must not
// have been replaced. This package is that one narrow capability:
//
//   - one GET of one named Secret in one namespace;
//   - the request asks for the metadata-only representation and offers NO fallback
//     (Accept: application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1), so a server that cannot give it answers
//     406 instead of sending the object;
//   - the answer is accepted only if it is exactly a meta.k8s.io/v1 PartialObjectMetadata for that name and namespace,
//     with a UID, and with nothing beside kind, apiVersion and metadata (a "data", "stringData" or "type" field, or any
//     other kind, is refused);
//   - only name, namespace, UID, resourceVersion and the labels the caller named are returned. Annotations are never
//     returned (a last-applied-configuration annotation can hold a Secret's data);
//   - time and response size are bounded, redirects are not followed, and no error carries a response body, a header
//     or a credential: errors are fixed phrases.
//
// Authentication and TLS come from client-go's own kubeconfig handling; this package parses no credential. Only static
// credentials are supported: a kubeconfig that names an exec or auth-provider plugin is refused before any transport
// exists, because such a plugin runs outside the request's deadline and writes to the caller's stderr. A path prefix
// in the kubeconfig's server URL is kept.
package secretmeta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

// Accept is the only representation requested. There is deliberately no second alternative.
const Accept = "application/json;as=PartialObjectMetadata;g=meta.k8s.io;v=v1"

// MaxBody bounds the response read (metadata with managed fields is a few kilobytes).
const MaxBody = 1 << 20

// MaxTimeout bounds the whole request, whatever the caller asks for.
const MaxTimeout = 60 * time.Second

var (
	dnsLabel     = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)
	dnsSubdomain = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)
)

// Request names the one Secret and what may be returned about it.
type Request struct {
	Kubeconfig string            // explicit path; never the environment's default
	Context    string            // explicit context name
	Namespace  string            // the Secret's namespace
	Name       string            // the Secret's name
	LabelKeys  []string          // labels that may be returned (others are dropped)
	Require    map[string]string // labels that must be present with exactly these values
	Timeout    time.Duration     // the whole request; capped at MaxTimeout
}

// Metadata is everything this package ever returns about a Secret.
type Metadata struct {
	Schema          string            `json:"schema"`
	Found           bool              `json:"found"`
	Namespace       string            `json:"namespace"`
	Name            string            `json:"name"`
	UID             string            `json:"uid,omitempty"`
	ResourceVersion string            `json:"resourceVersion,omitempty"`
	Labels          map[string]string `json:"labels,omitempty"`
}

// Schema of the output document.
const Schema = "c28-secret-metadata/v1"

// Error is a refusal or a failure, as a fixed phrase. It never wraps a transport error or a response.
type Error struct{ Reason string }

func (e *Error) Error() string { return e.Reason }

func refuse(format string, a ...any) error { return &Error{Reason: fmt.Sprintf(format, a...)} }

func (r Request) validate() error {
	if r.Kubeconfig == "" || r.Context == "" {
		return refuse("an explicit kubeconfig and context are required")
	}
	if len(r.Namespace) > 63 || !dnsLabel.MatchString(r.Namespace) {
		return refuse("the namespace is not a valid name")
	}
	if len(r.Name) > 253 || !dnsSubdomain.MatchString(r.Name) {
		return refuse("the Secret name is not a valid name")
	}
	if r.Timeout <= 0 || r.Timeout > MaxTimeout {
		return refuse("the timeout must be positive and at most %s", MaxTimeout)
	}
	return nil
}

// Get performs the one metadata-only read. A Secret that the API server says does not exist is (Found: false, nil);
// everything that is not a proven answer is an *Error.
func Get(ctx context.Context, r Request) (*Metadata, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: r.Kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: r.Context}).ClientConfig()
	if err != nil {
		return nil, refuse("the kubeconfig or its context cannot be used")
	}
	// A credential plugin would run outside this request's deadline and write to this process's stderr. This reader
	// supports static credentials only and refuses a plugin BEFORE any transport is built, so the plugin never starts.
	if cfg.ExecProvider != nil || cfg.AuthProvider != nil {
		return nil, refuse("the kubeconfig uses an exec or auth-provider credential plugin, which this reader does not run")
	}
	cfg.Timeout = r.Timeout
	client, err := rest.HTTPClientFor(cfg)
	if err != nil {
		return nil, refuse("no HTTP client can be built from the kubeconfig")
	}
	base, err := url.Parse(cfg.Host)
	if err != nil || base.Scheme != "https" || base.Host == "" {
		return nil, refuse("the kubeconfig's server is not an https URL")
	}
	return get(ctx, client, base, r)
}

func get(ctx context.Context, client *http.Client, base *url.URL, r Request) (*Metadata, error) {
	client.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	ctx, cancel := context.WithTimeout(ctx, r.Timeout)
	defer cancel()
	// The kubeconfig's server may carry a path prefix (an API reached through a proxy path): it is kept, and the
	// resource path is appended to it. Namespace and name are validated names, so nothing in them needs escaping.
	target := base.JoinPath("api", "v1", "namespaces", r.Namespace, "secrets", r.Name)
	target.RawQuery, target.Fragment = "", ""
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, refuse("the request cannot be built")
	}
	req.Header.Set("Accept", Accept)
	resp, err := client.Do(req)
	if err != nil {
		var ue *url.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) ||
			(errors.As(err, &ue) && ue.Timeout()) {
			return nil, refuse("the request timed out")
		}
		return nil, refuse("the request failed in transport")
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxBody+1))
	if err != nil {
		if errors.Is(ctx.Err(), context.DeadlineExceeded) {
			return nil, refuse("the request timed out")
		}
		return nil, refuse("the response could not be read")
	}
	if len(body) > MaxBody {
		return nil, refuse("the response exceeds %d bytes", MaxBody)
	}
	switch {
	case resp.StatusCode == http.StatusOK:
		// The declared type is checked as well as the body; a header alone would prove nothing.
		if mediaType, _, err := mime.ParseMediaType(resp.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
			return nil, refuse("the response is not declared as application/json")
		}
		return decode(body, r)
	case resp.StatusCode == http.StatusNotFound:
		if definitelyAbsent(body, r) {
			return &Metadata{Schema: Schema, Found: false, Namespace: r.Namespace, Name: r.Name}, nil
		}
		return nil, refuse("HTTP 404 that is not the API server's NotFound for this Secret")
	case resp.StatusCode == http.StatusNotAcceptable:
		return nil, refuse("the server does not offer the metadata-only representation (HTTP 406)")
	case resp.StatusCode >= 300 && resp.StatusCode < 400:
		return nil, refuse("the server answered with a redirect (HTTP %d), which is not followed", resp.StatusCode)
	default:
		return nil, refuse("HTTP %d", resp.StatusCode)
	}
}

// definitelyAbsent: only the API server's own Status with reason NotFound, naming this Secret, proves absence.
func definitelyAbsent(body []byte, r Request) bool {
	var st struct {
		Kind    string `json:"kind"`
		Reason  string `json:"reason"`
		Details struct {
			Name  string `json:"name"`
			Kind  string `json:"kind"`
			Group string `json:"group"`
		} `json:"details"`
	}
	if json.Unmarshal(body, &st) != nil {
		return false
	}
	return st.Kind == "Status" && st.Reason == "NotFound" && st.Details.Name == r.Name && st.Details.Kind == "secrets" &&
		st.Details.Group == ""
}

func decode(body []byte, r Request) (*Metadata, error) {
	var top map[string]json.RawMessage
	if json.Unmarshal(body, &top) != nil {
		return nil, refuse("the response is not a JSON object")
	}
	for key := range top {
		if key != "kind" && key != "apiVersion" && key != "metadata" {
			return nil, refuse("the response has a field beside kind, apiVersion and metadata: refused")
		}
	}
	var kind, apiVersion string
	if json.Unmarshal(top["kind"], &kind) != nil || json.Unmarshal(top["apiVersion"], &apiVersion) != nil ||
		kind != "PartialObjectMetadata" || apiVersion != "meta.k8s.io/v1" {
		return nil, refuse("the response is not a meta.k8s.io/v1 PartialObjectMetadata")
	}
	var meta struct {
		Name            string            `json:"name"`
		Namespace       string            `json:"namespace"`
		UID             string            `json:"uid"`
		ResourceVersion string            `json:"resourceVersion"`
		Labels          map[string]string `json:"labels"`
	}
	if json.Unmarshal(top["metadata"], &meta) != nil {
		return nil, refuse("the response's metadata cannot be decoded")
	}
	if meta.Name != r.Name || meta.Namespace != r.Namespace {
		return nil, refuse("the response is about another object")
	}
	if meta.UID == "" || meta.ResourceVersion == "" {
		return nil, refuse("the response has no UID or resourceVersion")
	}
	for key, want := range r.Require {
		if got, ok := meta.Labels[key]; !ok || got != want {
			return nil, refuse("a required label is missing or has another value")
		}
	}
	out := &Metadata{Schema: Schema, Found: true, Namespace: meta.Namespace, Name: meta.Name, UID: meta.UID,
		ResourceVersion: meta.ResourceVersion}
	for _, key := range r.LabelKeys {
		if value, ok := meta.Labels[key]; ok {
			if out.Labels == nil {
				out.Labels = map[string]string{}
			}
			out.Labels[key] = value
		}
	}
	return out, nil
}

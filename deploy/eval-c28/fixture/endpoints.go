package main

// Per-request round-robin routing for the ingress (ROUTING=endpoints-rr).
//
// Why: with ROUTING=service the ingress keeps one keep-alive pool to each target Service's ClusterIP, so the cluster
// (kube-proxy or Cilium) picks a backend per CONNECTION, not per request. With max_inflight=1 and a per-pod queue, an
// uneven connection spread becomes hot-pod queueing (S5a calibration, 2 replicas: one pod completed 57.7 % of the
// requests with an average queue of 2.8, the other 42.3 % with an empty queue). endpoints-rr removes that layer: the
// ingress discovers the READY endpoints of every target Service itself and sends each request straight to the next
// pod IP in a strict round-robin.
//
// Discovery is a namespaced LIST + WATCH of discovery.k8s.io/v1 EndpointSlices over the in-cluster REST API (standard
// library only; no client-go). One label-selected LIST (`kubernetes.io/service-name in (...)` and the EndpointSlice
// controller's managed-by label) is the sync point; a WATCH from its resourceVersion applies changes until the server
// ends it after resync seconds (or it fails), then the state is rebuilt from a fresh LIST. So the full state is
// re-read at least every resync (+ the client-side margin), and an event lost in a watch can never outlive a resync.
//
// Selection: an endpoint counts only with conditions.ready == true and conditions.terminating != true (a missing
// ready condition does NOT count), an IPv4/IPv6 address, and a TCP port named "http" in its slice. Endpoints are
// de-duplicated by ip:port (a pod may appear in two slices while the controller rebalances) and sorted by ip:port, so
// the list is stable and the rotation deterministic. Each target keeps its own atomic counter; request i goes to
// list[i mod len]. A change of the set swaps the list atomically (one pointer store) and bumps the target's
// generation; requests that already picked an endpoint finish there, every later pick sees the new list. A removed
// endpoint's keep-alive connections are closed: the idle ones at the swap, those of requests in flight at the swap
// when each request finishes (each endpoint has its own Transport; endpoint.release).
//
// Staleness: a failed LIST or WATCH keeps the LAST KNOWN set (routing continues; failing every request closed would
// turn a discovery outage into fixture failures) and is counted; c28_ingress_discovery_stale becomes 1 when the last
// successful LIST is older than the stale bound (or there never was one), so the harness can gate on it. Before the
// first successful LIST the ingress is not ready (/ready 503) and every routed request is a counted 503.
//
// Retries: none. The ingress never re-sends, and the outgoing request carries an empty body that net/http cannot
// rewind, which makes it non-replayable: the Transport's own silent re-send of an idempotent GET after a kept-alive
// connection fails (errServerClosedIdle and friends) is disabled too. The wire request is unchanged (an empty body on
// a GET is sent without Content-Length or Transfer-Encoding).

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	routingService     = "service"
	routingEndpointsRR = "endpoints-rr"

	// endpointPortName is the Service port name every fixture Service uses; the EndpointSlice carries it with the
	// resolved container port.
	endpointPortName = "http"
	serviceNameLabel = "kubernetes.io/service-name"
	managedByLabel   = "endpointslice.kubernetes.io/managed-by"
	managedByValue   = "endpointslice-controller.k8s.io"

	saDir = "/var/run/secrets/kubernetes.io/serviceaccount"

	// retiredSeriesKeep: a removed endpoint's dispatch counter is kept this long (well past the 60 s rate range and
	// several 15 s scrapes), then dropped, so pod churn over a long run cannot grow /metrics without bound.
	retiredSeriesKeep = 15 * time.Minute
)

// noReplayBody is an empty request body that net/http cannot rewind (no GetBody): a request carrying it is never
// replayed by the Transport. The Transport probes it, finds it empty and sends no body headers.
type noReplayBody struct{}

func (noReplayBody) Read([]byte) (int, error) { return 0, io.EOF }
func (noReplayBody) Close() error             { return nil }

// endpoint is one ready backend: ip:port, the pod it belongs to, and its own connection pool.
type endpoint struct {
	key     string // ip:port (net.JoinHostPort)
	pod     string // targetRef name, or the address when the slice names no pod
	tr      *http.Transport
	retired atomic.Bool // set when it leaves the set (before its idle connections are closed)
}

// release runs after a request on the endpoint finished (its body read to EOF, which returns the connection to the
// idle pool before the EOF is delivered, or closed): on a retired endpoint it closes the idle connections, so a
// request that was in flight at the swap cannot leave a kept-alive connection behind. With the swap's own close
// (after setting retired) every connection is closed by whichever of the two runs last.
func (e *endpoint) release() {
	if e.retired.Load() {
		e.tr.CloseIdleConnections()
	}
}

type endpointSet struct {
	gen uint64
	eps []*endpoint
}

// rrTarget is one upstream target: its Service, the current ready set and the rotation counter.
type rrTarget struct {
	name    string
	service string
	host    string // the Service host the request would have used (sent as Host)
	set     atomic.Pointer[endpointSet]
	next    atomic.Uint64
}

// pick returns the next endpoint in strict round-robin, or nil when no endpoint is ready.
func (t *rrTarget) pick() *endpoint {
	s := t.set.Load()
	if s == nil || len(s.eps) == 0 {
		return nil
	}
	i := t.next.Add(1) - 1
	return s.eps[i%uint64(len(s.eps))]
}

// rrRouter holds every target's endpoint set and the discovery state.
type rrRouter struct {
	namespace  string
	targets    map[string]*rrTarget // by target
	byService  map[string]*rrTarget // by Service name
	now        func() time.Time
	staleAfter time.Duration

	mu      sync.Mutex            // serializes apply(); guards slices and retired
	slices  map[string]sliceEntry // the discovered EndpointSlices by name
	retired map[string]time.Time  // dispatch-counter label -> when its endpoint left the set

	lastSync   atomic.Int64 // unix nanos of the last successful LIST (0: never)
	dispatched counterVec   // target, pod
	noReady    counterVec   // target
	errs       counterVec   // stage
}

// sliceEntry is the part of an EndpointSlice the router uses: its Service and its usable endpoints.
type sliceEntry struct {
	service string
	eps     []epRef
}

type epRef struct{ key, pod string }

// parseServiceHost maps an upstream base URL to its Service: http://<service>.<namespace>.svc[.cluster.local], no port
// (the Service port 80), no path. The namespace must be the ingress's own (its RBAC is namespaced).
func parseServiceHost(base, namespace string) (string, error) {
	u, err := url.Parse(base)
	if err != nil || u.Scheme != "http" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" {
		return "", fmt.Errorf("endpoints-rr needs http://<service>.<namespace>.svc, got %q", base)
	}
	if u.Port() != "" {
		return "", fmt.Errorf("endpoints-rr upstream %q must not name a port (the Service port is resolved by name %q)",
			base, endpointPortName)
	}
	host := strings.TrimSuffix(u.Hostname(), ".cluster.local")
	parts := strings.Split(host, ".")
	if len(parts) != 3 || parts[2] != "svc" || parts[0] == "" {
		return "", fmt.Errorf("endpoints-rr needs http://<service>.<namespace>.svc, got %q", base)
	}
	if parts[1] != namespace {
		return "", fmt.Errorf("upstream %q is outside the ingress namespace %q", base, namespace)
	}
	return parts[0], nil
}

func newRRRouter(upstreams map[string]string, namespace string, staleAfter time.Duration,
	now func() time.Time) (*rrRouter, error) {
	if namespace == "" || staleAfter <= 0 {
		return nil, fmt.Errorf("endpoints-rr needs a namespace and a positive stale bound")
	}
	r := &rrRouter{namespace: namespace, targets: map[string]*rrTarget{}, byService: map[string]*rrTarget{},
		now: now, staleAfter: staleAfter, slices: map[string]sliceEntry{}, retired: map[string]time.Time{}}
	for target, base := range upstreams {
		svc, err := parseServiceHost(base, namespace)
		if err != nil {
			return nil, err
		}
		if _, dup := r.byService[svc]; dup {
			return nil, fmt.Errorf("two targets route to Service %s", svc)
		}
		u, _ := url.Parse(base)
		t := &rrTarget{name: target, service: svc, host: u.Host}
		t.set.Store(&endpointSet{})
		r.targets[target] = t
		r.byService[svc] = t
	}
	return r, nil
}

// selector is the LIST/WATCH label selector: exactly the targets' Services, controller-managed slices only.
func (r *rrRouter) selector() string {
	svcs := make([]string, 0, len(r.byService))
	for s := range r.byService {
		svcs = append(svcs, s)
	}
	sort.Strings(svcs)
	return fmt.Sprintf("%s in (%s),%s=%s", serviceNameLabel, strings.Join(svcs, ","), managedByLabel, managedByValue)
}

// --- the EndpointSlice API objects (only the fields used) ---------------------------------------------------------
//
// Validation is strict and all-or-nothing (Codex review of PR #51): an answer is decoded with typed fields (a wrong
// JSON type is an error), the LIST envelope must carry a non-empty string metadata.resourceVersion and an items ARRAY
// (kind/apiVersion, when present, must be EndpointSliceList / discovery.k8s.io/v1), and EVERY returned slice must pass
// validateSlice before anything is installed: one bad slice fails the whole sync, nothing is installed and lastSync is
// not refreshed, so the previously validated snapshot stays in use (and turns stale). A malformed WATCH event ends the
// watch before it is applied; the loop re-lists. A returned slice outside the selector is malformed too.

type objectMeta struct {
	Name            string            `json:"name"`
	ResourceVersion string            `json:"resourceVersion"`
	Labels          map[string]string `json:"labels"`
}

type endpointSlice struct {
	Kind        *string    `json:"kind"`
	APIVersion  *string    `json:"apiVersion"`
	Metadata    objectMeta `json:"metadata"`
	AddressType string     `json:"addressType"`
	Endpoints   []struct {
		Addresses  []string `json:"addresses"`
		Conditions struct {
			Ready       *bool `json:"ready"`
			Terminating *bool `json:"terminating"`
		} `json:"conditions"`
		TargetRef *struct {
			Kind string `json:"kind"`
			Name string `json:"name"`
		} `json:"targetRef"`
	} `json:"endpoints"`
	Ports []struct {
		Name     *string `json:"name"`
		Port     *int32  `json:"port"`
		Protocol *string `json:"protocol"`
	} `json:"ports"`
}

const (
	sliceAPIVersion = "discovery.k8s.io/v1"
	maxListBytes    = 16 << 20
)

// parseSlice decodes one EndpointSlice object (typed: a field of the wrong JSON type is an error).
func parseSlice(raw []byte) (endpointSlice, error) {
	var s endpointSlice
	if len(raw) == 0 || raw[0] != '{' {
		return s, errors.New("an EndpointSlice is not a JSON object")
	}
	if err := json.Unmarshal(raw, &s); err != nil {
		return s, fmt.Errorf("decoding an EndpointSlice: %w", err)
	}
	if s.Kind != nil && *s.Kind != "EndpointSlice" {
		return s, fmt.Errorf("object kind %q is not EndpointSlice", *s.Kind)
	}
	if s.APIVersion != nil && *s.APIVersion != sliceAPIVersion {
		return s, fmt.Errorf("object apiVersion %q is not %s", *s.APIVersion, sliceAPIVersion)
	}
	return s, nil
}

// parseList validates a LIST answer's envelope and decodes every item; it installs nothing.
func parseList(body []byte) ([]endpointSlice, string, error) {
	var env struct {
		Kind       *string `json:"kind"`
		APIVersion *string `json:"apiVersion"`
		Metadata   *struct {
			ResourceVersion *string `json:"resourceVersion"`
		} `json:"metadata"`
		Items json.RawMessage `json:"items"`
	}
	if len(body) == 0 || body[0] != '{' {
		return nil, "", errors.New("the EndpointSlice list is not a JSON object")
	}
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, "", fmt.Errorf("decoding the EndpointSlice list: %w", err)
	}
	if env.Kind != nil && *env.Kind != "EndpointSliceList" {
		return nil, "", fmt.Errorf("list kind %q is not EndpointSliceList", *env.Kind)
	}
	if env.APIVersion != nil && *env.APIVersion != sliceAPIVersion {
		return nil, "", fmt.Errorf("list apiVersion %q is not %s", *env.APIVersion, sliceAPIVersion)
	}
	if env.Metadata == nil || env.Metadata.ResourceVersion == nil || *env.Metadata.ResourceVersion == "" {
		return nil, "", errors.New("the EndpointSlice list has no metadata.resourceVersion")
	}
	if len(env.Items) == 0 || env.Items[0] != '[' {
		return nil, "", errors.New("the EndpointSlice list's items is not an array")
	}
	var raws []json.RawMessage
	if err := json.Unmarshal(env.Items, &raws); err != nil {
		return nil, "", fmt.Errorf("decoding the list items: %w", err)
	}
	items := make([]endpointSlice, 0, len(raws))
	for i, raw := range raws {
		s, err := parseSlice(raw)
		if err != nil {
			return nil, "", fmt.Errorf("item %d: %w", i, err)
		}
		items = append(items, s)
	}
	return items, *env.Metadata.ResourceVersion, nil
}

// validateSlice checks one returned slice strictly and reduces it to its Service and READY endpoints: a name; the
// selector's labels (one of the targets' Services, managed by the EndpointSlice controller); address type IPv4/IPv6;
// every endpoint with addresses that parse as IPs of that family; every port numbered 1..65535; and, when any
// endpoint counts, a TCP port named http. Any failure is an error for the whole sync.
func (r *rrRouter) validateSlice(s endpointSlice) (sliceEntry, error) {
	name := s.Metadata.Name
	if name == "" {
		return sliceEntry{}, errors.New("a slice without metadata.name")
	}
	svc := s.Metadata.Labels[serviceNameLabel]
	if _, ours := r.byService[svc]; !ours || s.Metadata.Labels[managedByLabel] != managedByValue {
		return sliceEntry{}, fmt.Errorf("slice %s is outside the selector (service %q, managed-by %q)", name, svc,
			s.Metadata.Labels[managedByLabel])
	}
	if s.AddressType != "IPv4" && s.AddressType != "IPv6" {
		return sliceEntry{}, fmt.Errorf("slice %s: address type %q", name, s.AddressType)
	}
	port := int32(0)
	for _, p := range s.Ports {
		if p.Port == nil || *p.Port < 1 || *p.Port > 65535 {
			return sliceEntry{}, fmt.Errorf("slice %s: a port without a valid number", name)
		}
		proto := "TCP"
		if p.Protocol != nil {
			proto = *p.Protocol
		}
		if p.Name != nil && *p.Name == endpointPortName && proto == "TCP" {
			port = *p.Port
		}
	}
	out := sliceEntry{service: svc}
	for _, e := range s.Endpoints {
		if len(e.Addresses) == 0 {
			return sliceEntry{}, fmt.Errorf("slice %s: an endpoint without addresses", name)
		}
		var ip net.IP
		for _, a := range e.Addresses {
			if ip = net.ParseIP(a); ip == nil || (ip.To4() != nil) != (s.AddressType == "IPv4") {
				return sliceEntry{}, fmt.Errorf("slice %s: address %q is not an %s address", name, a, s.AddressType)
			}
		}
		if !isTrue(e.Conditions.Ready) || isTrue(e.Conditions.Terminating) {
			continue
		}
		if port == 0 {
			return sliceEntry{}, fmt.Errorf("slice %s: ready endpoints but no TCP port named %q", name, endpointPortName)
		}
		ip = net.ParseIP(e.Addresses[0])
		pod := ip.String()
		if e.TargetRef != nil && e.TargetRef.Kind == "Pod" && e.TargetRef.Name != "" {
			pod = e.TargetRef.Name
		}
		out.eps = append(out.eps, epRef{key: net.JoinHostPort(ip.String(), strconv.Itoa(int(port))), pod: pod})
	}
	return out, nil
}

func isTrue(b *bool) bool { return b != nil && *b }

func newEndpointTransport() *http.Transport {
	return &http.Transport{MaxIdleConnsPerHost: 1024, IdleConnTimeout: 90 * time.Second}
}

// replaceAll installs a fresh LIST as the whole state, only when EVERY slice validates (else nothing changes);
// setSlice/deleteSlice apply one WATCH event, setSlice only a valid slice. Each recomputes the target sets.
func (r *rrRouter) replaceAll(items []endpointSlice) error {
	next := map[string]sliceEntry{}
	for _, s := range items {
		e, err := r.validateSlice(s)
		if err != nil {
			return err
		}
		if _, dup := next[s.Metadata.Name]; dup {
			return fmt.Errorf("slice %s listed twice", s.Metadata.Name)
		}
		next[s.Metadata.Name] = e
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slices = next
	r.applyLocked()
	return nil
}

func (r *rrRouter) setSlice(s endpointSlice) error {
	e, err := r.validateSlice(s)
	if err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slices[s.Metadata.Name] = e
	r.applyLocked()
	return nil
}

func (r *rrRouter) deleteSlice(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.slices, name)
	r.applyLocked()
}

// applyLocked recomputes every target's ready set from the slices and swaps the ones that changed.
func (r *rrRouter) applyLocked() {
	want := map[string]map[string]epRef{}
	for _, s := range r.slices {
		if want[s.service] == nil {
			want[s.service] = map[string]epRef{}
		}
		for _, e := range s.eps {
			if prev, dup := want[s.service][e.key]; !dup || e.pod < prev.pod {
				want[s.service][e.key] = e // de-duplicated by ip:port (deterministic if two slices disagree)
			}
		}
	}
	now := r.now()
	for _, t := range r.targets {
		refs := make([]epRef, 0, len(want[t.service]))
		for _, e := range want[t.service] {
			refs = append(refs, e)
		}
		sort.Slice(refs, func(i, j int) bool { return refs[i].key < refs[j].key })
		cur := t.set.Load()
		if sameRefs(cur.eps, refs) {
			continue
		}
		keep := map[epRef]*endpoint{}
		for _, e := range cur.eps {
			keep[epRef{e.key, e.pod}] = e
		}
		next := &endpointSet{gen: cur.gen + 1, eps: make([]*endpoint, 0, len(refs))}
		for _, ref := range refs {
			e := keep[ref]
			if e == nil {
				e = &endpoint{key: ref.key, pod: ref.pod, tr: newEndpointTransport()}
			}
			delete(keep, ref)
			delete(r.retired, dispatchLabel(t.name, ref.pod))
			next.eps = append(next.eps, e)
		}
		t.set.Store(next) // the swap: every later pick sees only the new set
		for _, gone := range keep {
			gone.retired.Store(true)
			gone.tr.CloseIdleConnections()
			r.retired[dispatchLabel(t.name, gone.pod)] = now
		}
		log.Printf("endpoints-rr: %s generation %d: %d ready", t.name, next.gen, len(next.eps))
	}
	r.pruneLocked(now)
}

func sameRefs(cur []*endpoint, refs []epRef) bool {
	if len(cur) != len(refs) {
		return false
	}
	for i := range cur {
		if cur[i].key != refs[i].key || cur[i].pod != refs[i].pod {
			return false
		}
	}
	return true
}

// dispatchLabel: the endpoint's pod is "endpoint_pod", never "pod" (the scrape's own pod label names the ingress pod).
func dispatchLabel(target, pod string) string {
	return label("target", target) + "," + label("endpoint_pod", pod)
}

func (r *rrRouter) pruneLocked(now time.Time) {
	for k, at := range r.retired {
		if now.Sub(at) >= retiredSeriesKeep {
			r.dispatched.mu.Lock()
			delete(r.dispatched.m, k)
			r.dispatched.mu.Unlock()
			delete(r.retired, k)
		}
	}
}

// synced reports whether a LIST has ever succeeded (the ingress's readiness in this mode).
func (r *rrRouter) synced() bool { return r.lastSync.Load() > 0 }

func (r *rrRouter) markSynced() { r.lastSync.Store(r.now().UnixNano()) }

func (r *rrRouter) stale() bool {
	last := r.lastSync.Load()
	return last == 0 || r.now().Sub(time.Unix(0, last)) > r.staleAfter
}

func (r *rrRouter) writeMetrics(w io.Writer) {
	r.mu.Lock()
	r.pruneLocked(r.now())
	r.mu.Unlock()
	fmt.Fprintln(w, "# TYPE c28_ingress_dispatched_total counter")
	r.dispatched.write(w, "c28_ingress_dispatched_total")
	fmt.Fprintln(w, "# TYPE c28_ingress_no_ready_endpoints_total counter")
	r.noReady.write(w, "c28_ingress_no_ready_endpoints_total")
	names := make([]string, 0, len(r.targets))
	for n := range r.targets {
		names = append(names, n)
	}
	sort.Strings(names)
	fmt.Fprintln(w, "# TYPE c28_ingress_endpoints_ready gauge")
	for _, n := range names {
		fmt.Fprintf(w, "c28_ingress_endpoints_ready{%s} %d\n", label("target", n), len(r.targets[n].set.Load().eps))
	}
	// membership: one series per READY endpoint now in the set (what the executor compares with the live fixture pods)
	fmt.Fprintln(w, "# TYPE c28_ingress_endpoint gauge")
	for _, n := range names {
		for _, e := range r.targets[n].set.Load().eps {
			fmt.Fprintf(w, "c28_ingress_endpoint{%s} 1\n", dispatchLabel(n, e.pod))
		}
	}
	fmt.Fprintln(w, "# TYPE c28_ingress_endpoints_generation gauge")
	for _, n := range names {
		fmt.Fprintf(w, "c28_ingress_endpoints_generation{%s} %d\n", label("target", n), r.targets[n].set.Load().gen)
	}
	fmt.Fprintln(w, "# TYPE c28_ingress_discovery_errors_total counter")
	r.errs.mu.Lock()
	for _, stage := range []string{"list", "watch"} { // always present (rate() needs the zero)
		fmt.Fprintf(w, "c28_ingress_discovery_errors_total{%s} %d\n", label("stage", stage), r.errs.m[label("stage", stage)])
	}
	r.errs.mu.Unlock()
	fmt.Fprintf(w, "# TYPE c28_ingress_discovery_synced gauge\nc28_ingress_discovery_synced %d\n", b2i(r.synced()))
	if last := r.lastSync.Load(); last > 0 {
		fmt.Fprintf(w, "# TYPE c28_ingress_discovery_last_sync_timestamp_seconds gauge\n"+
			"c28_ingress_discovery_last_sync_timestamp_seconds %.3f\n", float64(last)/1e9)
		fmt.Fprintf(w, "# TYPE c28_ingress_discovery_sync_age_seconds gauge\nc28_ingress_discovery_sync_age_seconds %.3f\n",
			r.now().Sub(time.Unix(0, last)).Seconds())
	}
	fmt.Fprintf(w, "# TYPE c28_ingress_discovery_stale_bound_seconds gauge\nc28_ingress_discovery_stale_bound_seconds %g\n",
		r.staleAfter.Seconds())
	fmt.Fprintf(w, "# TYPE c28_ingress_discovery_stale gauge\nc28_ingress_discovery_stale %d\n", b2i(r.stale()))
}

func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}

// --- the in-cluster API -------------------------------------------------------------------------------------------

// kubeAPI is the minimal in-cluster REST access: base URL, a TLS client trusting the cluster CA, and the projected
// service-account token (re-read for every call: the kubelet rotates it). The token is never logged.
type kubeAPI struct {
	base   string
	client *http.Client
	token  func() (string, error)
}

func inClusterAPI() (*kubeAPI, string, error) {
	host, port := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT")
	if host == "" || port == "" {
		return nil, "", errors.New("endpoints-rr runs in a pod: KUBERNETES_SERVICE_HOST/PORT are not set")
	}
	ca, err := os.ReadFile(saDir + "/ca.crt")
	if err != nil {
		return nil, "", fmt.Errorf("cluster CA: %w", err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(ca) {
		return nil, "", errors.New("cluster CA: no certificate in ca.crt")
	}
	ns, err := os.ReadFile(saDir + "/namespace")
	if err != nil {
		return nil, "", fmt.Errorf("namespace: %w", err)
	}
	tr := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12},
		IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second}
	token := func() (string, error) {
		b, err := os.ReadFile(saDir + "/token")
		if err != nil {
			return "", fmt.Errorf("service-account token unreadable: %w", err)
		}
		return strings.TrimSpace(string(b)), nil
	}
	return &kubeAPI{base: "https://" + net.JoinHostPort(host, port), client: &http.Client{Transport: tr}, token: token},
		strings.TrimSpace(string(ns)), nil
}

func (a *kubeAPI) get(ctx context.Context, path string, q url.Values) (*http.Response, error) {
	tok, err := a.token()
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, a.base+path+"?"+q.Encode(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+tok)
	req.Header.Set("Accept", "application/json")
	resp, err := a.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		msg, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return nil, fmt.Errorf("GET %s: HTTP %d: %s", path, resp.StatusCode, strings.TrimSpace(string(msg)))
	}
	return resp, nil
}

func (r *rrRouter) slicesPath() string {
	return "/apis/discovery.k8s.io/v1/namespaces/" + r.namespace + "/endpointslices"
}

// list reads the full state (a consistent LIST), validates ALL of it, installs it and only then counts it as a sync.
// Any failure leaves the installed snapshot and lastSync unchanged.
func (r *rrRouter) list(ctx context.Context, api *kubeAPI) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := api.get(ctx, r.slicesPath(), url.Values{"labelSelector": {r.selector()}})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxListBytes+1))
	if err != nil {
		return "", fmt.Errorf("reading the EndpointSlice list: %w", err)
	}
	if len(body) > maxListBytes {
		return "", errors.New("the EndpointSlice list exceeds 16 MiB")
	}
	items, rv, err := parseList(body)
	if err != nil {
		return "", err
	}
	if err := r.replaceAll(items); err != nil {
		return "", err
	}
	r.markSynced()
	return rv, nil
}

type watchEvent struct {
	Type   string          `json:"type"`
	Object json.RawMessage `json:"object"`
}

// watch applies events from rv until the server closes the stream (after resync), the context ends or an error.
// A clean end returns nil (the caller re-lists).
func (r *rrRouter) watch(ctx context.Context, api *kubeAPI, rv string, resync time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, resync+15*time.Second)
	defer cancel()
	q := url.Values{"labelSelector": {r.selector()}, "watch": {"true"}, "resourceVersion": {rv},
		"allowWatchBookmarks": {"true"}, "timeoutSeconds": {strconv.Itoa(int(resync.Seconds()))}}
	resp, err := api.get(ctx, r.slicesPath(), q)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	dec := json.NewDecoder(resp.Body)
	for {
		var ev watchEvent
		if err := dec.Decode(&ev); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			if ctx.Err() != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("watch outlived its bound: %w", err)
			}
			return fmt.Errorf("decoding a watch event: %w", err)
		}
		switch ev.Type {
		case "ADDED", "MODIFIED", "DELETED":
			s, err := parseSlice(ev.Object)
			if err == nil && ev.Type == "DELETED" {
				_, err = r.validateSlice(s) // a malformed delete is malformed too: re-list instead
			}
			if err == nil && ev.Type == "DELETED" {
				r.deleteSlice(s.Metadata.Name)
			} else if err == nil {
				err = r.setSlice(s)
			}
			if err != nil {
				return fmt.Errorf("malformed %s event (not applied): %w", ev.Type, err)
			}
		case "BOOKMARK":
			var b struct {
				Metadata *struct {
					ResourceVersion *string `json:"resourceVersion"`
				} `json:"metadata"`
			}
			if err := json.Unmarshal(ev.Object, &b); err != nil || b.Metadata == nil || b.Metadata.ResourceVersion == nil ||
				*b.Metadata.ResourceVersion == "" {
				return errors.New("malformed BOOKMARK event")
			}
		case "ERROR":
			return fmt.Errorf("watch error event: %s", strings.TrimSpace(string(ev.Object)))
		default:
			return fmt.Errorf("unknown watch event type %q", ev.Type)
		}
	}
}

// run keeps the endpoint sets current: LIST, WATCH until the server ends it, repeat; a failure is counted, logged and
// retried after a second, with the last known sets kept.
func (r *rrRouter) run(ctx context.Context, api *kubeAPI, resync time.Duration) {
	for ctx.Err() == nil {
		rv, err := r.list(ctx, api)
		if err != nil {
			r.errs.inc(label("stage", "list"))
			log.Printf("endpoints-rr: list failed (keeping the last known sets): %v", err)
			sleepCtx(ctx, time.Second)
			continue
		}
		if err := r.watch(ctx, api, rv, resync); err != nil {
			r.errs.inc(label("stage", "watch"))
			log.Printf("endpoints-rr: watch failed (re-listing): %v", err)
			sleepCtx(ctx, time.Second)
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

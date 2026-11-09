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

type objectMeta struct {
	Name            string            `json:"name"`
	ResourceVersion string            `json:"resourceVersion"`
	Labels          map[string]string `json:"labels"`
}

type endpointSlice struct {
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

type endpointSliceList struct {
	Metadata objectMeta      `json:"metadata"`
	Items    []endpointSlice `json:"items"`
}

// usable reduces a slice to its Service and ready endpoints. A slice of another Service or manager is ignored (nil, no
// error); a slice whose endpoints cannot be addressed (no TCP port named http, an FQDN slice) is an error.
func (r *rrRouter) usable(s endpointSlice) (*sliceEntry, error) {
	svc := s.Metadata.Labels[serviceNameLabel]
	if _, ours := r.byService[svc]; !ours || s.Metadata.Labels[managedByLabel] != managedByValue {
		return nil, nil
	}
	ready := 0
	for _, e := range s.Endpoints {
		if isTrue(e.Conditions.Ready) && !isTrue(e.Conditions.Terminating) {
			ready++
		}
	}
	out := &sliceEntry{service: svc}
	if ready == 0 {
		return out, nil // e.g. the controller's placeholder slice of a Service with no endpoints (ports: null)
	}
	if s.AddressType != "IPv4" && s.AddressType != "IPv6" {
		return out, fmt.Errorf("slice %s: address type %q", s.Metadata.Name, s.AddressType)
	}
	port := int32(0)
	for _, p := range s.Ports {
		proto := "TCP"
		if p.Protocol != nil {
			proto = *p.Protocol
		}
		if p.Name != nil && *p.Name == endpointPortName && proto == "TCP" && p.Port != nil && *p.Port > 0 {
			port = *p.Port
		}
	}
	if port == 0 {
		return out, fmt.Errorf("slice %s: no TCP port named %q", s.Metadata.Name, endpointPortName)
	}
	for _, e := range s.Endpoints {
		if !isTrue(e.Conditions.Ready) || isTrue(e.Conditions.Terminating) || len(e.Addresses) == 0 {
			continue
		}
		ip := net.ParseIP(e.Addresses[0])
		if ip == nil {
			return out, fmt.Errorf("slice %s: address %q is not an IP", s.Metadata.Name, e.Addresses[0])
		}
		key := net.JoinHostPort(ip.String(), strconv.Itoa(int(port)))
		pod := ip.String()
		if e.TargetRef != nil && e.TargetRef.Kind == "Pod" && e.TargetRef.Name != "" {
			pod = e.TargetRef.Name
		}
		out.eps = append(out.eps, epRef{key: key, pod: pod})
	}
	return out, nil
}

func isTrue(b *bool) bool { return b != nil && *b }

func newEndpointTransport() *http.Transport {
	return &http.Transport{MaxIdleConnsPerHost: 1024, IdleConnTimeout: 90 * time.Second}
}

// replaceAll installs a fresh LIST as the whole state; setSlice/deleteSlice apply one WATCH event. Each recomputes the
// target sets.
func (r *rrRouter) replaceAll(items []endpointSlice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.slices = map[string]sliceEntry{}
	for _, s := range items {
		r.addLocked(s)
	}
	r.applyLocked()
}

func (r *rrRouter) setSlice(s endpointSlice) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.slices, s.Metadata.Name)
	r.addLocked(s)
	r.applyLocked()
}

func (r *rrRouter) deleteSlice(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.slices, name)
	r.applyLocked()
}

func (r *rrRouter) addLocked(s endpointSlice) {
	e, err := r.usable(s)
	if err != nil {
		r.errs.inc(label("stage", "slice"))
		log.Printf("endpoints-rr: %v (its endpoints are not used)", err)
	}
	if e != nil && err == nil {
		r.slices[s.Metadata.Name] = *e
	}
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

func dispatchLabel(target, pod string) string {
	return label("target", target) + "," + label("pod", pod)
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
	fmt.Fprintln(w, "# TYPE c28_ingress_endpoints_generation gauge")
	for _, n := range names {
		fmt.Fprintf(w, "c28_ingress_endpoints_generation{%s} %d\n", label("target", n), r.targets[n].set.Load().gen)
	}
	fmt.Fprintln(w, "# TYPE c28_ingress_discovery_errors_total counter")
	r.errs.mu.Lock()
	for _, stage := range []string{"list", "watch", "slice"} { // always present (rate() needs the zero)
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

// list reads the full state (a consistent LIST), installs it and returns its resourceVersion.
func (r *rrRouter) list(ctx context.Context, api *kubeAPI) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	resp, err := api.get(ctx, r.slicesPath(), url.Values{"labelSelector": {r.selector()}})
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	var l endpointSliceList
	if err := json.NewDecoder(resp.Body).Decode(&l); err != nil {
		return "", fmt.Errorf("decoding the EndpointSlice list: %w", err)
	}
	r.replaceAll(l.Items)
	r.markSynced()
	return l.Metadata.ResourceVersion, nil
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
			var s endpointSlice
			if err := json.Unmarshal(ev.Object, &s); err != nil {
				return fmt.Errorf("decoding a watched EndpointSlice: %w", err)
			}
			if ev.Type == "DELETED" {
				r.deleteSlice(s.Metadata.Name)
			} else {
				r.setSlice(s)
			}
		case "BOOKMARK":
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

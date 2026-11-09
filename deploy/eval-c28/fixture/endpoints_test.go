package main

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// epSpec describes one endpoint of a test EndpointSlice; ready/terminating nil means the condition is absent.
type epSpec struct {
	ip          string
	pod         string
	ready       *bool
	terminating *bool
}

var yes, no = func() *bool { b := true; return &b }(), func() *bool { b := false; return &b }()

func ready(ip, pod string) epSpec { return epSpec{ip: ip, pod: pod, ready: yes} }

// sliceJSON builds an EndpointSlice as the API serves it (decoded through the real JSON mapping).
func sliceJSON(name, svc string, port int, eps ...epSpec) map[string]any {
	items := []any{}
	for _, e := range eps {
		cond := map[string]any{}
		if e.ready != nil {
			cond["ready"] = *e.ready
		}
		if e.terminating != nil {
			cond["terminating"] = *e.terminating
		}
		item := map[string]any{"addresses": []string{e.ip}, "conditions": cond}
		if e.pod != "" {
			item["targetRef"] = map[string]any{"kind": "Pod", "name": e.pod, "namespace": "demo"}
		}
		items = append(items, item)
	}
	return map[string]any{"apiVersion": "discovery.k8s.io/v1", "kind": "EndpointSlice", "addressType": "IPv4",
		"metadata": map[string]any{"name": name, "namespace": "demo", "resourceVersion": "1",
			"labels": map[string]any{serviceNameLabel: svc, managedByLabel: managedByValue}},
		"endpoints": items,
		"ports":     []any{map[string]any{"name": "http", "port": port, "protocol": "TCP"}}}
}

func decodeSlice(t *testing.T, m map[string]any) endpointSlice {
	t.Helper()
	raw, _ := json.Marshal(m)
	var s endpointSlice
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatal(err)
	}
	return s
}

func testRouter(t *testing.T, now func() time.Time, targets ...string) *rrRouter {
	t.Helper()
	ups := map[string]string{}
	for _, n := range targets {
		ups[n] = "http://" + n + ".demo.svc"
	}
	r, err := newRRRouter(ups, "demo", 90*time.Second, now)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func mustReplace(t *testing.T, r *rrRouter, items []endpointSlice) {
	t.Helper()
	if err := r.replaceAll(items); err != nil {
		t.Fatal(err)
	}
}

func keys(r *rrRouter, target string) []string {
	var out []string
	for _, e := range r.targets[target].set.Load().eps {
		out = append(out, e.key)
	}
	return out
}

func TestRoundRobinIsDeterministicAndEven(t *testing.T) {
	r := testRouter(t, time.Now, "a")
	// unsorted input across two slices: the rotation follows the sorted ip:port order
	mustReplace(t, r, []endpointSlice{
		decodeSlice(t, sliceJSON("a-1", "a", 8080, ready("10.0.0.3", "p3"), ready("10.0.0.1", "p1"))),
		decodeSlice(t, sliceJSON("a-2", "a", 8080, ready("10.0.0.2", "p2")))})
	want := []string{"10.0.0.1:8080", "10.0.0.2:8080", "10.0.0.3:8080"}
	got := map[string]int{}
	for i := 0; i < 300; i++ {
		e := r.targets["a"].pick()
		if e.key != want[i%3] {
			t.Fatalf("pick %d went to %s, want %s (strict rotation)", i, e.key, want[i%3])
		}
		got[e.pod]++
	}
	if got["p1"] != 100 || got["p2"] != 100 || got["p3"] != 100 {
		t.Fatalf("uneven: %v", got)
	}
	// concurrent picks: still exactly even (one atomic counter per target)
	var wg sync.WaitGroup
	var mu sync.Mutex
	conc := map[string]int{}
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				e := r.targets["a"].pick()
				mu.Lock()
				conc[e.key]++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	for _, k := range want {
		if conc[k] != 800 {
			t.Fatalf("concurrent picks uneven: %v", conc)
		}
	}
}

func TestOnlyReadyNonTerminatingEndpointsCount(t *testing.T) {
	r := testRouter(t, time.Now, "a", "b")
	mustReplace(t, r, []endpointSlice{
		decodeSlice(t, sliceJSON("a-1", "a", 8080,
			ready("10.0.0.1", "ok"),
			epSpec{ip: "10.0.0.2", pod: "not-ready", ready: no},
			epSpec{ip: "10.0.0.3", pod: "no-condition"},                              // ready absent: does not count
			epSpec{ip: "10.0.0.4", pod: "terminating", ready: yes, terminating: yes}, // never counts
			epSpec{ip: "10.0.0.5", pod: "term-false", ready: yes, terminating: no})),
		decodeSlice(t, sliceJSON("a-2", "a", 8080, ready("10.0.0.1", "ok"))), // duplicate across slices
		decodeSlice(t, sliceJSON("b-1", "b", 8080, epSpec{ip: "10.1.0.1", pod: "starting", ready: no}))})
	if got := keys(r, "a"); strings.Join(got, ",") != "10.0.0.1:8080,10.0.0.5:8080" {
		t.Fatalf("a: %v", got)
	}
	if got := keys(r, "b"); len(got) != 0 {
		t.Fatalf("b has no ready endpoint: %v", got)
	}
	// the controller's placeholder slice of a Service without endpoints (endpoints and ports null) is valid and empty
	empty := sliceJSON("b-1", "b", 8080)
	empty["endpoints"], empty["ports"] = nil, nil
	if err := r.setSlice(decodeSlice(t, empty)); err != nil || len(keys(r, "b")) != 0 {
		t.Fatalf("placeholder slice: %v", err)
	}
}

// Malformed or partly invalid discovery answers install NOTHING (Codex review of PR #51, finding 2).
func TestAMalformedListIsRefusedAsAWhole(t *testing.T) {
	good := sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"))
	enc := func(v any) []byte { b, _ := json.Marshal(v); return b }
	for name, body := range map[string][]byte{
		"empty object":            []byte(`{}`),
		"not an object":           []byte(`[]`),
		"items not an array":      []byte(`{"metadata":{"resourceVersion":"5"},"items":{}}`),
		"items null":              []byte(`{"metadata":{"resourceVersion":"5"},"items":null}`),
		"no items":                []byte(`{"metadata":{"resourceVersion":"5"}}`),
		"no resourceVersion":      enc(map[string]any{"metadata": map[string]any{}, "items": []any{good}}),
		"empty resourceVersion":   enc(map[string]any{"metadata": map[string]any{"resourceVersion": ""}, "items": []any{good}}),
		"numeric resourceVersion": []byte(`{"metadata":{"resourceVersion":5},"items":[]}`),
		"wrong kind":              enc(map[string]any{"kind": "PodList", "metadata": map[string]any{"resourceVersion": "5"}, "items": []any{}}),
		"wrong apiVersion":        enc(map[string]any{"apiVersion": "discovery.k8s.io/v1beta1", "metadata": map[string]any{"resourceVersion": "5"}, "items": []any{}}),
		"item not an object":      []byte(`{"metadata":{"resourceVersion":"5"},"items":[1]}`),
		"item of a wrong type":    []byte(`{"metadata":{"resourceVersion":"5"},"items":[{"metadata":{"name":"a-1"},"endpoints":{}}]}`),
		"trailing data":           []byte(`{"metadata":{"resourceVersion":"5"},"items":[]} {}`),
	} {
		if _, _, err := parseList(body); err == nil {
			t.Errorf("%s: must be refused", name)
		}
	}
	items, rv, err := parseList(enc(map[string]any{"kind": "EndpointSliceList", "apiVersion": "discovery.k8s.io/v1",
		"metadata": map[string]any{"resourceVersion": "5"}, "items": []any{good}}))
	if err != nil || rv != "5" || len(items) != 1 {
		t.Fatalf("a well-formed list: %v %q %d", err, rv, len(items))
	}
}

func TestOneInvalidSliceFailsTheWholeSyncAndKeepsTheSnapshot(t *testing.T) {
	r := testRouter(t, time.Now, "a", "b")
	mustReplace(t, r, []endpointSlice{decodeSlice(t, sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1")))})
	good := sliceJSON("a-2", "a", 8080, ready("10.0.0.2", "p2"))
	bad := map[string]func() map[string]any{
		"outside the selector": func() map[string]any { return sliceJSON("x-1", "not-ours", 8080, ready("10.9.0.1", "x")) },
		"foreign manager": func() map[string]any {
			m := sliceJSON("a-9", "a", 8080, ready("10.9.0.2", "y"))
			m["metadata"].(map[string]any)["labels"].(map[string]any)[managedByLabel] = "someone-else"
			return m
		},
		"no http port": func() map[string]any {
			m := sliceJSON("b-1", "b", 8080, ready("10.1.0.9", "z"))
			m["ports"] = []any{map[string]any{"name": "metrics", "port": 9090, "protocol": "TCP"}}
			return m
		},
		"port without a number": func() map[string]any {
			m := sliceJSON("b-1", "b", 8080)
			m["ports"] = []any{map[string]any{"name": "http", "protocol": "TCP"}}
			return m
		},
		"bad address":      func() map[string]any { return sliceJSON("b-1", "b", 8080, ready("not-an-ip", "z")) },
		"wrong family":     func() map[string]any { return sliceJSON("b-1", "b", 8080, ready("fd00::1", "z")) },
		"no addresses":     func() map[string]any { return sliceJSON("b-1", "b", 8080, epSpec{pod: "z", ready: no}) },
		"FQDN slice":       func() map[string]any { m := sliceJSON("b-1", "b", 8080); m["addressType"] = "FQDN"; return m },
		"no name":          func() map[string]any { m := sliceJSON("", "b", 8080); return m },
		"a duplicate name": func() map[string]any { return sliceJSON("a-2", "a", 8080, ready("10.0.0.3", "p3")) },
	}
	for name, mk := range bad {
		m := mk()
		if name == "no addresses" {
			m["endpoints"].([]any)[0].(map[string]any)["addresses"] = []string{}
		}
		before := r.lastSync.Load()
		if err := r.replaceAll([]endpointSlice{decodeSlice(t, good), decodeSlice(t, m)}); err == nil {
			t.Errorf("%s: a list with one invalid slice must fail", name)
		}
		if got := strings.Join(keys(r, "a"), ","); got != "10.0.0.1:8080" || r.targets["a"].set.Load().gen != 1 ||
			r.lastSync.Load() != before {
			t.Fatalf("%s: something was installed: %s", name, got)
		}
		if name != "a duplicate name" {
			if err := r.setSlice(decodeSlice(t, m)); err == nil {
				t.Errorf("%s: a watched invalid slice must be refused", name)
			}
		}
	}
	if strings.Join(keys(r, "a"), ",") != "10.0.0.1:8080" {
		t.Fatal("the validated snapshot is kept")
	}
}

func TestUpstreamsForEndpointsModeAreServicesInTheOwnNamespace(t *testing.T) {
	for _, ok := range []string{"http://c28-x.demo.svc", "http://c28-x.demo.svc.cluster.local", "http://c28-x.demo.svc/"} {
		if svc, err := parseServiceHost(ok, "demo"); err != nil || svc != "c28-x" {
			t.Errorf("%s: %q %v", ok, svc, err)
		}
	}
	for _, bad := range []string{"http://c28-x.other.svc", "http://c28-x.demo.svc:8080", "http://10.0.0.1",
		"http://c28-x.demo", "https://c28-x.demo.svc", "http://c28-x.demo.svc/path"} {
		if _, err := parseServiceHost(bad, "demo"); err == nil {
			t.Errorf("%s must be refused", bad)
		}
	}
	if _, err := newRRRouter(map[string]string{"a": "http://s.demo.svc", "b": "http://s.demo.svc"}, "demo",
		time.Minute, time.Now); err == nil {
		t.Error("two targets on one Service must be refused")
	}
}

// backend is a test pod: counts requests, tracks open connections.
type backend struct {
	srv   *httptest.Server
	hits  atomic.Int64
	conns atomic.Int64
	ip    string
	port  int
}

func newBackend(t *testing.T, h func(b *backend, w http.ResponseWriter, r *http.Request)) *backend {
	t.Helper()
	b := &backend{}
	b.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.hits.Add(1)
		if h != nil {
			h(b, w, r)
			return
		}
		_, _ = io.WriteString(w, "ok")
	}))
	b.srv.Config.ConnState = func(_ net.Conn, s http.ConnState) {
		switch s {
		case http.StateNew:
			b.conns.Add(1)
		case http.StateClosed, http.StateHijacked:
			b.conns.Add(-1)
		}
	}
	b.srv.Start()
	t.Cleanup(b.srv.Close)
	host, port, _ := net.SplitHostPort(b.srv.Listener.Addr().String())
	b.ip = host
	b.port, _ = strconv.Atoi(port)
	return b
}

func (b *backend) slice(t *testing.T, name, svc, pod string) endpointSlice {
	return decodeSlice(t, sliceJSON(name, svc, b.port, ready(b.ip, pod)))
}

func rrIngress(t *testing.T, r *rrRouter, timeout time.Duration) (*ingress, *httptest.Server) {
	t.Helper()
	ups := map[string]string{}
	for n, tg := range r.targets {
		ups[n] = "http://" + tg.service + ".demo.svc"
	}
	g := newIngress(ups, timeout, time.Now).withEndpointsRR(r)
	srv := httptest.NewServer(g)
	t.Cleanup(srv.Close)
	return g, srv
}

func get(t *testing.T, url string) int {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	return resp.StatusCode
}

func metricsOf(t *testing.T, srv *httptest.Server) string {
	t.Helper()
	resp, err := http.Get(srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw)
}

func TestRequestsGoStraightToPodsEvenlyAndAreCountedAtDispatch(t *testing.T) {
	b1, b2 := newBackend(t, nil), newBackend(t, func(b *backend, w http.ResponseWriter, r *http.Request) {
		if r.Host != "a.demo.svc" || r.ContentLength != 0 || len(r.TransferEncoding) != 0 || r.URL.Path != "/work" {
			w.WriteHeader(http.StatusTeapot) // the wire request must look exactly like the Service-routed one
			return
		}
		_, _ = io.WriteString(w, "ok")
	})
	r := testRouter(t, time.Now, "a")
	mustReplace(t, r, []endpointSlice{b1.slice(t, "a-1", "a", "pod-1"), b2.slice(t, "a-2", "a", "pod-2")})
	r.markSynced()
	_, srv := rrIngress(t, r, 2*time.Second)
	for i := 0; i < 40; i++ {
		if c := get(t, srv.URL+"/t/a/work"); c != 200 {
			t.Fatalf("request %d: %d", i, c)
		}
	}
	if b1.hits.Load() != 20 || b2.hits.Load() != 20 {
		t.Fatalf("not even per request: %d / %d", b1.hits.Load(), b2.hits.Load())
	}
	m := metricsOf(t, srv)
	for _, want := range []string{`c28_ingress_routing{mode="endpoints-rr"} 1`,
		`c28_ingress_dispatched_total{target="a",pod="pod-1"} 20`, `c28_ingress_dispatched_total{target="a",pod="pod-2"} 20`,
		`c28_ingress_requests_total{target="a"} 40`, `c28_ingress_responses_total{target="a",code="200"} 40`,
		`c28_ingress_endpoints_ready{target="a"} 2`, `c28_ingress_endpoints_generation{target="a"} 1`} {
		if !strings.Contains(m, want) {
			t.Fatalf("metrics lack %q:\n%s", want, m)
		}
	}
}

func TestChurnSwapsTheSetAndRemovedEndpointsGetNothingAfterTheSwap(t *testing.T) {
	b1, b2, b3 := newBackend(t, nil), newBackend(t, nil), newBackend(t, nil)
	r := testRouter(t, time.Now, "a")
	mustReplace(t, r, []endpointSlice{b1.slice(t, "s1", "a", "p1"), b2.slice(t, "s2", "a", "p2")})
	r.markSynced()
	_, srv := rrIngress(t, r, 2*time.Second)
	const workers = 8
	stop := make(chan struct{})
	var removed atomic.Bool
	var bad atomic.Int64
	var wg, drained sync.WaitGroup // drained: every worker finished a request it started after the removal
	drained.Add(workers)
	for g := 0; g < workers; g++ { // load runs throughout the churn; one request in flight per worker
		wg.Add(1)
		go func() {
			defer wg.Done()
			signalled := false
			for {
				select {
				case <-stop:
					return
				default:
				}
				after := removed.Load()
				resp, err := http.Get(srv.URL + "/t/a/work")
				if err != nil {
					bad.Add(1)
				} else {
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					if resp.StatusCode != 200 {
						bad.Add(1)
					}
				}
				if after && !signalled {
					signalled = true
					drained.Done()
				}
			}
		}()
	}
	time.Sleep(150 * time.Millisecond)
	if err := r.setSlice(b3.slice(t, "s3", "a", "p3")); err != nil { // add under load
		t.Error(err)
	}
	time.Sleep(150 * time.Millisecond)
	r.deleteSlice("s1") // remove under load: p1 leaves the set
	removed.Store(true)
	// once every worker completed a request started after the swap, no request that picked p1 is still in flight
	drained.Wait()
	after := b1.hits.Load()
	time.Sleep(300 * time.Millisecond)
	close(stop)
	wg.Wait()
	if b1.hits.Load() != after {
		t.Fatalf("a removed endpoint received %d requests after the swap", b1.hits.Load()-after)
	}
	if b3.hits.Load() == 0 || b2.hits.Load() == 0 || bad.Load() != 0 {
		t.Fatalf("hits p2 %d p3 %d, failures %d", b2.hits.Load(), b3.hits.Load(), bad.Load())
	}
	if got := strings.Join(keys(r, "a"), ","); got != strings.Join(sortedKeys(b2, b3), ",") {
		t.Fatalf("set after churn: %s", got)
	}
	if gen := r.targets["a"].set.Load().gen; gen != 3 {
		t.Fatalf("generation %d, want 3 (initial, add, remove)", gen)
	}
	// the removed endpoint's idle keep-alive connections were closed at the swap
	deadline := time.Now().Add(2 * time.Second)
	for b1.conns.Load() != 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := b1.conns.Load(); n != 0 {
		t.Fatalf("%d connections to the removed endpoint stay open", n)
	}
}

func sortedKeys(bs ...*backend) []string {
	var out []string
	for _, b := range bs {
		out = append(out, net.JoinHostPort(b.ip, strconv.Itoa(b.port)))
	}
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

func TestNoReadyEndpointIsACounted503AndNotReadyBeforeTheFirstSync(t *testing.T) {
	r := testRouter(t, time.Now, "a")
	_, srv := rrIngress(t, r, time.Second)
	if c := get(t, srv.URL+"/ready"); c != http.StatusServiceUnavailable {
		t.Fatalf("ready before the first sync: %d", c)
	}
	if c := get(t, srv.URL+"/t/a/work"); c != http.StatusServiceUnavailable {
		t.Fatalf("no endpoint: %d", c)
	}
	mustReplace(t, r, nil)
	r.markSynced()
	if c := get(t, srv.URL+"/ready"); c != 200 {
		t.Fatalf("ready after the first sync: %d", c)
	}
	if c := get(t, srv.URL+"/t/a/work"); c != http.StatusServiceUnavailable {
		t.Fatalf("synced, still no endpoint: %d", c)
	}
	m := metricsOf(t, srv)
	for _, want := range []string{`c28_ingress_requests_total{target="a"} 2`,
		`c28_ingress_responses_total{target="a",code="503"} 2`, `c28_ingress_no_ready_endpoints_total{target="a"} 2`,
		`c28_ingress_duration_seconds_count{target="a",outcome="failure"} 2`, `c28_ingress_endpoints_ready{target="a"} 0`} {
		if !strings.Contains(m, want) {
			t.Fatalf("metrics lack %q:\n%s", want, m)
		}
	}
	if strings.Contains(m, "c28_ingress_dispatched_total{") {
		t.Fatal("nothing was dispatched")
	}
}

// A pod that reads a request on a kept-alive connection and hangs up without answering: net/http would silently
// re-send an idempotent GET (errServerClosedIdle); the ingress must not. Any re-send would reach the third handler
// call, which answers 200.
func TestNoRetriesEvenOnAKeptAliveConnection(t *testing.T) {
	var calls atomic.Int64
	b := newBackend(t, func(_ *backend, w http.ResponseWriter, r *http.Request) {
		switch calls.Add(1) {
		case 2:
			c, buf, err := w.(http.Hijacker).Hijack()
			if err == nil {
				_ = buf.Flush()
				_ = c.Close()
			}
		default:
			_, _ = io.WriteString(w, "ok")
		}
	})
	other := newBackend(t, nil)
	r := testRouter(t, time.Now, "a", "b")
	mustReplace(t, r, []endpointSlice{b.slice(t, "a-1", "a", "p"), other.slice(t, "b-1", "b", "q")})
	r.markSynced()
	_, srv := rrIngress(t, r, 2*time.Second)
	if c := get(t, srv.URL+"/t/a/work"); c != 200 { // opens the kept-alive connection
		t.Fatalf("first: %d", c)
	}
	if c := get(t, srv.URL+"/t/a/work"); c != http.StatusBadGateway {
		t.Fatalf("a hung-up request is a 502, not a retried success: %d", c)
	}
	if n := calls.Load(); n != 2 {
		t.Fatalf("the pod saw %d requests for 2 arrivals (a retry)", n)
	}
	if other.hits.Load() != 0 {
		t.Fatal("a failed request went to another endpoint")
	}
}

func TestATimedOutPodIsA504AsBefore(t *testing.T) {
	b := newBackend(t, func(_ *backend, w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(time.Second):
		case <-r.Context().Done():
		}
	})
	r := testRouter(t, time.Now, "a")
	mustReplace(t, r, []endpointSlice{b.slice(t, "a-1", "a", "p")})
	r.markSynced()
	_, srv := rrIngress(t, r, 100*time.Millisecond)
	if c := get(t, srv.URL+"/t/a/work"); c != http.StatusGatewayTimeout {
		t.Fatalf("%d", c)
	}
	if b.hits.Load() != 1 {
		t.Fatalf("hits %d", b.hits.Load())
	}
}

func TestStaleDiscoveryIsExportedAndTheLastSetIsKept(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	set := func(t time.Time) { mu.Lock(); now = t; mu.Unlock() }
	r := testRouter(t, clock, "a")
	m := func() string { rec := httptest.NewRecorder(); r.writeMetrics(rec); return rec.Body.String() }
	if s := m(); !strings.Contains(s, "c28_ingress_discovery_stale 1") || !strings.Contains(s, "c28_ingress_discovery_synced 0") ||
		strings.Contains(s, "sync_age") {
		t.Fatalf("never synced is stale:\n%s", s)
	}
	mustReplace(t, r, []endpointSlice{decodeSlice(t, sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p")))})
	r.markSynced()
	set(now.Add(90 * time.Second))
	s := m()
	for _, want := range []string{"c28_ingress_discovery_stale 0", "c28_ingress_discovery_synced 1",
		"c28_ingress_discovery_sync_age_seconds 90.000", "c28_ingress_discovery_last_sync_timestamp_seconds 1794225600.000",
		"c28_ingress_discovery_stale_bound_seconds 90"} {
		if !strings.Contains(s, want) {
			t.Fatalf("lacks %q:\n%s", want, s)
		}
	}
	set(now.Add(time.Second)) // beyond the bound
	if s := m(); !strings.Contains(s, "c28_ingress_discovery_stale 1") {
		t.Fatalf("stale beyond the bound:\n%s", s)
	}
	if len(keys(r, "a")) != 1 {
		t.Fatal("the last known set is kept while stale")
	}
}

func TestRetiredDispatchSeriesArePrunedAfterTheKeepTime(t *testing.T) {
	var mu sync.Mutex
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	r := testRouter(t, clock, "a")
	mustReplace(t, r, []endpointSlice{decodeSlice(t, sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1")))})
	r.dispatched.inc(dispatchLabel("a", "p1"))
	mustReplace(t, r, nil) // p1 leaves
	m := func() string { rec := httptest.NewRecorder(); r.writeMetrics(rec); return rec.Body.String() }
	if !strings.Contains(m(), `pod="p1"} 1`) {
		t.Fatal("kept right after removal")
	}
	mu.Lock()
	now = now.Add(retiredSeriesKeep)
	mu.Unlock()
	if strings.Contains(m(), `pod="p1"`) {
		t.Fatal("pruned after the keep time")
	}
}

// fakeAPI serves the EndpointSlice LIST and WATCH like the API server (plain HTTP; the TLS client is inClusterAPI's).
type fakeAPI struct {
	mu      sync.Mutex
	list    []map[string]any
	events  []string // pre-encoded watch events for the next watch
	status  int      // non-200 answers every request
	body    []byte   // when set, the raw LIST answer (HTTP 200)
	queries []string
	auth    []string
}

func (f *fakeAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.queries = append(f.queries, r.URL.Path+"?"+r.URL.RawQuery)
	f.auth = append(f.auth, r.Header.Get("Authorization"))
	if f.status != 0 {
		http.Error(w, `{"kind":"Status","code":403}`, f.status)
		return
	}
	if r.URL.Path != "/apis/discovery.k8s.io/v1/namespaces/demo/endpointslices" {
		http.NotFound(w, r)
		return
	}
	if r.URL.Query().Get("watch") == "true" {
		bw := bufio.NewWriter(w)
		for _, e := range f.events {
			_, _ = bw.WriteString(e + "\n")
		}
		_ = bw.Flush()
		f.events = nil
		return // the stream ends: the client re-lists
	}
	if f.body != nil {
		_, _ = w.Write(f.body)
		return
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"kind": "EndpointSliceList",
		"metadata": map[string]any{"resourceVersion": "100"}, "items": f.list})
}

func event(typ string, obj map[string]any) string {
	raw, _ := json.Marshal(map[string]any{"type": typ, "object": obj})
	return string(raw)
}

func TestListAndWatchAgainstTheAPI(t *testing.T) {
	f := &fakeAPI{list: []map[string]any{sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"), ready("10.0.0.2", "p2"))}}
	notReady := sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"), epSpec{ip: "10.0.0.2", pod: "p2", ready: no})
	f.events = []string{event("BOOKMARK", map[string]any{"metadata": map[string]any{"resourceVersion": "101"}}),
		event("MODIFIED", notReady), event("ADDED", sliceJSON("a-2", "a", 8080, ready("10.0.0.3", "p3")))}
	srv := httptest.NewServer(f)
	defer srv.Close()
	api := &kubeAPI{base: srv.URL, client: srv.Client(), token: func() (string, error) { return "test-token", nil }}
	r := testRouter(t, time.Now, "a", "b")
	rv, err := r.list(context.Background(), api)
	if err != nil || rv != "100" || strings.Join(keys(r, "a"), ",") != "10.0.0.1:8080,10.0.0.2:8080" || !r.synced() {
		t.Fatalf("list: %q %v %v", rv, err, keys(r, "a"))
	}
	if err := r.watch(context.Background(), api, rv, 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if got := strings.Join(keys(r, "a"), ","); got != "10.0.0.1:8080,10.0.0.3:8080" {
		t.Fatalf("after the watch: %s", got)
	}
	f.events = []string{event("DELETED", sliceJSON("a-2", "a", 8080))}
	if err := r.watch(context.Background(), api, rv, 30*time.Second); err != nil || len(keys(r, "a")) != 1 {
		t.Fatalf("delete: %v %v", err, keys(r, "a"))
	}
	f.events = []string{`{"type":"ERROR","object":{"kind":"Status","code":410}}`}
	if err := r.watch(context.Background(), api, rv, 30*time.Second); err == nil {
		t.Fatal("an ERROR event ends the watch with an error (the loop re-lists)")
	}
	f.mu.Lock()
	sel := "labelSelector=kubernetes.io%2Fservice-name+in+%28a%2Cb%29%2C" +
		"endpointslice.kubernetes.io%2Fmanaged-by%3Dendpointslice-controller.k8s.io"
	if !strings.Contains(f.queries[0], sel) || !strings.Contains(f.queries[1], "watch=true") ||
		!strings.Contains(f.queries[1], "resourceVersion=100") || !strings.Contains(f.queries[1], "timeoutSeconds=30") {
		t.Fatalf("queries: %v", f.queries)
	}
	for _, a := range f.auth {
		if a != "Bearer test-token" {
			t.Fatalf("authorization %q", a)
		}
	}
	f.mu.Unlock()

	// a failing API: the run loop counts the errors and keeps the last known set
	f.mu.Lock()
	f.status = http.StatusForbidden
	f.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 1500*time.Millisecond)
	defer cancel()
	r.run(ctx, api, time.Second)
	rec := httptest.NewRecorder()
	r.writeMetrics(rec)
	if !strings.Contains(rec.Body.String(), `c28_ingress_discovery_errors_total{stage="list"} `) ||
		strings.Contains(rec.Body.String(), `c28_ingress_discovery_errors_total{stage="list"} 0`) {
		t.Fatalf("list errors are counted:\n%s", rec.Body.String())
	}
	if len(keys(r, "a")) != 1 {
		t.Fatal("the last known set is kept through failed lists")
	}
}

func TestMalformedAnswersFromTheAPINeverCountAsASync(t *testing.T) {
	f := &fakeAPI{body: []byte(`{}`)}
	srv := httptest.NewServer(f)
	defer srv.Close()
	api := &kubeAPI{base: srv.URL, client: srv.Client(), token: func() (string, error) { return "t", nil }}
	m := func(r *rrRouter) string { rec := httptest.NewRecorder(); r.writeMetrics(rec); return rec.Body.String() }

	// a fresh ingress: HTTP 200 {} is not a sync (not ready, stale, the error counted)
	fresh := testRouter(t, time.Now, "a")
	ctx, cancel := context.WithTimeout(context.Background(), 1200*time.Millisecond)
	fresh.run(ctx, api, time.Second)
	cancel()
	if fresh.synced() || !strings.Contains(m(fresh), "c28_ingress_discovery_stale 1") ||
		strings.Contains(m(fresh), `c28_ingress_discovery_errors_total{stage="list"} 0`) {
		t.Fatalf("{} counted as a sync:\n%s", m(fresh))
	}

	// a synced ingress: a malformed or partly invalid LIST changes nothing (snapshot, lastSync), and is an error
	f.mu.Lock()
	f.body = nil
	f.list = []map[string]any{sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"))}
	f.mu.Unlock()
	r := testRouter(t, time.Now, "a")
	if _, err := r.list(context.Background(), api); err != nil {
		t.Fatal(err)
	}
	synced := r.lastSync.Load()
	bad := sliceJSON("a-2", "a", 8080, ready("10.0.0.2", "p2"))
	bad["ports"] = []any{map[string]any{"name": "metrics", "port": 9090}}
	for name, body := range map[string][]byte{
		"empty object":       []byte(`{}`),
		"items not an array": []byte(`{"metadata":{"resourceVersion":"7"},"items":"x"}`),
		"no resourceVersion": []byte(`{"metadata":{},"items":[]}`),
		"one invalid slice": func() []byte {
			b, _ := json.Marshal(map[string]any{"metadata": map[string]any{"resourceVersion": "7"},
				"items": []any{sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"), ready("10.0.0.3", "p3")), bad}})
			return b
		}(),
	} {
		f.mu.Lock()
		f.body = body
		f.mu.Unlock()
		time.Sleep(2 * time.Millisecond)
		if _, err := r.list(context.Background(), api); err == nil {
			t.Errorf("%s: accepted", name)
		}
		if r.lastSync.Load() != synced || strings.Join(keys(r, "a"), ",") != "10.0.0.1:8080" {
			t.Fatalf("%s: installed or counted as a sync", name)
		}
	}
	f.mu.Lock()
	f.body = []byte(`{}`)
	f.mu.Unlock()
	ctx, cancel = context.WithTimeout(context.Background(), 1200*time.Millisecond)
	r.run(ctx, api, time.Second)
	cancel()
	if !strings.Contains(m(r), `c28_ingress_discovery_errors_total{stage="list"} `) ||
		strings.Contains(m(r), `c28_ingress_discovery_errors_total{stage="list"} 0`) || r.lastSync.Load() != synced {
		t.Fatalf("the loop counts the malformed lists and keeps lastSync:\n%s", m(r))
	}
}

func TestAMalformedWatchEventIsNotAppliedAndEndsTheWatch(t *testing.T) {
	f := &fakeAPI{list: []map[string]any{sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"))}}
	srv := httptest.NewServer(f)
	defer srv.Close()
	api := &kubeAPI{base: srv.URL, client: srv.Client(), token: func() (string, error) { return "t", nil }}
	r := testRouter(t, time.Now, "a")
	rv, err := r.list(context.Background(), api)
	if err != nil {
		t.Fatal(err)
	}
	noPort := sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"), ready("10.0.0.2", "p2"))
	noPort["ports"] = nil
	for name, ev := range map[string]string{
		"invalid slice":         event("MODIFIED", noPort),
		"outside the selector":  event("ADDED", sliceJSON("x-1", "x", 8080, ready("10.9.0.1", "x"))),
		"object not an object":  `{"type":"ADDED","object":"x"}`,
		"no object":             `{"type":"MODIFIED"}`,
		"wrong kind":            `{"type":"ADDED","object":{"kind":"Pod","metadata":{"name":"a-1"}}}`,
		"bookmark without rv":   `{"type":"BOOKMARK","object":{"metadata":{}}}`,
		"delete without a name": `{"type":"DELETED","object":{"metadata":{}}}`,
		"not JSON":              `{"type":`,
	} {
		f.mu.Lock()
		f.events = []string{event("BOOKMARK", map[string]any{"metadata": map[string]any{"resourceVersion": "9"}}), ev,
			event("ADDED", sliceJSON("a-2", "a", 8080, ready("10.0.0.5", "p5")))} // never reached
		f.mu.Unlock()
		if err := r.watch(context.Background(), api, rv, 30*time.Second); err == nil {
			t.Errorf("%s: the watch must end with an error", name)
		}
		if got := strings.Join(keys(r, "a"), ","); got != "10.0.0.1:8080" {
			t.Fatalf("%s: applied: %s", name, got)
		}
	}
}

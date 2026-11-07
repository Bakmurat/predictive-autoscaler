// Command c28-fixture is the C2.8 evaluation fixture (EVAL-C28-MANIFEST sections 3 and 11.3): one binary, three modes.
//
//	MODE=server     the HTTP workload: one request = one fixed amount of CPU work and a fixed response size, bounded
//	                concurrency and queue (overflow is rejected, never queued without bound), no cache, no external
//	                dependency; /ready becomes ready only after the startup delay; /healthz; /metrics.
//	MODE=ingress    the independent ingress before the targets: counts every arrival per target (offered demand) before
//	                forwarding, so a target that rejects or drops work cannot hide demand; 2 s timeout, no retries.
//	MODE=tcpproxy   an evaluation-owned TCP route (the metrics query route or the forecast API route) that refuses and
//	                cuts connections during scheduled outage windows; TLS passes through untouched.
//
// Faults are scheduled, not toggled: a JSON file (a ConfigMap written before the block) lists absolute UTC windows, and
// every decision uses the process clock, so ConfigMap propagation delay never moves a fault boundary. Updating the file
// does not change the pod template (no trust reset is caused by the fault mechanism itself).
//
// Standard library only.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// latencyBuckets are the histogram bounds in seconds (the 2 s timeout and the SLO bounds 0.2/0.5 s are bucket edges).
var latencyBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.75, 1, 1.5, 2, 2.5, 5}

// --- fault schedule ---------------------------------------------------------------------------------------------

// window is one scheduled fault interval [Start, End).
type window struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
	// ExtraStartupSeconds applies to slow-start windows: a server process started inside the window becomes ready this
	// much later than its base delay.
	ExtraStartupSeconds int `json:"extra_startup_seconds,omitempty"`
}

// schedule is the fault file: outage windows for a tcpproxy, slow-start windows for a server.
type schedule struct {
	Outages    []window `json:"outages,omitempty"`
	SlowStarts []window `json:"slow_starts,omitempty"`
}

func loadSchedule(path string) (schedule, error) {
	var s schedule
	if path == "" {
		return s, nil
	}
	raw, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return s, nil // no faults scheduled
	}
	if err != nil {
		return s, err
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&s); err != nil {
		return s, fmt.Errorf("fault schedule %s: %w", path, err)
	}
	for _, w := range append(append([]window{}, s.Outages...), s.SlowStarts...) {
		if !w.End.After(w.Start) || w.ExtraStartupSeconds < 0 {
			return s, fmt.Errorf("fault schedule %s: invalid window %v..%v", path, w.Start, w.End)
		}
	}
	return s, nil
}

func inWindow(ws []window, t time.Time) (window, bool) {
	for _, w := range ws {
		if !t.Before(w.Start) && t.Before(w.End) {
			return w, true
		}
	}
	return window{}, false
}

// cachedSchedule re-reads the fault file at most every refresh (a ConfigMap update arrives without a restart).
type cachedSchedule struct {
	path    string
	refresh time.Duration
	mu      sync.Mutex
	at      time.Time
	s       schedule
	err     error
}

func (c *cachedSchedule) get(now time.Time) (schedule, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.at.IsZero() || now.Sub(c.at) >= c.refresh {
		c.s, c.err = loadSchedule(c.path)
		c.at = now
	}
	return c.s, c.err
}

// --- metrics ----------------------------------------------------------------------------------------------------

type histogram struct {
	mu     sync.Mutex
	counts []uint64 // per bucket, cumulative computed on output
	sum    float64
	n      uint64
}

func newHistogram() *histogram { return &histogram{counts: make([]uint64, len(latencyBuckets))} }

func (h *histogram) observe(sec float64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, b := range latencyBuckets {
		if sec <= b {
			h.counts[i]++
			break
		}
	}
	h.sum += sec
	h.n++
}

func (h *histogram) write(w io.Writer, name, labels string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	cum := uint64(0)
	sep := ""
	if labels != "" {
		sep = ","
	}
	for i, b := range latencyBuckets {
		cum += h.counts[i]
		fmt.Fprintf(w, "%s_bucket{%s%sle=%q} %d\n", name, labels, sep, strconv.FormatFloat(b, 'g', -1, 64), cum)
	}
	fmt.Fprintf(w, "%s_bucket{%s%sle=\"+Inf\"} %d\n", name, labels, sep, h.n)
	fmt.Fprintf(w, "%s_sum{%s} %s\n", name, labels, strconv.FormatFloat(h.sum, 'g', -1, 64))
	fmt.Fprintf(w, "%s_count{%s} %d\n", name, labels, h.n)
}

// counterVec is a counter keyed by a label string (already formatted, e.g. `target="a",code="200"`).
type counterVec struct {
	mu sync.Mutex
	m  map[string]uint64
}

func (c *counterVec) inc(labels string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.m == nil {
		c.m = map[string]uint64{}
	}
	c.m[labels]++
}

func (c *counterVec) write(w io.Writer, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	keys := make([]string, 0, len(c.m))
	for k := range c.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "%s{%s} %d\n", name, k, c.m[k])
	}
}

func label(k, v string) string { return fmt.Sprintf("%s=%q", k, v) }

// --- server mode ------------------------------------------------------------------------------------------------

type serverConfig struct {
	Target         string
	WorkIterations int
	ResponseBytes  int
	MaxInflight    int
	MaxQueue       int
	QueueTimeout   time.Duration
	StartupDelay   time.Duration
	FaultFile      string
}

type server struct {
	cfg       serverConfig
	start     time.Time
	readyAt   time.Time
	extra     time.Duration
	now       func() time.Time
	slots     chan struct{}
	waiting   atomic.Int64
	inflight  atomic.Int64
	requests  counterVec
	rejected  counterVec
	latency   *histogram
	body      []byte
	readyOnce sync.Once
	readyTime atomic.Int64 // unix nanos of the first ready answer (0 before)
}

func newServer(cfg serverConfig, now func() time.Time) (*server, error) {
	if cfg.WorkIterations < 0 || cfg.ResponseBytes < 0 || cfg.MaxInflight < 1 || cfg.MaxQueue < 0 {
		return nil, fmt.Errorf("invalid server configuration %+v", cfg)
	}
	s := &server{cfg: cfg, now: now, start: now(), slots: make(chan struct{}, cfg.MaxInflight), latency: newHistogram(),
		body: make([]byte, cfg.ResponseBytes)}
	for i := range s.body {
		s.body[i] = 'x'
	}
	sch, err := loadSchedule(cfg.FaultFile)
	if err != nil {
		return nil, err
	}
	// The slow-start fault: decided once, from the process start time, for the life of the process.
	if w, ok := inWindow(sch.SlowStarts, s.start); ok {
		s.extra = time.Duration(w.ExtraStartupSeconds) * time.Second
	}
	s.readyAt = s.start.Add(cfg.StartupDelay + s.extra)
	return s, nil
}

// work performs the fixed CPU work: WorkIterations chained SHA-256 rounds over 64 bytes. Deterministic.
func work(iterations int) [32]byte {
	var buf [64]byte
	sum := sha256.Sum256(buf[:])
	for i := 0; i < iterations; i++ {
		copy(buf[:32], sum[:])
		sum = sha256.Sum256(buf[:])
	}
	return sum
}

func (s *server) handleWork(w http.ResponseWriter, r *http.Request) {
	t0 := s.now()
	code := http.StatusOK
	defer func() {
		s.requests.inc(label("code", strconv.Itoa(code)))
		s.latency.observe(s.now().Sub(t0).Seconds())
	}()
	select {
	case s.slots <- struct{}{}:
	default:
		if s.waiting.Add(1) > int64(s.cfg.MaxQueue) {
			s.waiting.Add(-1)
			code = http.StatusServiceUnavailable
			s.rejected.inc(label("reason", "queue_full"))
			http.Error(w, "queue full", code)
			return
		}
		timer := time.NewTimer(s.cfg.QueueTimeout)
		select {
		case s.slots <- struct{}{}:
			timer.Stop()
			s.waiting.Add(-1)
		case <-timer.C:
			s.waiting.Add(-1)
			code = http.StatusServiceUnavailable
			s.rejected.inc(label("reason", "queue_timeout"))
			http.Error(w, "queue timeout", code)
			return
		case <-r.Context().Done():
			timer.Stop()
			s.waiting.Add(-1)
			code = 499
			s.rejected.inc(label("reason", "client_gone"))
			return
		}
	}
	s.inflight.Add(1)
	_ = work(s.cfg.WorkIterations)
	s.inflight.Add(-1)
	<-s.slots
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Length", strconv.Itoa(len(s.body)))
	_, _ = w.Write(s.body)
}

func (s *server) ready() bool { return !s.now().Before(s.readyAt) }

func (s *server) handleReady(w http.ResponseWriter, _ *http.Request) {
	if !s.ready() {
		http.Error(w, "starting", http.StatusServiceUnavailable)
		return
	}
	s.readyOnce.Do(func() { s.readyTime.Store(s.now().UnixNano()) })
	_, _ = io.WriteString(w, "ready\n")
}

func (s *server) handleMetrics(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	t := label("target", s.cfg.Target)
	fmt.Fprintln(w, "# TYPE c28_requests_total counter")
	s.requests.mu.Lock()
	keys := make([]string, 0, len(s.requests.m))
	for k := range s.requests.m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(w, "c28_requests_total{%s,%s} %d\n", t, k, s.requests.m[k])
	}
	s.requests.mu.Unlock()
	fmt.Fprintln(w, "# TYPE c28_rejected_total counter")
	s.rejected.mu.Lock()
	for k, v := range s.rejected.m {
		fmt.Fprintf(w, "c28_rejected_total{%s,%s} %d\n", t, k, v)
	}
	s.rejected.mu.Unlock()
	fmt.Fprintln(w, "# TYPE c28_request_duration_seconds histogram")
	s.latency.write(w, "c28_request_duration_seconds", t)
	fmt.Fprintf(w, "# TYPE c28_inflight gauge\nc28_inflight{%s} %d\n", t, s.inflight.Load())
	fmt.Fprintf(w, "# TYPE c28_queue gauge\nc28_queue{%s} %d\n", t, s.waiting.Load())
	fmt.Fprintf(w, "# TYPE c28_ready gauge\nc28_ready{%s} %d\n", t, map[bool]int{true: 1, false: 0}[s.ready()])
	fmt.Fprintf(w, "# TYPE c28_startup_delay_seconds gauge\nc28_startup_delay_seconds{%s,kind=\"configured\"} %g\n", t, s.cfg.StartupDelay.Seconds())
	fmt.Fprintf(w, "c28_startup_delay_seconds{%s,kind=\"slow_start_extra\"} %g\n", t, s.extra.Seconds())
	fmt.Fprintf(w, "# TYPE c28_process_start_time_seconds gauge\nc28_process_start_time_seconds{%s} %d\n", t, s.start.Unix())
	if rt := s.readyTime.Load(); rt > 0 {
		fmt.Fprintf(w, "# TYPE c28_first_ready_time_seconds gauge\nc28_first_ready_time_seconds{%s} %.3f\n", t, float64(rt)/1e9)
	}
}

func (s *server) mux() *http.ServeMux {
	m := http.NewServeMux()
	m.HandleFunc("/work", s.handleWork)
	m.HandleFunc("/ready", s.handleReady)
	m.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
	m.HandleFunc("/metrics", s.handleMetrics)
	return m
}

// --- ingress mode -----------------------------------------------------------------------------------------------

type ingress struct {
	upstreams map[string]string // target -> base URL
	client    *http.Client
	now       func() time.Time
	arrivals  counterVec
	responses counterVec
	mu        sync.Mutex
	latency   map[string]*histogram // by "target|outcome" (outcome success or failure: the SLO uses successful latency)
}

// parseUpstreams reads "target=http://host:port,target2=..."; targets are [a-z0-9-]+.
func parseUpstreams(spec string) (map[string]string, error) {
	out := map[string]string{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		k, v, ok := strings.Cut(part, "=")
		if !ok || k == "" || strings.Trim(k, "abcdefghijklmnopqrstuvwxyz0123456789-") != "" || !strings.HasPrefix(v, "http://") {
			return nil, fmt.Errorf("invalid upstream %q", part)
		}
		out[k] = strings.TrimRight(v, "/")
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no upstreams")
	}
	return out, nil
}

func newIngress(upstreams map[string]string, timeout time.Duration, now func() time.Time) *ingress {
	tr := &http.Transport{MaxIdleConnsPerHost: 1024, IdleConnTimeout: 90 * time.Second}
	return &ingress{upstreams: upstreams, now: now, latency: map[string]*histogram{},
		client: &http.Client{Timeout: timeout, Transport: tr,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}}
}

func (g *ingress) hist(target string) *histogram {
	g.mu.Lock()
	defer g.mu.Unlock()
	h := g.latency[target]
	if h == nil {
		h = newHistogram()
		g.latency[target] = h
	}
	return h
}

// ServeHTTP routes /t/<target>/<path> to the target; the arrival is counted before anything can fail.
func (g *ingress) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/metrics" {
		g.metrics(w)
		return
	}
	if r.URL.Path == "/healthz" || r.URL.Path == "/ready" {
		_, _ = io.WriteString(w, "ok\n")
		return
	}
	rest, ok := strings.CutPrefix(r.URL.Path, "/t/")
	target, path, _ := strings.Cut(rest, "/")
	base, known := g.upstreams[target]
	if !ok || !known {
		http.NotFound(w, r)
		return
	}
	t0 := g.now()
	g.arrivals.inc(label("target", target))
	code := 0
	defer func() {
		g.responses.inc(label("target", target) + "," + label("code", strconv.Itoa(code)))
		outcome := "success"
		if code < 200 || code >= 400 {
			outcome = "failure"
		}
		g.hist(target + "|" + outcome).observe(g.now().Sub(t0).Seconds())
	}()
	req, err := http.NewRequestWithContext(r.Context(), r.Method, base+"/"+path, nil)
	if err != nil {
		code = http.StatusBadGateway
		http.Error(w, err.Error(), code)
		return
	}
	resp, err := g.client.Do(req)
	if err != nil {
		code = http.StatusBadGateway
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() || errors.Is(err, context.DeadlineExceeded) {
			code = http.StatusGatewayTimeout
		}
		http.Error(w, "upstream: "+err.Error(), code)
		return
	}
	defer resp.Body.Close()
	code = resp.StatusCode
	w.WriteHeader(code)
	if _, err := io.Copy(w, resp.Body); err != nil {
		code = http.StatusBadGateway // a truncated answer is a failure, not a success
	}
}

func (g *ingress) metrics(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# TYPE c28_ingress_requests_total counter")
	g.arrivals.write(w, "c28_ingress_requests_total")
	fmt.Fprintln(w, "# TYPE c28_ingress_responses_total counter")
	g.responses.write(w, "c28_ingress_responses_total")
	fmt.Fprintln(w, "# TYPE c28_ingress_duration_seconds histogram")
	g.mu.Lock()
	targets := make([]string, 0, len(g.latency))
	for t := range g.latency {
		targets = append(targets, t)
	}
	g.mu.Unlock()
	sort.Strings(targets)
	for _, key := range targets {
		t, outcome, _ := strings.Cut(key, "|")
		g.hist(key).write(w, "c28_ingress_duration_seconds", label("target", t)+","+label("outcome", outcome))
	}
}

// --- tcpproxy mode ----------------------------------------------------------------------------------------------

type tcpProxy struct {
	upstream string
	faults   *cachedSchedule
	now      func() time.Time
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	refused  atomic.Int64
	cut      atomic.Int64
	accepted atomic.Int64
}

func (p *tcpProxy) outage() bool {
	s, err := p.faults.get(p.now())
	if err != nil {
		log.Printf("fault schedule unreadable, failing safe to no outage: %v", err)
		return false
	}
	_, in := inWindow(s.Outages, p.now())
	return in
}

func (p *tcpProxy) track(c net.Conn, add bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if add {
		p.conns[c] = struct{}{}
	} else {
		delete(p.conns, c)
	}
}

// cutAll closes every proxied connection (at the start of an outage, kept-alive connections must not survive it).
func (p *tcpProxy) cutAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for c := range p.conns {
		_ = c.Close()
		p.cut.Add(1)
	}
}

func (p *tcpProxy) serve(l net.Listener) error {
	go func() {
		was := false
		for range time.Tick(250 * time.Millisecond) {
			now := p.outage()
			if now && !was {
				p.cutAll()
			}
			was = now
		}
	}()
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		if p.outage() {
			p.refused.Add(1)
			_ = c.Close()
			continue
		}
		p.accepted.Add(1)
		go p.pipe(c)
	}
}

func (p *tcpProxy) pipe(c net.Conn) {
	up, err := net.DialTimeout("tcp", p.upstream, 5*time.Second)
	if err != nil {
		_ = c.Close()
		return
	}
	p.track(c, true)
	p.track(up, true)
	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, c); done <- struct{}{} }()
	go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
	<-done
	_ = c.Close()
	_ = up.Close()
	p.track(c, false)
	p.track(up, false)
}

func (p *tcpProxy) metrics(w http.ResponseWriter, _ *http.Request) {
	fmt.Fprintf(w, "# TYPE c28_proxy_connections_total counter\n")
	fmt.Fprintf(w, "c28_proxy_connections_total{result=\"accepted\"} %d\n", p.accepted.Load())
	fmt.Fprintf(w, "c28_proxy_connections_total{result=\"refused\"} %d\n", p.refused.Load())
	fmt.Fprintf(w, "c28_proxy_connections_total{result=\"cut\"} %d\n", p.cut.Load())
	fmt.Fprintf(w, "# TYPE c28_proxy_outage gauge\nc28_proxy_outage %d\n", map[bool]int{true: 1, false: 0}[p.outage()])
}

// --- main -------------------------------------------------------------------------------------------------------

func envInt(name string, def int) int {
	if v := os.Getenv(name); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			log.Fatalf("%s: %v", name, err)
		}
		return n
	}
	return def
}

func envSeconds(name string, def float64) time.Duration {
	if v := os.Getenv(name); v != "" {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil || f < 0 || math.IsInf(f, 0) {
			log.Fatalf("%s: invalid seconds %q", name, v)
		}
		return time.Duration(f * float64(time.Second))
	}
	return time.Duration(def * float64(time.Second))
}

func main() {
	addr := ":" + strconv.Itoa(envInt("PORT", 8080))
	switch mode := os.Getenv("MODE"); mode {
	case "server", "":
		s, err := newServer(serverConfig{Target: os.Getenv("TARGET"), WorkIterations: envInt("WORK_ITERATIONS", 20000),
			ResponseBytes: envInt("RESPONSE_BYTES", 512), MaxInflight: envInt("MAX_INFLIGHT", 4),
			MaxQueue: envInt("MAX_QUEUE", 64), QueueTimeout: envSeconds("QUEUE_TIMEOUT_SECONDS", 1.5),
			StartupDelay: envSeconds("STARTUP_DELAY_SECONDS", 20), FaultFile: os.Getenv("FAULT_FILE")}, time.Now)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("server %s: ready at %s (extra slow-start %s)", s.cfg.Target, s.readyAt.UTC().Format(time.RFC3339), s.extra)
		log.Fatal((&http.Server{Addr: addr, Handler: s.mux(), ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
	case "ingress":
		ups, err := parseUpstreams(os.Getenv("UPSTREAMS"))
		if err != nil {
			log.Fatal(err)
		}
		g := newIngress(ups, envSeconds("TIMEOUT_SECONDS", 2), time.Now)
		log.Fatal((&http.Server{Addr: addr, Handler: g, ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
	case "tcpproxy":
		p := &tcpProxy{upstream: os.Getenv("UPSTREAM"), now: time.Now, conns: map[net.Conn]struct{}{},
			faults: &cachedSchedule{path: os.Getenv("FAULT_FILE"), refresh: 5 * time.Second}}
		if p.upstream == "" {
			log.Fatal("UPSTREAM is required")
		}
		go func() {
			m := http.NewServeMux()
			m.HandleFunc("/metrics", p.metrics)
			m.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, "ok\n") })
			log.Fatal((&http.Server{Addr: ":" + strconv.Itoa(envInt("METRICS_PORT", 9090)), Handler: m,
				ReadHeaderTimeout: 5 * time.Second}).ListenAndServe())
		}()
		l, err := net.Listen("tcp", addr)
		if err != nil {
			log.Fatal(err)
		}
		log.Fatal(p.serve(l))
	default:
		log.Fatalf("unknown MODE %q", mode)
	}
}

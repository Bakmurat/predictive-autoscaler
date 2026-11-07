package main

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func writeSchedule(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "faults.json")
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestWorkIsDeterministic(t *testing.T) {
	if work(1000) != work(1000) || work(1000) == work(1001) {
		t.Fatal("the CPU work must be a deterministic function of the iteration count")
	}
}

func TestReadinessFollowsTheStartupDelayAndTheSlowStartWindow(t *testing.T) {
	t0 := time.Date(2026, 11, 7, 20, 10, 0, 0, time.UTC)
	now := t0
	clock := func() time.Time { return now }
	faults := writeSchedule(t, `{"slow_starts":[{"start":"2026-11-07T20:00:00Z","end":"2026-11-07T20:45:00Z","extra_startup_seconds":120}]}`)
	s, err := newServer(serverConfig{Target: "a", MaxInflight: 1, StartupDelay: 20 * time.Second, FaultFile: faults}, clock)
	if err != nil {
		t.Fatal(err)
	}
	ready := func() int {
		rec := httptest.NewRecorder()
		s.handleReady(rec, httptest.NewRequest("GET", "/ready", nil))
		return rec.Code
	}
	now = t0.Add(139 * time.Second)
	if ready() != http.StatusServiceUnavailable {
		t.Fatal("ready before 20 s + 120 s")
	}
	now = t0.Add(140 * time.Second)
	if ready() != http.StatusOK {
		t.Fatal("not ready after 20 s + 120 s")
	}
	// A process started after the window gets only its base delay.
	now = time.Date(2026, 11, 7, 20, 45, 0, 0, time.UTC)
	s2, _ := newServer(serverConfig{Target: "a", MaxInflight: 1, StartupDelay: 20 * time.Second, FaultFile: faults}, clock)
	if s2.extra != 0 || !s2.readyAt.Equal(now.Add(20*time.Second)) {
		t.Fatalf("after the window: extra %v ready at %v", s2.extra, s2.readyAt)
	}
}

func TestTheQueueIsBoundedAndOverflowIsRejected(t *testing.T) {
	s, err := newServer(serverConfig{Target: "a", WorkIterations: 1, ResponseBytes: 16, MaxInflight: 1, MaxQueue: 1,
		QueueTimeout: 2 * time.Second}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	s.slots <- struct{}{} // the only slot is busy
	var wg sync.WaitGroup
	codes := make(chan int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rec := httptest.NewRecorder()
			s.handleWork(rec, httptest.NewRequest("GET", "/work", nil))
			codes <- rec.Code
		}()
		time.Sleep(50 * time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond)
	<-s.slots // free the slot: the queued request proceeds
	wg.Wait()
	close(codes)
	got := map[int]int{}
	for c := range codes {
		got[c]++
	}
	if got[http.StatusOK] != 1 || got[http.StatusServiceUnavailable] != 1 {
		t.Fatalf("one queued and served, one rejected: %v", got)
	}
	rec := httptest.NewRecorder()
	s.handleMetrics(rec, httptest.NewRequest("GET", "/metrics", nil))
	m := rec.Body.String()
	for _, want := range []string{`c28_requests_total{target="a",code="200"} 1`, `c28_requests_total{target="a",code="503"} 1`,
		`c28_rejected_total{target="a",reason="queue_full"} 1`, `c28_request_duration_seconds_count{target="a"} 2`} {
		if !strings.Contains(m, want) {
			t.Fatalf("metrics lack %q:\n%s", want, m)
		}
	}
}

func TestTheResponseHasAFixedSizeAndNoCache(t *testing.T) {
	s, _ := newServer(serverConfig{Target: "a", WorkIterations: 10, ResponseBytes: 512, MaxInflight: 2}, time.Now)
	rec := httptest.NewRecorder()
	s.handleWork(rec, httptest.NewRequest("GET", "/work", nil))
	if rec.Code != 200 || rec.Body.Len() != 512 || rec.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("%d %d %q", rec.Code, rec.Body.Len(), rec.Header().Get("Cache-Control"))
	}
}

func TestIngressCountsArrivalsBeforeTheTargetCanFail(t *testing.T) {
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/work" && r.URL.Query().Get("x") == "" {
			time.Sleep(300 * time.Millisecond)
		}
		w.WriteHeader(503)
	}))
	defer slow.Close()
	g := newIngress(map[string]string{"a": slow.URL}, 100*time.Millisecond, time.Now)
	srv := httptest.NewServer(g)
	defer srv.Close()
	for i := 0; i < 3; i++ {
		resp, err := http.Get(srv.URL + "/t/a/work")
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusGatewayTimeout {
			t.Fatalf("a timed-out upstream is a 504, got %d", resp.StatusCode)
		}
	}
	resp, _ := http.Get(srv.URL + "/t/unknown/work")
	resp.Body.Close()
	resp, _ = http.Get(srv.URL + "/metrics")
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	m := string(raw)
	for _, want := range []string{`c28_ingress_requests_total{target="a"} 3`, `c28_ingress_responses_total{target="a",code="504"} 3`,
		`c28_ingress_duration_seconds_count{target="a",outcome="failure"} 3`} {
		if !strings.Contains(m, want) {
			t.Fatalf("metrics lack %q:\n%s", want, m)
		}
	}
	if strings.Contains(m, "unknown") {
		t.Fatal("an unknown target must not create series")
	}
}

func TestUpstreamSpecIsStrict(t *testing.T) {
	if _, err := parseUpstreams("a=http://x:1,b-2=http://y:2"); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"", "A=http://x", "a=https://x", "a", "a b=http://x"} {
		if _, err := parseUpstreams(bad); err == nil {
			t.Errorf("%q must be refused", bad)
		}
	}
}

func TestTCPProxyRefusesAndCutsDuringAnOutage(t *testing.T) {
	up, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer up.Close()
	go func() {
		for {
			c, err := up.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) { _, _ = io.Copy(c, c) }(c) // echo
		}
	}()
	t0 := time.Date(2026, 11, 7, 14, 14, 59, 0, time.UTC)
	var mu sync.Mutex
	now := t0
	clock := func() time.Time { mu.Lock(); defer mu.Unlock(); return now }
	faults := writeSchedule(t, `{"outages":[{"start":"2026-11-07T14:15:00Z","end":"2026-11-07T14:35:00Z"}]}`)
	p := &tcpProxy{upstream: up.Addr().String(), now: clock, conns: map[net.Conn]struct{}{},
		faults: &cachedSchedule{path: faults, refresh: time.Hour}}
	l, _ := net.Listen("tcp", "127.0.0.1:0")
	defer l.Close()
	go func() { _ = p.serve(l) }()
	c, err := net.Dial("tcp", l.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, _ = c.Write([]byte("hi"))
	buf := make([]byte, 2)
	if _, err := io.ReadFull(c, buf); err != nil || string(buf) != "hi" {
		t.Fatalf("before the outage the route works: %v %q", err, buf)
	}
	mu.Lock()
	now = t0.Add(2 * time.Second) // inside the outage
	mu.Unlock()
	time.Sleep(400 * time.Millisecond)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(buf); err == nil {
		t.Fatal("a kept-alive connection must be cut when the outage starts")
	}
	c2, err := net.Dial("tcp", l.Addr().String())
	if err == nil {
		_ = c2.SetReadDeadline(time.Now().Add(time.Second))
		_, rerr := c2.Read(buf)
		if rerr == nil {
			t.Fatal("a new connection during the outage must be refused")
		}
		c2.Close()
	}
	if p.refused.Load() == 0 || p.cut.Load() == 0 {
		t.Fatalf("refused %d cut %d", p.refused.Load(), p.cut.Load())
	}
}

func TestAMalformedScheduleIsAnError(t *testing.T) {
	for _, body := range []string{`{"outages":[{"start":"2026-11-07T14:35:00Z","end":"2026-11-07T14:15:00Z"}]}`,
		`{"outage":[]}`, `not json`} {
		if _, err := loadSchedule(writeSchedule(t, body)); err == nil {
			t.Errorf("%s must be refused", body)
		}
	}
	if s, err := loadSchedule(filepath.Join(t.TempDir(), "absent.json")); err != nil || len(s.Outages) != 0 {
		t.Fatal("an absent schedule means no faults")
	}
}

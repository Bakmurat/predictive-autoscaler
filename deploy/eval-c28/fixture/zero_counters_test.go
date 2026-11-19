package main

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// A counter that first appears with its first increment has no sample before the traffic: the stage B admission run of
// 2026-11-19 could not observe its first minute for exactly that reason. Every counter a run's verdict reads exists at
// zero in a FRESH process, before any request.
func TestTheCountersARunReadsExistAtZeroBeforeAnyTraffic(t *testing.T) {
	s, err := newServer(serverConfig{Target: "a", WorkIterations: 1, ResponseBytes: 8, MaxInflight: 1}, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	s.handleMetrics(rec, httptest.NewRequest("GET", "/metrics", nil))
	if want := `c28_requests_total{target="a",code="200"} 0` + "\n"; !strings.Contains(rec.Body.String(), want) {
		t.Fatalf("the fixture's request counter is missing before traffic:\n%s", rec.Body.String())
	}

	g := newIngress(map[string]string{"a": "http://a.demo.svc", "b": "http://b.demo.svc"}, time.Second, time.Now)
	rec = httptest.NewRecorder()
	g.metrics(rec)
	for _, target := range []string{"a", "b"} {
		if want := `c28_ingress_requests_total{target="` + target + `"} 0` + "\n"; !strings.Contains(rec.Body.String(), want) {
			t.Fatalf("the ingress arrival counter of %s is missing before traffic:\n%s", target, rec.Body.String())
		}
	}

	r := testRouter(t, time.Now, "a")
	m := func() string { rec := httptest.NewRecorder(); r.writeMetrics(rec); return rec.Body.String() }
	if strings.Contains(m(), "c28_ingress_dispatched_total{") {
		t.Fatal("no dispatch series can exist before discovery: the endpoints are not known yet")
	}
	mustReplace(t, r, []endpointSlice{decodeSlice(t, sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"), ready("10.0.0.2", "p2")))})
	for _, pod := range []string{"p1", "p2"} {
		if want := "c28_ingress_dispatched_total{" + dispatchLabel("a", pod) + "} 0\n"; !strings.Contains(m(), want) {
			t.Fatalf("the dispatch counter of %s is missing after discovery and before traffic:\n%s", pod, m())
		}
	}
}

func TestCreatingASeriesAtZeroNeverResetsACount(t *testing.T) {
	r := testRouter(t, time.Now, "a")
	m := func() string { rec := httptest.NewRecorder(); r.writeMetrics(rec); return rec.Body.String() }
	one := decodeSlice(t, sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1")))
	both := decodeSlice(t, sliceJSON("a-1", "a", 8080, ready("10.0.0.1", "p1"), ready("10.0.0.2", "p2")))
	mustReplace(t, r, []endpointSlice{one})
	for i := 0; i < 3; i++ {
		r.dispatched.inc(dispatchLabel("a", "p1"))
	}
	p1 := "c28_ingress_dispatched_total{" + dispatchLabel("a", "p1") + "} 3\n"
	p2 := "c28_ingress_dispatched_total{" + dispatchLabel("a", "p2") + "} 0\n"
	mustReplace(t, r, []endpointSlice{both}) // p2 joins: p1 keeps its count, p2 starts at zero
	if got := m(); !strings.Contains(got, p1) || !strings.Contains(got, p2) {
		t.Fatalf("a second endpoint joined:\n%s", got)
	}
	mustReplace(t, r, nil)                   // both leave (their series are kept for the keep time)
	mustReplace(t, r, []endpointSlice{both}) // and come back
	if got := m(); !strings.Contains(got, p1) || !strings.Contains(got, p2) {
		t.Fatalf("an endpoint that came back was reset:\n%s", got)
	}

	var c counterVec
	c.inc("x")
	c.ensure("x")
	c.ensure("y")
	if c.m["x"] != 1 || c.m["y"] != 0 || len(c.m) != 2 {
		t.Fatalf("ensure changed a count: %v", c.m)
	}
}

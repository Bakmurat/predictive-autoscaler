//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"
)

// An upstream that drops the SYN (a pod that is gone but still listed, a network blackhole) must not cost the ingress
// more than its request timeout per request. net/http keeps a dial running after its request has ended; unbounded,
// such dials last about two minutes each and pile up at the request rate.

// blackhole returns a loopback address whose connection attempts get no answer: a listening socket with a backlog of
// 0 that never accepts. Once its accept queue is full, Linux drops further SYNs, so a connect neither succeeds nor
// is refused. The connections that fill the queue stay open until the test ends.
func blackhole(t *testing.T) (string, int) {
	t.Helper()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_STREAM, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = syscall.Close(fd) })
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{Addr: [4]byte{127, 0, 0, 1}}); err != nil {
		t.Fatal(err)
	}
	if err := syscall.Listen(fd, 0); err != nil {
		t.Fatal(err)
	}
	sa, err := syscall.Getsockname(fd)
	if err != nil {
		t.Fatal(err)
	}
	port := sa.(*syscall.SockaddrInet4).Port
	addr := fmt.Sprintf("127.0.0.1:%d", port)
	for i := 0; i < 64; i++ {
		c, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err == nil {
			t.Cleanup(func() { _ = c.Close() })
			continue
		}
		var ne net.Error
		if errors.As(err, &ne) && ne.Timeout() {
			return addr, port
		}
		t.Fatalf("filling the accept queue: %v", err)
	}
	t.Skip("this kernel kept accepting connections on a full accept queue: no blackhole to test against")
	return "", 0
}

// settle waits until the process is back to at most `limit` goroutines and returns the count it last saw.
func settle(limit int, within time.Duration) int {
	deadline := time.Now().Add(within)
	for {
		n := runtime.NumGoroutine()
		if n <= limit || time.Now().After(deadline) {
			return n
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestADialToABlackholedUpstreamEndsAtTheRequestTimeout(t *testing.T) {
	addr, _ := blackhole(t)
	const timeout = 300 * time.Millisecond
	for name, tr := range map[string]*http.Transport{
		"service routing": newIngress(map[string]string{"a": "http://" + addr}, timeout, time.Now).client.Transport.(*http.Transport),
		"endpoints-rr":    upstreamTransport(timeout),
		"discovery":       discoveryTransport(nil, timeout), // the API server connection, bounded the same way
	} {
		if tr.DialContext == nil {
			t.Fatalf("%s: the transport dials without a bound", name)
		}
		t0 := time.Now()
		c, err := tr.DialContext(context.Background(), "tcp", addr) // no request deadline: only the dialer's own bound
		took := time.Since(t0)
		var ne net.Error
		if err == nil || !errors.As(err, &ne) || !ne.Timeout() {
			if c != nil {
				_ = c.Close()
			}
			t.Fatalf("%s: a dial into the blackhole must time out, got %v", name, err)
		}
		if took < timeout || took > timeout+time.Second {
			t.Fatalf("%s: the dial took %v, want about %v", name, took, timeout)
		}
	}
}

// Both routings, through the ingress as the generator uses it: every request is answered 504 at the timeout, and
// shortly after the last one nothing is left running for them.
func TestRequestsToABlackholedUpstreamLeaveNothingBehind(t *testing.T) {
	addr, port := blackhole(t)
	const timeout, requests = 300 * time.Millisecond, 40
	for _, mode := range []string{"service", "endpoints-rr"} {
		t.Run(mode, func(t *testing.T) {
			g := newIngress(map[string]string{"a": "http://" + addr}, timeout, time.Now)
			if mode == "endpoints-rr" {
				r := testRouter(t, time.Now, "a")
				r.dialTimeout = timeout
				mustReplace(t, r, []endpointSlice{decodeSlice(t, sliceJSON("a-1", "a", port, ready("127.0.0.1", "p1")))})
				g = newIngress(map[string]string{"a": "http://a.demo.svc"}, timeout, time.Now).withEndpointsRR(r)
			}
			srv := httptest.NewServer(g)
			defer srv.Close()
			client := &http.Client{Transport: &http.Transport{DisableKeepAlives: true}}
			base := settle(0, 200*time.Millisecond) // whatever runs before the requests (the server, earlier tests)
			var wg sync.WaitGroup
			codes := make([]int, requests)
			for i := range codes {
				wg.Add(1)
				go func() {
					defer wg.Done()
					resp, err := client.Get(srv.URL + "/t/a/work")
					if err != nil {
						return
					}
					_, _ = io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					codes[i] = resp.StatusCode
				}()
			}
			wg.Wait()
			for i, code := range codes {
				if code != http.StatusGatewayTimeout {
					t.Fatalf("request %d: a blackholed upstream is a 504 at the timeout, got %d", i, code)
				}
			}
			// An unbounded dial would still be running here (two goroutines each, for about two minutes).
			if n := settle(base+4, timeout+2*time.Second); n > base+4 {
				t.Fatalf("%d goroutines are still running %v after %d timed-out requests (before them: %d): "+
					"the dials were not bounded", n, timeout+2*time.Second, requests, base)
			}
		})
	}
}

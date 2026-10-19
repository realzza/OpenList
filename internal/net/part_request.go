package net

import (
	"context"
	"crypto/tls"
	"fmt"
	gonet "net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type partAttempt struct {
	i     int
	resp  *http.Response
	err   error
	trace *partTrace
}

// request sends a part's request. With HeaderTimeout set, a request that has
// no response headers by then is hedged: a second one goes out, and the
// first to answer is used. With edge failover on, the edge the first one
// waits on is marked, so the second connects elsewhere. done ends the
// winning request's context once its body has been read.
func (d *downloader) request(ctx context.Context, params *HttpRequestParams, ch *chunk) (*http.Response, *partTrace, func(), error) {
	traced := d.cfg.HeaderTimeout > 0 || log.IsLevelEnabled(log.DebugLevel)
	results := make(chan partAttempt, 2)
	var cancels []context.CancelFunc
	var traces []*partTrace
	launch := func() {
		actx, cancel := context.WithCancel(ctx)
		i := len(cancels)
		cancels = append(cancels, cancel)
		var tr *partTrace
		if traced {
			tr = newPartTrace(i > 0)
			actx = tr.attach(actx)
		}
		traces = append(traces, tr)
		go func() {
			resp, err := d.cfg.HttpClient(actx, params)
			results <- partAttempt{i, resp, err, tr}
		}()
	}
	launch()
	var hedge <-chan time.Time
	if d.cfg.HeaderTimeout > 0 {
		t := time.NewTimer(d.cfg.HeaderTimeout)
		defer t.Stop()
		hedge = t.C
	}
	running := 1
	for {
		select {
		case <-hedge:
			hedge = nil
			first := traces[0]
			if ip := first.remoteIP(); ip != "" && d.cfg.EdgeFailover > 0 {
				if u, err := url.Parse(params.URL); err == nil {
					MarkEdgeBad(u.Hostname(), ip)
				}
			}
			log.Debugf("chunk_%d no headers within %v, hedging; %v", ch.id, d.cfg.HeaderTimeout, first)
			launch()
			running++
		case a := <-results:
			running--
			if a.err == nil {
				for i, cancel := range cancels {
					if i != a.i {
						cancel()
					}
				}
				if running > 0 {
					go closeLosingParts(results, running)
				}
				return a.resp, a.trace, cancels[a.i], nil
			}
			cancels[a.i]()
			if running == 0 {
				return nil, a.trace, func() {}, a.err
			}
		}
	}
}

func closeLosingParts(results chan partAttempt, n int) {
	for ; n > 0; n-- {
		if a := <-results; a.resp != nil {
			_ = a.resp.Body.Close()
		}
	}
}

// partTrace records where a part request's time went (debug log) and which
// edge it waited on.
type partTrace struct {
	mu                                  sync.Mutex
	start                               time.Time
	hedge                               bool
	dns, conn, tlsDone, wrote, firstOut time.Duration
	ip                                  string
	reused, connected                   bool
	dialing                             map[string]time.Duration
	dials                               []string
}

func newPartTrace(hedge bool) *partTrace {
	return &partTrace{start: time.Now(), hedge: hedge, dialing: map[string]time.Duration{}}
}

func (t *partTrace) since() time.Duration { return max(time.Since(t.start), time.Microsecond) }

func (t *partTrace) attach(ctx context.Context) context.Context {
	return httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		DNSDone: func(httptrace.DNSDoneInfo) {
			t.mu.Lock()
			t.dns = t.since()
			t.mu.Unlock()
		},
		ConnectStart: func(_, addr string) {
			if isDNSDial(addr) {
				return
			}
			t.mu.Lock()
			t.dialing[addr] = t.since()
			t.mu.Unlock()
		},
		ConnectDone: func(_, addr string, err error) {
			if isDNSDial(addr) {
				return
			}
			t.mu.Lock()
			defer t.mu.Unlock()
			took := t.since() - t.dialing[addr]
			delete(t.dialing, addr)
			ip, _, _ := gonet.SplitHostPort(addr)
			if err != nil {
				t.dials = append(t.dials, fmt.Sprintf("%s err %.2fs", ip, took.Seconds()))
			} else {
				t.connected = true
				t.dials = append(t.dials, fmt.Sprintf("%s %.2fs", ip, took.Seconds()))
			}
		},
		TLSHandshakeDone: func(tls.ConnectionState, error) {
			t.mu.Lock()
			t.tlsDone = t.since()
			t.mu.Unlock()
		},
		GotConn: func(info httptrace.GotConnInfo) {
			t.mu.Lock()
			defer t.mu.Unlock()
			t.conn = t.since()
			t.reused = info.Reused
			if info.Conn != nil {
				t.ip, _, _ = gonet.SplitHostPort(info.Conn.RemoteAddr().String())
			}
		},
		WroteRequest: func(httptrace.WroteRequestInfo) {
			t.mu.Lock()
			t.wrote = t.since()
			t.mu.Unlock()
		},
		GotFirstResponseByte: func() {
			t.mu.Lock()
			t.firstOut = t.since()
			t.mu.Unlock()
		},
	})
}

// isDNSDial: the resolver's own UDP "dials" to its server show up in the
// same hooks; they aren't the edge.
func isDNSDial(addr string) bool {
	_, port, _ := gonet.SplitHostPort(addr)
	return port == "53"
}

func (t *partTrace) remoteIP() string {
	if t == nil {
		return ""
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.ip
}

func (t *partTrace) wasReused() bool {
	if t == nil {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reused
}

// String: times are since the request was sent; "-" = not reached.
// "stuck" names the step the request was in when it was given up.
func (t *partTrace) String() string {
	if t == nil {
		return "no trace"
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	f := func(d time.Duration) string {
		if d == 0 {
			return "-"
		}
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	ip := t.ip
	if ip == "" {
		ip = "-"
	}
	s := fmt.Sprintf("ip %s, dns %s, conn %s, tls %s, wrote %s, ttfb %s",
		ip, f(t.dns), f(t.conn), f(t.tlsDone), f(t.wrote), f(t.firstOut))
	if t.firstOut == 0 {
		stuck := "response"
		switch {
		case t.conn == 0 && t.dns == 0 && len(t.dials) == 0 && len(t.dialing) == 0:
			stuck = "dns"
		case t.conn == 0 && len(t.dialing) > 0 && !t.connected:
			stuck = "connect"
		case t.conn == 0 && t.tlsDone == 0 && t.connected:
			stuck = "tls"
		case t.conn == 0:
			stuck = "getconn"
		case t.wrote == 0:
			stuck = "write"
		}
		s += ", stuck " + stuck
	}
	if len(t.dials) > 0 || len(t.dialing) > 0 {
		var pending []string
		for addr, at := range t.dialing {
			ip, _, _ := gonet.SplitHostPort(addr)
			pending = append(pending, fmt.Sprintf("%s pending %.2fs", ip, (t.since()-at).Seconds()))
		}
		s += ", dials [" + strings.Join(append(append([]string{}, t.dials...), pending...), "; ") + "]"
	}
	if t.hedge {
		s += ", hedge"
	}
	return s
}

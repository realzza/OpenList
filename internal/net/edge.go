package net

import (
	"context"
	"crypto/tls"
	"fmt"
	gonet "net"
	"net/http"
	"net/http/httptrace"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	log "github.com/sirupsen/logrus"
)

// Edge failover.
//
// A CDN host such as Quark's resolves to one cluster of edge addresses at a
// time (one /24, a 60 s TTL), and each answer can be a different cluster
// anywhere in the CDN. From far away some clusters don't answer at all: on
// 2026-10-19 two of fifteen dropped every SYN from the US while the others
// connected in 0.2 s. While the resolver's cache holds such an answer, every
// new connection goes there and hangs, retries included, until the TTL runs
// out.
//
// With failover on, a connection that isn't up (TCP and TLS) within the
// stagger is raced by one to another cluster: an address from a different
// /24 that worked recently, or one from a second resolver's answer. The
// first to finish wins. Clusters that fail are tried last for a while.

type edgeKey struct{}

// WithEdgeFailover makes requests sent with ctx through RequestHttp race a
// second edge cluster when a connection isn't up after stagger.
func WithEdgeFailover(ctx context.Context, stagger time.Duration) context.Context {
	if stagger <= 0 {
		return ctx
	}
	return context.WithValue(ctx, edgeKey{}, stagger)
}

func edgeFailoverFrom(ctx context.Context) time.Duration {
	stagger, _ := ctx.Value(edgeKey{}).(time.Duration)
	return stagger
}

const (
	edgeBadFor  = 10 * time.Minute // a cluster that failed is tried last this long
	edgeGoodFor = 30 * time.Minute // a cluster that worked is an alternative this long
	edgeRacers  = 4                // connections in flight at most per dial
)

// edgeAltResolvers give a second opinion when no other cluster is known yet.
// Their caches hand out different clusters than the system resolver's.
var edgeAltResolvers = []string{"1.1.1.1:53", "8.8.8.8:53"}

// edgeNet is what is known about one edge cluster (a /24).
type edgeNet struct {
	ok, bad time.Time
	ips     []string
}

type edgeDialer struct {
	mu   sync.Mutex
	nets map[string]map[string]*edgeNet // host -> cluster -> state

	// replaced in tests
	now       func() time.Time
	lookup    func(ctx context.Context, host string) ([]string, error)
	altLookup func(ctx context.Context, host string) ([]string, error)
	dialTCP   func(ctx context.Context, network, addr string) (gonet.Conn, error)
	tlsConfig *tls.Config
}

func newEdgeDialer() *edgeDialer {
	d := &gonet.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	return &edgeDialer{
		nets:      map[string]map[string]*edgeNet{},
		now:       time.Now,
		lookup:    lookupIPv4(gonet.DefaultResolver),
		altLookup: altLookupIPv4,
		dialTCP:   d.DialContext,
		tlsConfig: &tls.Config{InsecureSkipVerify: conf.Conf != nil && conf.Conf.TlsInsecureSkipVerify},
	}
}

func lookupIPv4(r *gonet.Resolver) func(ctx context.Context, host string) ([]string, error) {
	return func(ctx context.Context, host string) ([]string, error) {
		ips, err := r.LookupIP(ctx, "ip4", host)
		out := make([]string, 0, len(ips))
		for _, ip := range ips {
			out = append(out, ip.String())
		}
		return out, err
	}
}

func altLookupIPv4(ctx context.Context, host string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	var out []string
	var err error
	for _, server := range edgeAltResolvers {
		r := &gonet.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, _ string) (gonet.Conn, error) {
			var d gonet.Dialer
			return d.DialContext(ctx, network, server)
		}}
		var ips []string
		ips, err = lookupIPv4(r)(ctx, host)
		out = append(out, ips...)
	}
	if len(out) > 0 {
		err = nil
	}
	return out, err
}

func edgeCluster(ip string) string {
	if parsed := gonet.ParseIP(ip); parsed != nil {
		if v4 := parsed.To4(); v4 != nil {
			return v4.Mask(gonet.CIDRMask(24, 32)).String()
		}
		return parsed.Mask(gonet.CIDRMask(64, 128)).String()
	}
	return ip
}

// candidates orders the addresses to try for host: the fresh answer's
// clusters that haven't failed, then clusters that worked recently, then
// the fresh ones that failed. The race takes them in this order, but each
// racer after the first prefers a cluster not tried yet.
func (e *edgeDialer) candidates(host string, fresh []string) (good, alt, bad []string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	now := e.now()
	nets := e.nets[host]
	if nets == nil {
		nets = map[string]*edgeNet{}
		e.nets[host] = nets
	}
	inFresh := map[string]bool{}
	for _, ip := range fresh {
		c := edgeCluster(ip)
		inFresh[c] = true
		n := nets[c]
		if n == nil {
			n = &edgeNet{}
			nets[c] = n
		}
		if !containsIP(n.ips, ip) && len(n.ips) < 16 {
			n.ips = append(n.ips, ip)
		}
		if n.failing(now) {
			bad = append(bad, ip)
		} else {
			good = append(good, ip)
		}
	}
	type known struct {
		at  time.Time
		ips []string
	}
	var others []known
	for c, n := range nets {
		if inFresh[c] || n.failing(now) || n.ok.IsZero() || now.Sub(n.ok) > edgeGoodFor {
			continue
		}
		others = append(others, known{n.ok, n.ips})
	}
	sort.Slice(others, func(i, j int) bool { return others[i].at.After(others[j].at) })
	for _, k := range others {
		alt = append(alt, k.ips...)
	}
	return good, alt, bad
}

func (n *edgeNet) failing(now time.Time) bool {
	return !n.bad.IsZero() && now.Sub(n.bad) < edgeBadFor && n.bad.After(n.ok)
}

func containsIP(ips []string, ip string) bool {
	for _, x := range ips {
		if x == ip {
			return true
		}
	}
	return false
}

func (e *edgeDialer) mark(host, ip string, ok bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	nets := e.nets[host]
	if nets == nil {
		nets = map[string]*edgeNet{}
		e.nets[host] = nets
	}
	c := edgeCluster(ip)
	n := nets[c]
	if n == nil {
		n = &edgeNet{}
		nets[c] = n
	}
	if !containsIP(n.ips, ip) && len(n.ips) < 16 {
		n.ips = append(n.ips, ip)
	}
	if ok {
		n.ok = e.now()
	} else {
		n.bad = e.now()
	}
}

// MarkEdgeBad records that the edge behind addr (ip:port, or ip) didn't
// answer a request in time, so its cluster is tried last for a while.
func MarkEdgeBad(host, addr string) {
	ip := addr
	if h, _, err := gonet.SplitHostPort(addr); err == nil {
		ip = h
	}
	if gonet.ParseIP(ip) == nil {
		return
	}
	edges().mark(host, ip, false)
}

var edgeOnce sync.Once
var edgeShared *edgeDialer

func edges() *edgeDialer {
	edgeOnce.Do(func() { edgeShared = newEdgeDialer() })
	return edgeShared
}

type edgeResult struct {
	conn gonet.Conn
	ip   string
	took time.Duration
	err  error
}

// DialTLSContext connects to addr (host:port) and completes the TLS
// handshake, racing clusters as described above when ctx asks for it.
func (e *edgeDialer) DialTLSContext(ctx context.Context, network, addr string) (gonet.Conn, error) {
	host, port, err := gonet.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	stagger := edgeFailoverFrom(ctx)
	if stagger <= 0 || gonet.ParseIP(host) != nil {
		return e.dialOne(ctx, network, addr, host)
	}
	fresh, err := e.lookup(ctx, host)
	if len(fresh) == 0 {
		if err == nil {
			err = fmt.Errorf("no addresses for %s", host)
		}
		return nil, err
	}
	good, alt, bad := e.candidates(host, fresh)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// A second opinion, ready by the time the first racer is overdue, when
	// no other working cluster is known.
	var altCh chan []string
	if len(alt) == 0 {
		altCh = make(chan []string, 1)
		go func() {
			ips, _ := e.altLookup(ctx, host)
			altCh <- ips
		}()
	}

	queue := append(append(append([]string{}, good...), alt...), bad...)
	tried, triedNet := map[string]bool{}, map[string]bool{}
	pickNext := func() (string, bool) {
		if altCh != nil {
			select {
			case ips := <-altCh:
				altCh = nil
				var add []string
				for _, ip := range ips {
					if !containsIP(queue, ip) {
						add = append(add, ip)
					}
				}
				// alternatives go before the fresh answer's failing clusters
				queue = append(append(append(append([]string{}, good...), alt...), add...), bad...)
			default:
			}
		}
		pick := -1
		for i, ip := range queue {
			if tried[ip] {
				continue
			}
			if pick < 0 {
				pick = i
			}
			if !triedNet[edgeCluster(ip)] {
				pick = i
				break
			}
		}
		if pick < 0 {
			return "", false
		}
		ip := queue[pick]
		tried[ip], triedNet[edgeCluster(ip)] = true, true
		return ip, true
	}

	results := make(chan edgeResult, len(queue)+16)
	start := time.Now()
	running := 0
	launched := map[string]time.Duration{}
	launch := func() bool {
		ip, ok := pickNext()
		if !ok {
			return false
		}
		running++
		launched[ip] = time.Since(start)
		go func() {
			c, err := e.dialOne(ctx, network, gonet.JoinHostPort(ip, port), host)
			results <- edgeResult{c, ip, time.Since(start), err}
		}()
		return true
	}
	launch()
	timer := time.NewTimer(stagger)
	defer timer.Stop()
	var log_ []string
	var firstErr error
	failed := map[string]bool{}
	for running > 0 {
		select {
		case <-timer.C:
			if running < edgeRacers {
				launch()
			}
			timer.Reset(stagger)
		case r := <-results:
			running--
			if r.err == nil {
				e.mark(host, r.ip, true)
				log_ = append(log_, fmt.Sprintf("%s ok %.2fs", r.ip, r.took.Seconds()))
				// a racer started a stagger or more before the winner finished
				// is overdue: its cluster goes to the back for a while
				for ip, at := range launched {
					if ip != r.ip && r.took-at >= stagger && !failed[ip] {
						e.mark(host, ip, false)
						log_ = append(log_, fmt.Sprintf("%s overdue", ip))
					}
				}
				if running > 0 {
					cancel()
					go closeLosers(results, running)
				}
				if len(log_) > 1 || running > 0 {
					log.Debugf("edge dial %s: %s (%d racing)", host, strings.Join(log_, ", "), running)
				}
				return r.conn, nil
			}
			failed[r.ip] = true
			if ctx.Err() == nil {
				e.mark(host, r.ip, false)
			}
			log_ = append(log_, fmt.Sprintf("%s failed %.2fs", r.ip, r.took.Seconds()))
			if firstErr == nil {
				firstErr = r.err
			}
			if ctx.Err() == nil && running < edgeRacers {
				launch() // a fast failure doesn't wait for the stagger
			}
		}
	}
	log.Debugf("edge dial %s: %s", host, strings.Join(log_, ", "))
	if firstErr == nil {
		firstErr = ctx.Err()
	}
	return nil, firstErr
}

func closeLosers(results chan edgeResult, n int) {
	for ; n > 0; n-- {
		if r := <-results; r.conn != nil {
			_ = r.conn.Close()
		}
	}
}

// dialOne is what http.Transport does itself without a DialTLSContext: TCP,
// then the TLS handshake with a 10 s limit. It reports the handshake to the
// request's httptrace, which the transport skips for custom TLS dialers.
func (e *edgeDialer) dialOne(ctx context.Context, network, addr, host string) (gonet.Conn, error) {
	raw, err := e.dialTCP(ctx, network, addr)
	if err != nil {
		return nil, err
	}
	cfg := e.tlsConfig.Clone()
	cfg.ServerName = host
	tc := tls.Client(raw, cfg)
	trace := httptrace.ContextClientTrace(ctx)
	if trace != nil && trace.TLSHandshakeStart != nil {
		trace.TLSHandshakeStart()
	}
	hctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	err = tc.HandshakeContext(hctx)
	if trace != nil && trace.TLSHandshakeDone != nil {
		trace.TLSHandshakeDone(tc.ConnectionState(), err)
	}
	if err != nil {
		_ = raw.Close()
		return nil, err
	}
	return tc, nil
}

var edgeClientOnce sync.Once
var edgeClient *http.Client

// EdgeHttpClient is HttpClient with the edge failover dialer. It has its own
// connection pool; requests use it only when their context asks for
// failover (see RequestHttp), so without the setting nothing changes.
func EdgeHttpClient() *http.Client {
	edgeClientOnce.Do(func() {
		edgeClient = NewHttpClient()
		edgeClient.CheckRedirect = HttpClient().CheckRedirect
		if st, ok := edgeClient.Transport.(*safeTransport); ok {
			if t, ok := st.base.(*http.Transport); ok {
				t.DialTLSContext = edges().DialTLSContext
			}
		}
	})
	return edgeClient
}

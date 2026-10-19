package net

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	gonet "net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

// testEdges: cluster 10.0.0.0/24 drops every SYN, everything else reaches
// the local TLS server.
func testEdges(t *testing.T, fresh, alt []string) (*edgeDialer, *[]string) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
	t.Cleanup(srv.Close)
	var mu sync.Mutex
	var dialed []string
	e := newEdgeDialer()
	e.lookup = func(context.Context, string) ([]string, error) { return fresh, nil }
	e.altLookup = func(context.Context, string) ([]string, error) { return alt, nil }
	e.tlsConfig = &tls.Config{InsecureSkipVerify: true}
	e.dialTCP = func(ctx context.Context, network, addr string) (gonet.Conn, error) {
		mu.Lock()
		dialed = append(dialed, addr)
		mu.Unlock()
		if strings.HasPrefix(addr, "10.0.0.") {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		var d gonet.Dialer
		return d.DialContext(ctx, network, srv.Listener.Addr().String())
	}
	return e, &dialed
}

func TestEdgeDialRacesAnotherCluster(t *testing.T) {
	e, dialed := testEdges(t, []string{"10.0.0.1", "10.0.0.2"}, []string{"10.1.0.7"})
	ctx := WithEdgeFailover(context.Background(), 50*time.Millisecond)
	begin := time.Now()
	c, err := e.DialTLSContext(ctx, "tcp", "cdn.test:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if took := time.Since(begin); took > 500*time.Millisecond {
		t.Errorf("took %v, want about one stagger", took)
	}
	// the second racer goes to the other cluster, not to 10.0.0.2
	if got := strings.Join(*dialed, ","); got != "10.0.0.1:443,10.1.0.7:443" {
		t.Errorf("dialed %s", got)
	}
	// the overdue cluster is tried last now, the one that worked first
	good, alt, bad := e.candidates("cdn.test", []string{"10.0.0.1", "10.0.0.2"})
	if len(good) != 0 || strings.Join(alt, ",") != "10.1.0.7" || len(bad) != 2 {
		t.Errorf("candidates good %v alt %v bad %v", good, alt, bad)
	}
	*dialed = nil
	begin = time.Now()
	c, err = e.DialTLSContext(ctx, "tcp", "cdn.test:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if took := time.Since(begin); took > 40*time.Millisecond {
		t.Errorf("second dial took %v, want no wait for the dead cluster", took)
	}
	if (*dialed)[0] != "10.1.0.7:443" {
		t.Errorf("second dial started with %s", (*dialed)[0])
	}
}

func TestEdgeDialOffDialsTheAnswerOnly(t *testing.T) {
	e, dialed := testEdges(t, []string{"10.1.0.1"}, []string{"10.2.0.1"})
	c, err := e.DialTLSContext(context.Background(), "tcp", "10.1.0.1:443")
	if err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if got := strings.Join(*dialed, ","); got != "10.1.0.1:443" {
		t.Errorf("dialed %s", got)
	}
}

func TestEdgeDialAllDead(t *testing.T) {
	e, _ := testEdges(t, []string{"10.0.0.1"}, nil)
	e.dialTCP = func(ctx context.Context, network, addr string) (gonet.Conn, error) {
		return nil, errors.New("connection refused")
	}
	_, err := e.DialTLSContext(WithEdgeFailover(context.Background(), 50*time.Millisecond), "tcp", "cdn.test:443")
	if err == nil || !strings.Contains(err.Error(), "refused") {
		t.Fatalf("err %v", err)
	}
}

// A part whose first request never answers is hedged after HeaderTimeout,
// long before the stall timeout.
func TestDownloadHedgesPartWithoutHeaders(t *testing.T) {
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i * 7)
	}
	client, ranges := newStallingClient(data, 20, -1)
	d := NewDownloader(func(d *Downloader) {
		d.Concurrency = 3
		d.PartSize = 10
		d.PartStallTimeout = 2 * time.Second
		d.HeaderTimeout = 50 * time.Millisecond
		d.HttpClient = client
	})
	begin := time.Now()
	rc, err := d.Download(context.Background(), &HttpRequestParams{
		Range: http_range.Range{Start: 0, Length: 60},
		Size:  int64(len(data)),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data[:60]) {
		t.Fatal("wrong bytes")
	}
	if took := time.Since(begin); took > time.Second {
		t.Errorf("took %v, want the hedge well before the 2 s stall timeout", took)
	}
	n := 0
	for _, r := range *ranges {
		if r == "20-10" {
			n++
		}
	}
	if n != 2 {
		t.Errorf("part at 20 requested %d times, want 2 (first + hedge): %v", n, *ranges)
	}
}

// A short request (one part) that fails to connect is retried when edge
// failover is on, instead of answering the client with an error.
func TestDownloadSinglePartRetriesNetError(t *testing.T) {
	data := []byte("0123456789abcdef")
	for _, on := range []bool{false, true} {
		var calls atomic.Int32
		d := NewDownloader(func(d *Downloader) {
			d.Concurrency = 3
			d.PartSize = 10
			d.PartStallTimeout = time.Second
			if on {
				d.EdgeFailover = time.Second
			}
			d.HttpClient = func(ctx context.Context, p *HttpRequestParams) (*http.Response, error) {
				if calls.Add(1) == 1 {
					return nil, &gonet.OpError{Op: "dial", Net: "tcp", Err: errors.New("i/o timeout")}
				}
				if on && edgeFailoverFrom(ctx) != time.Second {
					t.Error("request context doesn't ask for edge failover")
				}
				s, e := p.Range.Start, p.Range.Start+p.Range.Length
				h := http.Header{}
				h.Set("Content-Range", "bytes "+itoa(s)+"-"+itoa(e-1)+"/"+itoa(int64(len(data))))
				return &http.Response{Body: io.NopCloser(bytes.NewReader(data[s:e])), Header: h, ContentLength: e - s}, nil
			}
		})
		rc, err := d.Download(context.Background(), &HttpRequestParams{
			Range: http_range.Range{Start: 3, Length: 4},
			Size:  int64(len(data)),
		})
		var got []byte
		if err == nil {
			got, err = io.ReadAll(rc)
			_ = rc.Close()
		}
		if on && (err != nil || string(got) != "3456") {
			t.Errorf("failover on: got %q, %v", got, err)
		}
		if !on && err == nil {
			t.Errorf("failover off: want the error, got %q", got)
		}
	}
}

func itoa(n int64) string { return strconv.FormatInt(n, 10) }

package net

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

// stallingBody returns its bytes, then blocks until the request is cancelled.
type stallingBody struct {
	r   io.Reader
	ctx context.Context
}

func (b *stallingBody) Read(p []byte) (int, error) {
	if n, err := b.r.Read(p); n > 0 || err != io.EOF {
		return n, err
	}
	<-b.ctx.Done()
	return 0, b.ctx.Err()
}

func (b *stallingBody) Close() error { return nil }

// newStallingClient serves data, except that the first request for the part
// at stallAt either never answers (good < 0) or stalls after `good` bytes.
func newStallingClient(data []byte, stallAt, good int64) (HttpRequestFunc, *[]string) {
	var mu sync.Mutex
	var ranges []string
	stalled := false
	return func(ctx context.Context, params *HttpRequestParams) (*http.Response, error) {
		mu.Lock()
		ranges = append(ranges, fmt.Sprintf("%d-%d", params.Range.Start, params.Range.Length))
		stall := !stalled && params.Range.Start == stallAt
		if stall {
			stalled = true
		}
		mu.Unlock()
		if stall && good < 0 {
			<-ctx.Done()
			return nil, ctx.Err()
		}
		start, fin := params.Range.Start, params.Range.Start+params.Range.Length
		header := http.Header{}
		header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, fin-1, len(data)))
		var body io.ReadCloser = io.NopCloser(bytes.NewReader(data[start:fin]))
		if stall {
			body = &stallingBody{r: bytes.NewReader(data[start : start+good]), ctx: ctx}
		}
		return &http.Response{Body: body, Header: header, ContentLength: fin - start}, nil
	}, &ranges
}

func testStall(t *testing.T, stallAt, good int64, wantRange string) {
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i * 7)
	}
	client, ranges := newStallingClient(data, stallAt, good)
	d := NewDownloader(func(d *Downloader) {
		d.Concurrency = 3
		d.PartSize = 10
		d.PartStallTimeout = 100 * time.Millisecond
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
	if err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if !bytes.Equal(got, data[:60]) {
		t.Fatalf("stream corrupted after stall retry:\n got %v\nwant %v", got, data[:60])
	}
	if el := time.Since(begin); el > 3*time.Second {
		t.Errorf("stalled part was not retried promptly (took %v)", el)
	}
	if !containsString(*ranges, wantRange) {
		t.Errorf("expected retry of range %s, got %v", wantRange, *ranges)
	}
}

func TestDownloadResumesPartStalledMidBody(t *testing.T) {
	testStall(t, 20, 4, "24-6")
}

func TestDownloadRetriesPartWithNoResponse(t *testing.T) {
	testStall(t, 20, -1, "20-10")
}

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

// tricklingBody hands out 1 KB every 50 ms until the request is cancelled.
type tricklingBody struct {
	r   io.Reader
	ctx context.Context
}

func (b *tricklingBody) Read(p []byte) (int, error) {
	select {
	case <-b.ctx.Done():
		return 0, b.ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}
	return b.r.Read(p[:min(len(p), 1024)])
}

func (b *tricklingBody) Close() error { return nil }

func TestDownloadResumesSlowHeadPart(t *testing.T) {
	data := make([]byte, 8<<20)
	for i := range data {
		data[i] = byte(i * 7)
	}
	var mu sync.Mutex
	var ranges []string
	trickled := false
	client := func(ctx context.Context, params *HttpRequestParams) (*http.Response, error) {
		start, fin := params.Range.Start, params.Range.Start+params.Range.Length
		mu.Lock()
		ranges = append(ranges, fmt.Sprintf("%d-%d", start, params.Range.Length))
		trickle := !trickled && start == 0
		if trickle {
			trickled = true
		}
		mu.Unlock()
		header := http.Header{}
		header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, fin-1, len(data)))
		var body io.ReadCloser = io.NopCloser(bytes.NewReader(data[start:fin]))
		if trickle {
			body = &tricklingBody{r: bytes.NewReader(data[start:fin]), ctx: ctx}
		}
		return &http.Response{Body: body, Header: header, ContentLength: fin - start}, nil
	}
	d := NewDownloader(func(d *Downloader) {
		d.Concurrency = 3
		d.PartSize = 2 << 20
		d.PartStallTimeout = time.Second // slow-head window 0.5 s after 0.6 s grace
		d.HttpClient = client
	})
	begin := time.Now()
	rc, err := d.Download(context.Background(), &HttpRequestParams{
		Range: http_range.Range{Start: 0, Length: int64(len(data))},
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
	if !bytes.Equal(got, data) {
		t.Fatal("stream corrupted after resuming the slow part")
	}
	// Trickling at 20 KB/s, the 2 MB head part alone would take ~100 s.
	if el := time.Since(begin); el > 5*time.Second {
		t.Errorf("slow head part was not resumed (took %v)", el)
	}
	mu.Lock()
	defer mu.Unlock()
	resumed := false
	for _, r := range ranges {
		var s, l int64
		fmt.Sscanf(r, "%d-%d", &s, &l)
		if s > 0 && s < 2<<20 && s+l == 2<<20 {
			resumed = true
		}
	}
	if !resumed {
		t.Errorf("expected a resume of the head part from where it stopped, ranges=%v", ranges)
	}
}

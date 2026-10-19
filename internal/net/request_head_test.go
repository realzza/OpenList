package net

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

func TestDownloadHeadParts(t *testing.T) {
	data := make([]byte, 100)
	for i := range data {
		data[i] = byte(i * 7)
	}
	for _, tc := range []struct {
		name                string
		headParts, headSize int
		start, length       int64
		want                []string // requested ranges, start-length
	}{
		{"head then full parts, short last", 3, 5, 7, 80,
			[]string{"7-5", "12-5", "17-5", "22-20", "42-20", "62-20", "82-5"}},
		{"range shorter than the head", 3, 5, 0, 12,
			[]string{"0-5", "5-5", "10-2"}},
		{"range within one head part", 3, 5, 50, 4,
			[]string{"50-4"}},
		{"range of exactly the head", 2, 5, 0, 10,
			[]string{"0-5", "5-5"}},
		// off: the upstream layout, smallest part first
		{"off", 0, 5, 0, 70,
			[]string{"0-10", "10-20", "30-20", "50-20"}},
		{"head part not smaller than part size", 3, 20, 0, 70,
			[]string{"0-10", "10-20", "30-20", "50-20"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, ranges := newStallingClient(data, -1, 0)
			d := NewDownloader(func(d *Downloader) {
				d.Concurrency = 2
				d.PartSize = 20
				d.HeadParts = tc.headParts
				d.HeadPartSize = tc.headSize
				d.HttpClient = client
			})
			rc, err := d.Download(context.Background(), &HttpRequestParams{
				Range: http_range.Range{Start: tc.start, Length: tc.length},
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
			if !bytes.Equal(got, data[tc.start:tc.start+tc.length]) {
				t.Fatalf("read %d bytes that differ from the source", len(got))
			}
			slices.Sort(*ranges)
			want := slices.Clone(tc.want)
			slices.Sort(want)
			if !slices.Equal(*ranges, want) {
				t.Fatalf("requested %v, want %v", *ranges, tc.want)
			}
		})
	}
}

// Once the first part answers, the other head parts are requested together,
// not one more per answered request.
func TestDownloadRequestsHeadPartsTogether(t *testing.T) {
	data := make([]byte, 200)
	var mu sync.Mutex
	inflight, most := 0, 0
	client := func(ctx context.Context, params *HttpRequestParams) (*http.Response, error) {
		start, fin := params.Range.Start, params.Range.Start+params.Range.Length
		if start > 0 && start < 20 { // head parts after the first: hold the answer
			mu.Lock()
			inflight++
			most = max(most, inflight)
			mu.Unlock()
			select { // give the others a moment to arrive
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			mu.Lock()
			inflight--
			mu.Unlock()
		}
		header := http.Header{}
		header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, fin-1, len(data)))
		return &http.Response{Body: io.NopCloser(bytes.NewReader(data[start:fin])), Header: header,
			ContentLength: fin - start}, nil
	}
	d := NewDownloader(func(d *Downloader) {
		d.Concurrency = 8
		d.PartSize = 40
		d.HeadParts = 4
		d.HeadPartSize = 5
		d.HttpClient = client
	})
	rc, err := d.Download(context.Background(), &HttpRequestParams{
		Range: http_range.Range{Start: 0, Length: 200},
		Size:  int64(len(data)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.ReadAll(rc); err != nil {
		t.Fatal(err)
	}
	_ = rc.Close()
	if most < 3 {
		t.Fatalf("at most %d of the 3 head parts after the first were in flight together, want 3", most)
	}
}

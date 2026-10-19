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

// Once the first part answers, LaunchParts parts in all are requested
// together (head parts and full parts alike, within Concurrency), not one
// more per answered request.
func TestDownloadLaunchParts(t *testing.T) {
	data := make([]byte, 200)
	for i := range data {
		data[i] = byte(i * 3)
	}
	for _, tc := range []struct {
		name              string
		headParts, launch int
		wantTogether      int // parts after the first in flight before any answers
	}{
		{"off: one more per answered request", 4, 0, 1},
		{"the head parts", 4, 4, 3},
		{"the head parts and the first full parts", 4, 6, 5},
		{"capped by concurrency", 4, 20, 7},
		{"without head parts", 0, 4, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			inflight := 0
			release := make(chan struct{})
			client := func(ctx context.Context, params *HttpRequestParams) (*http.Response, error) {
				start, fin := params.Range.Start, params.Range.Start+params.Range.Length
				if start > 0 { // every part after the first: hold the answer
					mu.Lock()
					inflight++
					mu.Unlock()
					select {
					case <-release:
					case <-ctx.Done():
						return nil, ctx.Err()
					}
				}
				header := http.Header{}
				header.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, fin-1, len(data)))
				return &http.Response{Body: io.NopCloser(bytes.NewReader(data[start:fin])), Header: header,
					ContentLength: fin - start}, nil
			}
			d := NewDownloader(func(d *Downloader) {
				d.Concurrency = 8
				d.PartSize = 40
				d.HeadParts = tc.headParts
				d.HeadPartSize = 5
				d.LaunchParts = tc.launch
				d.HttpClient = client
			})
			// nothing reads yet, so only answered requests can start parts
			rc, err := d.Download(context.Background(), &HttpRequestParams{
				Range: http_range.Range{Start: 0, Length: 200},
				Size:  int64(len(data)),
			})
			if err != nil {
				t.Fatal(err)
			}
			together := func() int {
				mu.Lock()
				defer mu.Unlock()
				return inflight
			}
			for deadline := time.Now().Add(2 * time.Second); together() < tc.wantTogether && time.Now().Before(deadline); {
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond) // and no more than that
			got := together()
			close(release)
			read, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(read, data) {
				t.Fatalf("read %d bytes that differ from the source", len(read))
			}
			if got != tc.wantTogether {
				t.Fatalf("%d parts after the first were requested together, want %d", got, tc.wantTogether)
			}
		})
	}
}

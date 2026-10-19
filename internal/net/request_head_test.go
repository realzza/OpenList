package net

import (
	"bytes"
	"context"
	"io"
	"slices"
	"testing"

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

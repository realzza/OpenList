package quark

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/drivers/base"
	"github.com/OpenListTeam/OpenList/v4/internal/driver"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/go-resty/resty/v2"
)

// fakeQuark answers the two API calls a proxied download makes: the listing
// that resolves the path and file/download, which it counts.
func fakeQuark(t *testing.T, linkCache int) (*QuarkOrUC, *atomic.Int32) {
	t.Helper()
	var downloads atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/file/sort":
			fmt.Fprint(w, `{"status":200,"code":0,"data":{"list":[`+
				`{"fid":"f1","file_name":"movie.mkv","size":1048576,"file":true}]},"metadata":{"_total":1}}`)
		case "/file/download":
			downloads.Add(1)
			fmt.Fprint(w, `{"status":200,"code":0,"data":[{"download_url":"https://cdn.invalid/movie.mkv"}]}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)
	// conf.Conf is nil outside a booted server, so base.InitClient() is unusable.
	prev := base.RestyClient
	base.RestyClient = resty.New().SetTimeout(5 * time.Second)
	t.Cleanup(func() { base.RestyClient = prev })
	return &QuarkOrUC{
		Storage:  model.Storage{MountPath: "/" + t.Name()},
		Addition: Addition{Cookie: "x=1", RootID: driver.RootID{RootFolderID: "0"}, DownLinkCache: linkCache},
		config:   driver.Config{Name: "Quark", DefaultRoot: "0"},
		conf:     Conf{ua: "test", referer: "https://pan.quark.cn", api: srv.URL, pr: "ucpro"},
	}, &downloads
}

func TestDownLinkCacheSetsExpiration(t *testing.T) {
	for _, tc := range []struct {
		setting int
		want    *time.Duration
	}{
		{0, nil},
		{-5, nil},
		{300, func() *time.Duration { d := 300 * time.Second; return &d }()},
	} {
		d, _ := fakeQuark(t, tc.setting)
		link, err := d.getDownloadLink(&File{Fid: "f1"})
		if err != nil {
			t.Fatal(err)
		}
		if (link.Expiration == nil) != (tc.want == nil) || (tc.want != nil && *link.Expiration != *tc.want) {
			t.Fatalf("down_link_cache=%d: Expiration %v, want %v", tc.setting, link.Expiration, tc.want)
		}
	}
}

// A player opens a file with requests one after another (header, index,
// stream); each closes its link before the next arrives, as the WebDAV
// handler does. Without the cache every request costs a file/download call.
func TestDownLinkCacheReusesLinkAcrossSequentialRequests(t *testing.T) {
	for _, tc := range []struct {
		name      string
		setting   int
		wantCalls int32
	}{
		{"off", 0, 3},
		{"on", 300, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d, downloads := fakeQuark(t, tc.setting)
			for range 3 {
				link, _, err := op.Link(context.Background(), d, "/movie.mkv", model.LinkArgs{})
				if err != nil {
					t.Fatal(err)
				}
				if link.URL != "https://cdn.invalid/movie.mkv" || link.Concurrency != 3 {
					t.Fatalf("unexpected link: %+v", link)
				}
				_ = link.Close()
			}
			if got := downloads.Load(); got != tc.wantCalls {
				t.Fatalf("file/download called %d times, want %d", got, tc.wantCalls)
			}
		})
	}
}

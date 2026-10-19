package net

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/OpenListTeam/OpenList/v4/pkg/http_range"
)

// abortOnCloseReader behaves like the multi-thread downloader: closing it
// before everything was read reports an error.
type abortOnCloseReader struct{}

type abortOnCloseBody struct{ io.Reader }

func (abortOnCloseBody) Close() error { return errors.New("interrupted") }

func (abortOnCloseReader) RangeRead(_ context.Context, r http_range.Range) (io.ReadCloser, error) {
	return abortOnCloseBody{strings.NewReader("data")}, nil
}

func TestServeHTTPHeadIgnoresUnreadBody(t *testing.T) {
	for _, rng := range []string{"", "bytes=10-"} {
		request := httptest.NewRequest(http.MethodHead, "/file.mkv", nil)
		if rng != "" {
			request.Header.Set("Range", rng)
		}
		recorder := httptest.NewRecorder()
		if err := ServeHTTP(recorder, request, "file.mkv", time.Time{}, 100, abortOnCloseReader{}); err != nil {
			t.Fatalf("Range %q: HEAD returned error %v", rng, err)
		}
		if code := recorder.Code; code != http.StatusOK && code != http.StatusPartialContent {
			t.Errorf("Range %q: status = %d", rng, code)
		}
	}
}

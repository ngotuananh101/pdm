// Probe tests (Stage 3, Phase 2.1, TDD): the probe must verify Range
// support with a real ranged GET, never trusting HEAD or Accept-Ranges.
// A 206 is only accepted when its Content-Range matches the requested
// range; anything else is an error, never a silent mis-record.
package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

const (
	probeTestETag        = `"v1"`
	probeTestLastMod     = "Wed, 01 Jan 2025 00:00:00 GMT"
	probeTestWeakETag    = `W/"v1"`
	probeTestContentSize = 100
)

// probeServer is a controllable origin for probe tests. mode selects
// how it answers a ranged GET:
//
//	"support"          206 with a correct Content-Range
//	"ignore"           200 with the full body (Range ignored)
//	"ignore-chunked"   200 chunked, no Content-Length
//	"no-content-range" 206 without Content-Range
//	"wrong-range"      206 with Content-Range bytes 1-1/N
//	"zero-total"       206 with Content-Range bytes 0-0/0
//	"star-total"       206 with Content-Range bytes 0-0/*
//	"not-satisfiable"  416 with Content-Range bytes */N
//	"not-found"        404
//	"forbidden"        403
type probeServer struct {
	mu sync.Mutex

	content      []byte
	etag         string
	lastModified string
	mode         string
	head405      bool

	sawRange          string
	sawAcceptEncoding string
}

func newProbeServer(mode string) *probeServer {
	content := make([]byte, probeTestContentSize)
	for i := range content {
		content[i] = byte(i)
	}
	return &probeServer{
		content:      content,
		etag:         probeTestETag,
		lastModified: probeTestLastMod,
		mode:         mode,
	}
}

func (s *probeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.sawRange = r.Header.Get("Range")
	s.sawAcceptEncoding = r.Header.Get("Accept-Encoding")
	s.mu.Unlock()

	if s.head405 && r.Method == http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}
	w.Header().Set("ETag", s.etag)
	w.Header().Set("Last-Modified", s.lastModified)
	switch s.mode {
	case "ignore":
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
	case "ignore-chunked":
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content[:len(s.content)/2])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write(s.content[len(s.content)/2:])
	case "support":
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(s.content)))
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[:1])
	case "no-content-range":
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[:1])
	case "wrong-range":
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 1-1/%d", len(s.content)))
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[:1])
	case "zero-total":
		w.Header().Set("Content-Range", "bytes 0-0/0")
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[:1])
	case "star-total":
		w.Header().Set("Content-Range", "bytes 0-0/*")
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[:1])
	case "not-satisfiable":
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(s.content)))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	case "not-found":
		w.WriteHeader(http.StatusNotFound)
	case "forbidden":
		w.WriteHeader(http.StatusForbidden)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// probeAgainst runs the probe against srv with the production client.
func probeAgainst(t *testing.T, srv *httptest.Server) (ProbeResult, error) {
	t.Helper()
	cfg := testConfig()
	p := NewProber(cfg, NewHTTPClient(cfg))
	return p.Probe(context.Background(), srv.URL+"/file.bin")
}

func (s *probeServer) assertRangedRequest(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.sawRange != "bytes=0-0" {
		t.Fatalf("Range header = %q, want %q", s.sawRange, "bytes=0-0")
	}
	if s.sawAcceptEncoding != "identity" {
		t.Fatalf("Accept-Encoding header = %q, want %q", s.sawAcceptEncoding, "identity")
	}
}

func TestProbe_RangeSupported(t *testing.T) {
	ps := newProbeServer("support")
	srv := httptest.NewServer(ps)
	defer srv.Close()

	res, err := probeAgainst(t, srv)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !res.SupportsRange {
		t.Fatal("SupportsRange = false, want true")
	}
	if res.TotalBytes != probeTestContentSize {
		t.Fatalf("TotalBytes = %d, want %d", res.TotalBytes, probeTestContentSize)
	}
	if res.ETag != probeTestETag {
		t.Fatalf("ETag = %q, want %q", res.ETag, probeTestETag)
	}
	if res.LastModified != probeTestLastMod {
		t.Fatalf("LastModified = %q, want %q", res.LastModified, probeTestLastMod)
	}
	ps.assertRangedRequest(t)
}

func TestProbe_NoRangeSupport200(t *testing.T) {
	ps := newProbeServer("ignore")
	srv := httptest.NewServer(ps)
	defer srv.Close()

	res, err := probeAgainst(t, srv)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if res.SupportsRange {
		t.Fatal("SupportsRange = true, want false (server answered 200)")
	}
	if res.TotalBytes != probeTestContentSize {
		t.Fatalf("TotalBytes = %d, want %d", res.TotalBytes, probeTestContentSize)
	}
	if res.ETag != probeTestETag {
		t.Fatalf("ETag = %q, want %q", res.ETag, probeTestETag)
	}
	ps.assertRangedRequest(t)
}

func TestProbe_NoRangeSupportChunkedUnknownTotal(t *testing.T) {
	ps := newProbeServer("ignore-chunked")
	srv := httptest.NewServer(ps)
	defer srv.Close()

	res, err := probeAgainst(t, srv)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if res.SupportsRange {
		t.Fatal("SupportsRange = true, want false")
	}
	if res.TotalBytes != -1 {
		t.Fatalf("TotalBytes = %d, want -1 (unknown)", res.TotalBytes)
	}
}

func TestProbe_WeakETagDropped(t *testing.T) {
	ps := newProbeServer("support")
	ps.etag = probeTestWeakETag
	srv := httptest.NewServer(ps)
	defer srv.Close()

	res, err := probeAgainst(t, srv)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if res.ETag != "" {
		t.Fatalf("ETag = %q, want empty (weak ETag must not be stored)", res.ETag)
	}
	if res.LastModified != probeTestLastMod {
		t.Fatalf("LastModified = %q, want %q", res.LastModified, probeTestLastMod)
	}
	if !res.SupportsRange {
		t.Fatal("SupportsRange = false, want true")
	}
}

func TestProbe_BadContentRange(t *testing.T) {
	for _, mode := range []string{"no-content-range", "wrong-range", "zero-total"} {
		t.Run(mode, func(t *testing.T) {
			ps := newProbeServer(mode)
			srv := httptest.NewServer(ps)
			defer srv.Close()

			_, err := probeAgainst(t, srv)
			if err == nil {
				t.Fatalf("Probe with mode %q: expected error, got nil", mode)
			}
			var ce *CodedError
			if !errors.As(err, &ce) || ce.Code != string(domain.ErrorCodeSizeMismatch) {
				t.Fatalf("Probe error = %v, want size_mismatch CodedError", err)
			}
		})
	}
}

func TestProbe_StarTotalUnknown(t *testing.T) {
	ps := newProbeServer("star-total")
	srv := httptest.NewServer(ps)
	defer srv.Close()

	res, err := probeAgainst(t, srv)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !res.SupportsRange {
		t.Fatal("SupportsRange = false, want true")
	}
	if res.TotalBytes != -1 {
		t.Fatalf("TotalBytes = %d, want -1 (total unknown)", res.TotalBytes)
	}
}

func TestProbe_Head405StillProbes(t *testing.T) {
	// The probe never sends HEAD, so a server that refuses HEAD
	// with 405 must still probe fine over the ranged GET.
	ps := newProbeServer("support")
	ps.head405 = true
	srv := httptest.NewServer(ps)
	defer srv.Close()

	res, err := probeAgainst(t, srv)
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if !res.SupportsRange || res.TotalBytes != probeTestContentSize {
		t.Fatalf("Probe result = %+v, want range support with total %d", res, probeTestContentSize)
	}
}

func TestProbe_RangeNotSatisfiable(t *testing.T) {
	ps := newProbeServer("not-satisfiable")
	srv := httptest.NewServer(ps)
	defer srv.Close()

	_, err := probeAgainst(t, srv)
	if err == nil {
		t.Fatal("Probe with 416: expected error, got nil")
	}
}

func TestProbe_HTTPStatusErrors(t *testing.T) {
	cases := []struct {
		mode string
		code domain.ErrorCode
	}{
		{"not-found", domain.ErrorCodeHTTPNotFound},
		{"forbidden", domain.ErrorCodeHTTPAuth},
	}
	for _, tc := range cases {
		t.Run(tc.mode, func(t *testing.T) {
			ps := newProbeServer(tc.mode)
			srv := httptest.NewServer(ps)
			defer srv.Close()

			_, err := probeAgainst(t, srv)
			if err == nil {
				t.Fatalf("Probe with mode %q: expected error, got nil", tc.mode)
			}
			var ce *CodedError
			if !errors.As(err, &ce) || ce.Code != string(tc.code) {
				t.Fatalf("Probe error = %v, want %s CodedError", err, tc.code)
			}
		})
	}
}

func TestProbe_RedirectUsesFinalValidators(t *testing.T) {
	content := make([]byte, probeTestContentSize)
	for i := range content {
		content[i] = byte(i)
	}
	ps := &probeServer{
		content:      content,
		etag:         `"final"`,
		lastModified: probeTestLastMod,
		mode:         "support",
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/old", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/file.bin", http.StatusFound)
	})
	mux.HandleFunc("/file.bin", ps.ServeHTTP)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	cfg := testConfig()
	p := NewProber(cfg, srv.Client())
	res, err := p.Probe(context.Background(), srv.URL+"/old")
	if err != nil {
		t.Fatalf("Probe: %v", err)
	}
	if res.ETag != `"final"` {
		t.Fatalf("ETag = %q, want %q (validator after redirect)", res.ETag, `"final"`)
	}
	if !res.SupportsRange || res.TotalBytes != probeTestContentSize {
		t.Fatalf("Probe result = %+v, want range support with total %d", res, probeTestContentSize)
	}
	ps.assertRangedRequest(t)
}

func TestProbe_InvalidURL(t *testing.T) {
	cfg := testConfig()
	p := NewProber(cfg, NewHTTPClient(cfg))
	if _, err := p.Probe(context.Background(), "://bad-url"); err == nil {
		t.Fatal("Probe with invalid URL: expected error, got nil")
	}
}

func TestProbe_ContextCancelled(t *testing.T) {
	ps := newProbeServer("support")
	srv := httptest.NewServer(ps)
	defer srv.Close()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cfg := testConfig()
	p := NewProber(cfg, srv.Client())
	if _, err := p.Probe(ctx, srv.URL+"/file.bin"); err == nil {
		t.Fatal("Probe with cancelled context: expected error, got nil")
	}
}

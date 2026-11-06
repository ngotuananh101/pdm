// Segment worker tests (Stage 3, Phase 3.1, TDD).
//
// The worker downloads exactly the uncommitted remainder of one
// segment: Range + If-Range, 206 with a matching Content-Range only,
// WriteAt at the matching file offset, 64 KiB bounded reads, and
// cancellation via context.
package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

const (
	workerTestETag     = `"wv1"`
	workerTestWeakETag = `W/"wv1"`
	workerTestLastMod  = "Wed, 01 Jan 2025 00:00:00 GMT"
	workerTestSize     = 100
)

// workerServer is a controllable origin for worker tests. mode selects
// how it answers a ranged GET:
//
//	"range"        206 with the requested range (honors If-Range)
//	"wrong-range"  206 with Content-Range for a different range
//	"ignore"       200 with the full body (Range ignored)
//	"not-satisfiable" 416
//	"drop"         writes part of the range, then drops the connection
//	"etag-changed" 200 when If-Range carries the old ETag (validator changed)
type workerServer struct {
	mu sync.Mutex

	content      []byte
	etag         string
	lastModified string
	mode         string
	dropAfter    int // bytes to write before dropping, mode "drop"

	sawRange         string
	sawIfRange       string
	sawAcceptEnc     string
	rangeRequests    int
	lastRangeRequest string
}

func newWorkerServer(mode string) *workerServer {
	content := make([]byte, workerTestSize)
	for i := range content {
		content[i] = byte(i)
	}
	return &workerServer{
		content:      content,
		etag:         workerTestETag,
		lastModified: workerTestLastMod,
		mode:         mode,
		dropAfter:    10,
	}
}

func (s *workerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.sawRange = r.Header.Get("Range")
	s.sawIfRange = r.Header.Get("If-Range")
	s.sawAcceptEnc = r.Header.Get("Accept-Encoding")
	s.rangeRequests++
	s.lastRangeRequest = s.sawRange
	s.mu.Unlock()

	w.Header().Set("ETag", s.etag)
	w.Header().Set("Last-Modified", s.lastModified)
	switch s.mode {
	case "range":
		// Honor If-Range: a stale validator means the resource
		// changed, so answer 200 with the full body.
		if ir := r.Header.Get("If-Range"); ir != "" && ir != s.etag && ir != s.lastModified {
			w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write(s.content)
			return
		}
		start, end, ok := parseRangeHeader(r.Header.Get("Range"), len(s.content))
		if !ok {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.content)))
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[start : end+1])
	case "wrong-range":
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-24/%d", len(s.content)))
		w.Header().Set("Content-Length", "25")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[:25])
	case "ignore":
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
	case "not-satisfiable":
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(s.content)))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
	case "drop":
		start, end, ok := parseRangeHeader(r.Header.Get("Range"), len(s.content))
		if !ok {
			w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
			return
		}
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.content)))
		w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[start : start+s.dropAfter])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, _ := hj.Hijack()
		_ = conn.Close()
	case "etag-changed":
		// The validator the client sends is stale: answer 200.
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
	default:
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// parseRangeHeader parses "bytes=<start>-<end>" against a resource of
// size total. It returns the clamped range and false when malformed.
func parseRangeHeader(v string, total int) (int, int, bool) {
	var start, end int
	if _, err := fmt.Sscanf(v, "bytes=%d-%d", &start, &end); err != nil {
		return 0, 0, false
	}
	if start < 0 || end < start || end >= total {
		return 0, 0, false
	}
	return start, end, true
}

// workerFile creates a zero-filled temp file of workerTestSize bytes.
func workerFile(t *testing.T) (*os.File, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "part.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(workerTestSize); err != nil {
		t.Fatal(err)
	}
	return f, path
}

func workerTask(seg domain.Segment, url string) SegmentTask {
	return SegmentTask{
		Segment:      seg,
		URL:          url,
		TotalBytes:   workerTestSize,
		ETag:         workerTestETag,
		LastModified: workerTestLastMod,
	}
}

func TestWorker_DownloadsCorrectRange(t *testing.T) {
	ws := newWorkerServer("range")
	srv := httptest.NewServer(ws)
	defer srv.Close()

	f, path := workerFile(t)
	defer f.Close()

	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	seg := domain.Segment{Index: 1, StartOffset: 25, EndOffset: 49, Status: domain.SegmentPending}
	committed, err := w.DownloadSegment(context.Background(), f, workerTask(seg, srv.URL+"/file.bin"), nil)
	if err != nil {
		t.Fatalf("DownloadSegment: %v", err)
	}
	if committed != 25 {
		t.Fatalf("committed = %d, want 25", committed)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < workerTestSize; i++ {
		want := byte(0)
		if i >= 25 && i <= 49 {
			want = byte(i)
		}
		if data[i] != want {
			t.Fatalf("byte[%d] = %d, want %d", i, data[i], want)
		}
	}
	ws.mu.Lock()
	defer ws.mu.Unlock()
	if ws.sawRange != "bytes=25-49" {
		t.Fatalf("Range header = %q, want %q", ws.sawRange, "bytes=25-49")
	}
	if ws.sawIfRange != workerTestETag {
		t.Fatalf("If-Range header = %q, want %q", ws.sawIfRange, workerTestETag)
	}
	if ws.sawAcceptEnc != "identity" {
		t.Fatalf("Accept-Encoding header = %q, want %q", ws.sawAcceptEnc, "identity")
	}
}

func TestWorker_WrongContentRangeWritesNothing(t *testing.T) {
	ws := newWorkerServer("wrong-range")
	srv := httptest.NewServer(ws)
	defer srv.Close()

	f, path := workerFile(t)
	defer f.Close()

	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	seg := domain.Segment{Index: 1, StartOffset: 25, EndOffset: 49, Status: domain.SegmentPending}
	if _, err := w.DownloadSegment(context.Background(), f, workerTask(seg, srv.URL+"/file.bin"), nil); err == nil {
		t.Fatal("DownloadSegment with wrong Content-Range: expected error, got nil")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte[%d] = %d, want 0 (nothing must be written on Content-Range mismatch)", i, b)
		}
	}
}

func TestWorker_200IsErrorAndWritesNothing(t *testing.T) {
	ws := newWorkerServer("ignore")
	srv := httptest.NewServer(ws)
	defer srv.Close()

	f, path := workerFile(t)
	defer f.Close()

	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	seg := domain.Segment{Index: 0, StartOffset: 0, EndOffset: 49, Status: domain.SegmentPending}
	_, err := w.DownloadSegment(context.Background(), f, workerTask(seg, srv.URL+"/file.bin"), nil)
	if err == nil {
		t.Fatal("DownloadSegment with 200: expected error, got nil")
	}
	if !errors.Is(err, ErrRangeIgnored) {
		t.Fatalf("err = %v, want ErrRangeIgnored", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte[%d] = %d, want 0 (a 200 body must never land in a segment)", i, b)
		}
	}
}

func TestWorker_416IsErrorAndWritesNothing(t *testing.T) {
	ws := newWorkerServer("not-satisfiable")
	srv := httptest.NewServer(ws)
	defer srv.Close()

	f, path := workerFile(t)
	defer f.Close()

	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	seg := domain.Segment{Index: 0, StartOffset: 0, EndOffset: 49, Status: domain.SegmentPending}
	_, err := w.DownloadSegment(context.Background(), f, workerTask(seg, srv.URL+"/file.bin"), nil)
	if err == nil {
		t.Fatal("DownloadSegment with 416: expected error, got nil")
	}
	if !errors.Is(err, ErrRangeNotSatisfiable) {
		t.Fatalf("err = %v, want ErrRangeNotSatisfiable", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte[%d] = %d, want 0 (nothing must be written on 416)", i, b)
		}
	}
}

func TestWorker_DroppedConnectionReportsPartialWrite(t *testing.T) {
	ws := newWorkerServer("drop")
	srv := httptest.NewServer(ws)
	defer srv.Close()

	f, path := workerFile(t)
	defer f.Close()

	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	seg := domain.Segment{Index: 0, StartOffset: 0, EndOffset: 49, Status: domain.SegmentPending}
	committed, err := w.DownloadSegment(context.Background(), f, workerTask(seg, srv.URL+"/file.bin"), nil)
	if err == nil {
		t.Fatal("DownloadSegment with dropped connection: expected error, got nil")
	}
	if committed > seg.Size() {
		t.Fatalf("committed = %d, want <= segment size %d", committed, seg.Size())
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := int(committed); i < workerTestSize; i++ {
		if data[i] != 0 {
			t.Fatalf("byte[%d] = %d, want 0 (bytes past the reported write must be untouched)", i, data[i])
		}
	}
}

func TestWorker_ResumesFromCommittedBytes(t *testing.T) {
	ws := newWorkerServer("range")
	srv := httptest.NewServer(ws)
	defer srv.Close()

	f, path := workerFile(t)
	defer f.Close()
	// Simulate 10 bytes already committed at the start of the segment.
	if _, err := f.WriteAt(ws.content[25:35], 25); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	seg := domain.Segment{Index: 1, StartOffset: 25, EndOffset: 49, CommittedBytes: 10, Status: domain.SegmentPending}
	committed, err := w.DownloadSegment(context.Background(), f, workerTask(seg, srv.URL+"/file.bin"), nil)
	if err != nil {
		t.Fatalf("DownloadSegment: %v", err)
	}
	if committed != 25 {
		t.Fatalf("committed = %d, want 25", committed)
	}
	ws.mu.Lock()
	rangeReq := ws.lastRangeRequest
	ws.mu.Unlock()
	if rangeReq != "bytes=35-49" {
		t.Fatalf("Range header = %q, want %q (resume from committed offset)", rangeReq, "bytes=35-49")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := 25; i <= 49; i++ {
		if data[i] != byte(i) {
			t.Fatalf("byte[%d] = %d, want %d", i, data[i], i)
		}
	}
}

func TestWorker_IfRangeValidatorSelection(t *testing.T) {
	cases := []struct {
		name         string
		etag         string
		lastModified string
		wantIfRange  string
	}{
		{"strong ETag wins", workerTestETag, workerTestLastMod, workerTestETag},
		{"weak ETag falls back to Last-Modified", workerTestWeakETag, workerTestLastMod, workerTestLastMod},
		{"no validator sends no If-Range", "", "", ""},
		{"malformed Last-Modified sends no If-Range", "", "not-a-date", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ws := newWorkerServer("range")
			srv := httptest.NewServer(ws)
			defer srv.Close()

			f, _ := workerFile(t)
			defer f.Close()

			cfg := testConfig()
			w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
			task := workerTask(domain.Segment{Index: 0, StartOffset: 0, EndOffset: 9, Status: domain.SegmentPending}, srv.URL+"/file.bin")
			task.ETag = tc.etag
			task.LastModified = tc.lastModified
			if _, err := w.DownloadSegment(context.Background(), f, task, nil); err != nil {
				t.Fatalf("DownloadSegment: %v", err)
			}
			ws.mu.Lock()
			defer ws.mu.Unlock()
			if ws.sawIfRange != tc.wantIfRange {
				t.Fatalf("If-Range = %q, want %q", ws.sawIfRange, tc.wantIfRange)
			}
		})
	}
}

func TestWorker_TwoWorkersDoNotOverlap(t *testing.T) {
	ws := newWorkerServer("range")
	srv := httptest.NewServer(ws)
	defer srv.Close()

	f, path := workerFile(t)
	defer f.Close()

	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	segs := []domain.Segment{
		{Index: 0, StartOffset: 0, EndOffset: 49, Status: domain.SegmentPending},
		{Index: 1, StartOffset: 50, EndOffset: 99, Status: domain.SegmentPending},
	}
	var wg sync.WaitGroup
	errs := make([]error, len(segs))
	for i, seg := range segs {
		wg.Add(1)
		go func(i int, seg domain.Segment) {
			defer wg.Done()
			_, errs[i] = w.DownloadSegment(context.Background(), f, workerTask(seg, srv.URL+"/file.bin"), nil)
		}(i, seg)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("worker %d: %v", i, err)
		}
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i := range data {
		if data[i] != byte(i) {
			t.Fatalf("byte[%d] = %d, want %d (concurrent workers must not overlap)", i, data[i], i)
		}
	}
}

func TestWorker_ContextCancelled(t *testing.T) {
	// Server that streams one byte, then blocks until the client goes away.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", workerTestETag)
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-99/%d", workerTestSize))
		w.Header().Set("Content-Length", "100")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write([]byte{0})
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	defer srv.Close()

	f, _ := workerFile(t)
	defer f.Close()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	seg := domain.Segment{Index: 0, StartOffset: 0, EndOffset: 99, Status: domain.SegmentPending}
	_, err := w.DownloadSegment(ctx, f, workerTask(seg, srv.URL+"/file.bin"), nil)
	<-done
	if err == nil {
		t.Fatal("DownloadSegment with cancelled context: expected error, got nil")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

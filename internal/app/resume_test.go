// Service-level resume and manager-dispatch tests (Stage 3,
// Phase 4.1 and 4.4, TDD).
//
// With the manager wired in, Pause keeps the .part file and the
// segment plan, and Resume continues from the committed bytes: the
// server is only asked for the remaining ranges, and the finished
// file hashes correctly. Dispatch goes through the manager for Range
// servers and falls back to single-stream for servers without Range.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/engine"
	"github.com/ngotuananh101/pdm/internal/storage/sqlite"
)

// resumeServer is a slow Range origin for pause/resume tests. It
// honors Range + If-Range with ETag "mv1", streams ranged responses
// in 4 KiB chunks with a delay, and counts the bytes actually served
// in non-probe ranged responses (the probe, bytes=0-0, is excluded).
type resumeServer struct {
	mu sync.Mutex

	content    []byte
	chunkDelay time.Duration
	served     int64
}

func newResumeServer(size int, chunkDelay time.Duration) *resumeServer {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 251)
	}
	return &resumeServer{content: content, chunkDelay: chunkDelay}
}

func (s *resumeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	rangeHeader := r.Header.Get("Range")
	w.Header().Set("ETag", `"mv1"`)
	w.Header().Set("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
	if rangeHeader == "bytes=0-0" {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(s.content)))
		w.Header().Set("Content-Length", "1")
		w.WriteHeader(http.StatusPartialContent)
		_, _ = w.Write(s.content[:1])
		return
	}
	if rangeHeader == "" {
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
		return
	}
	if v := r.Header.Get("If-Range"); v != "" && v != `"mv1"` {
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
		return
	}
	start, end, ok := parseTestRange(rangeHeader, len(s.content))
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(s.content)))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.content)))
	w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	for off := start; off <= end; off += 4096 {
		chunkEnd := off + 4096
		if chunkEnd > end+1 {
			chunkEnd = end + 1
		}
		n, _ := w.Write(s.content[off:chunkEnd])
		s.mu.Lock()
		s.served += int64(n)
		s.mu.Unlock()
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		time.Sleep(s.chunkDelay)
	}
}

func (s *resumeServer) servedBytes() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.served
}

func (s *resumeServer) contentHash() string {
	sum := sha256.Sum256(s.content)
	return hex.EncodeToString(sum[:])
}

func parseTestRange(v string, total int) (int, int, bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(v, prefix) {
		return 0, 0, false
	}
	rest := strings.TrimPrefix(v, prefix)
	dash := strings.Index(rest, "-")
	if dash < 0 {
		return 0, 0, false
	}
	start, err1 := strconv.Atoi(rest[:dash])
	end, err2 := strconv.Atoi(rest[dash+1:])
	if err1 != nil || err2 != nil || start < 0 || end < start || end >= total {
		return 0, 0, false
	}
	return start, end, true
}

// newManagerService builds a repo-backed service with the manager
// wired in (segment repo + real prober against srv). The plain
// downloader is a fail stub: if the manager path is ever bypassed,
// the job fails loudly instead of silently downloading another way.
func newManagerService(t *testing.T, srv *httptest.Server) (*Service, string, *sqlite.DownloadRepository, *sqlite.SegmentRepository) {
	t.Helper()
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	segRepo := sqlite.NewSegmentRepository(db.DB)
	cfg := testConfig(t)
	cfg.MinSegmentSize = 1024
	cfg.SegmentsPerDownload = 4
	cfg.CheckpointInterval = 50 * time.Millisecond
	cfg.ProgressInterval = 50 * time.Millisecond
	cfg.MaxActiveDownloads = 1
	mgr := engine.NewManager(cfg, srv.Client(), nil, segRepo)
	svc := NewServiceWithRepository(cfg, &failStub{err: errors.New("downloader must not be called when manager is wired")}, repo)
	svc.SetSegmentRepository(segRepo)
	svc.SetManager(mgr)
	svc.SetProber(engine.NewProber(cfg, srv.Client()))
	t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })
	return svc, "", repo, segRepo
}

func addManagerJob(t *testing.T, svc *Service, srv *httptest.Server, dir string) string {
	t.Helper()
	id, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/file.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func sha256FileHex(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestResume_PauseResumeDownloadsOnlyRemainder(t *testing.T) {
	const size = 1_000_000
	for _, pauseAt := range []int64{250_000, 650_000} {
		t.Run(fmt.Sprintf("pause at %d", pauseAt), func(t *testing.T) {
			rs := newResumeServer(size, 15*time.Millisecond)
			srv := httptest.NewServer(rs)
			defer srv.Close()
			svc, _, _, _ := newManagerService(t, srv)
			dir := t.TempDir()
			id := addManagerJob(t, svc, srv, dir)

			waitFor(t, func() bool {
				return getJob(svc, id).DownloadedBytes >= pauseAt
			}, "download reaching pause point")
			downloadedAtPause := getJob(svc, id).DownloadedBytes
			servedBeforePause := rs.servedBytes()
			if downloadedAtPause <= 0 || downloadedAtPause >= size {
				t.Fatalf("DownloadedBytes at pause = %d, want a partial download (0 < n < %d)", downloadedAtPause, size)
			}
			if servedBeforePause <= 0 {
				t.Fatal("server served 0 bytes before pause")
			}

			if err := svc.Pause(id); err != nil {
				t.Fatalf("Pause: %v", err)
			}
			snap := getJob(svc, id)
			if snap.Status != domain.StatusPaused {
				t.Fatalf("status after Pause = %q, want paused", snap.Status)
			}
			if snap.TempPath == "" {
				t.Fatal("TempPath empty after Pause: the .part path must be kept")
			}
			if _, err := os.Stat(snap.TempPath); err != nil {
				t.Fatalf(".part missing after Pause: %v", err)
			}

			if err := svc.Resume(id); err != nil {
				t.Fatalf("Resume: %v", err)
			}
			deadline := time.Now().Add(10 * time.Second)
			for {
				snap := getJob(svc, id)
				if snap.Status == domain.StatusCompleted {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("job did not complete after Resume: status=%q downloaded=%d total=%d err=%q code=%q temp=%q served=%d",
						snap.Status, snap.DownloadedBytes, snap.TotalBytes, snap.ErrorMessage, snap.ErrorCode, snap.TempPath, rs.servedBytes())
				}
				time.Sleep(100 * time.Millisecond)
			}
			final := getJob(svc, id)
			if got := sha256FileHex(t, final.FinalPath); got != rs.contentHash() {
				t.Fatalf("final SHA-256 = %s, want %s", got, rs.contentHash())
			}

			// The resumed run must only fetch (roughly) the
			// remainder: the bytes already committed before the
			// pause, plus a bounded tail — progress reporting lags
			// the server by up to one progress interval, and
			// bytes written but not yet checkpointed are
			// re-fetched (up to ~one checkpoint interval of
			// writes per segment, plus one in-flight chunk each).
			// At the test's 50 ms intervals and 15 ms chunks
			// that tail is at most ~8 chunks per segment, i.e.
			// 128 KiB over 4 segments. A restart from scratch
			// would serve the whole file again on resume.
			servedAfterResume := rs.servedBytes() - servedBeforePause
			remaining := int64(size) - downloadedAtPause
			if servedAfterResume > remaining+128*1024 {
				t.Fatalf("resumed run served %d bytes, want at most %d (remaining %d + 128 KiB in-flight slack)", servedAfterResume, remaining+128*1024, remaining)
			}
			if servedAfterResume >= size {
				t.Fatalf("resumed run served %d bytes, want < %d (a full re-download)", servedAfterResume, size)
			}
		})
	}
}

func TestDispatch_RangeUsesManager(t *testing.T) {
	const size = 100_000
	rs := newResumeServer(size, 0)
	srv := httptest.NewServer(rs)
	defer srv.Close()
	svc, _, _, segRepo := newManagerService(t, srv)
	dir := t.TempDir()
	id := addManagerJob(t, svc, srv, dir)

	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusCompleted
	}, "job completed")
	final := getJob(svc, id)
	if final.FinalPath == "" {
		t.Fatal("FinalPath empty after manager download")
	}
	if got := sha256FileHex(t, final.FinalPath); got != rs.contentHash() {
		t.Fatalf("final SHA-256 = %s, want %s", got, rs.contentHash())
	}
	segs, err := segRepo.GetByDownload(context.Background(), id)
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if len(segs) != 4 {
		t.Fatalf("segment plan has %d segments, want 4 (Range download must go through the manager)", len(segs))
	}
	for i, s := range segs {
		if s.Status != domain.SegmentComplete {
			t.Fatalf("segment[%d].Status = %q, want complete", i, s.Status)
		}
	}
}

func TestDispatch_NoRangeUsesSingleStream(t *testing.T) {
	content := make([]byte, 50_000)
	for i := range content {
		content[i] = byte(i % 251)
	}
	wantHash := sha256.Sum256(content)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// No Range support at all: every request gets the full body.
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()
	svc, _, _, segRepo := newManagerService(t, srv)
	dir := t.TempDir()
	id := addManagerJob(t, svc, srv, dir)

	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusCompleted
	}, "job completed")
	final := getJob(svc, id)
	if final.FinalPath == "" {
		t.Fatal("FinalPath empty after single-stream fallback")
	}
	if got := sha256FileHex(t, final.FinalPath); got != hex.EncodeToString(wantHash[:]) {
		t.Fatalf("final SHA-256 = %s, want %s", got, hex.EncodeToString(wantHash[:]))
	}
	segs, err := segRepo.GetByDownload(context.Background(), id)
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if len(segs) != 0 {
		t.Fatalf("segment plan has %d segments, want 0 (no-Range download must use the single-stream fallback)", len(segs))
	}
}

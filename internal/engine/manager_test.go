// Manager tests (Stage 3, Phase 3.3, TDD).
//
// The manager runs one download: probe, then segmented download
// with up to SegmentsPerDownload workers when Range is verified, or
// the single-stream fallback otherwise. A 200/416 mid-download
// re-probes and restarts from scratch (decisions 2 and 3); cancel
// stops all workers and writes nothing after Run returns.
package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

const managerTestSize = 100_000

// managerServer is a controllable origin for manager tests. mode:
//
//	"range"       honors Range + If-Range, ETag "mv1"
//	"no-range"    always 200 with the full body
//	"etag-change" first probe says ETag "mv1"; a ranged GET with
//	              If-Range "mv1" answers 200 (validator changed);
//	              later probes say "v2" and v2 requests get 206
//	"probe-500"   any request with a Range header gets 500; a plain
//	              GET gets 200 (probe fails, plain GET works)
//	"flaky"       first ranged request per range gets 500, then 206
//	"slow"        streams ranged responses in 4 KiB chunks with a delay
type managerServer struct {
	mu sync.Mutex

	content []byte
	mode    string

	etag       string
	changed    bool
	probes     int
	rangeCalls map[string]int
	chunkDelay time.Duration
}

func newManagerServer(mode string, size int) *managerServer {
	content := make([]byte, size)
	for i := range content {
		content[i] = byte(i % 251)
	}
	return &managerServer{
		content:    content,
		mode:       mode,
		etag:       `"mv1"`,
		rangeCalls: map[string]int{},
		chunkDelay: 15 * time.Millisecond,
	}
}

func (s *managerServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	rangeHeader := r.Header.Get("Range")
	ifRange := r.Header.Get("If-Range")
	s.mu.Unlock()

	isProbe := rangeHeader == "bytes=0-0"
	switch s.mode {
	case "no-range":
		w.Header().Set("ETag", s.etag)
		w.Header().Set("Last-Modified", workerTestLastMod)
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
		return
	case "probe-500":
		if rangeHeader != "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
		return
	}

	// Modes with real Range support: probe counts, ETag changes.
	s.mu.Lock()
	if isProbe {
		s.probes++
	}
	etag := s.etag
	if s.mode == "etag-change" && s.changed {
		etag = `"v2"`
	}
	s.mu.Unlock()

	w.Header().Set("ETag", etag)
	w.Header().Set("Last-Modified", workerTestLastMod)

	if s.mode == "etag-change" && !isProbe && ifRange == `"mv1"` && !s.changed {
		// The validator changed mid-download: answer 200.
		s.mu.Lock()
		s.changed = true
		s.mu.Unlock()
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
		return
	}

	// A plain GET (no Range) gets the whole body.
	if rangeHeader == "" {
		w.Header().Set("Content-Length", fmt.Sprint(len(s.content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(s.content)
		return
	}

	start, end, ok := parseRangeHeader(rangeHeader, len(s.content))
	if !ok {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", len(s.content)))
		w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
		return
	}

	if s.mode == "flaky" && !isProbe {
		s.mu.Lock()
		s.rangeCalls[rangeHeader]++
		calls := s.rangeCalls[rangeHeader]
		s.mu.Unlock()
		if calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}

	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(s.content)))
	w.Header().Set("Content-Length", fmt.Sprint(end-start+1))
	w.WriteHeader(http.StatusPartialContent)
	if s.mode == "slow" {
		for off := start; off <= end; off += 4096 {
			chunkEnd := off + 4096
			if chunkEnd > end+1 {
				chunkEnd = end + 1
			}
			_, _ = w.Write(s.content[off:chunkEnd])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(s.chunkDelay)
		}
		return
	}
	_, _ = w.Write(s.content[start : end+1])
}

func (s *managerServer) probeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.probes
}

func newManagerTest(t *testing.T, mode string, size int) (*managerServer, *httptest.Server, *Manager, *fakeSegmentRepo) {
	t.Helper()
	ms := newManagerServer(mode, size)
	srv := httptest.NewServer(ms)
	cfg := testConfig()
	cfg.MinSegmentSize = 1024
	cfg.SegmentsPerDownload = 4
	cfg.CheckpointInterval = 50 * time.Millisecond
	cfg.BufferSize = 4 << 10
	repo := newFakeSegmentRepo()
	m := NewManager(cfg, NewHTTPClient(cfg), nil, repo)
	m.sleep = func(time.Duration) {}
	return ms, srv, m, repo
}

func managerReq(id, url, dest string) ManagerRequest {
	return ManagerRequest{
		ID:             id,
		URL:            url,
		DestinationDir: dest,
		Filename:       "file.bin",
		Mode:           domain.FinalizeOverwrite,
	}
}

func sha256File(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func TestManager_SegmentedDownload_SHA256(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "range", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	res, err := m.Run(context.Background(), managerReq("m1", srv.URL+"/file.bin", dest))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.UsedSegments {
		t.Fatal("UsedSegments = false, want true (server supports Range)")
	}
	if res.WrittenBytes != managerTestSize || res.TotalBytes != managerTestSize {
		t.Fatalf("WrittenBytes/TotalBytes = %d/%d, want %d/%d", res.WrittenBytes, res.TotalBytes, managerTestSize, managerTestSize)
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s", got, wantHash)
	}
	// The plan was persisted before the run, and every segment is complete.
	segs, err := repo.GetByDownload(context.Background(), "m1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if len(segs) != 4 {
		t.Fatalf("plan has %d segments, want 4", len(segs))
	}
	for i, s := range segs {
		if s.Status != domain.SegmentComplete {
			t.Fatalf("segment[%d].Status = %q, want complete", i, s.Status)
		}
		if s.CommittedBytes != s.Size() {
			t.Fatalf("segment[%d].CommittedBytes = %d, want %d", i, s.CommittedBytes, s.Size())
		}
	}
}

func TestManager_NoRangeFallsBackToSingle(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "no-range", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	res, err := m.Run(context.Background(), managerReq("m2", srv.URL+"/file.bin", dest))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.UsedSegments {
		t.Fatal("UsedSegments = true, want false (server ignores Range)")
	}
	if res.FinalPath == "" {
		t.Fatal("FinalPath empty after single-stream fallback")
	}
	if got := sha256File(t, res.FinalPath); got != wantHash {
		t.Fatalf("final SHA-256 = %s, want %s", got, wantHash)
	}
	// No segment plan is stored for a single-stream download.
	segs, err := repo.GetByDownload(context.Background(), "m2")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if len(segs) != 0 {
		t.Fatalf("plan has %d segments, want 0 (single-stream fallback)", len(segs))
	}
}

func TestManager_ETagChangeRestartsFromScratch(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "etag-change", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	res, err := m.Run(context.Background(), managerReq("m3", srv.URL+"/file.bin", dest))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.UsedSegments {
		t.Fatal("UsedSegments = false, want true (restarted segmented download)")
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s (restart must not mix old-version bytes)", got, wantHash)
	}
	if got := ms.probeCount(); got < 2 {
		t.Fatalf("probe count = %d, want >= 2 (re-probe after validator change)", got)
	}
	// The stale plan was deleted and a fresh one stored.
	if repo.deleteCalls < 1 {
		t.Fatalf("DeleteByDownload calls = %d, want >= 1 (stale plan removed before restart)", repo.deleteCalls)
	}
	if repo.insertPlans < 2 {
		t.Fatalf("InsertPlan calls = %d, want >= 2 (fresh plan after restart)", repo.insertPlans)
	}
}

func TestManager_CancelStopsWrites(t *testing.T) {
	_, srv, m, _ := newManagerTest(t, "slow", 200_000)
	defer srv.Close()

	dest := t.TempDir()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var res ManagerResult
	var runErr error
	go func() {
		defer close(done)
		res, runErr = m.Run(ctx, managerReq("m4", srv.URL+"/file.bin", dest))
	}()
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if runErr == nil {
		t.Fatal("Run after cancel: expected error, got nil")
	}
	// No writes after Run returns: the .part size must be stable.
	partPath := filepath.Join(dest, ".file.bin.m4.part")
	size1, err := fileSize(partPath)
	if err != nil {
		t.Fatalf("stat .part: %v (res=%+v)", err, res)
	}
	if size1 == 0 || size1 >= 200_000 {
		t.Fatalf(".part size after cancel = %d, want partial (0 < size < 200000)", size1)
	}
	time.Sleep(300 * time.Millisecond)
	size2, err := fileSize(partPath)
	if err != nil {
		t.Fatal(err)
	}
	if size2 != size1 {
		t.Fatalf(".part size changed after Run returned: %d -> %d (writes must stop)", size1, size2)
	}
}

func fileSize(path string) (int64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, err
	}
	return fi.Size(), nil
}

func TestManager_ProbeErrorFallsBackToSingle(t *testing.T) {
	ms, srv, m, _ := newManagerTest(t, "probe-500", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	res, err := m.Run(context.Background(), managerReq("m5", srv.URL+"/file.bin", dest))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if res.UsedSegments {
		t.Fatal("UsedSegments = true, want false (probe failed, plain GET works)")
	}
	if got := sha256File(t, res.FinalPath); got != wantHash {
		t.Fatalf("final SHA-256 = %s, want %s", got, wantHash)
	}
}

func TestManager_TransientWorkerErrorRetries(t *testing.T) {
	ms, srv, m, _ := newManagerTest(t, "flaky", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	res, err := m.Run(context.Background(), managerReq("m6", srv.URL+"/file.bin", dest))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if !res.UsedSegments {
		t.Fatal("UsedSegments = false, want true")
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s", got, wantHash)
	}
	// At least one range was requested twice (first attempt 500, retry 206).
	ms.mu.Lock()
	retried := false
	for _, calls := range ms.rangeCalls {
		if calls >= 2 {
			retried = true
		}
	}
	ms.mu.Unlock()
	if !retried {
		t.Fatal("no range was retried, want at least one 500 -> retry")
	}
}

func TestManager_PeriodicCheckpointPersistsMidDownload(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "slow", 200_000)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	done := make(chan error, 1)
	var res ManagerResult
	go func() {
		var err error
		res, err = m.Run(context.Background(), managerReq("m7", srv.URL+"/file.bin", dest))
		done <- err
	}()

	// While the download is still running, a periodic checkpoint
	// must already have published some committed bytes.
	deadline := time.Now().Add(5 * time.Second)
	sawProgress := false
	for time.Now().Before(deadline) {
		select {
		case err := <-done:
			t.Fatalf("Run finished before mid-download checkpoint was observed: %v", err)
		default:
		}
		if total := repo.committedTotal("m7"); total > 0 && total < 200_000 {
			sawProgress = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !sawProgress {
		t.Fatal("no mid-download checkpoint observed (committed stayed 0 until the end)")
	}

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not finish")
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s", got, wantHash)
	}
}

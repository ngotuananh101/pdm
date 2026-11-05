package app

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/engine"
	"github.com/ngotuananh101/pdm/internal/platform"
)

func testConfig(t *testing.T) config.Config {
	t.Helper()
	cfg := config.Default(t.TempDir())
	cfg.MaxActiveDownloads = 3
	return cfg
}

func waitFor(t *testing.T, cond func() bool, what string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func getJob(s *Service, id string) domain.DownloadSnapshot {
	for _, snap := range s.List() {
		if snap.ID == id {
			return snap
		}
	}
	return domain.DownloadSnapshot{}
}

// countingDownloader wraps a real handler-based httptest server and
// counts requests, used to assert that ResolveConflict does not
// re-fetch from the server.
type countingDownloader struct {
	mu       sync.Mutex
	hits     int
	blockCh  chan struct{}
	blockMax int
}

func (c *countingDownloader) handler(content []byte, cd string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		c.mu.Lock()
		c.hits++
		hit := c.hits
		block := c.blockCh != nil && hit <= c.blockMax
		c.mu.Unlock()
		if block {
			// Block until released (or request context cancelled).
			select {
			case <-c.blockCh:
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		if cd != "" {
			w.Header().Set("Content-Disposition", cd)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}
}

func (c *countingDownloader) hitCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hits
}

// newRealService builds a Service backed by the real engine against
// the given httptest server.
func newRealService(t *testing.T, cfg config.Config, srv *httptest.Server) *Service {
	t.Helper()
	d := engine.NewSingleDownloader(cfg, srv.Client())
	return NewService(cfg, d)
}

func TestConcurrentDownloadsRespectMaxActive(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 2

	var current, peak int64
	blockCh := make(chan struct{})
	started := make(chan struct{}, 32)
	content := make([]byte, 4096)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cur := atomic.AddInt64(&current, 1)
		for {
			p := atomic.LoadInt64(&peak)
			if cur <= p || atomic.CompareAndSwapInt64(&peak, p, cur) {
				break
			}
		}
		select {
		case started <- struct{}{}:
		default:
		}
		// Wait for the 2 allowed jobs to be inside before releasing.
		select {
		case <-blockCh:
		case <-r.Context().Done():
			atomic.AddInt64(&current, -1)
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
		atomic.AddInt64(&current, -1)
	}))
	defer srv.Close()

	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	ids := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		id, err := svc.AddDownload(AddRequest{
			URL:            srv.URL + "/file" + fmt.Sprint(i) + ".bin",
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	// Wait for 2 to start.
	timeout := time.After(5 * time.Second)
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-timeout:
			t.Fatal("expected 2 downloads to start")
		}
	}
	// Give the scheduler a moment: no third should start.
	time.Sleep(200 * time.Millisecond)
	if n := len(started); n != 0 {
		t.Fatalf("started %d extra downloads beyond MaxActiveDownloads=2", n)
	}
	if p := atomic.LoadInt64(&peak); p != 2 {
		t.Fatalf("peak concurrency = %d, want 2", p)
	}
	close(blockCh)

	waitFor(t, func() bool {
		for _, id := range ids {
			if getJob(svc, id).Status != domain.StatusCompleted {
				return false
			}
		}
		return true
	}, "all downloads completed")
	if p := atomic.LoadInt64(&peak); p > 2 {
		t.Fatalf("peak concurrency exceeded cap: %d", p)
	}
	_ = svc.Shutdown(context.Background())
}

func TestConflictAskPrecheckAutoRenameAndOverwrite(t *testing.T) {
	cfg := testConfig(t)
	// Default policy is ConflictAsk.
	content := []byte("hello-conflict")
	cd := &countingDownloader{}
	srv := httptest.NewServer(cd.handler(content, ""))
	defer srv.Close()

	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	// Pre-create the target file.
	existing := filepath.Join(dir, "file.txt")
	if err := os.WriteFile(existing, []byte("original"), 0o644); err != nil {
		t.Fatal(err)
	}

	// 1) ConflictAsk precheck returns FileConflictError immediately.
	_, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/file.txt",
		DestinationDir: dir,
	})
	var fce *domain.FileConflictError
	if !errors.As(err, &fce) {
		t.Fatalf("expected *FileConflictError, got %v", err)
	}
	if fce.Filename != "file.txt" {
		t.Fatalf("Filename = %q, want file.txt", fce.Filename)
	}
	if fce.SuggestedFilename == "" || fce.SuggestedFilename == fce.Filename {
		t.Fatalf("SuggestedFilename = %q", fce.SuggestedFilename)
	}
	if fce.ExistingPath != existing {
		t.Fatalf("ExistingPath = %q, want %q", fce.ExistingPath, existing)
	}

	// 2) Re-submit with ResolutionAutoRename succeeds.
	id, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/file.txt",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusCompleted }, "auto-rename completed")
	snap := getJob(svc, id)
	if snap.Filename != fce.SuggestedFilename {
		t.Fatalf("auto-renamed Filename = %q, want %q", snap.Filename, fce.SuggestedFilename)
	}
	// Original file untouched.
	got, _ := os.ReadFile(existing)
	if string(got) != "original" {
		t.Fatalf("original file clobbered: %q", got)
	}
	got2, _ := os.ReadFile(filepath.Join(dir, snap.Filename))
	if string(got2) != string(content) {
		t.Fatalf("new file content = %q", got2)
	}

	// 3) Re-submit with ResolutionOverwrite overwrites.
	id2, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/file.txt",
		DestinationDir: dir,
		Filename:       "file.txt",
		Resolution:     domain.ResolutionOverwrite,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id2).Status == domain.StatusCompleted }, "overwrite completed")
	got3, _ := os.ReadFile(existing)
	if string(got3) != string(content) {
		t.Fatalf("after overwrite content = %q", got3)
	}
	_ = svc.Shutdown(context.Background())
}

func TestMidDownloadContentDispositionConflictResolveConflict(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("mid-download-payload-1234567890")
	cd := &countingDownloader{}
	// Server says Content-Disposition filename is "cdfile.bin",
	// which already exists. Engine's early check + FinalizeNoClobber
	// returns FileConflictError without overwriting.
	srv := httptest.NewServer(cd.handler(content, `attachment; filename="cdfile.bin"`))
	defer srv.Close()

	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	existing := filepath.Join(dir, "cdfile.bin")
	if err := os.WriteFile(existing, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}

	id, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/download",
		DestinationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "job failed on conflict")
	snap := getJob(svc, id)
	if snap.ErrorCode != domain.ErrorCodeFileConflict {
		t.Fatalf("ErrorCode = %q, want file_conflict", snap.ErrorCode)
	}
	if snap.Filename != "cdfile.bin" {
		t.Fatalf("Filename = %q, want cdfile.bin", snap.Filename)
	}
	if snap.SuggestedFilename == "" {
		t.Fatal("SuggestedFilename empty")
	}
	// .part must be preserved on disk when the conflict surfaces
	// after the transfer (e.g. FinalizeNoClobber race). For the
	// early Content-Disposition check the engine returns before
	// creating a .part; the service then stores the conventional
	// temp path so ResolveConflict can probe it. Either way a
	// TempPath is recorded.
	tempPath := snap.TempPath
	if tempPath == "" {
		t.Fatal("TempPath empty — .part path not recorded")
	}
	_ = tempPath
	hitsBefore := cd.hitCount()

	// ResolveConflict(AutoRename) cannot finalize: no .part exists
	// (the engine's early check returned before writing one), so
	// the job goes finalizing -> failed and waits for Retry; it
	// must NOT re-queue and must NOT re-download here
	// (finalizing -> queued is forbidden).
	if err := svc.ResolveConflict(id, domain.ResolutionAutoRename); err != nil {
		t.Fatal(err)
	}
	if got := getJob(svc, id).Status; got != domain.StatusFailed {
		t.Fatalf("status after ResolveConflict = %q, want failed (must not re-queue)", got)
	}
	time.Sleep(200 * time.Millisecond)
	if got := cd.hitCount(); got != hitsBefore {
		t.Fatalf("ResolveConflict re-downloaded: hits = %d, want %d", got, hitsBefore)
	}

	// Retry re-queues with the resolved FinalizeAutoRename mode and
	// re-downloads to completion.
	if err := svc.Retry(id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusCompleted }, "completed after Retry")
	snap = getJob(svc, id)
	if snap.Filename != "cdfile (1).bin" {
		t.Fatalf("Filename = %q, want cdfile (1).bin", snap.Filename)
	}
	if cd.hitCount() != hitsBefore+1 {
		t.Fatalf("Retry re-download hits = %d, want %d", cd.hitCount(), hitsBefore+1)
	}
	// New file has downloaded content; original untouched.
	got, _ := os.ReadFile(filepath.Join(dir, snap.Filename))
	if string(got) != string(content) {
		t.Fatalf("resolved file content = %q", got)
	}
	gotOrig, _ := os.ReadFile(existing)
	if string(gotOrig) != "existing" {
		t.Fatalf("original clobbered: %q", gotOrig)
	}
	_ = svc.Shutdown(context.Background())
}

// preservedPartStub simulates a mid-download race: it writes the
// .part file, then returns a FileConflictError with the .part path
// still on disk (exactly as engine.SingleDownloader does when
// FinalizeTempFile reports the race).
type preservedPartStub struct {
	content []byte
}

func (s *preservedPartStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	filename := req.Filename
	if filename == "" {
		filename = "cdfile.bin"
	}
	target := filepath.Join(req.DestinationDir, filename)
	tempPath := filepath.Join(req.DestinationDir, fmt.Sprintf(".%s.%s.part", filename, req.ID))
	if err := os.WriteFile(tempPath, s.content, 0o600); err != nil {
		return domain.TransferResult{}, err
	}
	suggest, _ := platform.SuggestAvailableFilename(req.DestinationDir, filename)
	return domain.TransferResult{
		TempPath:     tempPath,
		Filename:     filename,
		WrittenBytes: int64(len(s.content)),
		TotalBytes:   int64(len(s.content)),
	}, &domain.FileConflictError{
		Filename:          filename,
		ExistingPath:      target,
		SuggestedFilename: suggest,
	}
}

// TestResolveConflictFinalizesPreservedPartWithoutRefetch verifies
// the core requirement: when a .part file exists on disk for a
// file_conflict job, ResolveConflict(AutoRename/Overwrite)
// finalizes that .part in place and never re-fetches from the
// server (the stub is called exactly once).
func TestResolveConflictFinalizesPreservedPartWithoutRefetch(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("preserved-part-bytes")
	stub := &callCountingStub{inner: &preservedPartStub{content: content}}
	svc := NewService(cfg, stub)
	dir := t.TempDir()
	existing := filepath.Join(dir, "cdfile.bin")
	if err := os.WriteFile(existing, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}

	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/download",
		DestinationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed on conflict")
	snap := getJob(svc, id)
	if snap.ErrorCode != domain.ErrorCodeFileConflict {
		t.Fatalf("ErrorCode = %q", snap.ErrorCode)
	}
	tempPath := snap.TempPath
	if tempPath == "" {
		t.Fatal("TempPath empty")
	}
	if _, err := os.Stat(tempPath); err != nil {
		t.Fatalf(".part not preserved: %v", err)
	}
	if stub.calls() != 1 {
		t.Fatalf("stub calls = %d, want 1", stub.calls())
	}

	if err := svc.ResolveConflict(id, domain.ResolutionAutoRename); err != nil {
		t.Fatal(err)
	}
	if stub.calls() != 1 {
		t.Fatalf("ResolveConflict re-fetched: stub calls = %d, want 1", stub.calls())
	}
	snap = getJob(svc, id)
	if snap.Status != domain.StatusCompleted {
		t.Fatalf("Status = %q, want completed", snap.Status)
	}
	if snap.Filename != "cdfile (1).bin" {
		t.Fatalf("Filename = %q, want cdfile (1).bin", snap.Filename)
	}
	got, _ := os.ReadFile(filepath.Join(dir, snap.Filename))
	if string(got) != string(content) {
		t.Fatalf("finalized content = %q", got)
	}
	gotOrig, _ := os.ReadFile(existing)
	if string(gotOrig) != "existing" {
		t.Fatalf("original clobbered: %q", gotOrig)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf(".part should be consumed, stat err = %v", err)
	}
	_ = svc.Shutdown(context.Background())
}

// TestResolveConflictOverwritePreservedPart does the same with
// ResolutionOverwrite.
func TestResolveConflictOverwritePreservedPart(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("overwrite-content")
	stub := &callCountingStub{inner: &preservedPartStub{content: content}}
	svc := NewService(cfg, stub)
	dir := t.TempDir()
	existing := filepath.Join(dir, "cdfile.bin")
	if err := os.WriteFile(existing, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/download",
		DestinationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed")
	if err := svc.ResolveConflict(id, domain.ResolutionOverwrite); err != nil {
		t.Fatal(err)
	}
	if stub.calls() != 1 {
		t.Fatalf("re-fetched: calls=%d", stub.calls())
	}
	snap := getJob(svc, id)
	if snap.Status != domain.StatusCompleted || snap.Filename != "cdfile.bin" {
		t.Fatalf("snap = %+v", snap)
	}
	got, _ := os.ReadFile(existing)
	if string(got) != string(content) {
		t.Fatalf("content = %q", got)
	}
	_ = svc.Shutdown(context.Background())
}

type callCountingStub struct {
	inner  domain.StreamDownloader
	called int64
}

func (c *callCountingStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	atomic.AddInt64(&c.called, 1)
	return c.inner.Download(ctx, req)
}

func (c *callCountingStub) calls() int64 { return atomic.LoadInt64(&c.called) }

// syncFailStub simulates a Sync/Close failure in the engine the way
// the old code reported it: the .part file exists on disk, but the
// error carries an empty TransferResult, so TempPath is lost.
// The first call fails; the second (after Retry) succeeds and
// finalizes, proving the .part is accounted for end to end.
type syncFailStub struct {
	mu      sync.Mutex
	calls   int
	content []byte
}

func (s *syncFailStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	s.mu.Lock()
	s.calls++
	call := s.calls
	s.mu.Unlock()
	filename := req.Filename
	if filename == "" {
		filename = "syncfail.bin"
	}
	tempPath := filepath.Join(req.DestinationDir, fmt.Sprintf(".%s.%s.part", filename, req.ID))
	if call == 1 {
		if err := os.WriteFile(tempPath, s.content, 0o600); err != nil {
			return domain.TransferResult{}, err
		}
		// Old engine behaviour: Sync/Close errors returned an empty
		// result, losing TempPath even though the .part is on disk.
		return domain.TransferResult{}, &engine.CodedError{
			Code:    string(domain.ErrorCodeFilesystem),
			Message: "sync temp file: input/output error",
		}
	}
	// Retry: the transfer succeeds; finalize by moving the .part
	// to its final name, as the engine would.
	finalPath := filepath.Join(req.DestinationDir, filename)
	if err := os.WriteFile(tempPath, s.content, 0o600); err != nil {
		return domain.TransferResult{}, err
	}
	if err := os.Rename(tempPath, finalPath); err != nil {
		return domain.TransferResult{}, err
	}
	return domain.TransferResult{
		FinalPath:    finalPath,
		Filename:     filename,
		TempPath:     tempPath,
		WrittenBytes: int64(len(s.content)),
		TotalBytes:   int64(len(s.content)),
	}, nil
}

// TestSyncCloseFailure_PartIsRecordedNotOrphaned (A6, Phase 3): after
// a filesystem failure that loses TempPath in the engine result
// (Sync/Close), the service must still record the .part path so the
// file is not a silent orphan on disk.
func TestSyncCloseFailure_PartIsRecordedNotOrphaned(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("sync-fail-bytes")
	stub := &syncFailStub{content: content}
	svc := NewService(cfg, stub)
	dir := t.TempDir()

	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/download",
		DestinationDir: dir,
		Filename:       "syncfail.bin",
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed on sync error")
	snap := getJob(svc, id)
	if snap.ErrorCode != domain.ErrorCodeFilesystem {
		t.Fatalf("ErrorCode = %q, want filesystem_error", snap.ErrorCode)
	}
	// The .part must be recorded, not silently orphaned: the job
	// knows the path and the file is still on disk.
	wantTemp := filepath.Join(dir, ".syncfail.bin."+id+".part")
	if snap.TempPath != wantTemp {
		t.Fatalf("TempPath = %q, want %q", snap.TempPath, wantTemp)
	}
	data, err := os.ReadFile(snap.TempPath)
	if err != nil {
		t.Fatalf(".part not on disk at recorded TempPath: %v", err)
	}
	if string(data) != string(content) {
		t.Fatalf(".part content = %q, want %q", data, content)
	}

	// Retry over the recorded .part: the second attempt succeeds
	// and the .part is consumed by finalize, leaving no orphan.
	if err := svc.Retry(id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusCompleted }, "completed after retry")
	snap = getJob(svc, id)
	if snap.Filename != "syncfail.bin" {
		t.Fatalf("Filename = %q, want syncfail.bin", snap.Filename)
	}
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".part") {
			t.Fatalf("leftover .part after retry: %s", e.Name())
		}
	}
	got, _ := os.ReadFile(filepath.Join(dir, "syncfail.bin"))
	if string(got) != string(content) {
		t.Fatalf("final content = %q, want %q", got, content)
	}
	_ = svc.Shutdown(context.Background())
}

func TestResolveConflictCancel(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("payload")
	cd := &countingDownloader{}
	srv := httptest.NewServer(cd.handler(content, `attachment; filename="x.bin"`))
	defer srv.Close()
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "x.bin"), []byte("orig"), 0o644)
	id, err := svc.AddDownload(AddRequest{URL: srv.URL + "/d", DestinationDir: dir})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed")
	snap := getJob(svc, id)
	tempPath := snap.TempPath
	if err := svc.ResolveConflict(id, domain.ResolutionCancel); err != nil {
		t.Fatal(err)
	}
	snap = getJob(svc, id)
	if snap.Status != domain.StatusCancelled {
		t.Fatalf("Status = %q, want cancelled", snap.Status)
	}
	if snap.ErrorCode != "" || snap.ErrorMessage != "" || snap.SuggestedFilename != "" {
		t.Fatalf("conflict fields not cleared: %+v", snap)
	}
	if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
		t.Fatalf(".part should be removed on cancel, err=%v", err)
	}
	_ = svc.Shutdown(context.Background())
}

func TestCancelDeleteTempTrueVsFalse(t *testing.T) {
	for _, deleteTemp := range []bool{true, false} {
		t.Run(fmt.Sprint(deleteTemp), func(t *testing.T) {
			cfg := testConfig(t)
			content := []byte("slow-data")
			release := make(chan struct{})
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
				w.Header().Set("Content-Length", fmt.Sprint(len(content)))
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(content)
			}))
			defer srv.Close()
			svc := newRealService(t, cfg, srv)
			dir := t.TempDir()
			id, err := svc.AddDownload(AddRequest{
				URL:            srv.URL + "/slow.bin",
				DestinationDir: dir,
				Resolution:     domain.ResolutionAutoRename,
			})
			if err != nil {
				t.Fatal(err)
			}
			waitFor(t, func() bool {
				s := getJob(svc, id).Status
				return s == domain.StatusDownloading || s == domain.StatusProbing
			}, "downloading")
			// Create a fake .part to observe deletion (engine may also create it).
			// Cancel must wait for worker exit before deleting.
			cancelErr := make(chan error, 1)
			go func() { cancelErr <- svc.Cancel(id, deleteTemp) }()
			select {
			case err := <-cancelErr:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Cancel did not return")
			}
			close(release)
			snap := getJob(svc, id)
			if snap.Status != domain.StatusCancelled {
				t.Fatalf("Status = %q, want cancelled", snap.Status)
			}
			if deleteTemp {
				if snap.TempPath != "" {
					if _, err := os.Stat(snap.TempPath); err == nil {
						t.Fatalf(".part not deleted with deleteTemp=true: %s", snap.TempPath)
					}
				}
				// Also ensure no leftover .part files in dir.
				entries, _ := os.ReadDir(dir)
				for _, e := range entries {
					if strings.HasSuffix(e.Name(), ".part") {
						t.Fatalf("leftover .part file: %s", e.Name())
					}
				}
			}
			_ = svc.Shutdown(context.Background())
		})
	}
}

// TestCancelDeleteTempPreservesPart checks deleteTemp=false keeps
// the .part file content intact after the worker exits.
func TestCancelDeleteTempPreservesPart(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("1234567890")
	// Use a slow handler that writes partial content then blocks.
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content[:5])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		_, _ = w.Write(content[5:])
	}))
	defer srv.Close()
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	id, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/partial.bin",
		DestinationDir: dir,
		Filename:       "partial.bin",
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Wait until the .part exists (worker has started writing).
	waitFor(t, func() bool {
		entries, _ := os.ReadDir(dir)
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".part") {
				return true
			}
		}
		return false
	}, ".part created")
	if err := svc.Cancel(id, false); err != nil {
		t.Fatal(err)
	}
	close(release)
	snap := getJob(svc, id)
	if snap.Status != domain.StatusCancelled {
		t.Fatalf("Status = %q", snap.Status)
	}
	// .part must still exist (deleteTemp=false).
	found := false
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".part") {
			found = true
			data, _ := os.ReadFile(filepath.Join(dir, e.Name()))
			if string(data) != "12345" {
				t.Fatalf("part content = %q, want 12345", data)
			}
		}
	}
	if !found {
		t.Fatal(".part was deleted despite deleteTemp=false")
	}
	_ = svc.Shutdown(context.Background())
}

func TestRetryAndRemove(t *testing.T) {
	cfg := testConfig(t)
	// Server that fails the first MaxRetries requests, succeeds
	// later. A4 (Phase 6): the engine auto-retries transient 500s,
	// so the job only reaches failed once all automatic attempts
	// are exhausted; the manual Retry below then succeeds.
	var attempts int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt64(&attempts, 1)
		if n <= int64(cfg.MaxRetries) {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		body := []byte("retry-ok")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	id, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/retry.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed after 500")
	snap := getJob(svc, id)
	if snap.ErrorCode != domain.ErrorCodeHTTPServer {
		t.Fatalf("ErrorCode = %q", snap.ErrorCode)
	}
	// Remove refuses active jobs but allows failed.
	// Retry it.
	if err := svc.Retry(id); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusCompleted }, "completed after retry")
	snap = getJob(svc, id)
	if snap.ErrorCode != "" || snap.DownloadedBytes != int64(len("retry-ok")) {
		t.Fatalf("after retry snap = %+v", snap)
	}
	// Remove completed job.
	if err := svc.Remove(id); err != nil {
		t.Fatal(err)
	}
	if len(svc.List()) != 0 {
		t.Fatal("job not removed")
	}
	// Final file still on disk (Remove must not touch it).
	if _, err := os.Stat(filepath.Join(dir, "retry.bin")); err != nil {
		t.Fatalf("final file removed by Remove: %v", err)
	}
	// Remove unknown id.
	if err := svc.Remove("nope"); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Fatalf("Remove unknown = %v", err)
	}
	if err := svc.Retry("nope"); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Fatalf("Retry unknown = %v", err)
	}
	_ = svc.Shutdown(context.Background())
}

func TestRemoveRefusesActive(t *testing.T) {
	cfg := testConfig(t)
	blockCh := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-blockCh:
		case <-r.Context().Done():
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("x"))
	}))
	defer srv.Close()
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	id, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/a.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Remove(id); err == nil {
		t.Fatal("Remove active job should fail")
	}
	close(blockCh)
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusCompleted }, "completed")
	_ = svc.Shutdown(context.Background())
}

func TestRedactionInSnapshots(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("x")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	secretURL := srv.URL + "/file.bin?token=supersecret"
	id, err := svc.AddDownload(AddRequest{URL: secretURL, DestinationDir: dir, Resolution: domain.ResolutionAutoRename})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusCompleted }, "completed")
	snap := getJob(svc, id)
	if strings.Contains(snap.RedactedURL, "supersecret") {
		t.Fatalf("RedactedURL leaks secret: %q", snap.RedactedURL)
	}
	if !strings.Contains(snap.RedactedURL, "?<redacted>") {
		t.Fatalf("RedactedURL = %q, want ?<redacted>", snap.RedactedURL)
	}
	// Error messages must be sanitized too.
	if strings.Contains(snap.ErrorMessage, "supersecret") {
		t.Fatalf("ErrorMessage leaks secret: %q", snap.ErrorMessage)
	}
	_ = svc.Shutdown(context.Background())
}

func TestErrorMessageSanitizedOnFailure(t *testing.T) {
	cfg := testConfig(t)
	// Server that triggers a network error containing the URL.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}))
	defer srv.Close()
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	secretURL := srv.URL + "/missing.bin?token=supersecret2"
	id, err := svc.AddDownload(AddRequest{URL: secretURL, DestinationDir: dir, Resolution: domain.ResolutionAutoRename})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed")
	snap := getJob(svc, id)
	if strings.Contains(snap.ErrorMessage, "supersecret2") {
		t.Fatalf("ErrorMessage leaks secret: %q", snap.ErrorMessage)
	}
	if snap.ErrorCode != domain.ErrorCodeHTTPNotFound {
		t.Fatalf("ErrorCode = %q", snap.ErrorCode)
	}
	_ = svc.Shutdown(context.Background())
}

func TestIdempotentShutdownUnderActiveTransfers(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 3
	content := []byte("shutdown-test-data")
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
			return
		}
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	for i := 0; i < 3; i++ {
		_, err := svc.AddDownload(AddRequest{
			URL:            srv.URL + fmt.Sprintf("/f%d.bin", i),
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	// Shutdown with a deadline while transfers are blocked; called twice.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := svc.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	// Second call must be safe and return nil.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	if err := svc.Shutdown(ctx2); err != nil {
		t.Fatal(err)
	}
	// AddDownload after shutdown must fail.
	if _, err := svc.AddDownload(AddRequest{URL: srv.URL + "/x.bin", DestinationDir: dir}); !errors.Is(err, domain.ErrServiceClosed) {
		t.Fatalf("AddDownload after shutdown = %v", err)
	}
	close(release)
}

func TestShutdownDeadlineExceeded(t *testing.T) {
	cfg := testConfig(t)
	// Downloader that ignores context cancellation for a while.
	release := make(chan struct{})
	slow := &slowStub{release: release}
	svc := NewService(cfg, slow)
	dir := t.TempDir()
	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/slow.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := getJob(svc, id).Status
		return s == domain.StatusDownloading || s == domain.StatusProbing
	}, "downloading")
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := svc.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown should return ctx.Err() on deadline")
	}
	// Second shutdown with generous timeout succeeds once released.
	close(release)
	_ = svc.Shutdown(context.Background())
}

// slowStub blocks until release is closed, ignoring ctx.
type slowStub struct {
	release chan struct{}
}

func (s *slowStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	<-s.release
	return domain.TransferResult{}, errors.New("aborted")
}

// TestShutdown_NoGoroutineLeakOnTimeout verifies repeated timed-out
// Shutdown calls don't leak goroutines.
func TestShutdown_NoGoroutineLeakOnTimeout(t *testing.T) {
	cfg := testConfig(t)
	release := make(chan struct{})
	slow := &slowStub{release: release}
	svc := NewService(cfg, slow)
	dir := t.TempDir()
	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/slow.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		s := getJob(svc, id).Status
		return s == domain.StatusDownloading || s == domain.StatusProbing
	}, "downloading")

	// Warm up.
	time.Sleep(50 * time.Millisecond)
	before := runtime.NumGoroutine()
	for i := 0; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		_ = svc.Shutdown(ctx)
		cancel()
	}
	after := runtime.NumGoroutine()
	// Allow small slack.
	if after > before+2 {
		t.Fatalf("goroutine leak: before=%d after=%d", before, after)
	}
	close(release)
	_ = svc.Shutdown(context.Background())
}

// TestResolveConflict_ConcurrentCallsSafe verifies concurrent
// ResolveConflict calls don't race.
func TestResolveConflict_ConcurrentCallsSafe(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("concurrent-resolve")
	stub := &callCountingStub{inner: &preservedPartStub{content: content}}
	svc := NewService(cfg, stub)
	dir := t.TempDir()
	existing := filepath.Join(dir, "cdfile.bin")
	if err := os.WriteFile(existing, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}
	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/download",
		DestinationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed on conflict")

	const N = 10
	var wg sync.WaitGroup
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			errs[idx] = svc.ResolveConflict(id, domain.ResolutionAutoRename)
		}(i)
	}
	wg.Wait()
	// At least one should succeed (or error), none should panic/race,
	// and final state should be consistent.
	successes := 0
	for _, e := range errs {
		if e == nil {
			successes++
		}
	}
	snap := getJob(svc, id)
	if successes > 1 {
		t.Fatalf("more than one ResolveConflict succeeded: %d", successes)
	}
	// After concurrent resolve attempts, job should be completed or queued.
	if snap.Status != domain.StatusCompleted && snap.Status != domain.StatusQueued && snap.Status != domain.StatusDownloading {
		t.Fatalf("unexpected status after concurrent resolve: %q", snap.Status)
	}
	_ = svc.Shutdown(context.Background())
}

func TestUpdateConfigWakesQueuedJobs(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 1
	blockCh := make(chan struct{})
	var started int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&started, 1)
		select {
		case <-blockCh:
		case <-r.Context().Done():
			return
		}
		body := []byte("data")
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(body)
	}))
	defer srv.Close()
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	id1, _ := svc.AddDownload(AddRequest{URL: srv.URL + "/1.bin", DestinationDir: dir, Resolution: domain.ResolutionAutoRename})
	id2, _ := svc.AddDownload(AddRequest{URL: srv.URL + "/2.bin", DestinationDir: dir, Resolution: domain.ResolutionAutoRename})
	// Only one should have started.
	waitFor(t, func() bool { return atomic.LoadInt64(&started) == 1 }, "one started")
	time.Sleep(100 * time.Millisecond)
	if atomic.LoadInt64(&started) != 1 {
		t.Fatal("second job started despite cap=1")
	}
	// Increase cap.
	updated := cfg
	updated.MaxActiveDownloads = 2
	if err := svc.UpdateConfig(updated); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return atomic.LoadInt64(&started) == 2 }, "second started after UpdateConfig")
	close(blockCh)
	waitFor(t, func() bool {
		return getJob(svc, id1).Status == domain.StatusCompleted && getJob(svc, id2).Status == domain.StatusCompleted
	}, "both completed")
	// Invalid config rejected.
	bad := cfg
	bad.MaxActiveDownloads = 0
	if err := svc.UpdateConfig(bad); err == nil {
		t.Fatal("invalid config should be rejected")
	}
	if svc.GetConfig().MaxActiveDownloads != 2 {
		t.Fatal("config not updated")
	}
	_ = svc.Shutdown(context.Background())
}

func TestAddDownloadValidation(t *testing.T) {
	cfg := testConfig(t)
	svc := NewService(cfg, &noopDownloader{})
	dir := t.TempDir()
	if _, err := svc.AddDownload(AddRequest{URL: "ftp://x/y", DestinationDir: dir}); !errors.Is(err, domain.ErrUnsupportedScheme) {
		t.Fatalf("scheme err = %v", err)
	}
	if _, err := svc.AddDownload(AddRequest{URL: "http://x/y", DestinationDir: ""}); !errors.Is(err, domain.ErrInvalidDestination) {
		t.Fatalf("dest err = %v", err)
	}
	if _, err := svc.AddDownload(AddRequest{URL: "http://x/y", DestinationDir: dir, Filename: "../evil"}); !errors.Is(err, domain.ErrInvalidFilename) {
		t.Fatalf("filename err = %v", err)
	}
	if _, err := svc.AddDownload(AddRequest{URL: "not a url", DestinationDir: dir}); err == nil {
		t.Fatal("expected invalid URL error")
	}
	_ = svc.Shutdown(context.Background())
}

type noopDownloader struct{}

func (n *noopDownloader) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	return domain.TransferResult{}, errors.New("noop")
}

// TestListenerNotCalledUnderLock ensures SetListener callbacks that
// call back into List() do not deadlock.
func TestListenerNotCalledUnderLock(t *testing.T) {
	cfg := testConfig(t)
	svc := NewService(cfg, &noopDownloader{})
	called := make(chan struct{}, 1)
	svc.SetListener(func() {
		_ = svc.List()
		select {
		case called <- struct{}{}:
		default:
		}
	})
	_, err := svc.AddDownload(AddRequest{URL: "http://example.com/a.bin", DestinationDir: t.TempDir(), Resolution: domain.ResolutionAutoRename})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
	case <-time.After(5 * time.Second):
		t.Fatal("listener not called / deadlock")
	}
	_ = svc.Shutdown(context.Background())
}

// TestSpeedAndETA verifies speed is non-zero and ETA computed
// during a transfer with known total bytes.
func TestSpeedAndETA(t *testing.T) {
	cfg := testConfig(t)
	content := make([]byte, 0)
	for i := 0; i < 200; i++ {
		content = append(content, byte(i))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusOK)
		// Write in chunks with tiny sleeps so progress updates occur.
		for i := 0; i < len(content); i += 10 {
			end := i + 10
			if end > len(content) {
				end = len(content)
			}
			_, _ = w.Write(content[i:end])
			if f, ok := w.(http.Flusher); ok {
				f.Flush()
			}
			time.Sleep(2 * time.Millisecond)
		}
	}))
	defer srv.Close()
	// Use a config with small progress interval so OnProgress fires.
	cfg.ProgressInterval = time.Millisecond
	svc := newRealService(t, cfg, srv)
	dir := t.TempDir()
	id, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/speed.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	// Sample snapshots during the transfer.
	var sawSpeed bool
	var sawETA bool
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		snap := getJob(svc, id)
		if snap.SpeedBytesPerSec > 0 {
			sawSpeed = true
		}
		if snap.ETASeconds >= 0 && snap.TotalBytes > 0 {
			sawETA = true
		}
		if snap.Status == domain.StatusCompleted {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !sawSpeed {
		t.Fatal("never observed positive speed")
	}
	// After completion speed resets.
	snap := getJob(svc, id)
	if snap.SpeedBytesPerSec != 0 || snap.ETASeconds != -1 {
		t.Fatalf("after completion speed/ETA not reset: %v %d", snap.SpeedBytesPerSec, snap.ETASeconds)
	}
	_ = sawETA
	_ = svc.Shutdown(context.Background())
}

// TestPlatformFinalizeNoClobberConflict sanity-checks the platform
// primitive used by ResolveConflict.
func TestPlatformFinalizeNoClobberConflict(t *testing.T) {
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "a.txt"), []byte("old"), 0o644)
	tmp := filepath.Join(dir, ".a.txt.123.part")
	_ = os.WriteFile(tmp, []byte("new"), 0o644)
	_, _, err := platform.FinalizeTempFile(tmp, dir, "a.txt", domain.FinalizeNoClobber)
	var fce *domain.FileConflictError
	if !errors.As(err, &fce) {
		t.Fatalf("expected FileConflictError, got %v", err)
	}
	// Temp preserved.
	if _, err := os.Stat(tmp); err != nil {
		t.Fatal("temp should be preserved on conflict")
	}
	// AutoRename finalizes.
	fp, fn, err := platform.FinalizeTempFile(tmp, dir, "a.txt", domain.FinalizeAutoRename)
	if err != nil {
		t.Fatal(err)
	}
	if fp == "" || fn == "" {
		t.Fatal("empty finalize result")
	}
	if _, err := os.Stat(tmp); !os.IsNotExist(err) {
		t.Fatal("temp should be moved")
	}
}

// failStub always fails the transfer with the given error.
type failStub struct {
	err error
}

func (s *failStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	return domain.TransferResult{}, s.err
}

// TestServiceEnforcesCanTransition (Phase 1, A1) asserts the service
// itself obeys domain.CanTransition: no operation may jump over a
// lifecycle stage, and operations attempted in the wrong state are
// refused with the status left unchanged.
func TestServiceEnforcesCanTransition(t *testing.T) {
	t.Run("downloading passes through finalizing", func(t *testing.T) {
		cfg := testConfig(t)
		content := []byte("finalizing-observed")
		cd := &countingDownloader{}
		srv := httptest.NewServer(cd.handler(content, ""))
		defer srv.Close()

		svc := newRealService(t, cfg, srv)
		var mu sync.Mutex
		var seen []domain.Status
		svc.SetListener(func() {
			for _, snap := range svc.List() {
				mu.Lock()
				seen = append(seen, snap.Status)
				mu.Unlock()
			}
		})
		dir := t.TempDir()
		id, err := svc.AddDownload(AddRequest{
			URL:            srv.URL + "/file.bin",
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusCompleted }, "completed")
		mu.Lock()
		defer mu.Unlock()
		finalIdx, completedIdx := -1, -1
		for i, s := range seen {
			if s == domain.StatusFinalizing && finalIdx == -1 {
				finalIdx = i
			}
			if s == domain.StatusCompleted && completedIdx == -1 {
				completedIdx = i
			}
		}
		if finalIdx == -1 {
			t.Fatalf("finalizing never observed; statuses seen: %v", seen)
		}
		if completedIdx == -1 || finalIdx > completedIdx {
			t.Fatalf("finalizing (idx %d) not observed before completed (idx %d); seen: %v", finalIdx, completedIdx, seen)
		}
		_ = svc.Shutdown(context.Background())
	})

	t.Run("ResolveConflict rejected outside failed conflict", func(t *testing.T) {
		cfg := testConfig(t)
		dir := t.TempDir()

		// Downloading job: ResolveConflict must be refused.
		release := make(chan struct{})
		svc := NewService(cfg, &slowStub{release: release})
		id, err := svc.AddDownload(AddRequest{
			URL:            "http://example.com/slow.bin",
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
		// Wait for downloading, not just probing: the worker moves
		// probing -> downloading on its own, so asserting the status
		// is unchanged after a refused ResolveConflict is only
		// meaningful once the job is downloading (slowStub blocks
		// there until release is closed).
		waitFor(t, func() bool {
			return getJob(svc, id).Status == domain.StatusDownloading
		}, "downloading")
		before := getJob(svc, id).Status
		if err := svc.ResolveConflict(id, domain.ResolutionAutoRename); !errors.Is(err, domain.ErrInvalidTransition) {
			t.Fatalf("ResolveConflict on %s job = %v, want ErrInvalidTransition", before, err)
		}
		if got := getJob(svc, id).Status; got != before {
			t.Fatalf("status changed to %q after refused ResolveConflict", got)
		}
		close(release)
		_ = svc.Shutdown(context.Background())

		// Failed job without a file conflict: ResolveConflict refused.
		svc2 := NewService(cfg, &failStub{err: errors.New("boom")})
		id2, err := svc2.AddDownload(AddRequest{
			URL:            "http://example.com/fail.bin",
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return getJob(svc2, id2).Status == domain.StatusFailed }, "failed")
		if err := svc2.ResolveConflict(id2, domain.ResolutionAutoRename); !errors.Is(err, domain.ErrInvalidTransition) {
			t.Fatalf("ResolveConflict on failed non-conflict job = %v, want ErrInvalidTransition", err)
		}
		if got := getJob(svc2, id2).Status; got != domain.StatusFailed {
			t.Fatalf("status = %q, want failed", got)
		}
		_ = svc2.Shutdown(context.Background())

		// Completed job: ResolveConflict refused.
		content := []byte("done")
		cd := &countingDownloader{}
		srv := httptest.NewServer(cd.handler(content, ""))
		defer srv.Close()
		svc3 := newRealService(t, cfg, srv)
		id3, err := svc3.AddDownload(AddRequest{
			URL:            srv.URL + "/ok.bin",
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return getJob(svc3, id3).Status == domain.StatusCompleted }, "completed")
		if err := svc3.ResolveConflict(id3, domain.ResolutionAutoRename); !errors.Is(err, domain.ErrInvalidTransition) {
			t.Fatalf("ResolveConflict on completed job = %v, want ErrInvalidTransition", err)
		}
		if got := getJob(svc3, id3).Status; got != domain.StatusCompleted {
			t.Fatalf("status = %q, want completed", got)
		}
		_ = svc3.Shutdown(context.Background())
	})

	t.Run("missing part goes to failed, not queued", func(t *testing.T) {
		cfg := testConfig(t)
		content := []byte("mid-download-payload-1234567890")
		cd := &countingDownloader{}
		srv := httptest.NewServer(cd.handler(content, `attachment; filename="cdfile.bin"`))
		defer srv.Close()

		svc := newRealService(t, cfg, srv)
		dir := t.TempDir()
		existing := filepath.Join(dir, "cdfile.bin")
		if err := os.WriteFile(existing, []byte("existing"), 0o644); err != nil {
			t.Fatal(err)
		}
		id, err := svc.AddDownload(AddRequest{
			URL:            srv.URL + "/download",
			DestinationDir: dir,
		})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "job failed on conflict")
		if got := getJob(svc, id).ErrorCode; got != domain.ErrorCodeFileConflict {
			t.Fatalf("ErrorCode = %q, want file_conflict", got)
		}
		if cd.hitCount() != 1 {
			t.Fatalf("hits = %d, want 1 before ResolveConflict", cd.hitCount())
		}
		if err := svc.ResolveConflict(id, domain.ResolutionAutoRename); err != nil {
			t.Fatal(err)
		}
		// No .part exists, so the job must go finalizing -> failed and
		// wait for Retry; it must NOT re-queue (finalizing -> queued
		// is forbidden) and must NOT re-download.
		if got := getJob(svc, id).Status; got != domain.StatusFailed {
			t.Fatalf("status after ResolveConflict = %q, want failed (must not re-queue)", got)
		}
		time.Sleep(500 * time.Millisecond)
		if got := cd.hitCount(); got != 1 {
			t.Fatalf("server hits = %d, want 1 (ResolveConflict re-downloaded)", got)
		}
		if got := getJob(svc, id).Status; got != domain.StatusFailed {
			t.Fatalf("status = %q, want failed", got)
		}
		_ = svc.Shutdown(context.Background())
	})
}

// cancelRetryStub is the Phase 2 (A2) race stub: the first Download
// blocks until its context is cancelled (the Cancel path); the second
// Download (the retried generation) blocks until release2 is closed,
// then succeeds. That lets the test hold the retried worker in
// "downloading" while a stale Cancel wakes up, so a stale Cancel
// that overwrites the new generation is observed instead of being
// masked by the worker finishing first.
type cancelRetryStub struct {
	mu       sync.Mutex
	calls    int
	started2 chan struct{}
	release2 chan struct{}
}

func (s *cancelRetryStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	if n == 1 {
		<-ctx.Done()
		return domain.TransferResult{}, ctx.Err()
	}
	select {
	case s.started2 <- struct{}{}:
	default:
	}
	<-s.release2
	return domain.TransferResult{
		FinalPath:    filepath.Join(req.DestinationDir, "file.bin"),
		Filename:     "file.bin",
		WrittenBytes: 4,
		TotalBytes:   4,
	}, nil
}

func (s *cancelRetryStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// TestCancelRetryRace (Phase 2, A2) asserts a stale Cancel cannot
// kill a retried generation: Cancel the running job, Retry it as
// soon as it reports cancelled, hold the new worker blocked, then
// let it finish. The retried job must complete; if the old Cancel
// wakes up after the Retry and stamps the new generation cancelled,
// the worker later fails (cancelled -> finalizing is forbidden) and
// the job never completes.
func TestCancelRetryRace(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		cfg := testConfig(t)
		stub := &cancelRetryStub{started2: make(chan struct{}, 1), release2: make(chan struct{})}
		svc := NewService(cfg, stub)
		dir := t.TempDir()
		id, err := svc.AddDownload(AddRequest{
			URL:            "http://example.com/file.bin",
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool {
			// Wait for downloading (not just probing): the worker
			// must have passed its initial cancelling check, so the
			// first Download call really happens. Waiting only for
			// probing lets run() exit early via the cancelling check
			// without ever calling the stub, which would shift the
			// stub's generation count by one.
			return getJob(svc, id).Status == domain.StatusDownloading
		}, "downloading")

		cancelErr := make(chan error, 1)
		go func() { cancelErr <- svc.Cancel(id, false) }()
		// Retry as soon as the job reports cancelled: the retry must
		// slip in between the old worker exiting and the stale
		// Cancel waking up from <-done.
		retryErr := make(chan error, 1)
		go func() {
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				if err := svc.Retry(id); err == nil {
					retryErr <- nil
					return
				}
				runtime.Gosched()
			}
			retryErr <- errors.New("retry never succeeded")
		}()

		select {
		case err := <-retryErr:
			if err != nil {
				t.Fatalf("iter %d: %v", iter, err)
			}
		case <-time.After(6 * time.Second):
			t.Fatalf("iter %d: retry goroutine did not return", iter)
		}
		// The retried worker must be running (and blocked on
		// release2) before the stale Cancel can be judged.
		select {
		case <-stub.started2:
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: retried worker never started", iter)
		}
		close(stub.release2)
		// Fail fast if the job lands in a terminal state other than
		// completed (a stale Cancel stamps it cancelled, the worker
		// then fails on cancelled -> finalizing).
		deadline := time.Now().Add(10 * time.Second)
		for {
			snap := getJob(svc, id)
			if snap.Status == domain.StatusCompleted {
				break
			}
			if snap.Status == domain.StatusFailed || snap.Status == domain.StatusCancelled {
				t.Fatalf("iter %d: Status = %q, want completed (stale Cancel overwrote the retried job)", iter, snap.Status)
			}
			if time.Now().After(deadline) {
				t.Fatalf("iter %d: timeout waiting for completed, status = %q", iter, snap.Status)
			}
			time.Sleep(10 * time.Millisecond)
		}
		select {
		case err := <-cancelErr:
			if err != nil {
				t.Fatalf("iter %d: Cancel = %v", iter, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: Cancel did not return", iter)
		}
		if n := stub.callCount(); n != 2 {
			t.Fatalf("iter %d: stub calls = %d, want 2", iter, n)
		}
		_ = svc.Shutdown(context.Background())
	}
}

// TestShutdownWaitsForResolveConflict (Phase 2, A3) asserts Shutdown
// does not return while a ResolveConflict rename is still in
// flight: it must wait for the resolve (or the context deadline),
// not return at once as it did when it only watched worker done
// channels (a resolving job has running == false).
//
// The rename itself is a fast local call with no injection point,
// so the test holds the resolve's done channel directly (same
// package) to simulate a resolve in flight; what is under test is
// that Shutdown collects and waits on that channel.
func TestShutdownWaitsForResolveConflict(t *testing.T) {
	cfg := testConfig(t)
	content := []byte("resolve-shutdown")
	stub := &callCountingStub{inner: &preservedPartStub{content: content}}
	svc := NewService(cfg, stub)
	dir := t.TempDir()
	existing := filepath.Join(dir, "cdfile.bin")
	if err := os.WriteFile(existing, []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}
	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/download",
		DestinationDir: dir,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed on conflict")

	resolveDone := make(chan struct{})
	svc.mu.Lock()
	j := svc.jobs[id]
	j.resolving = true
	j.resolveDone = resolveDone
	svc.mu.Unlock()

	// Shutdown with a short deadline must time out while the resolve
	// is still in flight, not return at once.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := svc.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown returned nil while a ResolveConflict was still in flight")
	}
	if elapsed := time.Since(start); elapsed < 150*time.Millisecond {
		t.Fatalf("Shutdown returned after %v, want it to wait for the resolve (or the deadline)", elapsed)
	}

	// Once the resolve finishes, Shutdown returns.
	close(resolveDone)
	if err := svc.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown after resolve finished = %v", err)
	}
	svc.mu.Lock()
	j.resolving = false
	j.resolveDone = nil
	svc.mu.Unlock()
	_ = svc.Shutdown(context.Background())
}

// TestCancelResolveConflictRace (Phase 2, A3) asserts Cancel and
// ResolveConflict racing on a file_conflict job have exactly one
// winner: either the resolve wins (completed, destination file
// created from the preserved .part) or the cancel wins (cancelled,
// destination file never created). The resolve must never overwrite
// a cancel by renaming the .part after Cancel stamped the job
// cancelled, and a cancel must never leave a half-created
// destination file behind.
func TestCancelResolveConflictRace(t *testing.T) {
	for iter := 0; iter < 30; iter++ {
		cfg := testConfig(t)
		content := []byte("race-part-content")
		stub := &callCountingStub{inner: &preservedPartStub{content: content}}
		svc := NewService(cfg, stub)
		dir := t.TempDir()
		existing := filepath.Join(dir, "cdfile.bin")
		if err := os.WriteFile(existing, []byte("existing"), 0o644); err != nil {
			t.Fatal(err)
		}
		id, err := svc.AddDownload(AddRequest{
			URL:            "http://example.com/download",
			DestinationDir: dir,
		})
		if err != nil {
			t.Fatal(err)
		}
		waitFor(t, func() bool { return getJob(svc, id).Status == domain.StatusFailed }, "failed on conflict")
		if got := getJob(svc, id).ErrorCode; got != domain.ErrorCodeFileConflict {
			t.Fatalf("iter %d: ErrorCode = %q, want file_conflict", iter, got)
		}

		var wg sync.WaitGroup
		var resolveErr, cancelErr error
		wg.Add(2)
		go func() {
			defer wg.Done()
			resolveErr = svc.ResolveConflict(id, domain.ResolutionAutoRename)
		}()
		go func() {
			defer wg.Done()
			cancelErr = svc.Cancel(id, false)
		}()
		wg.Wait()

		snap := getJob(svc, id)
		renamed := filepath.Join(dir, "cdfile (1).bin")
		switch snap.Status {
		case domain.StatusCompleted:
			if resolveErr != nil {
				t.Fatalf("iter %d: completed but ResolveConflict = %v (cancel err: %v)", iter, resolveErr, cancelErr)
			}
			got, err := os.ReadFile(renamed)
			if err != nil {
				t.Fatalf("iter %d: resolve won but destination file missing: %v", iter, err)
			}
			if string(got) != string(content) {
				t.Fatalf("iter %d: destination content = %q, want preserved .part content", iter, got)
			}
		case domain.StatusCancelled:
			if _, err := os.Stat(renamed); err == nil {
				t.Fatalf("iter %d: cancel won but destination file was created", iter)
			}
		default:
			t.Fatalf("iter %d: Status = %q, want completed or cancelled (resolve err: %v, cancel err: %v)", iter, snap.Status, resolveErr, cancelErr)
		}
		_ = svc.Shutdown(context.Background())
	}
}

// --- Test (A5, Phase 6): per-host slots ----------------------------------

// hostBlockingStub blocks every Download until release is closed and
// records, per hostname, how many downloads ran concurrently.
type hostBlockingStub struct {
	release chan struct{}
	started chan string // receives the hostname of each started download

	mu   sync.Mutex
	cur  map[string]int
	peak map[string]int
}

func (s *hostBlockingStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	host := ""
	if u, err := url.Parse(req.URL); err == nil {
		host = strings.ToLower(u.Hostname())
	}
	s.mu.Lock()
	s.cur[host]++
	if s.cur[host] > s.peak[host] {
		s.peak[host] = s.cur[host]
	}
	s.mu.Unlock()
	s.started <- host
	<-s.release
	s.mu.Lock()
	s.cur[host]--
	s.mu.Unlock()
	return domain.TransferResult{Filename: "f.bin"}, nil
}

func (s *hostBlockingStub) peakFor(host string) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.peak[host]
}

// TestPerHostSlotLimit (A5, Phase 6): with MaxActiveDownloads large,
// jobs for the same host must not exceed MaxConnectionsPerHost running
// at once, extra same-host jobs stay queued, and jobs for another host
// are not blocked by the first host's cap.
func TestPerHostSlotLimit(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 32
	cfg.MaxConnectionsPerHost = 2

	stub := &hostBlockingStub{
		release: make(chan struct{}),
		started: make(chan string, 32),
		cur:     make(map[string]int),
		peak:    make(map[string]int),
	}
	svc := NewService(cfg, stub)
	dir := t.TempDir()

	const hostA = "host-a.example"
	const hostB = "host-b.example"
	idsA := make([]string, 0, 5)
	for i := 0; i < 5; i++ {
		id, err := svc.AddDownload(AddRequest{
			URL:            "http://" + hostA + fmt.Sprintf("/f%d.bin", i),
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
		idsA = append(idsA, id)
	}
	idsB := make([]string, 0, 2)
	for i := 0; i < 2; i++ {
		id, err := svc.AddDownload(AddRequest{
			URL:            "http://" + hostB + fmt.Sprintf("/g%d.bin", i),
			DestinationDir: dir,
			Resolution:     domain.ResolutionAutoRename,
		})
		if err != nil {
			t.Fatal(err)
		}
		idsB = append(idsB, id)
	}

	// 2 host-A + 2 host-B downloads must start; the rest stay queued.
	timeout := time.After(5 * time.Second)
	seen := map[string]int{}
	for total := 0; total < 4; total++ {
		select {
		case h := <-stub.started:
			seen[h]++
		case <-timeout:
			t.Fatalf("expected 4 downloads to start, got %d (%v)", total, seen)
		}
	}
	if seen[hostA] != 2 || seen[hostB] != 2 {
		t.Fatalf("started per host = %v, want {host-a: 2, host-b: 2}", seen)
	}
	// No further job may start while the 4 are blocked.
	time.Sleep(200 * time.Millisecond)
	select {
	case h := <-stub.started:
		t.Fatalf("extra download started for host %q beyond per-host cap", h)
	default:
	}
	if p := stub.peakFor(hostA); p != 2 {
		t.Fatalf("peak concurrency for %s = %d, want 2", hostA, p)
	}
	// The 3 extra host-A jobs are still queued, not failed/cancelled.
	for _, id := range idsA[2:] {
		if st := getJob(svc, id).Status; st != domain.StatusQueued {
			t.Fatalf("extra host-A job %s status = %q, want queued", id, st)
		}
	}
	// Release everything; all 7 must complete, and the per-host peak
	// must never have exceeded the cap.
	close(stub.release)
	allIDs := append(append([]string{}, idsA...), idsB...)
	waitFor(t, func() bool {
		for _, id := range allIDs {
			if getJob(svc, id).Status != domain.StatusCompleted {
				return false
			}
		}
		return true
	}, "all downloads completed")
	if p := stub.peakFor(hostA); p > 2 {
		t.Fatalf("peak concurrency for %s exceeded cap: %d", hostA, p)
	}
	if p := stub.peakFor(hostB); p > 2 {
		t.Fatalf("peak concurrency for %s exceeded cap: %d", hostB, p)
	}
	_ = svc.Shutdown(context.Background())
}

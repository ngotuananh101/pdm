// Probe wiring tests (Stage 3, Phase 2.2, TDD): after the probe, the
// job carries real validators in RAM and in the repository, and a
// probe failure still falls back to single-stream instead of failing
// the job. The run flow itself stays single-stream; only the probe
// call is added (switching the run path is Phase 4.4).
package app

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/engine"
	"github.com/ngotuananh101/pdm/internal/storage/sqlite"
)

// stubProber is a RangeProber returning a canned result or error. When
// started/release are set, Probe signals started on entry and blocks
// until release closes or ctx is cancelled.
type stubProber struct {
	mu        sync.Mutex
	result    engine.ProbeResult
	err       error
	calls     int
	started   chan struct{}
	startOnce sync.Once
	release   chan struct{}
}

func (p *stubProber) Probe(ctx context.Context, url string) (engine.ProbeResult, error) {
	p.mu.Lock()
	p.calls++
	p.mu.Unlock()
	if p.started != nil {
		p.startOnce.Do(func() { close(p.started) })
	}
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return engine.ProbeResult{}, ctx.Err()
		}
	}
	return p.result, p.err
}

func (p *stubProber) probeCalls() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

// blockingStub is a downloader that blocks until release closes (or
// ctx is cancelled), then succeeds with a 100-byte result.
type blockingStub struct {
	release chan struct{}
	calls   atomic.Int64
}

func (b *blockingStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	b.calls.Add(1)
	select {
	case <-b.release:
	case <-ctx.Done():
		return domain.TransferResult{}, ctx.Err()
	}
	return domain.TransferResult{Filename: "file.bin", WrittenBytes: 100, TotalBytes: 100}, nil
}

func (b *blockingStub) callCount() int64 { return b.calls.Load() }

// startProbedJob builds a repo-backed service with the given prober
// and a blocking downloader, adds one download, and waits until the
// job is downloading (so the probe has already run). It returns the
// service, job id, repo, and downloader; close dl.release to let the
// job finish.
func startProbedJob(t *testing.T, prober engine.RangeProber) (*Service, string, *sqlite.DownloadRepository, *blockingStub) {
	t.Helper()
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 1
	dl := &blockingStub{release: make(chan struct{})}
	svc := NewServiceWithRepository(cfg, dl, repo)
	svc.SetProber(prober)
	t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })
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
		return getJob(svc, id).Status == domain.StatusDownloading
	}, "job downloading")
	return svc, id, repo, dl
}

func TestProbe_ValidatorsPersistedToEntityAndRepo(t *testing.T) {
	prober := &stubProber{result: engine.ProbeResult{
		TotalBytes:    100,
		ETag:          `"v1"`,
		LastModified:  "Wed, 01 Jan 2025 00:00:00 GMT",
		SupportsRange: true,
	}}
	svc, id, repo, dl := startProbedJob(t, prober)

	snap := getJob(svc, id)
	if snap.ETag != `"v1"` {
		t.Fatalf("snapshot ETag = %q, want %q", snap.ETag, `"v1"`)
	}
	if snap.LastModified != "Wed, 01 Jan 2025 00:00:00 GMT" {
		t.Fatalf("snapshot LastModified = %q", snap.LastModified)
	}
	if !snap.SupportsRange {
		t.Fatal("snapshot SupportsRange = false, want true")
	}
	if snap.TotalBytes != 100 {
		t.Fatalf("snapshot TotalBytes = %d, want 100", snap.TotalBytes)
	}
	row, err := repo.GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if row.ETag != `"v1"` || row.LastModified != "Wed, 01 Jan 2025 00:00:00 GMT" || !row.SupportsRange || row.TotalBytes != 100 {
		t.Fatalf("DB row = %+v, want validators ETag %q, total 100, range true", row, `"v1"`)
	}
	if prober.probeCalls() != 1 {
		t.Fatalf("probe calls = %d, want 1", prober.probeCalls())
	}

	close(dl.release)
	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusCompleted
	}, "job completed")
}

func TestProbe_WeakETagNotStored(t *testing.T) {
	prober := &stubProber{result: engine.ProbeResult{
		TotalBytes:    100,
		ETag:          `W/"v1"`,
		LastModified:  "Wed, 01 Jan 2025 00:00:00 GMT",
		SupportsRange: true,
	}}
	svc, id, repo, dl := startProbedJob(t, prober)

	snap := getJob(svc, id)
	if snap.ETag != "" {
		t.Fatalf("snapshot ETag = %q, want empty (weak ETag must not be stored)", snap.ETag)
	}
	if snap.LastModified != "Wed, 01 Jan 2025 00:00:00 GMT" {
		t.Fatalf("snapshot LastModified = %q, want it stored", snap.LastModified)
	}
	if !snap.SupportsRange {
		t.Fatal("snapshot SupportsRange = false, want true")
	}
	row, err := repo.GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if row.ETag != "" {
		t.Fatalf("DB ETag = %q, want empty (weak ETag must not be stored)", row.ETag)
	}

	close(dl.release)
	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusCompleted
	}, "job completed")
}

func TestProbe_ErrorFallsBackToSingleStream(t *testing.T) {
	prober := &stubProber{err: errors.New("probe boom")}
	svc, id, _, dl := startProbedJob(t, prober)

	snap := getJob(svc, id)
	if snap.ETag != "" || snap.SupportsRange {
		t.Fatalf("snapshot after failed probe = ETag %q range %v, want empty/false", snap.ETag, snap.SupportsRange)
	}
	if snap.TotalBytes != -1 {
		t.Fatalf("snapshot TotalBytes = %d, want -1 (unknown after failed probe)", snap.TotalBytes)
	}
	if dl.callCount() != 1 {
		t.Fatalf("downloader calls = %d, want 1 (single-stream fallback ran)", dl.callCount())
	}

	close(dl.release)
	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusCompleted
	}, "job completed after probe failure")
}

func TestProbe_PauseDuringProbeParksWithoutDownload(t *testing.T) {
	prober := &stubProber{
		result: engine.ProbeResult{
			TotalBytes:    100,
			ETag:          `"v1"`,
			SupportsRange: true,
		},
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	cfg := testConfig(t)
	dl := &blockingStub{release: make(chan struct{})}
	svc := NewServiceWithRepository(cfg, dl, repo)
	svc.SetProber(prober)
	t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })

	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/file.bin",
		DestinationDir: t.TempDir(),
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-prober.started:
	case <-time.After(5 * time.Second):
		t.Fatal("probe never started")
	}
	// Pause while the probe is still blocked: the probe aborts on
	// context cancellation and the job parks without downloading.
	if err := svc.Pause(id); err != nil {
		t.Fatal(err)
	}
	close(prober.release)
	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusPaused
	}, "job paused")
	if dl.callCount() != 0 {
		t.Fatalf("downloader calls = %d, want 0 (paused during probe)", dl.callCount())
	}
}

func TestProbe_RealProberAgainstRangeServer(t *testing.T) {
	content := []byte(strings.Repeat("range-server content ", 50))
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Range") == "bytes=0-0" {
			w.Header().Set("ETag", `"live"`)
			w.Header().Set("Last-Modified", "Wed, 01 Jan 2025 00:00:00 GMT")
			w.Header().Set("Content-Range", fmt.Sprintf("bytes 0-0/%d", len(content)))
			w.Header().Set("Content-Length", "1")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write(content[:1])
			return
		}
		w.Header().Set("ETag", `"live"`)
		w.Header().Set("Content-Length", fmt.Sprint(len(content)))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(content)
	}))
	defer srv.Close()

	cfg := testConfig(t)
	dir := t.TempDir()
	dl := engine.NewSingleDownloader(cfg, srv.Client())
	svc := NewService(cfg, dl)
	svc.SetProber(engine.NewProber(cfg, srv.Client()))
	t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })

	id, err := svc.AddDownload(AddRequest{
		URL:            srv.URL + "/file.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusCompleted
	}, "job completed")
	snap := getJob(svc, id)
	if snap.ETag != `"live"` {
		t.Fatalf("snapshot ETag = %q, want %q", snap.ETag, `"live"`)
	}
	if !snap.SupportsRange {
		t.Fatal("snapshot SupportsRange = false, want true")
	}
	if snap.TotalBytes != int64(len(content)) {
		t.Fatalf("snapshot TotalBytes = %d, want %d", snap.TotalBytes, len(content))
	}
	data, err := os.ReadFile(snap.FinalPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(data, content) {
		t.Fatal("downloaded file content does not match server content")
	}
}

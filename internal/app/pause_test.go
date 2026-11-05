// Pause/Resume tests (Phase 7, U3).
//
// Semantics under test (decided in the fixes plan): Pause cancels the
// worker context, waits for the worker to exit, keeps the .part file,
// and parks the job in paused. Resume re-queues the job from the
// beginning — Stage 1 has no HTTP Range resume, so the next worker
// starts a fresh single-stream transfer (the engine truncates the
// preserved .part with O_TRUNC).
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// pauseStub writes a partial .part file, then blocks: on the first
// call until its context is cancelled (the Pause path), on later
// calls until release2 is closed (the fresh transfer after Resume).
type pauseStub struct {
	mu       sync.Mutex
	calls    int
	started1 chan struct{} // closed when the first Download starts
	started2 chan struct{} // closed when the second Download starts
	release2 chan struct{}
}

func (s *pauseStub) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	s.mu.Lock()
	s.calls++
	n := s.calls
	s.mu.Unlock()
	filename := req.Filename
	if filename == "" {
		filename = "file.bin"
	}
	tempPath := filepath.Join(req.DestinationDir, fmt.Sprintf(".%s.%s.part", filename, req.ID))
	// A partial download: bytes on disk that Resume must NOT resume
	// from — the next Download call starts a fresh transfer.
	if err := os.WriteFile(tempPath, []byte("partial-bytes"), 0o600); err != nil {
		return domain.TransferResult{}, err
	}
	if n == 1 {
		close(s.started1)
		<-ctx.Done()
		return domain.TransferResult{
			TempPath:     tempPath,
			Filename:     filename,
			WrittenBytes: int64(len("partial-bytes")),
			TotalBytes:   100,
		}, ctx.Err()
	}
	close(s.started2)
	<-s.release2
	return domain.TransferResult{
		FinalPath:    filepath.Join(req.DestinationDir, filename),
		Filename:     filename,
		WrittenBytes: 100,
		TotalBytes:   100,
	}, nil
}

func (s *pauseStub) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func newPauseStub() *pauseStub {
	return &pauseStub{
		started1: make(chan struct{}),
		started2: make(chan struct{}),
		release2: make(chan struct{}),
	}
}

func addPauseJob(t *testing.T, svc *Service, dir string) string {
	t.Helper()
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
	}, "downloading")
	return id
}

// TestPauseDownloadingJob: Pause on a running job stops the worker,
// parks the job in paused, and keeps the .part file on disk.
func TestPauseDownloadingJob(t *testing.T) {
	cfg := testConfig(t)
	stub := newPauseStub()
	svc := NewService(cfg, stub)
	dir := t.TempDir()
	id := addPauseJob(t, svc, dir)

	if err := svc.Pause(id); err != nil {
		t.Fatalf("Pause = %v", err)
	}
	snap := getJob(svc, id)
	if snap.Status != domain.StatusPaused {
		t.Fatalf("Status = %q, want paused", snap.Status)
	}
	if snap.TempPath == "" {
		t.Fatal("TempPath empty after Pause: the .part path must be kept")
	}
	got, err := os.ReadFile(snap.TempPath)
	if err != nil {
		t.Fatalf(".part missing after Pause: %v", err)
	}
	if string(got) != "partial-bytes" {
		t.Fatalf(".part content = %q, want the partial bytes", got)
	}
	if n := stub.callCount(); n != 1 {
		t.Fatalf("stub calls = %d, want 1 (the worker must have stopped, not restarted)", n)
	}
	_ = svc.Shutdown(context.Background())
}

// TestResumeRedownloadsFromStart: Resume re-queues a paused job and
// the next worker starts a fresh transfer — DownloadedBytes resets to
// 0 and the downloader is invoked a second time.
func TestResumeRedownloadsFromStart(t *testing.T) {
	cfg := testConfig(t)
	stub := newPauseStub()
	svc := NewService(cfg, stub)
	dir := t.TempDir()
	id := addPauseJob(t, svc, dir)

	if err := svc.Pause(id); err != nil {
		t.Fatalf("Pause = %v", err)
	}
	partPath := getJob(svc, id).TempPath
	if partPath == "" {
		t.Fatal("TempPath empty after Pause")
	}
	if err := svc.Resume(id); err != nil {
		t.Fatalf("Resume = %v", err)
	}
	// The resumed worker must start a fresh transfer.
	select {
	case <-stub.started2:
	case <-time.After(5 * time.Second):
		t.Fatal("resumed worker never started")
	}
	snap := getJob(svc, id)
	if snap.Status != domain.StatusDownloading {
		t.Fatalf("Status = %q, want downloading after Resume", snap.Status)
	}
	if snap.DownloadedBytes != 0 {
		t.Fatalf("DownloadedBytes = %d, want 0 (Resume restarts from the beginning)", snap.DownloadedBytes)
	}
	// The preserved .part is still on disk when the fresh transfer
	// starts; the engine truncates it (O_TRUNC) instead of resuming
	// from its offset.
	if _, err := os.Stat(partPath); err != nil {
		t.Fatalf(".part gone before the resumed transfer: %v", err)
	}
	close(stub.release2)
	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusCompleted
	}, "completed after Resume")
	if n := stub.callCount(); n != 2 {
		t.Fatalf("stub calls = %d, want 2 (one fresh transfer after Resume)", n)
	}
	// Pause after completion must be rejected.
	if err := svc.Pause(id); !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("Pause on completed job = %v, want ErrInvalidTransition", err)
	}
	_ = svc.Shutdown(context.Background())
}

// TestPauseQueuedJob: Pause on a job that has not started yet parks
// it without starting any transfer; Resume re-queues it and it is
// dispatched once a slot frees up.
func TestPauseQueuedJob(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 1
	stub := newPauseStub()
	svc := NewService(cfg, stub)
	dir := t.TempDir()

	id1, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/first.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return getJob(svc, id1).Status == domain.StatusDownloading
	}, "first job downloading")

	id2, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/second.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	if st := getJob(svc, id2).Status; st != domain.StatusQueued {
		t.Fatalf("second job Status = %q, want queued", st)
	}
	if err := svc.Pause(id2); err != nil {
		t.Fatalf("Pause queued job = %v", err)
	}
	if st := getJob(svc, id2).Status; st != domain.StatusPaused {
		t.Fatalf("second job Status = %q, want paused", st)
	}
	if n := stub.callCount(); n != 1 {
		t.Fatalf("stub calls = %d, want 1 (a paused queued job must not start)", n)
	}

	if err := svc.Resume(id2); err != nil {
		t.Fatalf("Resume = %v", err)
	}
	if st := getJob(svc, id2).Status; st != domain.StatusQueued {
		t.Fatalf("second job Status after Resume = %q, want queued (slot busy)", st)
	}
	// Pausing the running job frees the slot for the resumed one.
	if err := svc.Pause(id1); err != nil {
		t.Fatalf("Pause first job = %v", err)
	}
	select {
	case <-stub.started2:
	case <-time.After(5 * time.Second):
		t.Fatal("resumed queued job never started")
	}
	close(stub.release2)
	waitFor(t, func() bool {
		return getJob(svc, id2).Status == domain.StatusCompleted
	}, "queued job completed after Resume")
	_ = svc.Shutdown(context.Background())
}

// TestPauseRejectedForNonActive: Pause and Resume refuse jobs that
// are not in a state that allows them, and unknown ids report
// ErrDownloadNotFound.
func TestPauseRejectedForNonActive(t *testing.T) {
	cfg := testConfig(t)
	if err := NewService(cfg, newPauseStub()).Pause("no-such-id"); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Fatalf("Pause unknown id = %v, want ErrDownloadNotFound", err)
	}
	if err := NewService(cfg, newPauseStub()).Resume("no-such-id"); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Fatalf("Resume unknown id = %v, want ErrDownloadNotFound", err)
	}

	t.Run("cancelled", func(t *testing.T) {
		stub := newPauseStub()
		svc := NewService(cfg, stub)
		dir := t.TempDir()
		id := addPauseJob(t, svc, dir)
		if err := svc.Cancel(id, false); err != nil {
			t.Fatalf("Cancel = %v", err)
		}
		if st := getJob(svc, id).Status; st != domain.StatusCancelled {
			t.Fatalf("Status = %q, want cancelled", st)
		}
		if err := svc.Pause(id); !errors.Is(err, domain.ErrInvalidTransition) {
			t.Fatalf("Pause cancelled job = %v, want ErrInvalidTransition", err)
		}
		if err := svc.Resume(id); !errors.Is(err, domain.ErrInvalidTransition) {
			t.Fatalf("Resume cancelled job = %v, want ErrInvalidTransition", err)
		}
		_ = svc.Shutdown(context.Background())
	})

	t.Run("paused", func(t *testing.T) {
		stub := newPauseStub()
		svc := NewService(cfg, stub)
		dir := t.TempDir()
		id := addPauseJob(t, svc, dir)
		// Resume only works from paused.
		if err := svc.Resume(id); !errors.Is(err, domain.ErrInvalidTransition) {
			t.Fatalf("Resume downloading job = %v, want ErrInvalidTransition", err)
		}
		if err := svc.Pause(id); err != nil {
			t.Fatalf("Pause = %v", err)
		}
		if err := svc.Pause(id); !errors.Is(err, domain.ErrInvalidTransition) {
			t.Fatalf("second Pause = %v, want ErrInvalidTransition", err)
		}
		// A paused job can still be cancelled (paused -> cancelled
		// is allowed); with deleteTemp=false the .part is kept.
		partPath := getJob(svc, id).TempPath
		if err := svc.Cancel(id, false); err != nil {
			t.Fatalf("Cancel paused job = %v", err)
		}
		if st := getJob(svc, id).Status; st != domain.StatusCancelled {
			t.Fatalf("Status = %q, want cancelled", st)
		}
		if _, err := os.Stat(partPath); err != nil {
			t.Fatalf(".part missing after Cancel(deleteTemp=false) of a paused job: %v", err)
		}
		_ = svc.Shutdown(context.Background())
	})
}

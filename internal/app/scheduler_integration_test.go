// Service + scheduler integration tests (Phase 4, Task 4.3).
//
// These tests drive the real Service (with a stub downloader) and
// assert the queue runs through the scheduler: a job paused while
// queued must never start, even after a slot frees, and Resume
// starts exactly one new worker for it.
package app

import (
	"context"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// TestPauseQueuedJobNeverStartsAfterSlotFrees: with one global slot,
// job A runs (blocked in the stub) and job B waits queued. Pausing B
// removes it from the scheduler's pending queue; cancelling A then
// frees the slot, but B must not start. Resuming B re-queues it and
// exactly one new worker runs it to completion.
func TestPauseQueuedJobNeverStartsAfterSlotFrees(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 1
	stub := newPauseStub()
	svc := NewService(cfg, stub)
	defer func() { _ = svc.Shutdown(context.Background()) }()
	dir := t.TempDir()

	idA := addPauseJob(t, svc, dir)

	idB, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/b.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := getJob(svc, idB).Status; got != domain.StatusQueued {
		t.Fatalf("job B status = %q, want queued (slot is full)", got)
	}

	// Pause the queued job: no worker exists for it yet.
	if err := svc.Pause(idB); err != nil {
		t.Fatalf("Pause(B) = %v", err)
	}
	if got := getJob(svc, idB).Status; got != domain.StatusPaused {
		t.Fatalf("job B status = %q, want paused", got)
	}

	// Free the slot by cancelling A. B is paused, not queued, so
	// the scheduler must not start any worker for it.
	if err := svc.Cancel(idA, false); err != nil {
		t.Fatalf("Cancel(A) = %v", err)
	}
	waitFor(t, func() bool {
		return getJob(svc, idA).Status == domain.StatusCancelled
	}, "A cancelled")
	time.Sleep(100 * time.Millisecond)
	if n := stub.callCount(); n != 1 {
		t.Fatalf("stub calls = %d, want 1: paused job B must never start", n)
	}

	// Resume B: exactly one new worker runs it to completion.
	if err := svc.Resume(idB); err != nil {
		t.Fatalf("Resume(B) = %v", err)
	}
	select {
	case <-stub.started2:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for B's worker to start after Resume")
	}
	close(stub.release2)
	waitFor(t, func() bool {
		return getJob(svc, idB).Status == domain.StatusCompleted
	}, "B completed")
	if n := stub.callCount(); n != 2 {
		t.Fatalf("stub calls = %d, want 2 (one worker per generation)", n)
	}
}

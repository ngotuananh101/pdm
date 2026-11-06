// Checkpoint tests (Stage 3, Phase 3.2, TDD).
//
// Checkpoint order per ARCH §8: snapshot the dirty offsets, sync
// the file, then record the synced offsets in storage, then publish.
// Storage must never advance past synced data, and a sync or commit
// failure must stop the job with storage unadvanced.
package engine

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// failingSyncer simulates a disk sync failure.
type failingSyncer struct {
	err error
}

func (f *failingSyncer) Sync() error { return f.err }

// countingSyncer records how many times Sync was called.
type countingSyncer struct {
	mu    sync.Mutex
	calls int
	file  *os.File
}

func (c *countingSyncer) Sync() error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	return c.file.Sync()
}

func (c *countingSyncer) syncCalls() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

func checkpointPlan(t *testing.T, repo *fakeSegmentRepo) {
	t.Helper()
	ctx := context.Background()
	// One segment covering the whole 100-byte file, so notes of up
	// to 100 bytes fit inside segment 0.
	if err := repo.InsertPlan(ctx, "d1", domain.SplitSegments(100, 1, 1)); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
}

func committedOf(t *testing.T, repo *fakeSegmentRepo, index int) int64 {
	t.Helper()
	segs, err := repo.GetByDownload(context.Background(), "d1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	return segs[index].CommittedBytes
}

func TestCheckpoint_CommitsOnlySyncedBytes(t *testing.T) {
	repo := newFakeSegmentRepo()
	checkpointPlan(t, repo)

	path := filepath.Join(t.TempDir(), "part.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cp := NewCheckpointer(f, repo, "d1")
	ctx := context.Background()

	// Write 60 bytes of segment 0, checkpoint, then simulate a crash:
	// bytes written after the checkpoint must not be trusted.
	if _, err := f.WriteAt(make([]byte, 60), 0); err != nil {
		t.Fatal(err)
	}
	cp.NoteWritten(0, 60)
	if err := cp.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := committedOf(t, repo, 0); got != 60 {
		t.Fatalf("committed[0] after checkpoint = %d, want 60", got)
	}

	// 40 more bytes, noted but never checkpointed, then "crash":
	// drop the in-memory checkpointer state entirely.
	if _, err := f.WriteAt(make([]byte, 40), 60); err != nil {
		t.Fatal(err)
	}
	cp.NoteWritten(0, 100)
	// Crash: only storage is trusted now.
	if got := committedOf(t, repo, 0); got != 60 {
		t.Fatalf("committed[0] after crash = %d, want 60 (only checkpointed bytes are trusted)", got)
	}
	// The file itself holds 100 bytes, but only 60 are committed:
	// a resume must re-download bytes 60..99.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != 100 {
		t.Fatalf("file size = %d, want 100", len(data))
	}
}

func TestCheckpoint_SyncErrorStopsJobWithoutAdvancing(t *testing.T) {
	repo := newFakeSegmentRepo()
	checkpointPlan(t, repo)

	cp := NewCheckpointerWithSyncer(
		&failingSyncer{err: errors.New("simulated sync failure")},
		repo, "d1",
	)
	cp.NoteWritten(0, 60)
	if err := cp.Checkpoint(context.Background()); err == nil {
		t.Fatal("Checkpoint with failing sync: expected error, got nil")
	}
	if got := committedOf(t, repo, 0); got != 0 {
		t.Fatalf("committed[0] after failed sync = %d, want 0 (DB must not advance before data is synced)", got)
	}
}

func TestCheckpoint_CommitErrorKeepsDirtyForRetry(t *testing.T) {
	repo := newFakeSegmentRepo()
	checkpointPlan(t, repo)

	path := filepath.Join(t.TempDir(), "part.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cp := NewCheckpointer(f, repo, "d1")
	ctx := context.Background()

	repo.failUpdate = true
	cp.NoteWritten(0, 60)
	if err := cp.Checkpoint(ctx); err == nil {
		t.Fatal("Checkpoint with failing commit: expected error, got nil")
	}
	if got := committedOf(t, repo, 0); got != 0 {
		t.Fatalf("committed[0] after failed commit = %d, want 0", got)
	}
	// The dirty value is retained: once storage recovers, the next
	// checkpoint publishes the same offset.
	repo.failUpdate = false
	if err := cp.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint after storage recovers: %v", err)
	}
	if got := committedOf(t, repo, 0); got != 60 {
		t.Fatalf("committed[0] after retry = %d, want 60", got)
	}
}

func TestCheckpoint_NeverRegresses(t *testing.T) {
	repo := newFakeSegmentRepo()
	checkpointPlan(t, repo)

	path := filepath.Join(t.TempDir(), "part.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	cp := NewCheckpointer(f, repo, "d1")
	ctx := context.Background()
	cp.NoteWritten(0, 60)
	if err := cp.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	// A stale note (smaller than the committed value) must not move
	// storage backwards.
	cp.NoteWritten(0, 30)
	if err := cp.Checkpoint(ctx); err != nil {
		t.Fatalf("Checkpoint: %v", err)
	}
	if got := committedOf(t, repo, 0); got != 60 {
		t.Fatalf("committed[0] = %d, want 60 (must never regress)", got)
	}
}

func TestCheckpoint_RunTicksAtInterval(t *testing.T) {
	repo := newFakeSegmentRepo()
	checkpointPlan(t, repo)

	path := filepath.Join(t.TempDir(), "part.bin")
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	syncer := &countingSyncer{file: f}
	cp := NewCheckpointerWithSyncer(syncer, repo, "d1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cp.Run(ctx, 20*time.Millisecond) }()

	cp.NoteWritten(0, 40)
	// Wait until a tick publishes the note (bounded wait).
	deadline := time.Now().Add(2 * time.Second)
	for committedOf(t, repo, 0) != 40 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := committedOf(t, repo, 0); got != 40 {
		t.Fatalf("committed[0] after Run ticks = %d, want 40", got)
	}
	if syncer.syncCalls() == 0 {
		t.Fatal("Sync was never called by Run")
	}

	// Stopping the context ends Run.
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run after cancel = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancel")
	}
}

func TestCheckpoint_RunStopsJobOnSyncError(t *testing.T) {
	repo := newFakeSegmentRepo()
	checkpointPlan(t, repo)

	cp := NewCheckpointerWithSyncer(
		&failingSyncer{err: errors.New("simulated sync failure")},
		repo, "d1",
	)
	cp.NoteWritten(0, 40)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cp.Run(ctx, 10*time.Millisecond) }()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("Run with failing sync: expected error, got nil")
		}
		if got := committedOf(t, repo, 0); got != 0 {
			t.Fatalf("committed[0] after failed Run sync = %d, want 0", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after sync error")
	}
}

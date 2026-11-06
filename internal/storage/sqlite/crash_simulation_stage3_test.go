// Crash simulation tests, Stage 3 level (Phase 6.2, TDD).
//
// Stage 2 covered the DB-unavailable class (corrupt file, closed
// handle) in crash_simulation_test.go. The test here covers the
// "after commit" crash point of ARCH §13 for segment state: bytes
// recorded by a checkpoint before the crash must survive a close +
// reopen of the database file, and bytes never committed must still
// read back as zero — the reopened plan is exactly what the next
// run may resume from, no more and no less.
package sqlite

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// TestCrash_CommittedSegmentsSurviveReopen inserts a download and its
// segment plan, commits part of it (segment 0 fully, segment 1 half),
// then closes the database without any further write — the crash —
// and reopens the same file: the committed bytes and statuses must
// come back exactly, and the untouched segments must still read 0.
func TestCrash_CommittedSegmentsSurviveReopen(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pdm.db")
	ctx := context.Background()

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	repo := NewDownloadRepository(db.DB)
	segRepo := NewSegmentRepository(db.DB)
	d := fullDownload("reopen-1")
	d.TotalBytes = 100_000
	if err := repo.Insert(ctx, d); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	plan := domain.SplitSegments(100_000, 4, 1024)
	if err := segRepo.InsertPlan(ctx, d.ID, plan); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	if err := segRepo.UpdateCommitted(ctx, d.ID, 0, plan[0].Size()); err != nil {
		t.Fatalf("UpdateCommitted seg0: %v", err)
	}
	if err := segRepo.MarkStatus(ctx, d.ID, 0, domain.SegmentComplete); err != nil {
		t.Fatalf("MarkStatus seg0: %v", err)
	}
	if err := segRepo.UpdateCommitted(ctx, d.ID, 1, plan[1].Size()/2); err != nil {
		t.Fatalf("UpdateCommitted seg1: %v", err)
	}
	if err := segRepo.MarkStatus(ctx, d.ID, 1, domain.SegmentRunning); err != nil {
		t.Fatalf("MarkStatus seg1: %v", err)
	}
	// Crash: close with no further write and no clean shutdown step.
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(path)
	if err != nil {
		t.Fatalf("reopen after crash: %v", err)
	}
	defer db2.Close()
	segs, err := NewSegmentRepository(db2.DB).GetByDownload(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetByDownload after reopen: %v", err)
	}
	if len(segs) != 4 {
		t.Fatalf("segments after reopen = %d, want 4", len(segs))
	}
	wantCommitted := []int64{plan[0].Size(), plan[1].Size() / 2, 0, 0}
	wantStatus := []domain.SegmentStatus{
		domain.SegmentComplete, domain.SegmentRunning,
		domain.SegmentPending, domain.SegmentPending,
	}
	for i, s := range segs {
		if s.CommittedBytes != wantCommitted[i] {
			t.Fatalf("segment[%d].CommittedBytes after reopen = %d, want %d", i, s.CommittedBytes, wantCommitted[i])
		}
		if s.Status != wantStatus[i] {
			t.Fatalf("segment[%d].Status after reopen = %q, want %q", i, s.Status, wantStatus[i])
		}
		if s.StartOffset != plan[i].StartOffset || s.EndOffset != plan[i].EndOffset {
			t.Fatalf("segment[%d] offsets after reopen = %d-%d, want %d-%d", i, s.StartOffset, s.EndOffset, plan[i].StartOffset, plan[i].EndOffset)
		}
	}
	row, err := NewDownloadRepository(db2.DB).GetByID(ctx, d.ID)
	if err != nil {
		t.Fatalf("GetByID after reopen: %v", err)
	}
	if row.TotalBytes != 100_000 || row.ETag != d.ETag || !row.SupportsRange {
		t.Fatalf("download row after reopen = total %d etag %q supportsRange %v, want 100000 %q true", row.TotalBytes, row.ETag, row.SupportsRange, d.ETag)
	}
}

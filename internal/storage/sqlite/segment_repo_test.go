// Tests for the SQLite segment repository (Stage 3, Phase 1.3, TDD).
package sqlite

import (
	"context"
	"errors"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

func openSegmentRepo(t *testing.T) (*DB, *DownloadRepository, *SegmentRepository) {
	t.Helper()
	db, downloads := openTestRepo(t)
	return db, downloads, NewSegmentRepository(db.DB)
}

func assertSegmentsEqual(t *testing.T, got, want []*domain.Segment) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d segments, want %d", len(got), len(want))
	}
	for i := range want {
		g, w := got[i], want[i]
		if g.DownloadID != w.DownloadID || g.Index != w.Index ||
			g.StartOffset != w.StartOffset || g.EndOffset != w.EndOffset ||
			g.CommittedBytes != w.CommittedBytes || g.Status != w.Status {
			t.Errorf("segment[%d] mismatch:\n got: %+v\nwant: %+v", i, g, w)
		}
	}
}

func TestInsertPlanAndGetByDownload(t *testing.T) {
	_, downloads, repo := openSegmentRepo(t)
	ctx := context.Background()
	if err := downloads.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert download: %v", err)
	}

	plan := domain.SplitSegments(100, 4, 1)
	if err := repo.InsertPlan(ctx, "d1", plan); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	got, err := repo.GetByDownload(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	want := []*domain.Segment{
		{DownloadID: "d1", Index: 0, StartOffset: 0, EndOffset: 24, Status: domain.SegmentPending},
		{DownloadID: "d1", Index: 1, StartOffset: 25, EndOffset: 49, Status: domain.SegmentPending},
		{DownloadID: "d1", Index: 2, StartOffset: 50, EndOffset: 74, Status: domain.SegmentPending},
		{DownloadID: "d1", Index: 3, StartOffset: 75, EndOffset: 99, Status: domain.SegmentPending},
	}
	assertSegmentsEqual(t, got, want)
}

func TestInsertPlanRejectsMissingDownload(t *testing.T) {
	_, _, repo := openSegmentRepo(t)
	plan := domain.SplitSegments(100, 4, 1)
	if err := repo.InsertPlan(context.Background(), "no-such-download", plan); err == nil {
		t.Error("InsertPlan for missing download succeeded, want FK error")
	}
}

func TestInsertPlanIsAtomic(t *testing.T) {
	_, downloads, repo := openSegmentRepo(t)
	ctx := context.Background()
	if err := downloads.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert download: %v", err)
	}

	// Two segments share index 0: the second insert violates the
	// primary key, so the whole plan must roll back.
	bad := []domain.Segment{
		{Index: 0, StartOffset: 0, EndOffset: 49, Status: domain.SegmentPending},
		{Index: 0, StartOffset: 50, EndOffset: 99, Status: domain.SegmentPending},
	}
	if err := repo.InsertPlan(ctx, "d1", bad); err == nil {
		t.Fatal("InsertPlan with duplicate index succeeded, want error")
	}
	got, err := repo.GetByDownload(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("GetByDownload after failed InsertPlan = %d segments, want 0 (rolled back)", len(got))
	}
}

func TestUpdateCommittedAdvancesOnly(t *testing.T) {
	_, downloads, repo := openSegmentRepo(t)
	ctx := context.Background()
	if err := downloads.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert download: %v", err)
	}
	if err := repo.InsertPlan(ctx, "d1", domain.SplitSegments(100, 4, 1)); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}

	if err := repo.UpdateCommitted(ctx, "d1", 0, 10); err != nil {
		t.Fatalf("UpdateCommitted 0 -> 10: %v", err)
	}
	// Equal value is a no-op success, not a regression.
	if err := repo.UpdateCommitted(ctx, "d1", 0, 10); err != nil {
		t.Fatalf("UpdateCommitted 10 -> 10: %v", err)
	}
	// Going backwards must be refused and must not change the value.
	if err := repo.UpdateCommitted(ctx, "d1", 0, 5); !errors.Is(err, domain.ErrCommittedBytesRegression) {
		t.Errorf("UpdateCommitted 10 -> 5: err = %v, want ErrCommittedBytesRegression", err)
	}
	got, err := repo.GetByDownload(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if got[0].CommittedBytes != 10 {
		t.Errorf("segment[0].CommittedBytes = %d, want 10 (regression must not stick)", got[0].CommittedBytes)
	}
	// Committed bytes past the end of the segment are invalid.
	if err := repo.UpdateCommitted(ctx, "d1", 0, 26); err == nil {
		t.Error("UpdateCommitted past segment end succeeded, want error")
	}
	got, err = repo.GetByDownload(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if got[0].CommittedBytes != 10 {
		t.Errorf("segment[0].CommittedBytes = %d, want 10 after overshoot attempt", got[0].CommittedBytes)
	}
}

func TestUpdateCommittedMissingSegment(t *testing.T) {
	_, downloads, repo := openSegmentRepo(t)
	ctx := context.Background()
	if err := downloads.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert download: %v", err)
	}
	if err := repo.UpdateCommitted(ctx, "d1", 7, 10); !errors.Is(err, domain.ErrSegmentNotFound) {
		t.Errorf("UpdateCommitted missing segment: err = %v, want ErrSegmentNotFound", err)
	}
}

func TestMarkStatus(t *testing.T) {
	_, downloads, repo := openSegmentRepo(t)
	ctx := context.Background()
	if err := downloads.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert download: %v", err)
	}
	if err := repo.InsertPlan(ctx, "d1", domain.SplitSegments(100, 4, 1)); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}

	if err := repo.MarkStatus(ctx, "d1", 1, domain.SegmentRunning); err != nil {
		t.Fatalf("MarkStatus running: %v", err)
	}
	if err := repo.MarkStatus(ctx, "d1", 1, domain.SegmentComplete); err != nil {
		t.Fatalf("MarkStatus complete: %v", err)
	}
	got, err := repo.GetByDownload(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if got[1].Status != domain.SegmentComplete {
		t.Errorf("segment[1].Status = %q, want %q", got[1].Status, domain.SegmentComplete)
	}
	if got[0].Status != domain.SegmentPending {
		t.Errorf("segment[0].Status = %q, want %q (untouched)", got[0].Status, domain.SegmentPending)
	}

	if err := repo.MarkStatus(ctx, "d1", 1, "bogus"); err == nil {
		t.Error("MarkStatus with invalid status succeeded, want error")
	}
	if err := repo.MarkStatus(ctx, "d1", 9, domain.SegmentRunning); !errors.Is(err, domain.ErrSegmentNotFound) {
		t.Errorf("MarkStatus missing segment: err = %v, want ErrSegmentNotFound", err)
	}
}

func TestDeleteByDownload(t *testing.T) {
	_, downloads, repo := openSegmentRepo(t)
	ctx := context.Background()
	for _, id := range []string{"d1", "d2"} {
		if err := downloads.Insert(ctx, fullDownload(id)); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
		if err := repo.InsertPlan(ctx, id, domain.SplitSegments(100, 4, 1)); err != nil {
			t.Fatalf("InsertPlan %s: %v", id, err)
		}
	}

	if err := repo.DeleteByDownload(ctx, "d1"); err != nil {
		t.Fatalf("DeleteByDownload d1: %v", err)
	}
	got, err := repo.GetByDownload(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByDownload d1: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("GetByDownload d1 after delete = %d segments, want 0", len(got))
	}
	// The other download's segments are untouched.
	got, err = repo.GetByDownload(ctx, "d2")
	if err != nil {
		t.Fatalf("GetByDownload d2: %v", err)
	}
	if len(got) != 4 {
		t.Errorf("GetByDownload d2 = %d segments, want 4", len(got))
	}
	// Deleting again is a no-op, not an error.
	if err := repo.DeleteByDownload(ctx, "d1"); err != nil {
		t.Errorf("second DeleteByDownload: %v, want nil", err)
	}
}

func TestDeleteDownloadCascadesSegments(t *testing.T) {
	_, downloads, repo := openSegmentRepo(t)
	ctx := context.Background()
	if err := downloads.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert download: %v", err)
	}
	if err := repo.InsertPlan(ctx, "d1", domain.SplitSegments(100, 4, 1)); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	if err := downloads.Delete(ctx, "d1"); err != nil {
		t.Fatalf("Delete download: %v", err)
	}
	got, err := repo.GetByDownload(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("GetByDownload after download delete = %d segments, want 0 (FK cascade)", len(got))
	}
}

func TestGetByDownloadEmpty(t *testing.T) {
	_, downloads, repo := openSegmentRepo(t)
	ctx := context.Background()
	if err := downloads.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert download: %v", err)
	}
	got, err := repo.GetByDownload(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByDownload: %v", err)
	}
	if len(got) != 0 {
		t.Errorf("GetByDownload with no segments = %d, want 0", len(got))
	}
}

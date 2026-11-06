// Tests for the SQLite download repository (Phase 2.2/2.3, TDD).
package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

func openTestRepo(t *testing.T) (*DB, *DownloadRepository) {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "pdm.db"))
	if err != nil {
		t.Fatalf("Open test DB: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, NewDownloadRepository(db.DB)
}

// fullDownload builds a download with every persisted field set,
// including the Stage 3 validators (ETag / LastModified / SupportsRange).
func fullDownload(id string) *domain.Download {
	now := time.Date(2026, 11, 6, 8, 0, 0, 0, time.UTC)
	return &domain.Download{
		ID:              id,
		URL:             "https://example.com/" + id + ".bin",
		Filename:        id + ".bin",
		DestinationDir:  "/tmp/dest",
		TempPath:        "/tmp/dest/" + id + ".bin.part",
		FinalPath:       "/tmp/dest/" + id + ".bin",
		Status:          domain.StatusQueued,
		TotalBytes:      4096,
		DownloadedBytes: 1024,
		ETag:            `"abc123"`,
		LastModified:    "Wed, 05 Nov 2026 12:00:00 GMT",
		SupportsRange:   true,
		ErrorCode:       domain.ErrorCodeNetwork,
		ErrorMessage:    "boom",
		CreatedAt:       now,
		UpdatedAt:       now,
	}
}

// assertDownloadsEqual compares every persisted field of two downloads.
// Times compare by instant (storage is UTC RFC 3339 text), not by
// monotonic clock or location pointer.
func assertDownloadsEqual(t *testing.T, got, want *domain.Download) {
	t.Helper()
	if got == nil || want == nil {
		t.Fatalf("assertDownloadsEqual: got=%v want=%v, one is nil", got, want)
	}
	if got.ID != want.ID || got.URL != want.URL ||
		got.Filename != want.Filename || got.DestinationDir != want.DestinationDir ||
		got.TempPath != want.TempPath || got.FinalPath != want.FinalPath ||
		got.Status != want.Status ||
		got.TotalBytes != want.TotalBytes || got.DownloadedBytes != want.DownloadedBytes ||
		got.ETag != want.ETag || got.LastModified != want.LastModified ||
		got.SupportsRange != want.SupportsRange ||
		got.ErrorCode != want.ErrorCode || got.ErrorMessage != want.ErrorMessage {
		t.Errorf("download mismatch:\n got: %+v\nwant: %+v", got, want)
	}
	if !got.CreatedAt.Equal(want.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, want.CreatedAt)
	}
	if !got.UpdatedAt.Equal(want.UpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", got.UpdatedAt, want.UpdatedAt)
	}
}

// queueOrder returns the download IDs in queue_position order, reading
// the raw column so the test asserts the stored order, not just ListAll.
func queueOrder(t *testing.T, db *DB) []string {
	t.Helper()
	rows, err := db.Query(`SELECT id FROM downloads ORDER BY queue_position ASC, id ASC`)
	if err != nil {
		t.Fatalf("query queue order: %v", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan queue order: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("queue order rows: %v", err)
	}
	return ids
}

func queuePosition(t *testing.T, db *DB, id string) int64 {
	t.Helper()
	var pos int64
	if err := db.QueryRow(`SELECT queue_position FROM downloads WHERE id = ?`, id).Scan(&pos); err != nil {
		t.Fatalf("query queue_position for %q: %v", id, err)
	}
	return pos
}

func TestInsertAndGetRoundTrip(t *testing.T) {
	_, repo := openTestRepo(t)
	ctx := context.Background()

	want := fullDownload("d1")
	if err := repo.Insert(ctx, want); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, err := repo.GetByID(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	assertDownloadsEqual(t, got, want)
}

func TestInsertAndGetUnknownSizeAndEmptyValidators(t *testing.T) {
	db, repo := openTestRepo(t)
	ctx := context.Background()

	// Unknown total size and no validators yet: TotalBytes -1 maps to
	// NULL, empty ETag/LastModified map to NULL, SupportsRange false.
	want := fullDownload("d1")
	want.TotalBytes = -1
	want.ETag = ""
	want.LastModified = ""
	want.SupportsRange = false
	want.ErrorCode = ""
	want.ErrorMessage = ""
	if err := repo.Insert(ctx, want); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, err := repo.GetByID(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	assertDownloadsEqual(t, got, want)

	// The stored values really are NULL, not 0 / empty string sentinels.
	var total any
	var etag any
	var lastMod any
	if err := db.QueryRow(
		`SELECT total_bytes, etag, last_modified FROM downloads WHERE id = 'd1'`,
	).Scan(&total, &etag, &lastMod); err != nil {
		t.Fatalf("raw select: %v", err)
	}
	if total != nil {
		t.Errorf("total_bytes stored = %v, want NULL", total)
	}
	if etag != nil {
		t.Errorf("etag stored = %v, want NULL", etag)
	}
	if lastMod != nil {
		t.Errorf("last_modified stored = %v, want NULL", lastMod)
	}
}

func TestInsertDuplicateIDFails(t *testing.T) {
	_, repo := openTestRepo(t)
	ctx := context.Background()
	if err := repo.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("first Insert: %v", err)
	}
	if err := repo.Insert(ctx, fullDownload("d1")); err == nil {
		t.Error("second Insert with same ID succeeded, want error")
	}
}

func TestGetByIDNotFound(t *testing.T) {
	_, repo := openTestRepo(t)
	got, err := repo.GetByID(context.Background(), "missing")
	if !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Errorf("GetByID missing: err = %v, want ErrDownloadNotFound", err)
	}
	if got != nil {
		t.Errorf("GetByID missing: got %+v, want nil", got)
	}
}

func TestUpdateWritesAllFields(t *testing.T) {
	_, repo := openTestRepo(t)
	ctx := context.Background()
	if err := repo.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	updated := fullDownload("d1")
	updated.URL = "https://example.com/other.bin"
	updated.Filename = "other.bin"
	updated.DestinationDir = "/tmp/other"
	updated.TempPath = "/tmp/other/other.bin.part"
	updated.FinalPath = "/tmp/other/other.bin"
	updated.Status = domain.StatusDownloading
	updated.TotalBytes = 8192
	updated.DownloadedBytes = 4096
	updated.ETag = `"changed"`
	updated.LastModified = "Thu, 06 Nov 2026 08:00:00 GMT"
	updated.SupportsRange = false
	updated.ErrorCode = domain.ErrorCodeHTTPServer
	updated.ErrorMessage = "server exploded"
	updated.UpdatedAt = time.Date(2026, 11, 6, 9, 30, 0, 0, time.UTC)
	if err := repo.Update(ctx, updated); err != nil {
		t.Fatalf("Update: %v", err)
	}
	got, err := repo.GetByID(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	assertDownloadsEqual(t, got, updated)
}

func TestUpdateNotFound(t *testing.T) {
	_, repo := openTestRepo(t)
	err := repo.Update(context.Background(), fullDownload("missing"))
	if !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Errorf("Update missing: err = %v, want ErrDownloadNotFound", err)
	}
}

func TestUpdateStatusOnlyChangesStatusAndUpdatedAt(t *testing.T) {
	_, repo := openTestRepo(t)
	ctx := context.Background()
	if err := repo.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	before, err := repo.GetByID(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByID before: %v", err)
	}
	newUpdatedAt := time.Date(2026, 11, 6, 10, 0, 0, 0, time.UTC)
	if err := repo.UpdateStatus(ctx, "d1", domain.StatusDownloading, newUpdatedAt); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}
	after, err := repo.GetByID(ctx, "d1")
	if err != nil {
		t.Fatalf("GetByID after: %v", err)
	}
	if after.Status != domain.StatusDownloading {
		t.Errorf("status = %q, want %q", after.Status, domain.StatusDownloading)
	}
	if !after.UpdatedAt.Equal(newUpdatedAt) {
		t.Errorf("UpdatedAt = %v, want %v", after.UpdatedAt, newUpdatedAt)
	}
	// Everything else must be untouched.
	want := *before
	want.Status = domain.StatusDownloading
	want.UpdatedAt = newUpdatedAt
	assertDownloadsEqual(t, after, &want)
}

func TestUpdateStatusNotFound(t *testing.T) {
	_, repo := openTestRepo(t)
	err := repo.UpdateStatus(context.Background(), "missing", domain.StatusFailed, time.Now().UTC())
	if !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Errorf("UpdateStatus missing: err = %v, want ErrDownloadNotFound", err)
	}
}

func TestDeleteRemovesRow(t *testing.T) {
	_, repo := openTestRepo(t)
	ctx := context.Background()
	if err := repo.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert: %v", err)
	}
	if err := repo.Delete(ctx, "d1"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := repo.GetByID(ctx, "d1"); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Errorf("GetByID after delete: err = %v, want ErrDownloadNotFound", err)
	}
	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("ListAll after delete = %d rows, want 0", len(all))
	}
	// Deleting again reports not found.
	if err := repo.Delete(ctx, "d1"); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Errorf("second Delete: err = %v, want ErrDownloadNotFound", err)
	}
}

func TestDeleteCascadesSegments(t *testing.T) {
	db, repo := openTestRepo(t)
	ctx := context.Background()
	if err := repo.Insert(ctx, fullDownload("d1")); err != nil {
		t.Fatalf("Insert d1: %v", err)
	}
	if err := repo.Insert(ctx, fullDownload("d2")); err != nil {
		t.Fatalf("Insert d2: %v", err)
	}
	// One segment per download, inserted directly: the repo must not
	// break the FK, and deleting d1 must cascade only its own segments.
	for _, id := range []string{"d1", "d2"} {
		if _, err := db.Exec(
			`INSERT INTO segments(download_id, "index", start_offset, end_offset, committed_bytes, status) VALUES(?, 0, 0, 4095, 0, 'pending')`,
			id,
		); err != nil {
			t.Fatalf("insert segment for %s: %v", id, err)
		}
	}
	if err := repo.Delete(ctx, "d1"); err != nil {
		t.Fatalf("Delete d1: %v", err)
	}
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM segments WHERE download_id = 'd1'`).Scan(&n); err != nil {
		t.Fatalf("count d1 segments: %v", err)
	}
	if n != 0 {
		t.Errorf("segments for deleted d1 = %d, want 0 (FK cascade)", n)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM segments WHERE download_id = 'd2'`).Scan(&n); err != nil {
		t.Fatalf("count d2 segments: %v", err)
	}
	if n != 1 {
		t.Errorf("segments for surviving d2 = %d, want 1", n)
	}
}

func TestListAllReturnsQueueOrder(t *testing.T) {
	db, repo := openTestRepo(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if err := repo.Insert(ctx, fullDownload(id)); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
	}
	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("ListAll = %d rows, want 3", len(all))
	}
	for i, want := range []string{"a", "b", "c"} {
		if all[i].ID != want {
			t.Errorf("ListAll[%d].ID = %q, want %q", i, all[i].ID, want)
		}
	}
	// Stored positions are max+1 from 0.
	if got := queuePosition(t, db, "a"); got != 0 {
		t.Errorf("queue_position a = %d, want 0", got)
	}
	if got := queuePosition(t, db, "b"); got != 1 {
		t.Errorf("queue_position b = %d, want 1", got)
	}
	if got := queuePosition(t, db, "c"); got != 2 {
		t.Errorf("queue_position c = %d, want 2", got)
	}
}

func TestListAllEmpty(t *testing.T) {
	_, repo := openTestRepo(t)
	all, err := repo.ListAll(context.Background())
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	if len(all) != 0 {
		t.Errorf("ListAll on empty DB = %d rows, want 0", len(all))
	}
}

func TestInsertAfterDeleteAppendsAtEnd(t *testing.T) {
	db, repo := openTestRepo(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c"} {
		if err := repo.Insert(ctx, fullDownload(id)); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
	}
	if err := repo.Delete(ctx, "b"); err != nil {
		t.Fatalf("Delete b: %v", err)
	}
	// Delete leaves a gap (a=0, c=2); the next insert takes max+1 = 3.
	if err := repo.Insert(ctx, fullDownload("d")); err != nil {
		t.Fatalf("Insert d: %v", err)
	}
	if got := queueOrder(t, db); len(got) != 3 || got[0] != "a" || got[1] != "c" || got[2] != "d" {
		t.Errorf("queue order after delete+insert = %v, want [a c d]", got)
	}
	if got := queuePosition(t, db, "d"); got != 3 {
		t.Errorf("queue_position d = %d, want 3 (max+1, gap not renumbered)", got)
	}
}

func TestMoveReordersQueue(t *testing.T) {
	db, repo := openTestRepo(t)
	ctx := context.Background()
	for _, id := range []string{"a", "b", "c", "d"} {
		if err := repo.Insert(ctx, fullDownload(id)); err != nil {
			t.Fatalf("Insert %s: %v", id, err)
		}
	}
	// Move c to the front.
	if err := repo.Move(ctx, "c", 0); err != nil {
		t.Fatalf("Move c to 0: %v", err)
	}
	if got := queueOrder(t, db); len(got) != 4 || got[0] != "c" || got[1] != "a" || got[2] != "b" || got[3] != "d" {
		t.Errorf("order after Move(c,0) = %v, want [c a b d]", got)
	}
	// Move a to the end.
	if err := repo.Move(ctx, "a", 3); err != nil {
		t.Fatalf("Move a to 3: %v", err)
	}
	if got := queueOrder(t, db); len(got) != 4 || got[0] != "c" || got[1] != "b" || got[2] != "d" || got[3] != "a" {
		t.Errorf("order after Move(a,3) = %v, want [c b d a]", got)
	}
	// Moving to its current position is a no-op.
	if err := repo.Move(ctx, "b", 1); err != nil {
		t.Fatalf("Move b to 1 (no-op): %v", err)
	}
	if got := queueOrder(t, db); len(got) != 4 || got[0] != "c" || got[1] != "b" || got[2] != "d" || got[3] != "a" {
		t.Errorf("order after no-op Move = %v, want [c b d a]", got)
	}
	// Out-of-range positions clamp to the ends.
	if err := repo.Move(ctx, "b", 99); err != nil {
		t.Fatalf("Move b to 99: %v", err)
	}
	if got := queueOrder(t, db); got[3] != "b" {
		t.Errorf("order after Move(b,99) = %v, want b last", got)
	}
	if err := repo.Move(ctx, "b", -5); err != nil {
		t.Fatalf("Move b to -5: %v", err)
	}
	if got := queueOrder(t, db); got[0] != "b" {
		t.Errorf("order after Move(b,-5) = %v, want b first", got)
	}
	// Positions stay dense 0..n-1 after reorders.
	for i, id := range queueOrder(t, db) {
		if got := queuePosition(t, db, id); got != int64(i) {
			t.Errorf("queue_position %s = %d, want %d after moves", id, got, i)
		}
	}
	// ListAll follows the moved order.
	all, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatalf("ListAll: %v", err)
	}
	want := queueOrder(t, db)
	for i := range want {
		if all[i].ID != want[i] {
			t.Errorf("ListAll[%d].ID = %q, want %q", i, all[i].ID, want[i])
		}
	}
}

func TestMoveNotFound(t *testing.T) {
	_, repo := openTestRepo(t)
	ctx := context.Background()
	if err := repo.Insert(ctx, fullDownload("a")); err != nil {
		t.Fatalf("Insert a: %v", err)
	}
	if err := repo.Move(ctx, "missing", 0); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Errorf("Move missing: err = %v, want ErrDownloadNotFound", err)
	}
}

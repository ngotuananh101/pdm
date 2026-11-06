// Phase 6.2 tests: crash and inconsistency simulation at Stage 2
// level (ARCH §13). Stage 2 can only test the DB-commit + .part side:
// there is no segment checkpoint, no Range resume and no durable
// size/validator yet, so a crash between a file write and the DB
// commit, or between rename and the completed commit, is simulated by
// seeding the DB row + files in the post-crash state and running
// restart recovery over them. Every case must leave the destination
// file untouched, never join bytes from different versions, and land
// the job in a clear state.
package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// TestCrash_DownloadingWithPartAndExistingDestNotOverwritten simulates
// a kill after the worker wrote (and synced) bytes but before the DB
// recorded anything past downloading: the .part holds 512 bytes, the
// DB says downloading/512, and a finished file already exists at the
// destination from an earlier download. Recovery must re-queue the job
// and leave both files byte-identical: no rename, no overwrite, no
// mixing of the .part bytes into the destination.
func TestCrash_DownloadingWithPartAndExistingDestNotOverwritten(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()

	partContent := make([]byte, 512)
	for i := range partContent {
		partContent[i] = 0xAB
	}
	destContent := []byte("user-data-from-an-earlier-download")
	dest := filepath.Join(dir, "a.bin")
	if err := os.WriteFile(dest, destContent, 0o644); err != nil {
		t.Fatal(err)
	}
	d := seededDownload("crash-dl-dest", "a.bin", domain.StatusDownloading, dir)
	d.DownloadedBytes = 512
	d.TempPath = filepath.Join(dir, ".a.bin.crash-dl-dest.part")
	if err := os.WriteFile(d.TempPath, partContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if dispatch := seedAndInstall(t, svc, repo, d); !dispatch {
		t.Fatal("downloading job with .part should be dispatched after recovery")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusQueued {
		t.Fatalf("status = %q, want queued", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusQueued {
		t.Fatalf("DB status = %q, want queued", got)
	}
	if got, err := os.ReadFile(dest); err != nil || string(got) != string(destContent) {
		t.Fatalf("destination file changed after recovery: %q (err %v), want %q", got, err, destContent)
	}
	if got, err := os.ReadFile(d.TempPath); err != nil || len(got) != 512 || got[0] != 0xAB {
		t.Fatalf(".part file changed after recovery: len %d (err %v)", len(got), err)
	}
}

// TestCrash_FinalizingPartAndFinalBothPresent simulates a kill between
// rename and the completed commit where the outcome is ambiguous: the
// .part is still there and a file already exists at the destination.
// Recovery must fail the job (never mark completed on file existence
// alone, ARCH §5) and must not delete, rename or modify either file:
// the user checks the destination before retrying.
func TestCrash_FinalizingPartAndFinalBothPresent(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()

	partContent := []byte("partial-bytes")
	finalContent := []byte("finished-bytes-maybe")
	d := seededDownload("crash-fin-both", "a.bin", domain.StatusFinalizing, dir)
	d.TempPath = filepath.Join(dir, ".a.bin.crash-fin-both.part")
	d.FinalPath = filepath.Join(dir, "a.bin")
	if err := os.WriteFile(d.TempPath, partContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(d.FinalPath, finalContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if dispatch := seedAndInstall(t, svc, repo, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusFailed {
		t.Fatalf("DB status = %q, want failed", got)
	}
	if got, _ := os.ReadFile(d.TempPath); string(got) != string(partContent) {
		t.Fatalf(".part changed after recovery: %q, want %q", got, partContent)
	}
	if got, _ := os.ReadFile(d.FinalPath); string(got) != string(finalContent) {
		t.Fatalf("destination changed after recovery: %q, want %q", got, finalContent)
	}
}

// TestCrash_OrphanPartWithoutDBRowIgnored covers the DB-missing side of
// the inconsistency: a .part file on disk with no DB row must not
// resurrect a job, must not be deleted, and must not block loading the
// jobs that do have rows.
func TestCrash_OrphanPartWithoutDBRowIgnored(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()

	orphan := filepath.Join(dir, ".orphan.bin.orphan-id.part")
	orphanContent := []byte("orphan-bytes")
	if err := os.WriteFile(orphan, orphanContent, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := svc.LoadFromRepository(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := len(svc.List()); got != 0 {
		t.Fatalf("jobs = %d, want 0: orphan .part must not create a job", got)
	}
	if got, err := os.ReadFile(orphan); err != nil || string(got) != string(orphanContent) {
		t.Fatalf("orphan .part changed after load: %q (err %v)", got, err)
	}
}

// TestCrash_CompletedRowMissingFinalKeptVisible documents the Stage 2
// limit for the reverse inconsistency: a completed row whose final
// file is gone is kept completed and visible, not re-failed and not
// re-downloaded — Stage 2 has no durable size/validator to verify a
// completed file with, so recovery does not second-guess terminal
// rows. The missing file is not recreated.
func TestCrash_CompletedRowMissingFinalKeptVisible(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()

	d := seededDownload("crash-done-gone", "a.bin", domain.StatusCompleted, dir)
	d.DownloadedBytes = 1024
	d.FinalPath = filepath.Join(dir, "a.bin")
	// No file at FinalPath: the DB says completed, the disk disagrees.
	if dispatch := seedAndInstall(t, svc, repo, d); dispatch {
		t.Fatal("completed job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusCompleted {
		t.Fatalf("status = %q, want completed (kept as stored)", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusCompleted {
		t.Fatalf("DB status = %q, want completed", got)
	}
	if _, err := os.Stat(d.FinalPath); !os.IsNotExist(err) {
		t.Fatalf("recovery must not recreate the missing final file (stat err = %v)", err)
	}
}

// TestCrash_UnknownStatusRowKeptNotDispatched loads a row whose status
// was written by a newer version together with a normal queued row:
// the unknown row stays visible but is never dispatched, and the load
// continues so the queued row still dispatches.
func TestCrash_UnknownStatusRowKeptNotDispatched(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	ctx := context.Background()
	dir := t.TempDir()

	unknown := seededDownload("crash-unknown", "u.bin", domain.Status("bogus-status"), dir)
	queued := seededDownload("crash-unknown-queued", "q.bin", domain.StatusQueued, dir)
	for _, d := range []*domain.Download{unknown, queued} {
		if err := repo.Insert(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	if err := svc.LoadFromRepository(ctx); err != nil {
		t.Fatal(err)
	}
	if got := len(svc.List()); got != 2 {
		t.Fatalf("jobs = %d, want 2: unknown-status row must not stop the load", got)
	}
	if got := getJob(svc, unknown.ID).Status; got != domain.Status("bogus-status") {
		t.Fatalf("unknown status = %q, want it kept as stored", got)
	}
	// The queued row is dispatched on load; the blocking downloader
	// holds it in downloading.
	waitFor(t, func() bool {
		return getJob(svc, queued.ID).Status == domain.StatusDownloading
	}, "queued job downloading after load")
}

// TestCrash_AddDownloadInvalidDestinationDirRejected covers the invalid
// path case at the service boundary: a destination that exists as a
// regular file is rejected before anything is persisted, so no DB row
// and no job appear for the failed add.
func TestCrash_AddDownloadInvalidDestinationDirRejected(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()

	notADir := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(notADir, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/a.bin",
		DestinationDir: notADir,
		Resolution:     domain.ResolutionAutoRename,
	}); err == nil {
		t.Fatal("AddDownload with a file as destination dir should fail")
	}
	if got := len(svc.List()); got != 0 {
		t.Fatalf("jobs = %d, want 0 after rejected add", got)
	}
	rows, err := repo.ListAll(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("DB rows = %d, want 0 after rejected add", len(rows))
	}
}

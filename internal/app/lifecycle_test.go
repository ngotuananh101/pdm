// Phase 5.2/5.3/5.4 tests: startup load, restart recovery, and
// shutdown (checkpoint + close DB).
package app

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/storage/sqlite"
)

// seededDownload builds a persisted download with the given status.
// The row is inserted by the test via repo.Insert, which assigns
// queue_position in insertion order.
func seededDownload(id, filename string, status domain.Status, dir string) *domain.Download {
	now := time.Now().UTC()
	return &domain.Download{
		ID:             id,
		URL:            "http://example.com/" + filename,
		Filename:       filename,
		DestinationDir: dir,
		Status:         status,
		TotalBytes:     -1,
		CreatedAt:      now,
		UpdatedAt:      now,
	}
}

// newRecoveryService builds a service backed by repo, shut down on
// test cleanup. The blocking downloader holds dispatched jobs in
// downloading so tests can observe the dispatched state.
func newRecoveryService(t *testing.T, repo domain.DownloadRepository) *Service {
	t.Helper()
	cfg := testConfig(t)
	svc := NewServiceWithRepository(cfg, blockOnContextDownloader{}, repo)
	t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })
	return svc
}

// installRestored runs the unexported install step for one persisted
// download and returns whether it should be dispatched. It exercises
// recovery without the scheduler, so the recovered status is directly
// observable.
func installRestored(t *testing.T, svc *Service, d *domain.Download) bool {
	t.Helper()
	svc.mu.Lock()
	defer svc.mu.Unlock()
	dispatch, err := svc.installRestoredLocked(d)
	if err != nil {
		t.Fatal(err)
	}
	return dispatch
}

func seedAndInstall(t *testing.T, svc *Service, repo *sqlite.DownloadRepository, d *domain.Download) bool {
	t.Helper()
	if err := repo.Insert(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	return installRestored(t, svc, d)
}

func rowStatus(t *testing.T, repo *sqlite.DownloadRepository, id string) domain.Status {
	t.Helper()
	row, err := repo.GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return row.Status
}

// ---------- 5.2 startup load ----------

func TestLoadFromRepository_RestoresJobs(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	ctx := context.Background()
	queued := seededDownload("load-queued", "q.bin", domain.StatusQueued, dir)
	paused := seededDownload("load-paused", "p.bin", domain.StatusPaused, dir)
	done := seededDownload("load-done", "d.bin", domain.StatusCompleted, dir)
	done.FinalPath = filepath.Join(dir, "d.bin")
	for _, d := range []*domain.Download{queued, paused, done} {
		if err := repo.Insert(ctx, d); err != nil {
			t.Fatal(err)
		}
	}

	svc := newRecoveryService(t, repo)
	if err := svc.LoadFromRepository(ctx); err != nil {
		t.Fatal(err)
	}
	list := svc.List()
	if len(list) != 3 {
		t.Fatalf("loaded %d jobs, want 3", len(list))
	}
	if list[0].ID != queued.ID || list[1].ID != paused.ID || list[2].ID != done.ID {
		t.Fatalf("order = %q %q %q, want queued/paused/done", list[0].ID, list[1].ID, list[2].ID)
	}
	// The queued job is dispatched on load (the blocking downloader
	// holds it in downloading).
	waitFor(t, func() bool {
		return getJob(svc, queued.ID).Status == domain.StatusDownloading
	}, "queued job downloading after load")
	if got := getJob(svc, paused.ID).Status; got != domain.StatusPaused {
		t.Fatalf("paused job status = %q, want paused", got)
	}
	if got := getJob(svc, done.ID).Status; got != domain.StatusCompleted {
		t.Fatalf("completed job status = %q, want completed", got)
	}
}

func TestLoadFromRepository_EmptyDBNilRepoAndSecondLoad(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	if err := svc.LoadFromRepository(context.Background()); err != nil {
		t.Fatalf("LoadFromRepository on empty DB: %v", err)
	}
	if got := len(svc.List()); got != 0 {
		t.Fatalf("jobs = %d, want 0", got)
	}
	if err := svc.LoadFromRepository(context.Background()); err != nil {
		t.Fatalf("second LoadFromRepository: %v", err)
	}
	if got := len(svc.List()); got != 0 {
		t.Fatalf("jobs after second load = %d, want 0", got)
	}

	// No repo: no-op, no panic.
	svc2 := NewService(testConfig(t), blockOnContextDownloader{})
	t.Cleanup(func() { _ = svc2.Shutdown(context.Background()) })
	if err := svc2.LoadFromRepository(context.Background()); err != nil {
		t.Fatalf("LoadFromRepository without repo: %v", err)
	}
}

func TestLoadFromRepository_RecoversFinalizingEndToEnd(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	dir := t.TempDir()
	d := seededDownload("load-final", "f.bin", domain.StatusFinalizing, dir)
	d.TempPath = filepath.Join(dir, ".f.bin.load-final.part")
	if err := repo.Insert(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	svc := newRecoveryService(t, repo)
	if err := svc.LoadFromRepository(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("finalizing job after load = %q, want failed", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusFailed {
		t.Fatalf("DB status = %q, want failed", got)
	}
}

// ---------- 5.3 restart recovery ----------

func TestRecovery_ProbingRequeued(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	d := seededDownload("rec-probing", "a.bin", domain.StatusProbing, t.TempDir())
	if dispatch := seedAndInstall(t, svc, repo, d); !dispatch {
		t.Fatal("probing job should be dispatched after recovery")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusQueued {
		t.Fatalf("status = %q, want queued", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusQueued {
		t.Fatalf("DB status = %q, want queued", got)
	}
}

func TestRecovery_DownloadingWithPartRequeued(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()
	d := seededDownload("rec-dl-part", "a.bin", domain.StatusDownloading, dir)
	d.DownloadedBytes = 512
	d.TempPath = filepath.Join(dir, ".a.bin.rec-dl-part.part")
	if err := os.WriteFile(d.TempPath, make([]byte, 512), 0o644); err != nil {
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
}

func TestRecovery_DownloadingMissingPartFailed(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()
	d := seededDownload("rec-dl-nopart", "a.bin", domain.StatusDownloading, dir)
	d.DownloadedBytes = 512
	d.TempPath = filepath.Join(dir, ".a.bin.rec-dl-nopart.part")
	// No .part file on disk, although metadata says bytes were
	// downloaded: the job must fail, not silently restart.
	if dispatch := seedAndInstall(t, svc, repo, d); dispatch {
		t.Fatal("downloading job without .part must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusFailed {
		t.Fatalf("DB status = %q, want failed", got)
	}
	row, err := repo.GetByID(context.Background(), d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.ErrorMessage == "" {
		t.Fatal("failed recovery should record an error message")
	}
}

func TestRecovery_DownloadingMissingPartNoBytesRequeued(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()
	d := seededDownload("rec-dl-zero", "a.bin", domain.StatusDownloading, dir)
	d.TempPath = filepath.Join(dir, ".a.bin.rec-dl-zero.part")
	// Nothing was downloaded yet, so there is nothing to lose: the
	// engine recreates the .part on dispatch.
	if dispatch := seedAndInstall(t, svc, repo, d); !dispatch {
		t.Fatal("downloading job with zero bytes should be dispatched after recovery")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusQueued {
		t.Fatalf("status = %q, want queued", got)
	}
}

func TestRecovery_DownloadingConventionalPartRequeued(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()
	d := seededDownload("rec-dl-conv", "a.bin", domain.StatusDownloading, dir)
	d.DownloadedBytes = 256
	// TempPath was never recorded; the engine's conventional path
	// (".<filename>.<id>.part") holds the .part file.
	part := filepath.Join(dir, ".a.bin.rec-dl-conv.part")
	if err := os.WriteFile(part, make([]byte, 256), 0o644); err != nil {
		t.Fatal(err)
	}
	if dispatch := seedAndInstall(t, svc, repo, d); !dispatch {
		t.Fatal("downloading job with conventional .part should be dispatched after recovery")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusQueued {
		t.Fatalf("status = %q, want queued", got)
	}
}

func TestRecovery_DownloadingConventionalPartMissingWithBytesFailed(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()
	d := seededDownload("rec-dl-conv-missing", "a.bin", domain.StatusDownloading, dir)
	d.DownloadedBytes = 100
	// No TempPath and no conventional .part file, although metadata
	// says bytes were downloaded.
	if dispatch := seedAndInstall(t, svc, repo, d); dispatch {
		t.Fatal("downloading job without any .part must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed", got)
	}
}

func TestRecovery_FinalizingWithPartFailed(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()
	d := seededDownload("rec-fin-part", "a.bin", domain.StatusFinalizing, dir)
	d.TempPath = filepath.Join(dir, ".a.bin.rec-fin-part.part")
	if err := os.WriteFile(d.TempPath, []byte("partial"), 0o644); err != nil {
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
}

func TestRecovery_FinalizingPartGoneFinalExistsFailed(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	dir := t.TempDir()
	d := seededDownload("rec-fin-final", "a.bin", domain.StatusFinalizing, dir)
	d.TempPath = filepath.Join(dir, ".a.bin.rec-fin-final.part")
	d.FinalPath = filepath.Join(dir, "a.bin")
	if err := os.WriteFile(d.FinalPath, []byte("finished"), 0o644); err != nil {
		t.Fatal(err)
	}
	// The .part is gone and a file exists at the destination, but
	// ARCH §5 forbids marking completed merely because a file
	// exists: Stage 2 has no durable size/validator to verify it
	// with, so the job must fail and let the user check.
	if dispatch := seedAndInstall(t, svc, repo, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed (never completed on file existence alone)", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusFailed {
		t.Fatalf("DB status = %q, want failed", got)
	}
}

func TestRecovery_FinalizingNothingExistsFailed(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	d := seededDownload("rec-fin-none", "a.bin", domain.StatusFinalizing, t.TempDir())
	if dispatch := seedAndInstall(t, svc, repo, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed", got)
	}
}

func TestRecovery_TerminalAndPausedKept(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	for _, status := range []domain.Status{
		domain.StatusCompleted, domain.StatusCancelled,
		domain.StatusFailed, domain.StatusPaused,
	} {
		d := seededDownload("rec-keep-"+string(status), "a.bin", status, t.TempDir())
		if dispatch := seedAndInstall(t, svc, repo, d); dispatch {
			t.Fatalf("%s job should not be dispatched", status)
		}
		if got := getJob(svc, d.ID).Status; got != status {
			t.Fatalf("status = %q, want %q", got, status)
		}
		if got := rowStatus(t, repo, d.ID); got != status {
			t.Fatalf("DB status = %q, want %q", got, status)
		}
	}
}

func TestRecovery_QueuedDispatched(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newRecoveryService(t, repo)
	d := seededDownload("rec-queued", "a.bin", domain.StatusQueued, t.TempDir())
	if dispatch := seedAndInstall(t, svc, repo, d); !dispatch {
		t.Fatal("queued job should be dispatched after recovery")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusQueued {
		t.Fatalf("status = %q, want queued", got)
	}
}

// ---------- 5.4 shutdown ----------

func TestShutdown_CheckpointSyncsPostTransitionFields(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pdm.db")
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	repo := sqlite.NewDownloadRepository(db.DB)
	cfg := testConfig(t)
	svc := NewServiceWithRepository(cfg, &failStub{err: errors.New("boom")}, repo)
	id, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/fail.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusFailed
	}, "failed")
	// run() records the error in RAM after the failed transition, so
	// the row still has empty error fields: only the shutdown
	// checkpoint syncs them.
	row, err := repo.GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if row.ErrorMessage != "" {
		t.Fatalf("DB error_message before shutdown = %q, want empty", row.ErrorMessage)
	}
	if err := svc.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	// Second Shutdown is a no-op success.
	if err := svc.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	// Reopen the same file with a fresh handle: the checkpoint must
	// have synced, and the database must not be corrupt.
	db2, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	repo2 := sqlite.NewDownloadRepository(db2.DB)
	row2, err := repo2.GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if row2.ErrorMessage != "boom" {
		t.Fatalf("DB error_message after shutdown = %q, want %q", row2.ErrorMessage, "boom")
	}
	if row2.ErrorCode != domain.ErrorCodeNetwork {
		t.Fatalf("DB error_code after shutdown = %q, want network_error", row2.ErrorCode)
	}
	if row2.Status != domain.StatusFailed {
		t.Fatalf("DB status after shutdown = %q, want failed", row2.Status)
	}
}

func TestShutdown_DeadlineExceededDoesNotClaimSuccess(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "pdm.db")
	db, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	repo := sqlite.NewDownloadRepository(db.DB)
	cfg := testConfig(t)
	release := make(chan struct{})
	svc := NewServiceWithRepository(cfg, &slowStub{release: release}, repo)
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
	// The timed-out shutdown must not have closed the database: the
	// retry below still persists the final state.
	close(release)
	if err := svc.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
	// The worker was cancelled by Shutdown, so run() parks it in
	// cancelled (cancellation takes precedence over the stub's
	// error); the point is the retry persisted the final state.
	db2, err := sqlite.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db2.Close()
	repo2 := sqlite.NewDownloadRepository(db2.DB)
	row2, err := repo2.GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if row2.Status != domain.StatusCancelled {
		t.Fatalf("DB status after shutdown retry = %q, want cancelled", row2.Status)
	}
}

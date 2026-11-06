// Phase 5.1 tests: every status change persists to the repository,
// and a database failure never reports false success.
package app

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/storage/sqlite"
)

// blockOnContextDownloader blocks until its context is cancelled, then
// returns the context error. It lets tests hold a job in downloading
// while other jobs stay queued behind the slot limit.
type blockOnContextDownloader struct{}

func (blockOnContextDownloader) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	<-ctx.Done()
	return domain.TransferResult{}, ctx.Err()
}

// failingRepo wraps a real repository and fails every write while fail
// is set, so tests can prove the service never reports success for a
// status change the database rejected.
type failingRepo struct {
	inner domain.DownloadRepository
	fail  bool
	err   error
}

var _ domain.DownloadRepository = (*failingRepo)(nil)

func (f *failingRepo) Insert(ctx context.Context, d *domain.Download) error {
	if f.fail {
		return f.err
	}
	return f.inner.Insert(ctx, d)
}

func (f *failingRepo) GetByID(ctx context.Context, id string) (*domain.Download, error) {
	return f.inner.GetByID(ctx, id)
}

func (f *failingRepo) ListAll(ctx context.Context) ([]*domain.Download, error) {
	return f.inner.ListAll(ctx)
}

func (f *failingRepo) Update(ctx context.Context, d *domain.Download) error {
	if f.fail {
		return f.err
	}
	return f.inner.Update(ctx, d)
}

func (f *failingRepo) UpdateStatus(ctx context.Context, id string, status domain.Status, updatedAt time.Time) error {
	if f.fail {
		return f.err
	}
	return f.inner.UpdateStatus(ctx, id, status, updatedAt)
}

func (f *failingRepo) Delete(ctx context.Context, id string) error {
	if f.fail {
		return f.err
	}
	return f.inner.Delete(ctx, id)
}

func (f *failingRepo) Move(ctx context.Context, id string, position int) error {
	if f.fail {
		return f.err
	}
	return f.inner.Move(ctx, id, position)
}

// openTestRepo opens a fresh database in a temp dir and returns the
// open handle and a download repository over it. The caller owns the
// handle; tests register its Close as a cleanup.
func openTestRepo(t *testing.T) (*sqlite.DB, *sqlite.DownloadRepository) {
	t.Helper()
	db, err := sqlite.Open(filepath.Join(t.TempDir(), "pdm.db"))
	if err != nil {
		t.Fatal(err)
	}
	return db, sqlite.NewDownloadRepository(db.DB)
}

// newPersistedService builds a service with MaxActiveDownloads=1 and a
// blocking downloader, backed by repo. The service is shut down on
// test cleanup.
func newPersistedService(t *testing.T, repo domain.DownloadRepository) *Service {
	t.Helper()
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 1
	svc := NewServiceWithRepository(cfg, blockOnContextDownloader{}, repo)
	t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })
	return svc
}

// addBlockerAndQueued starts one blocked download (occupying the only
// slot) and adds a second download that stays queued. It returns both
// IDs.
func addBlockerAndQueued(t *testing.T, svc *Service) (blocker, queued string) {
	t.Helper()
	dir := t.TempDir()
	var err error
	blocker, err = svc.AddDownload(AddRequest{
		URL:            "http://example.com/blocker.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool {
		return getJob(svc, blocker).Status == domain.StatusDownloading
	}, "blocker downloading")
	queued, err = svc.AddDownload(AddRequest{
		URL:            "http://example.com/queued.bin",
		DestinationDir: dir,
		Resolution:     domain.ResolutionAutoRename,
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := getJob(svc, queued).Status; got != domain.StatusQueued {
		t.Fatalf("queued job status = %q, want queued", got)
	}
	return blocker, queued
}

func TestPersist_AddDownloadRowInDB(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newPersistedService(t, repo)
	_, queued := addBlockerAndQueued(t, svc)

	row, err := repo.GetByID(context.Background(), queued)
	if err != nil {
		t.Fatal(err)
	}
	if row.URL != "http://example.com/queued.bin" {
		t.Fatalf("row URL = %q", row.URL)
	}
	if row.Status != domain.StatusQueued {
		t.Fatalf("row status = %q, want queued", row.Status)
	}
}

func TestPersist_PauseWritesDB(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newPersistedService(t, repo)
	_, queued := addBlockerAndQueued(t, svc)

	if err := svc.Pause(queued); err != nil {
		t.Fatal(err)
	}
	row, err := repo.GetByID(context.Background(), queued)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != domain.StatusPaused {
		t.Fatalf("DB status after Pause = %q, want paused", row.Status)
	}
	if got := getJob(svc, queued).Status; got != domain.StatusPaused {
		t.Fatalf("RAM status after Pause = %q, want paused", got)
	}
}

func TestPersist_CancelWritesDB(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newPersistedService(t, repo)
	_, queued := addBlockerAndQueued(t, svc)

	if err := svc.Cancel(queued, false); err != nil {
		t.Fatal(err)
	}
	row, err := repo.GetByID(context.Background(), queued)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != domain.StatusCancelled {
		t.Fatalf("DB status after Cancel = %q, want cancelled", row.Status)
	}
	if got := getJob(svc, queued).Status; got != domain.StatusCancelled {
		t.Fatalf("RAM status after Cancel = %q, want cancelled", got)
	}
}

func TestPersist_RemoveDeletesRow(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	svc := newPersistedService(t, repo)
	_, queued := addBlockerAndQueued(t, svc)

	if err := svc.Cancel(queued, false); err != nil {
		t.Fatal(err)
	}
	if err := svc.Remove(queued); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.GetByID(context.Background(), queued); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Fatalf("GetByID after Remove = %v, want ErrDownloadNotFound", err)
	}
}

func TestPersist_DBErrorKeepsRAMState(t *testing.T) {
	db, realRepo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	fr := &failingRepo{inner: realRepo, err: errors.New("boom")}
	svc := newPersistedService(t, fr)
	_, queued := addBlockerAndQueued(t, svc)

	fr.fail = true
	// Pause must fail and leave the RAM status untouched.
	if err := svc.Pause(queued); err == nil {
		t.Fatal("Pause with failing DB should return an error")
	}
	if got := getJob(svc, queued).Status; got != domain.StatusQueued {
		t.Fatalf("RAM status after failed Pause = %q, want queued", got)
	}
	row, err := realRepo.GetByID(context.Background(), queued)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != domain.StatusQueued {
		t.Fatalf("DB status after failed Pause = %q, want queued", row.Status)
	}
	// Cancel must fail the same way.
	if err := svc.Cancel(queued, false); err == nil {
		t.Fatal("Cancel with failing DB should return an error")
	}
	if got := getJob(svc, queued).Status; got != domain.StatusQueued {
		t.Fatalf("RAM status after failed Cancel = %q, want queued", got)
	}
	// AddDownload must fail and create no job.
	if _, err := svc.AddDownload(AddRequest{
		URL:            "http://example.com/third.bin",
		DestinationDir: t.TempDir(),
		Resolution:     domain.ResolutionAutoRename,
	}); err == nil {
		t.Fatal("AddDownload with failing DB should return an error")
	}
	if got := len(svc.List()); got != 2 {
		t.Fatalf("job count after failed Add = %d, want 2", got)
	}
}

func TestServiceWithoutRepository_NilRepoSafe(t *testing.T) {
	cfg := testConfig(t)
	cfg.MaxActiveDownloads = 1
	svc := NewService(cfg, blockOnContextDownloader{})
	t.Cleanup(func() { _ = svc.Shutdown(context.Background()) })
	_, queued := addBlockerAndQueued(t, svc)
	if err := svc.Pause(queued); err != nil {
		t.Fatalf("Pause without repo: %v", err)
	}
	if err := svc.Cancel(queued, false); err != nil {
		t.Fatalf("Cancel without repo: %v", err)
	}
	if err := svc.Remove(queued); err != nil {
		t.Fatalf("Remove without repo: %v", err)
	}
}

// Recovery of finalizing jobs (Stage 3, Phase 4.3, TDD).
//
// A job that was finalizing when the previous process ended is now
// reconciled instead of always failing (Stage 2): when every segment
// is committed, the .part size matches the expected total, and the
// validator still matches a fresh probe, the finalize continues
// (rename) and the job completes. Anything unverifiable fails, and
// nothing is deleted or overwritten.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/engine"
	"github.com/ngotuananh101/pdm/internal/storage/sqlite"
)

const recoveryTestSize = 100_000

func recoveryContent() []byte {
	content := make([]byte, recoveryTestSize)
	for i := range content {
		content[i] = byte(i % 251)
	}
	return content
}

func recoveryContentHash(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

// seedFinalizingJob inserts a finalizing download row plus, when
// segments is non-empty, its segment plan, and writes the .part file
// with partContent. It returns the download.
func seedFinalizingJob(t *testing.T, repo *sqlite.DownloadRepository, segRepo *sqlite.SegmentRepository, id, dir string, content, partContent []byte, segments []domain.Segment, supportsRange bool) *domain.Download {
	t.Helper()
	d := seededDownload(id, "a.bin", domain.StatusFinalizing, dir)
	d.TotalBytes = recoveryTestSize
	d.ETag = `"mv1"`
	d.LastModified = "Wed, 01 Jan 2025 00:00:00 GMT"
	d.SupportsRange = supportsRange
	d.DownloadedBytes = recoveryTestSize
	d.TempPath = filepath.Join(dir, ".a.bin."+id+".part")
	if err := repo.Insert(context.Background(), d); err != nil {
		t.Fatal(err)
	}
	if len(segments) > 0 {
		if err := segRepo.InsertPlan(context.Background(), id, segments); err != nil {
			t.Fatalf("InsertPlan: %v", err)
		}
	}
	if partContent != nil {
		if err := os.WriteFile(d.TempPath, partContent, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return d
}

func completeSegments() []domain.Segment {
	segs := domain.SplitSegments(recoveryTestSize, 4, 1024)
	for i := range segs {
		segs[i].CommittedBytes = segs[i].Size()
		segs[i].Status = domain.SegmentComplete
	}
	return segs
}

func recoveryProber(result engine.ProbeResult, err error) *stubProber {
	return &stubProber{result: result, err: err}
}

func matchingProbe() engine.ProbeResult {
	return engine.ProbeResult{
		TotalBytes:    recoveryTestSize,
		ETag:          `"mv1"`,
		LastModified:  "Wed, 01 Jan 2025 00:00:00 GMT",
		SupportsRange: true,
	}
}

func TestRecovery_FinalizingSegmentedVerifiableCompletes(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	segRepo := sqlite.NewSegmentRepository(db.DB)
	dir := t.TempDir()
	content := recoveryContent()
	d := seedFinalizingJob(t, repo, segRepo, "fin-seg-ok", dir, content, content, completeSegments(), true)

	svc := newRecoveryService(t, repo)
	svc.SetSegmentRepository(segRepo)
	svc.SetProber(recoveryProber(matchingProbe(), nil))
	if dispatch := installRestored(t, svc, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusCompleted {
		t.Fatalf("status = %q, want completed", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusCompleted {
		t.Fatalf("DB status = %q, want completed", got)
	}
	row, err := repo.GetByID(context.Background(), d.ID)
	if err != nil {
		t.Fatal(err)
	}
	if row.FinalPath != filepath.Join(dir, "a.bin") {
		t.Fatalf("DB FinalPath = %q, want %q", row.FinalPath, filepath.Join(dir, "a.bin"))
	}
	if got := recoveryContentHash(mustReadFile(t, row.FinalPath)); got != recoveryContentHash(content) {
		t.Fatalf("final SHA-256 = %s, want %s", got, recoveryContentHash(content))
	}
	if _, err := os.Stat(d.TempPath); !os.IsNotExist(err) {
		t.Fatalf(".part still exists after recovery finalize (err = %v)", err)
	}
}

func TestRecovery_FinalizingIncompleteSegmentFails(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	segRepo := sqlite.NewSegmentRepository(db.DB)
	dir := t.TempDir()
	content := recoveryContent()
	segs := completeSegments()
	segs[2].CommittedBytes = segs[2].Size() / 2
	segs[2].Status = domain.SegmentRunning
	d := seedFinalizingJob(t, repo, segRepo, "fin-seg-part", dir, content, content, segs, true)

	svc := newRecoveryService(t, repo)
	svc.SetSegmentRepository(segRepo)
	svc.SetProber(recoveryProber(matchingProbe(), nil))
	if dispatch := installRestored(t, svc, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed (a segment is not complete)", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusFailed {
		t.Fatalf("DB status = %q, want failed", got)
	}
	// Nothing was renamed or deleted: the .part stays for Retry.
	if _, err := os.Stat(d.TempPath); err != nil {
		t.Fatalf(".part missing after failed recovery: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.bin")); !os.IsNotExist(err) {
		t.Fatal("destination file appeared after a failed recovery finalize")
	}
}

func TestRecovery_FinalizingPartSizeMismatchFails(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	segRepo := sqlite.NewSegmentRepository(db.DB)
	dir := t.TempDir()
	content := recoveryContent()
	// The .part is truncated although the plan claims complete.
	d := seedFinalizingJob(t, repo, segRepo, "fin-size-bad", dir, content, content[:50_000], completeSegments(), true)

	svc := newRecoveryService(t, repo)
	svc.SetSegmentRepository(segRepo)
	svc.SetProber(recoveryProber(matchingProbe(), nil))
	if dispatch := installRestored(t, svc, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed (.part size does not match TotalBytes)", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.bin")); !os.IsNotExist(err) {
		t.Fatal("destination file appeared after a failed recovery finalize")
	}
}

func TestRecovery_FinalizingValidatorMismatchFails(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	segRepo := sqlite.NewSegmentRepository(db.DB)
	dir := t.TempDir()
	content := recoveryContent()
	d := seedFinalizingJob(t, repo, segRepo, "fin-etag-bad", dir, content, content, completeSegments(), true)

	changed := matchingProbe()
	changed.ETag = `"v2"`
	svc := newRecoveryService(t, repo)
	svc.SetSegmentRepository(segRepo)
	svc.SetProber(recoveryProber(changed, nil))
	if dispatch := installRestored(t, svc, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed (validator changed since the download)", got)
	}
	if _, err := os.Stat(d.TempPath); err != nil {
		t.Fatalf(".part missing after failed recovery: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.bin")); !os.IsNotExist(err) {
		t.Fatal("destination file appeared after a failed recovery finalize")
	}
}

func TestRecovery_FinalizingSingleStreamVerifiableCompletes(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	segRepo := sqlite.NewSegmentRepository(db.DB)
	dir := t.TempDir()
	content := recoveryContent()
	// No segments: a single-stream job. Size + validator are enough.
	d := seedFinalizingJob(t, repo, segRepo, "fin-single-ok", dir, content, content, nil, false)

	svc := newRecoveryService(t, repo)
	svc.SetSegmentRepository(segRepo)
	svc.SetProber(recoveryProber(matchingProbe(), nil))
	if dispatch := installRestored(t, svc, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusCompleted {
		t.Fatalf("status = %q, want completed", got)
	}
	if got := rowStatus(t, repo, d.ID); got != domain.StatusCompleted {
		t.Fatalf("DB status = %q, want completed", got)
	}
	if got := recoveryContentHash(mustReadFile(t, filepath.Join(dir, "a.bin"))); got != recoveryContentHash(content) {
		t.Fatalf("final SHA-256 = %s, want %s", got, recoveryContentHash(content))
	}
}

func TestRecovery_FinalizingProbeErrorFails(t *testing.T) {
	db, repo := openTestRepo(t)
	t.Cleanup(func() { _ = db.Close() })
	segRepo := sqlite.NewSegmentRepository(db.DB)
	dir := t.TempDir()
	content := recoveryContent()
	d := seedFinalizingJob(t, repo, segRepo, "fin-probe-err", dir, content, content, completeSegments(), true)

	svc := newRecoveryService(t, repo)
	svc.SetSegmentRepository(segRepo)
	svc.SetProber(recoveryProber(engine.ProbeResult{}, errors.New("probe boom")))
	if dispatch := installRestored(t, svc, d); dispatch {
		t.Fatal("finalizing job must not be dispatched")
	}
	if got := getJob(svc, d.ID).Status; got != domain.StatusFailed {
		t.Fatalf("status = %q, want failed (validator cannot be verified)", got)
	}
	if _, err := os.Stat(filepath.Join(dir, "a.bin")); !os.IsNotExist(err) {
		t.Fatal("destination file appeared after a failed recovery finalize")
	}
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return data
}

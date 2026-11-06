// Crash simulation tests, Stage 3 level (Phase 6.2, TDD).
//
// Stage 2 covered the DB-commit + .part side of ARCH §13 in
// crash_simulation_test.go. The tests here cover the Stage 3 side at
// the service boundary: a completed job must be inert — cancelling it
// after completion is rejected and must not touch the finished file,
// write anything, or resurrect a .part.
package app

import (
	"context"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// TestCrash_CancelAfterCompletedLeavesFinalUntouched downloads a file
// to completion through the manager, then cancels it (with deleteTemp,
// the strongest form): Cancel must be rejected, the job must stay
// completed in RAM and in the DB, the finished file must keep the
// exact fixture SHA-256, and no .part file may reappear. Nothing may
// be written after completion.
func TestCrash_CancelAfterCompletedLeavesFinalUntouched(t *testing.T) {
	const size = 100_000
	rs := newResumeServer(size, 0)
	srv := httptest.NewServer(rs)
	defer srv.Close()
	svc, _, repo, _ := newManagerService(t, srv)
	dir := t.TempDir()
	id := addManagerJob(t, svc, srv, dir)

	waitFor(t, func() bool {
		return getJob(svc, id).Status == domain.StatusCompleted
	}, "job completed")
	final := getJob(svc, id)
	if final.FinalPath == "" {
		t.Fatal("FinalPath empty after completed download")
	}
	if got := sha256FileHex(t, final.FinalPath); got != rs.contentHash() {
		t.Fatalf("final SHA-256 before Cancel = %s, want %s", got, rs.contentHash())
	}
	partPath := filepath.Join(dir, ".file.bin."+id+".part")
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf(".part still exists after completion (err = %v)", err)
	}

	if err := svc.Cancel(id, true); err == nil {
		t.Fatal("Cancel on a completed job: expected error, got nil")
	} else if !errors.Is(err, domain.ErrInvalidTransition) {
		t.Fatalf("Cancel on a completed job: err = %v, want ErrInvalidTransition", err)
	}

	after := getJob(svc, id)
	if after.Status != domain.StatusCompleted {
		t.Fatalf("status after Cancel = %q, want completed", after.Status)
	}
	if after.FinalPath != final.FinalPath {
		t.Fatalf("FinalPath after Cancel = %q, want %q", after.FinalPath, final.FinalPath)
	}
	if got := sha256FileHex(t, after.FinalPath); got != rs.contentHash() {
		t.Fatalf("final SHA-256 after Cancel = %s, want %s (file must be untouched)", got, rs.contentHash())
	}
	row, err := repo.GetByID(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	if row.Status != domain.StatusCompleted {
		t.Fatalf("DB status after Cancel = %q, want completed", row.Status)
	}
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf(".part reappeared after Cancel (err = %v)", err)
	}
}

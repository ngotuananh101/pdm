// Phase 6.2 tests: crash and inconsistency simulation at the SQLite
// layer (ARCH §13). A real disk-full (ENOSPC) filesystem cannot be
// simulated portably in a unit test, so the DB-unavailable class is
// covered here by a closed handle and a corrupt file; the service
// side of a failing write is already covered by
// TestPersist_DBErrorKeepsRAMState in internal/app.
package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// TestOpenCorruptDBReturnsError: a database file that is not SQLite
// at all (e.g. truncated or overwritten by a crash mid-write) must
// fail Open with an error, never a nil DB and never a panic, so the
// caller can report it instead of starting on a broken store.
func TestCrash_OpenCorruptDBReturnsError(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pdm.db")
	if err := os.WriteFile(path, []byte("this is not a sqlite database"), 0o644); err != nil {
		t.Fatal(err)
	}
	db, err := Open(path)
	if err == nil {
		_ = db.Close()
		t.Fatal("Open on a corrupt database file should fail")
	}
	if db != nil {
		t.Fatal("Open on failure must return a nil *DB")
	}
}

// TestRepoClosedDBReturnsError: every repository operation on a closed
// database must return an error instead of panicking or reporting
// success, so a shutdown-time or crash-time caller can tell the write
// did not land.
func TestCrash_RepoClosedDBReturnsError(t *testing.T) {
	db, repo := openTestRepo(t)
	ctx := context.Background()
	seed := fullDownload("closed-1")
	if err := repo.Insert(ctx, seed); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	cases := map[string]func() error{
		"Insert":       func() error { return repo.Insert(ctx, fullDownload("closed-2")) },
		"GetByID":      func() error { _, err := repo.GetByID(ctx, seed.ID); return err },
		"Update":       func() error { return repo.Update(ctx, seed) },
		"UpdateStatus": func() error { return repo.UpdateStatus(ctx, seed.ID, domain.StatusPaused, now) },
		"Delete":       func() error { return repo.Delete(ctx, seed.ID) },
		"ListAll":      func() error { _, err := repo.ListAll(ctx); return err },
		"Move":         func() error { return repo.Move(ctx, seed.ID, 0) },
	}
	for name, op := range cases {
		if err := op(); err == nil {
			t.Fatalf("%s on a closed DB should return an error", name)
		}
	}
}

// TestInsertSameFilenameDifferentIDAllowed: the repository does not
// own filename uniqueness — two rows may name the same destination
// file (the service/conflict policy decides what happens at finalize
// time). Both rows must be stored and retrievable with distinct queue
// positions, and inserting the second must not disturb the first.
func TestCrash_InsertSameFilenameDifferentIDAllowed(t *testing.T) {
	_, repo := openTestRepo(t)
	ctx := context.Background()
	first := fullDownload("dup-1")
	second := fullDownload("dup-2")
	second.Filename = first.Filename
	second.DestinationDir = first.DestinationDir
	second.FinalPath = first.FinalPath
	second.TempPath = filepath.Join(first.DestinationDir, ".dup-2.bin.part")
	for _, d := range []*domain.Download{first, second} {
		if err := repo.Insert(ctx, d); err != nil {
			t.Fatal(err)
		}
	}
	got, err := repo.GetByID(ctx, first.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.Filename != first.Filename || got.Status != first.Status {
		t.Fatalf("first row changed after second insert: %+v", got)
	}
	rows, err := repo.ListAll(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(rows))
	}
	if rows[0].ID != first.ID || rows[1].ID != second.ID {
		t.Fatalf("order = %q %q, want dup-1 dup-2", rows[0].ID, rows[1].ID)
	}
}

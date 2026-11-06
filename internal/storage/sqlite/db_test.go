// Tests for opening the PDM SQLite database (Phase 1.4, TDD).
package sqlite

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

func queryInt(t *testing.T, db *sql.DB, pragma string) int {
	t.Helper()
	var v int
	if err := db.QueryRow(`PRAGMA ` + pragma).Scan(&v); err != nil {
		t.Fatalf("PRAGMA %s: %v", pragma, err)
	}
	return v
}

func TestOpenNewDB(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pdm.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open new DB: %v", err)
	}
	defer db.Close()

	// The file must exist on disk.
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("DB file missing after Open: %v", err)
	}
	// Migrations ran at open.
	version, err := CurrentVersion(db.DB)
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if version != 1 {
		t.Errorf("CurrentVersion = %d, want 1", version)
	}
	// Pragmas (ARCH §9): foreign keys on, WAL journal, busy timeout set.
	if got := queryInt(t, db.DB, "foreign_keys"); got != 1 {
		t.Errorf("PRAGMA foreign_keys = %d, want 1", got)
	}
	var journal string
	if err := db.QueryRow(`PRAGMA journal_mode`).Scan(&journal); err != nil {
		t.Fatalf("PRAGMA journal_mode: %v", err)
	}
	if journal != "wal" {
		t.Errorf("PRAGMA journal_mode = %q, want \"wal\"", journal)
	}
	if got := queryInt(t, db.DB, "busy_timeout"); got != BusyTimeoutMillis {
		t.Errorf("PRAGMA busy_timeout = %d, want %d", got, BusyTimeoutMillis)
	}
}

func TestOpenExistingDBPreservesData(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "pdm.db")

	db, err := Open(path)
	if err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if _, err := db.Exec(
		`INSERT INTO downloads(id, url, filename, destination_dir, temp_path, status, created_at, updated_at) VALUES('d1', 'https://example.com/f', 'f', '/tmp', '/tmp/f.part', 'queued', '2026-11-06T00:00:00Z', '2026-11-06T00:00:00Z')`,
	); err != nil {
		t.Fatalf("insert download: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	db2, err := Open(path)
	if err != nil {
		t.Fatalf("second Open: %v", err)
	}
	defer db2.Close()
	var status string
	if err := db2.QueryRow(`SELECT status FROM downloads WHERE id = 'd1'`).Scan(&status); err != nil {
		t.Fatalf("select after reopen: %v", err)
	}
	if status != "queued" {
		t.Errorf("status after reopen = %q, want \"queued\"", status)
	}
	version, err := CurrentVersion(db2.DB)
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if version != 1 {
		t.Errorf("CurrentVersion after reopen = %d, want 1", version)
	}
}

func TestOpenCreatesParentDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deep", "pdm.db")
	db, err := Open(path)
	if err != nil {
		t.Fatalf("Open with missing parent dirs: %v", err)
	}
	_ = db.Close()
	if _, err := os.Stat(path); err != nil {
		t.Errorf("DB file missing after Open: %v", err)
	}
}

func TestOpenInvalidPath(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatalf("write blocker: %v", err)
	}
	if _, err := Open(filepath.Join(blocker, "pdm.db")); err == nil {
		t.Error("Open under a file path succeeded, want error")
	}
	if _, err := Open(""); err == nil {
		t.Error("Open with empty path succeeded, want error")
	}
}

// Tests for the SQLite migration runner (Phase 1.2, TDD).
package sqlite

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// openTestDB returns an in-memory DB limited to a single connection, so
// every statement sees the same database. Without SetMaxOpenConns(1) each
// :memory: connection would be a separate empty database.
func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func tableExists(t *testing.T, db *sql.DB, name string) bool {
	t.Helper()
	var count int
	err := db.QueryRow(
		`SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?`, name,
	).Scan(&count)
	if err != nil {
		t.Fatalf("tableExists(%s): %v", name, err)
	}
	return count == 1
}

func tableColumns(t *testing.T, db *sql.DB, table string) map[string]bool {
	t.Helper()
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		t.Fatalf("PRAGMA table_info(%s): %v", table, err)
	}
	defer rows.Close()
	cols := map[string]bool{}
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			t.Fatalf("scan table_info(%s): %v", table, err)
		}
		cols[name] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows table_info(%s): %v", table, err)
	}
	return cols
}

func TestMigrateEmptyDBCreatesSchema(t *testing.T) {
	db := openTestDB(t)

	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate on empty DB: %v", err)
	}
	for _, table := range []string{"downloads", "segments", "settings", "schema_migrations"} {
		if !tableExists(t, db, table) {
			t.Errorf("table %s missing after Migrate", table)
		}
	}
	version, err := CurrentVersion(db)
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if version != 1 {
		t.Errorf("CurrentVersion = %d, want 1", version)
	}
}

func TestMigrateSchemaMatchesSpec(t *testing.T) {
	db := openTestDB(t)
	if err := Migrate(db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}

	downloads := tableColumns(t, db, "downloads")
	for _, col := range []string{
		"id", "url", "filename", "destination_dir", "temp_path", "final_path",
		"status", "total_bytes", "downloaded_bytes", "etag", "last_modified",
		"supports_range", "error_code", "error_message", "created_at",
		"updated_at", "queue_position",
	} {
		if !downloads[col] {
			t.Errorf("downloads missing column %s", col)
		}
	}

	segments := tableColumns(t, db, "segments")
	for _, col := range []string{
		"download_id", "index", "start_offset", "end_offset",
		"committed_bytes", "status",
	} {
		if !segments[col] {
			t.Errorf("segments missing column %s", col)
		}
	}

	settings := tableColumns(t, db, "settings")
	for _, col := range []string{"key", "value"} {
		if !settings[col] {
			t.Errorf("settings missing column %s", col)
		}
	}

	// total_bytes must accept NULL (unknown size, ARCH §5).
	if _, err := db.Exec(
		`INSERT INTO downloads(id, url, filename, destination_dir, temp_path, status, total_bytes, created_at, updated_at) VALUES('d1', 'https://example.com/f', 'f', '/tmp', '/tmp/f.part', 'queued', NULL, '2026-11-06T00:00:00Z', '2026-11-06T00:00:00Z')`,
	); err != nil {
		t.Errorf("insert with NULL total_bytes: %v", err)
	}

	// segments.download_id must reference downloads(id): with foreign keys
	// on, inserting a segment for a missing download fails.
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("PRAGMA foreign_keys: %v", err)
	}
	_, err := db.Exec(
		`INSERT INTO segments(download_id, "index", start_offset, end_offset, status) VALUES('missing', 0, 0, 99, 'pending')`,
	)
	if err == nil {
		t.Error("insert segment with unknown download_id succeeded, want FK violation")
	}
}

func TestMigrateIsIdempotent(t *testing.T) {
	db := openTestDB(t)

	if err := Migrate(db); err != nil {
		t.Fatalf("first Migrate: %v", err)
	}
	if err := Migrate(db); err != nil {
		t.Fatalf("second Migrate: %v", err)
	}
	version, err := CurrentVersion(db)
	if err != nil {
		t.Fatalf("CurrentVersion: %v", err)
	}
	if version != 1 {
		t.Errorf("CurrentVersion after second Migrate = %d, want 1", version)
	}

	// Data written between runs survives a re-migrate.
	if _, err := db.Exec(
		`INSERT INTO downloads(id, url, filename, destination_dir, temp_path, status, created_at, updated_at) VALUES('d1', 'https://example.com/f', 'f', '/tmp', '/tmp/f.part', 'queued', '2026-11-06T00:00:00Z', '2026-11-06T00:00:00Z')`,
	); err != nil {
		t.Fatalf("insert download: %v", err)
	}
	if err := Migrate(db); err != nil {
		t.Fatalf("third Migrate: %v", err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM downloads WHERE id = 'd1'`).Scan(&count); err != nil {
		t.Fatalf("count downloads: %v", err)
	}
	if count != 1 {
		t.Errorf("downloads with id d1 = %d, want 1 after re-migrate", count)
	}
}

func TestBackupDBFile(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "pdm.db")
	want := []byte("fake sqlite bytes")
	if err := os.WriteFile(src, want, 0o644); err != nil {
		t.Fatalf("write src: %v", err)
	}
	dst := filepath.Join(dir, "backup", "pdm.db")
	if err := BackupDBFile(src, dst); err != nil {
		t.Fatalf("BackupDBFile: %v", err)
	}
	got, err := os.ReadFile(dst)
	if err != nil {
		t.Fatalf("read backup: %v", err)
	}
	if string(got) != string(want) {
		t.Errorf("backup content = %q, want %q", got, want)
	}
	if err := BackupDBFile(filepath.Join(dir, "no-such.db"), dst); err == nil {
		t.Error("BackupDBFile on missing source succeeded, want error")
	}
}

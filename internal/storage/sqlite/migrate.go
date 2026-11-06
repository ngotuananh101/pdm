// Package sqlite provides the SQLite storage layer for PDM: opening the
// database file, running embedded schema migrations, and (in later
// phases) the download and settings repositories.
//
// Only this package imports modernc.org/sqlite; the rest of the app
// talks to it through database/sql and domain interfaces.
package sqlite

import (
	"database/sql"
	"embed"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

//go:embed migrations/*.sql
var migrationsFS embed.FS

// migration is one parsed migration file: NNN_name.sql.
type migration struct {
	version  int
	filename string
	sql      string
}

// loadMigrations reads the embedded migration files in version order.
// A file that does not follow the NNN_name.sql naming, or a duplicated
// version, is an error: a silently skipped migration would leave the
// schema behind the code that expects it.
func loadMigrations() ([]migration, error) {
	entries, err := migrationsFS.ReadDir("migrations")
	if err != nil {
		return nil, fmt.Errorf("sqlite: read embedded migrations: %w", err)
	}
	var out []migration
	seen := map[int]string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".sql") {
			continue
		}
		base := strings.TrimSuffix(e.Name(), ".sql")
		num, _, ok := strings.Cut(base, "_")
		if !ok {
			return nil, fmt.Errorf("sqlite: migration %q does not follow NNN_name.sql", e.Name())
		}
		version, err := strconv.Atoi(num)
		if err != nil || version <= 0 {
			return nil, fmt.Errorf("sqlite: migration %q has invalid version %q", e.Name(), num)
		}
		if prev, dup := seen[version]; dup {
			return nil, fmt.Errorf("sqlite: duplicate migration version %d in %q and %q", version, prev, e.Name())
		}
		seen[version] = e.Name()
		raw, err := migrationsFS.ReadFile("migrations/" + e.Name())
		if err != nil {
			return nil, fmt.Errorf("sqlite: read migration %q: %w", e.Name(), err)
		}
		out = append(out, migration{version: version, filename: e.Name(), sql: string(raw)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].version < out[j].version })
	return out, nil
}

// Migrate brings db up to date with the embedded migrations. It creates
// the schema_migrations table if needed, then applies each not-yet-applied
// migration in version order, each in its own transaction, recording the
// version in the same transaction. Re-running Migrate on an up-to-date
// database is a no-op (idempotent).
func Migrate(db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("sqlite: Migrate called with nil DB")
	}
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY)`); err != nil {
		return fmt.Errorf("sqlite: create schema_migrations: %w", err)
	}
	applied, err := appliedVersions(db)
	if err != nil {
		return err
	}
	migrations, err := loadMigrations()
	if err != nil {
		return err
	}
	for _, m := range migrations {
		if applied[m.version] {
			continue
		}
		if err := applyMigration(db, m); err != nil {
			return err
		}
	}
	return nil
}

// appliedVersions returns the set of migration versions already recorded.
func appliedVersions(db *sql.DB) (map[int]bool, error) {
	rows, err := db.Query(`SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: list applied migrations: %w", err)
	}
	defer rows.Close()
	applied := map[int]bool{}
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, fmt.Errorf("sqlite: scan applied migration: %w", err)
		}
		applied[v] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: list applied migrations: %w", err)
	}
	return applied, nil
}

// applyMigration runs one migration file and its version record in a
// single transaction, so a failed migration leaves neither partial schema
// nor a recorded version behind.
func applyMigration(db *sql.DB, m migration) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("sqlite: begin migration %d (%s): %w", m.version, m.filename, err)
	}
	if _, err := tx.Exec(m.sql); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("sqlite: migration %d (%s): %w", m.version, m.filename, err)
	}
	if _, err := tx.Exec(`INSERT INTO schema_migrations(version) VALUES (?)`, m.version); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("sqlite: record migration %d (%s): %w", m.version, m.filename, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("sqlite: commit migration %d (%s): %w", m.version, m.filename, err)
	}
	return nil
}

// CurrentVersion returns the highest applied migration version, or 0 when
// no migration has been applied yet.
func CurrentVersion(db *sql.DB) (int, error) {
	if db == nil {
		return 0, fmt.Errorf("sqlite: CurrentVersion called with nil DB")
	}
	var v sql.NullInt64
	if err := db.QueryRow(`SELECT MAX(version) FROM schema_migrations`).Scan(&v); err != nil {
		return 0, fmt.Errorf("sqlite: read current version: %w", err)
	}
	if !v.Valid {
		return 0, nil
	}
	return int(v.Int64), nil
}

// BackupDBFile copies the SQLite database file at src to dst, along with
// any -wal/-shm sidecar files present. It is written ahead of the first
// incompatible migration (ARCH §9: back up before an incompatible
// migration): 001 is the initial schema, so nothing calls it yet; a
// future migration that cannot roll forward from the old schema must call
// it first.
//
// The caller must ensure no connection has the database open (or has
// checkpointed WAL) when calling, otherwise the copied files may not
// represent a consistent snapshot.
func BackupDBFile(src, dst string) error {
	if err := copyFile(src, dst); err != nil {
		return fmt.Errorf("sqlite: backup database: %w", err)
	}
	for _, suffix := range []string{"-wal", "-shm"} {
		if _, err := os.Stat(src + suffix); err == nil {
			if err := copyFile(src+suffix, dst+suffix); err != nil {
				return fmt.Errorf("sqlite: backup database sidecar %s: %w", suffix, err)
			}
		}
	}
	return nil
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	if dir := filepath.Dir(dst); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		_ = out.Close()
		return err
	}
	return out.Close()
}

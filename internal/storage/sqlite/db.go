// Opening the PDM SQLite database (Phase 1.4).
package sqlite

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "modernc.org/sqlite"
)

// BusyTimeoutMillis is the SQLite busy_timeout applied on Open: a writer
// waits up to 5 seconds for a lock instead of failing immediately with
// "database is locked". 5000 ms is long enough to ride out a concurrent
// checkpoint or a short write from another connection, and short enough
// that a genuinely stuck writer surfaces as an error instead of hanging
// the UI. Combined with the single-connection pool below, lock contention
// should be rare; the timeout is a safety net, not the primary mechanism.
const BusyTimeoutMillis = 5000

// DefaultDBFilename is the SQLite database file name inside the app
// data directory ("pdm.db"); cmd/pdm opens
// filepath.Join(appDataDir, DefaultDBFilename). Tests use the same name
// for temporary databases.
const DefaultDBFilename = "pdm.db"

// DB is an opened PDM SQLite database, ready to use: migrations have run
// and the pragmas from ARCH §9 are in effect. It embeds *sql.DB, so the
// usual Query/Exec methods are available; Close releases the file.
type DB struct {
	*sql.DB
}

// Open opens (creating parent directories and the file as needed) the
// SQLite database at path and prepares it for use:
//
//   - a single connection (SetMaxOpenConns(1)): one sequential writer,
//     which keeps the per-connection pragmas below in effect for every
//     statement and avoids "database is locked" from the pool opening a
//     second connection;
//   - PRAGMA foreign_keys = ON (ARCH §9, enforced for segments);
//   - PRAGMA journal_mode = WAL (ARCH §9);
//   - PRAGMA busy_timeout = BusyTimeoutMillis;
//   - embedded migrations run via Migrate.
//
// A nil *DB and a non-nil error are returned on failure; Open never
// panics and never leaves a half-opened database behind.
func Open(path string) (*DB, error) {
	if path == "" {
		return nil, fmt.Errorf("sqlite: empty database path")
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, fmt.Errorf("sqlite: create database dir: %w", err)
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, fmt.Errorf("sqlite: open %s: %w", path, err)
	}
	// One sequential writer; see the doc comment above.
	db.SetMaxOpenConns(1)
	openErr := func(format string, args ...any) (*DB, error) {
		_ = db.Close()
		return nil, fmt.Errorf(format, args...)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		return openErr("sqlite: enable foreign_keys: %w", err)
	}
	if _, err := db.Exec(`PRAGMA journal_mode = WAL`); err != nil {
		return openErr("sqlite: enable WAL: %w", err)
	}
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA busy_timeout = %d`, BusyTimeoutMillis)); err != nil {
		return openErr("sqlite: set busy_timeout: %w", err)
	}
	if err := Migrate(db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("sqlite: migrate %s: %w", path, err)
	}
	return &DB{DB: db}, nil
}

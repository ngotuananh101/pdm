// SQLite settings repository (Phase 3.1) and one-time JSON migration
// (Phase 3.2).
package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"

	"github.com/ngotuananh101/pdm/internal/config"
)

// Storage choice for config.Config (ARCH §9, schema 001): the whole
// Config is stored as ONE JSON value under the single key "config" in
// the settings table — not as one row per field.
//
// Why one JSON value instead of many keys:
//   - The write is atomic: a single upserted row replaces the whole
//     config, so a crash can never leave half the fields new and half
//     old (per-key writes would need a transaction to get the same
//     guarantee, for no benefit — the UI always reads/writes the
//     whole Config at once).
//   - It matches the Stage 1 JSON file exactly (same encoding, same
//     merge-over-fallback read semantics), so the one-time migration
//     is a plain move of the file's bytes, and an older/partial value
//     keeps fallback values for absent fields instead of zeroing them.
//   - The only cost is that SQL cannot query a single field —
//     nothing in the app needs to, settings are always used whole.
const settingsKey = "config"

// SettingsRepository is the SQLite implementation of
// config.SettingsStore. It owns exactly one row in the settings table
// (key "config"); the caller keeps ownership of db (open it with Open,
// close it when done).
type SettingsRepository struct {
	db *sql.DB
}

// NewSettingsRepository returns a repository backed by db.
func NewSettingsRepository(db *sql.DB) *SettingsRepository {
	return &SettingsRepository{db: db}
}

var _ config.SettingsStore = (*SettingsRepository)(nil)

// dbOrErr returns the backing DB or an error when the repository was
// built with a nil DB.
func (r *SettingsRepository) dbOrErr() (*sql.DB, error) {
	if r == nil || r.db == nil {
		return nil, fmt.Errorf("sqlite: SettingsRepository used with nil DB")
	}
	return r.db, nil
}

// GetSettings returns the stored config, or fallback when the settings
// table holds no row yet (first run). A stored value that is not valid
// JSON, or that fails Config.Validate, returns fallback plus an error
// — mirroring config.Load, so corrupt settings can never prevent
// startup. Fields absent from the stored JSON keep the fallback value
// (merge semantics, same as config.Load for a partial file).
func (r *SettingsRepository) GetSettings(ctx context.Context, fallback config.Config) (config.Config, error) {
	db, err := r.dbOrErr()
	if err != nil {
		return fallback, err
	}
	var raw string
	err = db.QueryRowContext(ctx, `SELECT value FROM settings WHERE key = ?`, settingsKey).Scan(&raw)
	if errors.Is(err, sql.ErrNoRows) {
		return fallback, nil
	}
	if err != nil {
		return fallback, fmt.Errorf("sqlite: GetSettings: %w", err)
	}
	cfg := fallback
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return fallback, fmt.Errorf("sqlite: stored settings are not valid JSON: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fallback, fmt.Errorf("sqlite: stored settings are invalid: %w", err)
	}
	return cfg, nil
}

// SaveSettings validates cfg and writes it as the single "config" row,
// replacing any previous value. It never writes an invalid config.
func (r *SettingsRepository) SaveSettings(ctx context.Context, cfg config.Config) error {
	db, err := r.dbOrErr()
	if err != nil {
		return err
	}
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("sqlite: refusing to save invalid config: %w", err)
	}
	data, err := json.Marshal(cfg)
	if err != nil {
		return fmt.Errorf("sqlite: encode settings: %w", err)
	}
	if _, err := db.ExecContext(ctx,
		`INSERT INTO settings(key, value) VALUES(?, ?) `+
			`ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		settingsKey, string(data)); err != nil {
		return fmt.Errorf("sqlite: SaveSettings: %w", err)
	}
	return nil
}

// isEmpty reports whether the settings table holds no rows at all.
func (r *SettingsRepository) isEmpty(ctx context.Context) (bool, error) {
	db, err := r.dbOrErr()
	if err != nil {
		return false, err
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM settings`).Scan(&n); err != nil {
		return false, fmt.Errorf("sqlite: count settings: %w", err)
	}
	return n == 0, nil
}

// MigrateSettingsFromJSON moves the Stage 1 JSON settings file
// (<appDataDir>/settings.json) into the SQLite settings table, exactly
// once.
//
//   - The settings table is non-empty → (false, nil): migration already
//     ran (or settings were saved by the app), the file is left alone.
//   - No JSON file → (false, nil): nothing to migrate, first run.
//   - Valid JSON file → its config is written to SQLite and the file is
//     deleted, but only after the write succeeds: (true, nil).
//   - Invalid JSON (unparsable, or failing Config.Validate) → (false,
//     error): the fallback config is used by the caller, the file is
//     left untouched so the user can inspect or repair it, and startup
//     is never blocked. The caller should log the error as a warning.
//
// A failure to delete the file after a successful write returns an
// error too, but the settings are already in SQLite: the next startup
// sees a non-empty settings table and treats the migration as done,
// so the leftover file is never re-applied.
func MigrateSettingsFromJSON(ctx context.Context, db *sql.DB, appDataDir string, fallback config.Config) (bool, error) {
	if db == nil {
		return false, fmt.Errorf("sqlite: MigrateSettingsFromJSON called with nil DB")
	}
	repo := NewSettingsRepository(db)
	empty, err := repo.isEmpty(ctx)
	if err != nil {
		return false, err
	}
	if !empty {
		return false, nil
	}
	jsonPath := config.SettingsPath(appDataDir)
	if _, err := os.Stat(jsonPath); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("sqlite: stat settings file: %w", err)
	}
	cfg, err := config.Load(appDataDir, fallback)
	if err != nil {
		return false, fmt.Errorf("sqlite: migrate settings from JSON: %w", err)
	}
	if err := repo.SaveSettings(ctx, cfg); err != nil {
		return false, fmt.Errorf("sqlite: migrate settings from JSON: %w", err)
	}
	if err := os.Remove(jsonPath); err != nil {
		return false, fmt.Errorf("sqlite: migrated settings but could not remove %s: %w", jsonPath, err)
	}
	return true, nil
}

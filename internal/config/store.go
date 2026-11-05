// Persisted user settings (Phase 7, U1).
//
// Stage 1 has no SQLite yet (that is Stage 2), so settings are stored
// as JSON in <AppDataDir>/settings.json. The file holds the full
// Config: the UI only edits three fields (download dir, conflict
// policy, max active downloads) and must never clobber the rest, so
// Load merges the file over the supplied fallback instead of
// zero-filling absent fields.
//
// Load never fails hard: a missing, unreadable, or invalid file
// returns the fallback config plus an error, so a corrupt settings
// file can never prevent startup.
package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
)

// SettingsFilename is the file name for persisted settings inside the
// application data directory.
const SettingsFilename = "settings.json"

// SettingsPath returns the full path of the persisted settings file
// inside appDataDir.
func SettingsPath(appDataDir string) string {
	return filepath.Join(appDataDir, SettingsFilename)
}

// Save validates cfg and writes it as JSON to
// <dir>/settings.json, atomically (temporary file + rename) so a
// crash mid-write never leaves a half-written settings file behind.
// It never writes an invalid config.
func Save(dir string, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config: refusing to save invalid config: %w", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("config: create settings dir: %w", err)
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("config: encode settings: %w", err)
	}
	tmpPath := SettingsPath(dir) + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("config: write settings: %w", err)
	}
	if err := os.Rename(tmpPath, SettingsPath(dir)); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("config: commit settings: %w", err)
	}
	return nil
}

// Load reads <dir>/settings.json and returns the stored config. The
// stored values are merged over fallback, so fields absent from the
// file (e.g. written by an older version) keep the fallback value
// instead of becoming zero. On any failure — missing file, unreadable
// file, invalid JSON, or a config that fails Validate — Load returns
// fallback and a non-nil error.
func Load(dir string, fallback Config) (Config, error) {
	data, err := os.ReadFile(SettingsPath(dir))
	if err != nil {
		return fallback, fmt.Errorf("config: read settings: %w", err)
	}
	cfg := fallback
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fallback, fmt.Errorf("config: parse settings: %w", err)
	}
	if err := cfg.Validate(); err != nil {
		return fallback, fmt.Errorf("config: stored settings are invalid: %w", err)
	}
	return cfg, nil
}

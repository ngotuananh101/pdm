// Tests for the SQLite settings repository (Phase 3.1, TDD).
//
// Storage choice (see settings_repo.go): the whole config.Config is
// stored as one JSON value under a single key in the settings table.
package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
)

func testSettingsConfig(dir string) config.Config {
	cfg := config.Default(dir)
	cfg.FileConflictPolicy = config.ConflictAutoRename
	cfg.MaxActiveDownloads = 7
	cfg.MaxRetries = 5
	cfg.StallTimeout = 45 * time.Second
	cfg.BufferSize = 32 << 10
	return cfg
}

func TestSettingsRepositorySaveGetRoundTrip(t *testing.T) {
	db, _ := openTestRepo(t)
	repo := NewSettingsRepository(db.DB)
	ctx := context.Background()

	want := testSettingsConfig(filepath.Join(t.TempDir(), "downloads"))
	if err := repo.SaveSettings(ctx, want); err != nil {
		t.Fatalf("SaveSettings = %v", err)
	}
	got, err := repo.GetSettings(ctx, config.Default("/fallback"))
	if err != nil {
		t.Fatalf("GetSettings = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetSettings = %+v, want %+v", got, want)
	}
}

func TestSettingsRepositoryGetEmptyReturnsFallback(t *testing.T) {
	db, _ := openTestRepo(t)
	repo := NewSettingsRepository(db.DB)
	ctx := context.Background()

	fallback := config.Default("/fallback/downloads")
	got, err := repo.GetSettings(ctx, fallback)
	if err != nil {
		t.Fatalf("GetSettings on empty settings table = %v, want nil error", err)
	}
	if !reflect.DeepEqual(got, fallback) {
		t.Fatalf("GetSettings = %+v, want fallback %+v", got, fallback)
	}
}

func TestSettingsRepositorySaveOverwrites(t *testing.T) {
	db, _ := openTestRepo(t)
	repo := NewSettingsRepository(db.DB)
	ctx := context.Background()

	first := testSettingsConfig(filepath.Join(t.TempDir(), "first"))
	if err := repo.SaveSettings(ctx, first); err != nil {
		t.Fatalf("first SaveSettings = %v", err)
	}
	second := config.Default(filepath.Join(t.TempDir(), "second"))
	second.MaxActiveDownloads = 2
	if err := repo.SaveSettings(ctx, second); err != nil {
		t.Fatalf("second SaveSettings = %v", err)
	}
	got, err := repo.GetSettings(ctx, config.Default("/fallback"))
	if err != nil {
		t.Fatalf("GetSettings = %v", err)
	}
	if !reflect.DeepEqual(got, second) {
		t.Fatalf("GetSettings = %+v, want second save %+v", got, second)
	}
	// Still a single row: the whole config lives under one key.
	var n int
	if err := db.QueryRow(`SELECT COUNT(*) FROM settings`).Scan(&n); err != nil {
		t.Fatalf("count settings rows: %v", err)
	}
	if n != 1 {
		t.Errorf("settings rows = %d, want 1", n)
	}
}

func TestSettingsRepositorySaveRejectsInvalidConfig(t *testing.T) {
	db, _ := openTestRepo(t)
	repo := NewSettingsRepository(db.DB)
	ctx := context.Background()

	bad := config.Default(filepath.Join(t.TempDir(), "downloads"))
	bad.MaxActiveDownloads = 999 // outside Validate bounds.
	if err := repo.SaveSettings(ctx, bad); err == nil {
		t.Fatal("SaveSettings with invalid config should return an error")
	}
	// Nothing was persisted for the rejected save.
	fallback := config.Default("/fallback/downloads")
	got, err := repo.GetSettings(ctx, fallback)
	if err != nil {
		t.Fatalf("GetSettings after rejected save = %v", err)
	}
	if !reflect.DeepEqual(got, fallback) {
		t.Fatalf("GetSettings = %+v, want fallback %+v", got, fallback)
	}
}

func TestSettingsRepositoryGetCorruptValueReturnsFallback(t *testing.T) {
	db, _ := openTestRepo(t)
	repo := NewSettingsRepository(db.DB)
	ctx := context.Background()

	if _, err := db.Exec(`INSERT INTO settings(key, value) VALUES('config', '{not json')`); err != nil {
		t.Fatalf("insert corrupt settings row: %v", err)
	}
	fallback := config.Default("/fallback/downloads")
	got, err := repo.GetSettings(ctx, fallback)
	if err == nil {
		t.Fatal("GetSettings on a corrupt stored value should return an error")
	}
	if !reflect.DeepEqual(got, fallback) {
		t.Fatalf("GetSettings = %+v, want fallback %+v", got, fallback)
	}
}

func TestSettingsRepositoryGetPartialValueKeepsFallbackFields(t *testing.T) {
	db, _ := openTestRepo(t)
	repo := NewSettingsRepository(db.DB)
	ctx := context.Background()

	// Only the download dir is present: every other field must keep
	// the fallback value instead of silently becoming zero — the same
	// merge semantics as config.Load for a partial JSON file.
	if _, err := db.Exec(`INSERT INTO settings(key, value) VALUES('config', '{"DefaultDownloadDir":"/tmp/kept"}')`); err != nil {
		t.Fatalf("insert partial settings row: %v", err)
	}
	fallback := config.Default("/fallback/downloads")
	want := fallback
	want.DefaultDownloadDir = "/tmp/kept"
	got, err := repo.GetSettings(ctx, fallback)
	if err != nil {
		t.Fatalf("GetSettings = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("GetSettings = %+v, want %+v", got, want)
	}
}

func TestSettingsRepositoryNilDB(t *testing.T) {
	repo := NewSettingsRepository(nil)
	ctx := context.Background()
	if _, err := repo.GetSettings(ctx, config.Default("/fallback")); err == nil {
		t.Error("GetSettings with nil DB should return an error")
	}
	if err := repo.SaveSettings(ctx, config.Default("/tmp/x")); err == nil {
		t.Error("SaveSettings with nil DB should return an error")
	}
}

// Tests for the one-time JSON migration (Phase 3.2, TDD).
//
// MigrateSettingsFromJSON moves <AppDataDir>/settings.json into the
// SQLite settings table exactly once: only when the table is empty
// and a valid JSON file exists. A corrupt file is left untouched and
// reported as an error (caller logs a warning and uses defaults);
// a second run after a successful migration is a no-op.
func TestMigrateSettingsFromJSONValid(t *testing.T) {
	dir := t.TempDir()
	db, _ := openTestRepo(t)
	ctx := context.Background()

	want := testSettingsConfig(filepath.Join(dir, "downloads"))
	if err := config.Save(dir, want); err != nil {
		t.Fatalf("config.Save JSON fixture = %v", err)
	}
	migrated, err := MigrateSettingsFromJSON(ctx, db.DB, dir, config.Default("/fallback"))
	if err != nil {
		t.Fatalf("MigrateSettingsFromJSON = %v", err)
	}
	if !migrated {
		t.Fatal("MigrateSettingsFromJSON = false, want true for a valid JSON file")
	}
	repo := NewSettingsRepository(db.DB)
	got, err := repo.GetSettings(ctx, config.Default("/fallback"))
	if err != nil {
		t.Fatalf("GetSettings after migrate = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("settings after migrate = %+v, want %+v", got, want)
	}
	if _, err := os.Stat(config.SettingsPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("settings.json still exists after successful migrate (stat err = %v)", err)
	}
}

func TestMigrateSettingsFromJSONCorruptKeepsFile(t *testing.T) {
	dir := t.TempDir()
	db, _ := openTestRepo(t)
	ctx := context.Background()

	raw := []byte("{not json")
	if err := os.WriteFile(config.SettingsPath(dir), raw, 0o644); err != nil {
		t.Fatal(err)
	}
	fallback := config.Default("/fallback/downloads")
	migrated, err := MigrateSettingsFromJSON(ctx, db.DB, dir, fallback)
	if err == nil {
		t.Fatal("MigrateSettingsFromJSON on corrupt JSON should return an error")
	}
	if migrated {
		t.Error("MigrateSettingsFromJSON = true, want false for corrupt JSON")
	}
	// Defaults are used and the file is left untouched.
	repo := NewSettingsRepository(db.DB)
	got, err := repo.GetSettings(ctx, fallback)
	if err != nil {
		t.Fatalf("GetSettings after failed migrate = %v", err)
	}
	if !reflect.DeepEqual(got, fallback) {
		t.Fatalf("GetSettings = %+v, want fallback %+v", got, fallback)
	}
	kept, err := os.ReadFile(config.SettingsPath(dir))
	if err != nil {
		t.Fatalf("settings.json missing after failed migrate: %v", err)
	}
	if string(kept) != string(raw) {
		t.Fatalf("settings.json changed after failed migrate: got %q, want %q", kept, raw)
	}
}

func TestMigrateSettingsFromJSONSecondRunIsNoop(t *testing.T) {
	dir := t.TempDir()
	db, _ := openTestRepo(t)
	ctx := context.Background()

	first := testSettingsConfig(filepath.Join(dir, "downloads"))
	if err := config.Save(dir, first); err != nil {
		t.Fatalf("config.Save JSON fixture = %v", err)
	}
	if migrated, err := MigrateSettingsFromJSON(ctx, db.DB, dir, config.Default("/fallback")); err != nil || !migrated {
		t.Fatalf("first MigrateSettingsFromJSON = (%v, %v), want (true, nil)", migrated, err)
	}
	// A JSON file appears again (e.g. restored from backup) with
	// different values: the second run must not touch the DB or the file.
	second := config.Default(filepath.Join(dir, "other"))
	second.MaxActiveDownloads = 2
	if err := config.Save(dir, second); err != nil {
		t.Fatalf("config.Save second JSON fixture = %v", err)
	}
	migrated, err := MigrateSettingsFromJSON(ctx, db.DB, dir, config.Default("/fallback"))
	if err != nil {
		t.Fatalf("second MigrateSettingsFromJSON = %v", err)
	}
	if migrated {
		t.Error("second MigrateSettingsFromJSON = true, want false (migration runs once)")
	}
	repo := NewSettingsRepository(db.DB)
	got, err := repo.GetSettings(ctx, config.Default("/fallback"))
	if err != nil {
		t.Fatalf("GetSettings after second migrate = %v", err)
	}
	if !reflect.DeepEqual(got, first) {
		t.Fatalf("settings after second migrate = %+v, want first migrate %+v", got, first)
	}
	if _, err := os.Stat(config.SettingsPath(dir)); err != nil {
		t.Fatalf("settings.json should still exist after no-op second migrate: %v", err)
	}
}

func TestMigrateSettingsFromJSONNoFile(t *testing.T) {
	dir := t.TempDir()
	db, _ := openTestRepo(t)
	ctx := context.Background()

	fallback := config.Default("/fallback/downloads")
	migrated, err := MigrateSettingsFromJSON(ctx, db.DB, dir, fallback)
	if err != nil {
		t.Fatalf("MigrateSettingsFromJSON with no JSON file = %v, want nil error", err)
	}
	if migrated {
		t.Error("MigrateSettingsFromJSON = true, want false when no JSON file exists")
	}
}

// Adapter settings-persistence tests (Phase 7, U1; store switched to
// SQLite in Stage 2, Phase 3).
//
// SaveConfig must write the full current config through the injected
// settings store — editing only the four UI-owned fields — so
// settings the UI does not own survive a save, and a later GetSettings
// returns exactly what was saved.
package ui

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/app"
	"github.com/ngotuananh101/pdm/internal/config"
)

// fakeSettingsStore is an in-memory config.SettingsStore for adapter
// tests: it records what SaveConfig persisted without touching disk
// or SQL (the SQLite behaviour itself is covered in
// internal/storage/sqlite).
type fakeSettingsStore struct {
	saved *config.Config
}

func (f *fakeSettingsStore) GetSettings(_ context.Context, fallback config.Config) (config.Config, error) {
	if f.saved != nil {
		return *f.saved, nil
	}
	return fallback, nil
}

func (f *fakeSettingsStore) SaveSettings(_ context.Context, cfg config.Config) error {
	c := cfg
	f.saved = &c
	return nil
}

var _ config.SettingsStore = (*fakeSettingsStore)(nil)

func TestAdapterSaveConfigPersistsAndPreservesOtherFields(t *testing.T) {
	cfg := config.Default(t.TempDir())
	// Non-UI fields with distinctive values: SaveConfig must not
	// clobber them when it persists the three UI-owned fields.
	cfg.SegmentsPerDownload = 8
	cfg.MaxRetries = 5
	cfg.BufferSize = 32 << 10
	cfg.StallTimeout = 45 * time.Second

	svc := app.NewService(cfg, nil)
	ad := NewAppServiceAdapter(svc)
	store := &fakeSettingsStore{}
	ad.SetSettingsStore(store)

	if err := ad.SaveConfig(ScratchConfig{
		DefaultDownloadDir:  "/tmp/elsewhere",
		ConflictPolicy:      ConflictAutoRename,
		MaxActiveDownloads:  7,
		SegmentsPerDownload: 8,
	}); err != nil {
		t.Fatalf("SaveConfig = %v", err)
	}

	want := cfg
	want.DefaultDownloadDir = "/tmp/elsewhere"
	want.FileConflictPolicy = config.ConflictAutoRename
	want.MaxActiveDownloads = 7
	if store.saved == nil {
		t.Fatal("SaveConfig did not persist anything through the settings store")
	}
	if !reflect.DeepEqual(*store.saved, want) {
		t.Fatalf("persisted config = %+v, want %+v", *store.saved, want)
	}

	// In-memory config agrees with what was persisted.
	if gc := ad.GetConfig(); gc.DefaultDownloadDir != "/tmp/elsewhere" ||
		gc.ConflictPolicy != ConflictAutoRename || gc.MaxActiveDownloads != 7 {
		t.Fatalf("GetConfig after SaveConfig = %+v", gc)
	}
}

func TestAdapterSaveConfigInvalidReturnsError(t *testing.T) {
	cfg := config.Default(t.TempDir())
	svc := app.NewService(cfg, nil)
	ad := NewAppServiceAdapter(svc)
	store := &fakeSettingsStore{}
	ad.SetSettingsStore(store)

	if err := ad.SaveConfig(ScratchConfig{
		DefaultDownloadDir:  "/tmp/elsewhere",
		ConflictPolicy:      ConflictAsk,
		MaxActiveDownloads:  0, // outside Validate bounds.
		SegmentsPerDownload: 4,
	}); err == nil {
		t.Fatal("SaveConfig with MaxActiveDownloads 0 should return an error")
	}
	// Nothing was persisted for the rejected save.
	if store.saved != nil {
		t.Fatalf("invalid SaveConfig persisted %+v, want nothing persisted", *store.saved)
	}
}

func TestAdapterSaveConfigWithoutStoreReturnsError(t *testing.T) {
	cfg := config.Default(t.TempDir())
	svc := app.NewService(cfg, nil)
	ad := NewAppServiceAdapter(svc)
	// No SetSettingsStore call: SaveConfig must fail loudly instead
	// of silently writing settings the next startup would ignore.
	if err := ad.SaveConfig(ScratchConfig{
		DefaultDownloadDir:  "/tmp/elsewhere",
		ConflictPolicy:      ConflictAsk,
		MaxActiveDownloads:  3,
		SegmentsPerDownload: 4,
	}); err == nil {
		t.Fatal("SaveConfig without a settings store should return an error")
	}
}

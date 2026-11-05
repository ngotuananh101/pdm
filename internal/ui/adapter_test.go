// Adapter settings-persistence tests (Phase 7, U1).
//
// SaveConfig must write the full current config to disk — editing
// only the three UI-owned fields — so settings the UI does not own
// survive a save, and a later Load returns exactly what was saved.
package ui

import (
	"reflect"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/app"
	"github.com/ngotuananh101/pdm/internal/config"
)

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
	dir := t.TempDir()
	ad.settingsDir = dir

	if err := ad.SaveConfig(ScratchConfig{
		DefaultDownloadDir: "/tmp/elsewhere",
		ConflictPolicy:     ConflictAutoRename,
		MaxActiveDownloads: 7,
	}); err != nil {
		t.Fatalf("SaveConfig = %v", err)
	}

	want := cfg
	want.DefaultDownloadDir = "/tmp/elsewhere"
	want.FileConflictPolicy = config.ConflictAutoRename
	want.MaxActiveDownloads = 7
	got, err := config.Load(dir, config.Default("/fallback"))
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted config = %+v, want %+v", got, want)
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
	ad.settingsDir = t.TempDir()

	if err := ad.SaveConfig(ScratchConfig{
		DefaultDownloadDir: "/tmp/elsewhere",
		ConflictPolicy:     ConflictAsk,
		MaxActiveDownloads: 0, // outside Validate bounds.
	}); err == nil {
		t.Fatal("SaveConfig with MaxActiveDownloads 0 should return an error")
	}
	// Nothing was persisted for the rejected save.
	if _, err := config.Load(ad.settingsDir, cfg); err == nil {
		t.Fatal("Load should fail: no settings file must exist after a rejected save")
	}
}

// Adapter tests for the SegmentsPerDownload setting (Stage 3, Phase 5.2, TDD).
//
// SaveConfig must carry the UI-owned SegmentsPerDownload field through to
// the persisted config like the other settings, and must reject values
// outside 1..16 without persisting anything.
package ui

import (
	"testing"

	"github.com/ngotuananh101/pdm/internal/app"
	"github.com/ngotuananh101/pdm/internal/config"
)

func TestAdapterSaveConfigPersistsSegmentsPerDownload(t *testing.T) {
	cfg := config.Default(t.TempDir())
	svc := app.NewService(cfg, nil)
	ad := NewAppServiceAdapter(svc)
	store := &fakeSettingsStore{}
	ad.SetSettingsStore(store)

	if err := ad.SaveConfig(ScratchConfig{
		DefaultDownloadDir:  "/tmp/elsewhere",
		ConflictPolicy:      ConflictAsk,
		MaxActiveDownloads:  3,
		SegmentsPerDownload: 8,
	}); err != nil {
		t.Fatalf("SaveConfig = %v", err)
	}
	if store.saved == nil {
		t.Fatal("SaveConfig did not persist anything through the settings store")
	}
	if store.saved.SegmentsPerDownload != 8 {
		t.Errorf("persisted SegmentsPerDownload = %d, want 8", store.saved.SegmentsPerDownload)
	}
	if gc := ad.GetConfig(); gc.SegmentsPerDownload != 8 {
		t.Errorf("GetConfig after SaveConfig SegmentsPerDownload = %d, want 8", gc.SegmentsPerDownload)
	}
}

func TestAdapterSaveConfigInvalidSegmentsReturnsError(t *testing.T) {
	for _, segs := range []int{0, 17, -1} {
		cfg := config.Default(t.TempDir())
		svc := app.NewService(cfg, nil)
		ad := NewAppServiceAdapter(svc)
		store := &fakeSettingsStore{}
		ad.SetSettingsStore(store)

		if err := ad.SaveConfig(ScratchConfig{
			DefaultDownloadDir:  "/tmp/elsewhere",
			ConflictPolicy:      ConflictAsk,
			MaxActiveDownloads:  3,
			SegmentsPerDownload: segs,
		}); err == nil {
			t.Errorf("SaveConfig with SegmentsPerDownload %d should return an error", segs)
		}
		if store.saved != nil {
			t.Errorf("invalid SaveConfig (segments %d) persisted %+v, want nothing persisted", segs, *store.saved)
		}
	}
}

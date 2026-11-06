// Tests for loading a pre-Stage-3 stored config from SQLite
// (Stage 3, Phase 5.1, TDD).
//
// Mirrors the JSON-file case in internal/config: a stored "config" row
// written before SegmentsPerDownload existed must load with the default
// (4) for the absent field instead of failing validation on zero.
package sqlite

import (
	"context"
	"testing"

	"github.com/ngotuananh101/pdm/internal/config"
)

// TestSettingsRepositoryGetOldValueWithoutSegmentsKeepsDefault inserts a
// full pre-Stage-3 JSON value — every field except SegmentsPerDownload —
// and expects GetSettings to keep the fallback default (4).
func TestSettingsRepositoryGetOldValueWithoutSegmentsKeepsDefault(t *testing.T) {
	db, _ := openTestRepo(t)
	repo := NewSettingsRepository(db.DB)
	ctx := context.Background()

	oldJSON := `{"DefaultDownloadDir":"/tmp/x","FileConflictPolicy":"ask","MaxActiveDownloads":3,"MaxConnectionsPerHost":4,"MinSegmentSize":1048576,"BufferSize":65536,"ProgressInterval":500000000,"CheckpointInterval":5000000000,"MaxRetries":3,"StallTimeout":30000000000}`
	if _, err := db.Exec(`INSERT INTO settings(key, value) VALUES('config', ?)`, oldJSON); err != nil {
		t.Fatalf("insert old settings row: %v", err)
	}
	got, err := repo.GetSettings(ctx, config.Default("/fallback/downloads"))
	if err != nil {
		t.Fatalf("GetSettings of old value without SegmentsPerDownload = %v", err)
	}
	if got.SegmentsPerDownload != config.DefaultSegmentsPerDownload {
		t.Errorf("SegmentsPerDownload = %d, want default %d", got.SegmentsPerDownload, config.DefaultSegmentsPerDownload)
	}
}

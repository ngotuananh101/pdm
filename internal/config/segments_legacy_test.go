// Tests for loading a pre-Stage-3 stored config (Stage 3, Phase 5.1, TDD).
//
// SegmentsPerDownload already exists in config.Config (default 4,
// Validate 1..16); what this file pins down is the migration case the
// plan calls out: a stored value written before the field existed must
// load with the default instead of failing validation on the zero value.
package config

import (
	"os"
	"testing"
)

// TestLoadOldConfigWithoutSegmentsKeepsDefault writes a full pre-Stage-3
// JSON value — every field except SegmentsPerDownload — and expects Load
// to keep the fallback default (4) for the absent field.
func TestLoadOldConfigWithoutSegmentsKeepsDefault(t *testing.T) {
	dir := t.TempDir()
	oldJSON := `{"DefaultDownloadDir":"/tmp/x","FileConflictPolicy":"ask","MaxActiveDownloads":3,"MaxConnectionsPerHost":4,"MinSegmentSize":1048576,"BufferSize":65536,"ProgressInterval":500000000,"CheckpointInterval":5000000000,"MaxRetries":3,"StallTimeout":30000000000}`
	if err := os.WriteFile(SettingsPath(dir), []byte(oldJSON), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := Load(dir, Default("/fallback/downloads"))
	if err != nil {
		t.Fatalf("Load of old config without SegmentsPerDownload = %v", err)
	}
	if got.SegmentsPerDownload != DefaultSegmentsPerDownload {
		t.Errorf("SegmentsPerDownload = %d, want default %d", got.SegmentsPerDownload, DefaultSegmentsPerDownload)
	}
}

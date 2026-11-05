// Tests for the persisted settings store (Phase 7, U1).
//
// Settings live in <AppDataDir>/settings.json. A missing, unreadable,
// or invalid file must fall back to the supplied defaults instead of
// crashing startup, and saving must never persist an invalid config.
package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func testSettingsConfig(dir string) Config {
	cfg := Default(dir)
	cfg.FileConflictPolicy = ConflictAutoRename
	cfg.MaxActiveDownloads = 7
	cfg.MaxRetries = 5
	cfg.StallTimeout = 45 * time.Second
	return cfg
}

func TestSaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	want := testSettingsConfig(filepath.Join(dir, "downloads"))

	if err := Save(dir, want); err != nil {
		t.Fatalf("Save = %v", err)
	}
	got, err := Load(dir, Default("/fallback"))
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load = %+v, want %+v", got, want)
	}
}

func TestLoadMissingFileReturnsFallback(t *testing.T) {
	fallback := Default("/fallback/downloads")
	got, err := Load(t.TempDir(), fallback)
	if err == nil {
		t.Fatal("Load on a missing file should return an error")
	}
	if !reflect.DeepEqual(got, fallback) {
		t.Fatalf("Load = %+v, want fallback %+v", got, fallback)
	}
}

func TestLoadCorruptFileReturnsFallback(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(SettingsPath(dir), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	fallback := Default("/fallback/downloads")
	got, err := Load(dir, fallback)
	if err == nil {
		t.Fatal("Load on a corrupt file should return an error")
	}
	if !reflect.DeepEqual(got, fallback) {
		t.Fatalf("Load = %+v, want fallback %+v", got, fallback)
	}
}

func TestLoadInvalidConfigReturnsFallback(t *testing.T) {
	dir := t.TempDir()
	cfg := testSettingsConfig(filepath.Join(dir, "downloads"))
	cfg.MaxActiveDownloads = 999 // outside Validate bounds.
	if err := Save(dir, cfg); err == nil {
		t.Fatal("Save should refuse an invalid config")
	}
	// Write the invalid config by hand: Load must still refuse it.
	if err := os.WriteFile(SettingsPath(dir), []byte(`{"DefaultDownloadDir":"/tmp/x","FileConflictPolicy":"ask","MaxActiveDownloads":999,"SegmentsPerDownload":4,"MaxConnectionsPerHost":4,"MinSegmentSize":1048576,"BufferSize":65536,"ProgressInterval":500000000,"CheckpointInterval":5000000000,"MaxRetries":3,"StallTimeout":30000000000}`), 0o644); err != nil {
		t.Fatal(err)
	}
	fallback := Default("/fallback/downloads")
	got, err := Load(dir, fallback)
	if err == nil {
		t.Fatal("Load on an invalid config should return an error")
	}
	if !reflect.DeepEqual(got, fallback) {
		t.Fatalf("Load = %+v, want fallback %+v", got, fallback)
	}
}

func TestLoadPartialFileKeepsFallbackFields(t *testing.T) {
	dir := t.TempDir()
	// Only the download dir is present: every other field must keep
	// the fallback value instead of silently becoming zero.
	if err := os.WriteFile(SettingsPath(dir), []byte(`{"DefaultDownloadDir":"/tmp/kept"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	fallback := Default("/fallback/downloads")
	want := fallback
	want.DefaultDownloadDir = "/tmp/kept"
	got, err := Load(dir, fallback)
	if err != nil {
		t.Fatalf("Load = %v", err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Load = %+v, want %+v", got, want)
	}
}

func TestSaveRejectsInvalidConfig(t *testing.T) {
	dir := t.TempDir()
	cfg := Default(filepath.Join(dir, "downloads"))
	cfg.BufferSize = 1 << 30 // above the Validate ceiling.
	if err := Save(dir, cfg); err == nil {
		t.Fatal("Save should refuse an invalid config")
	}
	if _, err := os.Stat(SettingsPath(dir)); !os.IsNotExist(err) {
		t.Fatalf("invalid Save must not create %s (stat err = %v)", SettingsPath(dir), err)
	}
}

package config

import (
	"strings"
	"testing"
	"time"
)

func TestDefault(t *testing.T) {
	cfg := Default("/tmp/downloads")
	if cfg.DefaultDownloadDir != "/tmp/downloads" {
		t.Errorf("DefaultDownloadDir = %q, want %q", cfg.DefaultDownloadDir, "/tmp/downloads")
	}
	if cfg.FileConflictPolicy != ConflictAsk {
		t.Errorf("FileConflictPolicy = %q, want %q", cfg.FileConflictPolicy, ConflictAsk)
	}
	if cfg.MaxActiveDownloads != 3 {
		t.Errorf("MaxActiveDownloads = %d, want 3", cfg.MaxActiveDownloads)
	}
	if cfg.SegmentsPerDownload != 4 {
		t.Errorf("SegmentsPerDownload = %d, want 4", cfg.SegmentsPerDownload)
	}
	if cfg.MaxConnectionsPerHost != 4 {
		t.Errorf("MaxConnectionsPerHost = %d, want 4", cfg.MaxConnectionsPerHost)
	}
	if cfg.MinSegmentSize != 1<<20 {
		t.Errorf("MinSegmentSize = %d, want %d", cfg.MinSegmentSize, 1<<20)
	}
	if cfg.BufferSize != 64<<10 {
		t.Errorf("BufferSize = %d, want %d", cfg.BufferSize, 64<<10)
	}
	if cfg.ProgressInterval != 500*time.Millisecond {
		t.Errorf("ProgressInterval = %s, want 500ms", cfg.ProgressInterval)
	}
	if cfg.CheckpointInterval != 5*time.Second {
		t.Errorf("CheckpointInterval = %s, want 5s", cfg.CheckpointInterval)
	}
	if cfg.MaxRetries != 3 {
		t.Errorf("MaxRetries = %d, want 3", cfg.MaxRetries)
	}
	if cfg.StallTimeout != 30*time.Second {
		t.Errorf("StallTimeout = %s, want 30s", cfg.StallTimeout)
	}
}

func TestDefaultValidate(t *testing.T) {
	if err := Default("/tmp/downloads").Validate(); err != nil {
		t.Errorf("default config should validate, got %v", err)
	}
}

func TestFileConflictPolicyIsValid(t *testing.T) {
	valid := []FileConflictPolicy{ConflictAsk, ConflictAutoRename, ConflictOverwrite}
	for _, p := range valid {
		if !p.IsValid() {
			t.Errorf("%q should be valid", p)
		}
	}
	invalid := []FileConflictPolicy{"", "ASK", "always", "rename", "auto-rename", "overwrite ", " ask"}
	for _, p := range invalid {
		if p.IsValid() {
			t.Errorf("%q should be invalid", p)
		}
	}
}

func TestPolicyConstants(t *testing.T) {
	if string(ConflictAsk) != "ask" {
		t.Errorf("ConflictAsk = %q, want \"ask\"", ConflictAsk)
	}
	if string(ConflictAutoRename) != "auto_rename" {
		t.Errorf("ConflictAutoRename = %q, want \"auto_rename\"", ConflictAutoRename)
	}
	if string(ConflictOverwrite) != "overwrite" {
		t.Errorf("ConflictOverwrite = %q, want \"overwrite\"", ConflictOverwrite)
	}
}

func TestValidateBoundaries(t *testing.T) {
	type mutation struct {
		name string
		fn   func(*Config)
	}
	baseline := Default("/tmp/x")

	cases := []struct {
		name    string
		mutate  func(*Config)
		wantErr bool
	}{
		{"baseline ok", func(c *Config) {}, false},
		// DefaultDownloadDir
		{"empty dir", func(c *Config) { c.DefaultDownloadDir = "" }, true},
		{"whitespace dir", func(c *Config) { c.DefaultDownloadDir = "   " }, true},
		{"relative dir ok", func(c *Config) { c.DefaultDownloadDir = "downloads" }, false},
		// FileConflictPolicy
		{"policy empty", func(c *Config) { c.FileConflictPolicy = "" }, true},
		{"policy invalid", func(c *Config) { c.FileConflictPolicy = "bogus" }, true},
		{"policy auto_rename ok", func(c *Config) { c.FileConflictPolicy = ConflictAutoRename }, false},
		{"policy overwrite ok", func(c *Config) { c.FileConflictPolicy = ConflictOverwrite }, false},
		// MaxActiveDownloads 1..32
		{"maxActive 0", func(c *Config) { c.MaxActiveDownloads = 0 }, true},
		{"maxActive 1 ok", func(c *Config) { c.MaxActiveDownloads = 1 }, false},
		{"maxActive 32 ok", func(c *Config) { c.MaxActiveDownloads = 32 }, false},
		{"maxActive 33", func(c *Config) { c.MaxActiveDownloads = 33 }, true},
		{"maxActive -1", func(c *Config) { c.MaxActiveDownloads = -1 }, true},
		// SegmentsPerDownload 1..16
		{"segments 0", func(c *Config) { c.SegmentsPerDownload = 0 }, true},
		{"segments 1 ok", func(c *Config) { c.SegmentsPerDownload = 1 }, false},
		{"segments 16 ok", func(c *Config) { c.SegmentsPerDownload = 16 }, false},
		{"segments 17", func(c *Config) { c.SegmentsPerDownload = 17 }, true},
		// MaxConnectionsPerHost 1..16
		{"conns 0", func(c *Config) { c.MaxConnectionsPerHost = 0 }, true},
		{"conns 1 ok", func(c *Config) { c.MaxConnectionsPerHost = 1 }, false},
		{"conns 16 ok", func(c *Config) { c.MaxConnectionsPerHost = 16 }, false},
		{"conns 17", func(c *Config) { c.MaxConnectionsPerHost = 17 }, true},
		// MinSegmentSize >= 64<<10
		{"minSeg 0", func(c *Config) { c.MinSegmentSize = 0 }, true},
		{"minSeg 64KiB-1", func(c *Config) { c.MinSegmentSize = (64 << 10) - 1 }, true},
		{"minSeg 64KiB ok", func(c *Config) { c.MinSegmentSize = 64 << 10 }, false},
		{"minSeg 1MiB ok", func(c *Config) { c.MinSegmentSize = 1 << 20 }, false},
		{"minSeg 64MiB ok", func(c *Config) { c.MinSegmentSize = 64 << 20 }, false},
		{"minSeg 64MiB+1", func(c *Config) { c.MinSegmentSize = (64 << 20) + 1 }, true},
		// BufferSize >= 4<<10
		{"buffer 0", func(c *Config) { c.BufferSize = 0 }, true},
		{"buffer 4KiB-1", func(c *Config) { c.BufferSize = (4 << 10) - 1 }, true},
		{"buffer 4KiB ok", func(c *Config) { c.BufferSize = 4 << 10 }, false},
		{"buffer 64KiB ok", func(c *Config) { c.BufferSize = 64 << 10 }, false},
		{"buffer 1MiB ok", func(c *Config) { c.BufferSize = 1 << 20 }, false},
		{"buffer 1GiB", func(c *Config) { c.BufferSize = 1 << 30 }, true},
		// ProgressInterval >= 50ms
		{"progress 0", func(c *Config) { c.ProgressInterval = 0 }, true},
		{"progress 49ms", func(c *Config) { c.ProgressInterval = 49 * time.Millisecond }, true},
		{"progress 50ms ok", func(c *Config) { c.ProgressInterval = 50 * time.Millisecond }, false},
		{"progress negative", func(c *Config) { c.ProgressInterval = -time.Second }, true},
		{"progress 5s ok", func(c *Config) { c.ProgressInterval = 5 * time.Second }, false},
		{"progress 1h", func(c *Config) { c.ProgressInterval = time.Hour }, true},
		// CheckpointInterval >= 250ms
		{"checkpoint 0", func(c *Config) { c.CheckpointInterval = 0 }, true},
		{"checkpoint 249ms", func(c *Config) { c.CheckpointInterval = 249 * time.Millisecond }, true},
		{"checkpoint 250ms ok", func(c *Config) { c.CheckpointInterval = 250 * time.Millisecond }, false},
		{"checkpoint 60s ok", func(c *Config) { c.CheckpointInterval = 60 * time.Second }, false},
		{"checkpoint 61s", func(c *Config) { c.CheckpointInterval = 61 * time.Second }, true},
		// MaxRetries 0..20
		{"retries -1", func(c *Config) { c.MaxRetries = -1 }, true},
		{"retries 0 ok", func(c *Config) { c.MaxRetries = 0 }, false},
		{"retries 20 ok", func(c *Config) { c.MaxRetries = 20 }, false},
		{"retries 21", func(c *Config) { c.MaxRetries = 21 }, true},
		// StallTimeout >= 100ms
		{"stall 0", func(c *Config) { c.StallTimeout = 0 }, true},
		{"stall 99ms", func(c *Config) { c.StallTimeout = 99 * time.Millisecond }, true},
		{"stall 100ms ok", func(c *Config) { c.StallTimeout = 100 * time.Millisecond }, false},
		{"stall 5m ok", func(c *Config) { c.StallTimeout = 5 * time.Minute }, false},
		{"stall 1h", func(c *Config) { c.StallTimeout = time.Hour }, true},
	}

	_ = baseline
	for _, tc := range cases {
		cfg := Default("/tmp/x")
		tc.mutate(&cfg)
		err := cfg.Validate()
		if tc.wantErr && err == nil {
			t.Errorf("%s: expected error, got nil", tc.name)
		}
		if !tc.wantErr && err != nil {
			t.Errorf("%s: expected no error, got %v", tc.name, err)
		}
	}

	// Subtests exercise each mutation table to ensure coverage without extra scaffolding.
	for _, m := range []mutation{{"noop", func(c *Config) {}}} {
		cfg := Default("/tmp/x")
		m.fn(&cfg)
		_ = cfg.Validate()
	}
}

func TestValidateErrorMentionsField(t *testing.T) {
	cfg := Default("/tmp/x")
	cfg.MaxActiveDownloads = 0
	err := cfg.Validate()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(strings.ToLower(err.Error()), "max active") {
		t.Errorf("error should mention field, got %v", err)
	}
}

func TestClone(t *testing.T) {
	orig := Default("/tmp/downloads")
	orig.FileConflictPolicy = ConflictAutoRename
	cloned := orig.Clone()
	if cloned != orig {
		t.Fatalf("clone mismatch: got %+v, want %+v", cloned, orig)
	}
	cloned.MaxActiveDownloads = 99
	cloned.DefaultDownloadDir = "/other"
	if orig.MaxActiveDownloads == 99 {
		t.Error("mutating clone affected original MaxActiveDownloads")
	}
	if orig.DefaultDownloadDir == "/other" {
		t.Error("mutating clone affected original DefaultDownloadDir")
	}
}

func TestCloneZeroValue(t *testing.T) {
	var zero Config
	cloned := zero.Clone()
	if cloned != zero {
		t.Errorf("zero clone mismatch")
	}
}

func TestDefaultEmptyDirFailsValidation(t *testing.T) {
	cfg := Default("")
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for empty download dir")
	}
}

func TestDefaultDoesNotMutateAcrossCalls(t *testing.T) {
	a := Default("/a")
	b := Default("/b")
	if a.DefaultDownloadDir != "/a" || b.DefaultDownloadDir != "/b" {
		t.Errorf("defaults leaked: a=%q b=%q", a.DefaultDownloadDir, b.DefaultDownloadDir)
	}
	a.MaxRetries = 0
	if b.MaxRetries == 0 {
		t.Error("mutating one default affected another")
	}
}

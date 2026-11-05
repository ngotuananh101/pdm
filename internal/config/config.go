// Package config provides application configuration, defaults, and validation.
package config

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

// FileConflictPolicy controls what happens when a destination file already exists.
type FileConflictPolicy string

const (
	ConflictAsk        FileConflictPolicy = "ask"
	ConflictAutoRename FileConflictPolicy = "auto_rename"
	ConflictOverwrite  FileConflictPolicy = "overwrite"
)

// IsValid reports whether p is a known file conflict policy.
func (p FileConflictPolicy) IsValid() bool {
	switch p {
	case ConflictAsk, ConflictAutoRename, ConflictOverwrite:
		return true
	default:
		return false
	}
}

// Default bounds and defaults for Config fields.
const (
	DefaultMaxActiveDownloads          = 3
	DefaultSegmentsPerDownload         = 4
	DefaultMaxConnectionsPerHost       = 4
	DefaultMinSegmentSize        int64 = 1 << 20  // 1 MiB
	DefaultBufferSize                  = 64 << 10 // 64 KiB

	minMaxActiveDownloads    = 1
	maxMaxActiveDownloads    = 32
	minSegmentsPerDownload   = 1
	maxSegmentsPerDownload   = 16
	minMaxConnectionsPerHost = 1
	maxMaxConnectionsPerHost = 16

	minMinSegmentSize     int64 = 64 << 10 // 64 KiB
	minBufferSize               = 4 << 10  // 4 KiB
	minProgressInterval         = 50 * time.Millisecond
	minCheckpointInterval       = 250 * time.Millisecond
	minMaxRetries               = 0
	maxMaxRetries               = 20
	minStallTimeout             = 100 * time.Millisecond

	maxMinSegmentSize     int64 = 64 << 20 // 64 MiB
	maxBufferSize               = 1 << 20  // 1 MiB
	maxProgressInterval         = 5 * time.Second
	maxCheckpointInterval       = 60 * time.Second
	maxStallTimeout             = 5 * time.Minute

	DefaultProgressInterval   = 500 * time.Millisecond
	DefaultCheckpointInterval = 5 * time.Second
	DefaultMaxRetries         = 3
	DefaultStallTimeout       = 30 * time.Second
)

// Config holds user-tunable application settings.
type Config struct {
	DefaultDownloadDir    string
	FileConflictPolicy    FileConflictPolicy
	MaxActiveDownloads    int
	SegmentsPerDownload   int
	MaxConnectionsPerHost int
	MinSegmentSize        int64
	BufferSize            int
	ProgressInterval      time.Duration
	CheckpointInterval    time.Duration
	MaxRetries            int
	StallTimeout          time.Duration
}

// Default returns a Config with all default values, except DefaultDownloadDir
// which is set from the argument.
func Default(defaultDownloadDir string) Config {
	return Config{
		DefaultDownloadDir:    defaultDownloadDir,
		FileConflictPolicy:    ConflictAsk,
		MaxActiveDownloads:    DefaultMaxActiveDownloads,
		SegmentsPerDownload:   DefaultSegmentsPerDownload,
		MaxConnectionsPerHost: DefaultMaxConnectionsPerHost,
		MinSegmentSize:        DefaultMinSegmentSize,
		BufferSize:            DefaultBufferSize,
		ProgressInterval:      DefaultProgressInterval,
		CheckpointInterval:    DefaultCheckpointInterval,
		MaxRetries:            DefaultMaxRetries,
		StallTimeout:          DefaultStallTimeout,
	}
}

// Validate checks that all fields are within acceptable bounds.
func (c Config) Validate() error {
	if strings.TrimSpace(c.DefaultDownloadDir) == "" {
		return errors.New("config: default download dir must not be empty")
	}
	if !c.FileConflictPolicy.IsValid() {
		return fmt.Errorf("config: invalid file conflict policy %q", string(c.FileConflictPolicy))
	}
	if c.MaxActiveDownloads < minMaxActiveDownloads || c.MaxActiveDownloads > maxMaxActiveDownloads {
		return fmt.Errorf("config: max active downloads must be %d..%d, got %d", minMaxActiveDownloads, maxMaxActiveDownloads, c.MaxActiveDownloads)
	}
	if c.SegmentsPerDownload < minSegmentsPerDownload || c.SegmentsPerDownload > maxSegmentsPerDownload {
		return fmt.Errorf("config: segments per download must be %d..%d, got %d", minSegmentsPerDownload, maxSegmentsPerDownload, c.SegmentsPerDownload)
	}
	if c.MaxConnectionsPerHost < minMaxConnectionsPerHost || c.MaxConnectionsPerHost > maxMaxConnectionsPerHost {
		return fmt.Errorf("config: max connections per host must be %d..%d, got %d", minMaxConnectionsPerHost, maxMaxConnectionsPerHost, c.MaxConnectionsPerHost)
	}
	if c.MinSegmentSize < minMinSegmentSize || c.MinSegmentSize > maxMinSegmentSize {
		return fmt.Errorf("config: min segment size must be %d..%d, got %d", minMinSegmentSize, maxMinSegmentSize, c.MinSegmentSize)
	}
	if c.BufferSize < minBufferSize || c.BufferSize > maxBufferSize {
		return fmt.Errorf("config: buffer size must be %d..%d, got %d", minBufferSize, maxBufferSize, c.BufferSize)
	}
	if c.ProgressInterval < minProgressInterval || c.ProgressInterval > maxProgressInterval {
		return fmt.Errorf("config: progress interval must be %s..%s, got %s", minProgressInterval, maxProgressInterval, c.ProgressInterval)
	}
	if c.CheckpointInterval < minCheckpointInterval || c.CheckpointInterval > maxCheckpointInterval {
		return fmt.Errorf("config: checkpoint interval must be %s..%s, got %s", minCheckpointInterval, maxCheckpointInterval, c.CheckpointInterval)
	}
	if c.MaxRetries < minMaxRetries || c.MaxRetries > maxMaxRetries {
		return fmt.Errorf("config: max retries must be %d..%d, got %d", minMaxRetries, maxMaxRetries, c.MaxRetries)
	}
	if c.StallTimeout < minStallTimeout || c.StallTimeout > maxStallTimeout {
		return fmt.Errorf("config: stall timeout must be %s..%s, got %s", minStallTimeout, maxStallTimeout, c.StallTimeout)
	}
	return nil
}

// Clone returns an independent copy of the config.
func (c Config) Clone() Config {
	return c
}

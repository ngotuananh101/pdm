// Package domain defines the core download entity, its lifecycle state
// machine, and the immutable snapshots handed to the UI layer.
//
// The package is intentionally free of external dependencies (stdlib only)
// and knows nothing about Fyne, SQLite, or the HTTP engine.
package domain

import "time"

// Status is the lifecycle state of a download.
type Status string

const (
	StatusQueued      Status = "queued"
	StatusProbing     Status = "probing"
	StatusDownloading Status = "downloading"
	StatusFinalizing  Status = "finalizing"
	StatusCompleted   Status = "completed"
	StatusPaused      Status = "paused"
	StatusCancelled   Status = "cancelled"
	StatusFailed      Status = "failed"
)

// IsActive reports whether the download is still progressing through the
// pipeline (queued, probing, downloading, or finalizing).
func (s Status) IsActive() bool {
	switch s {
	case StatusQueued, StatusProbing, StatusDownloading, StatusFinalizing:
		return true
	default:
		return false
	}
}

// IsTerminal reports whether the download reached an end state
// (completed, cancelled, or failed). Note that a cancelled or failed
// download may still be re-queued for retry via CanTransition.
func (s Status) IsTerminal() bool {
	switch s {
	case StatusCompleted, StatusCancelled, StatusFailed:
		return true
	default:
		return false
	}
}

// CanTransition reports whether moving from one status to another is
// allowed by the state diagram in docs/ARCHITECTURE.md §5:
//
//	queued      -> probing, paused, cancelled, failed
//	probing     -> downloading, paused, cancelled, failed
//	downloading -> finalizing, paused, cancelled, failed
//	finalizing  -> completed, failed
//	paused      -> queued, cancelled
//	failed      -> queued          (retry)
//	failed      -> finalizing      (conflict resolution only: ResolveConflict
//	                               finalizes the preserved .part file of a
//	                               failed file_conflict job without
//	                               re-downloading)
//	failed      -> cancelled       (conflict resolution only: the user
//	                               cancels a failed file_conflict job;
//	                               ResolveConflict(ResolutionCancel)
//	                               removes the .part file)
//	cancelled   -> queued          (retry)
//	completed   -> (terminal, no outgoing transitions)
func CanTransition(from, to Status) bool {
	switch from {
	case StatusQueued:
		switch to {
		case StatusProbing, StatusPaused, StatusCancelled, StatusFailed:
			return true
		}
	case StatusProbing:
		switch to {
		case StatusDownloading, StatusPaused, StatusCancelled, StatusFailed:
			return true
		}
	case StatusDownloading:
		switch to {
		case StatusFinalizing, StatusPaused, StatusCancelled, StatusFailed:
			return true
		}
	case StatusFinalizing:
		switch to {
		case StatusCompleted, StatusFailed:
			return true
		}
	case StatusPaused:
		switch to {
		case StatusQueued, StatusCancelled:
			return true
		}
	case StatusFailed:
		switch to {
		case StatusQueued:
			return true
		case StatusFinalizing:
			// Only for conflict resolution: ResolveConflict finalizes
			// the preserved .part file of a failed file_conflict job
			// without re-downloading.
			return true
		case StatusCancelled:
			// Only for conflict resolution: ResolveConflict with
			// ResolutionCancel moves a failed file_conflict job to
			// cancelled and removes its .part file.
			return true
		}
	case StatusCancelled:
		if to == StatusQueued {
			return true
		}
	}
	return false
}

// ConflictResolution describes how a destination-file name clash is
// resolved. ResolutionUnset means the policy has not been chosen yet
// (e.g. the user still has to decide when asked).
type ConflictResolution string

const (
	ResolutionUnset      ConflictResolution = ""
	ResolutionAutoRename ConflictResolution = "auto_rename"
	ResolutionOverwrite  ConflictResolution = "overwrite"
	ResolutionCancel     ConflictResolution = "cancel"
)

// Download is the mutable domain entity for a single download job.
// It is owned by the app/engine layer; the UI only ever receives
// DownloadSnapshot copies.
type Download struct {
	ID              string
	URL             string
	Filename        string
	DestinationDir  string
	TempPath        string
	FinalPath       string
	Status          Status
	TotalBytes      int64 // -1 if unknown
	DownloadedBytes int64
	// Resource validators from the probe (ARCH §5). Empty / false until
	// Stage 3 sets real values; they are plain data, no behavior.
	ETag              string
	LastModified      string
	SupportsRange     bool
	SpeedBytesPerSec  float64
	ETASeconds        int64 // -1 if unknown
	ErrorCode         ErrorCode
	ErrorMessage      string
	SuggestedFilename string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// DownloadSnapshot is an immutable, value-semantics copy of a Download
// that is safe to hand to the UI or to logs. RedactedURL never contains
// userinfo, query secrets, or fragments.
type DownloadSnapshot struct {
	ID                string
	URL               string
	RedactedURL       string
	Filename          string
	DestinationDir    string
	TempPath          string
	FinalPath         string
	Status            Status
	TotalBytes        int64 // -1 if unknown
	DownloadedBytes   int64
	ETag              string
	LastModified      string
	SupportsRange     bool
	SpeedBytesPerSec  float64
	ETASeconds        int64 // -1 if unknown
	ErrorCode         ErrorCode
	ErrorMessage      string
	SuggestedFilename string
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

// Snapshot returns an immutable copy of d with RedactedURL populated
// via RedactURL. A nil receiver yields the zero snapshot.
func (d *Download) Snapshot() DownloadSnapshot {
	if d == nil {
		return DownloadSnapshot{}
	}
	return DownloadSnapshot{
		ID:                d.ID,
		URL:               d.URL,
		RedactedURL:       RedactURL(d.URL),
		Filename:          d.Filename,
		DestinationDir:    d.DestinationDir,
		TempPath:          d.TempPath,
		FinalPath:         d.FinalPath,
		Status:            d.Status,
		TotalBytes:        d.TotalBytes,
		DownloadedBytes:   d.DownloadedBytes,
		ETag:              d.ETag,
		LastModified:      d.LastModified,
		SupportsRange:     d.SupportsRange,
		SpeedBytesPerSec:  d.SpeedBytesPerSec,
		ETASeconds:        d.ETASeconds,
		ErrorCode:         d.ErrorCode,
		ErrorMessage:      d.ErrorMessage,
		SuggestedFilename: d.SuggestedFilename,
		CreatedAt:         d.CreatedAt,
		UpdatedAt:         d.UpdatedAt,
	}
}

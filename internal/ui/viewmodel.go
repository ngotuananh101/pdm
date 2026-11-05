package ui

import (
	"fmt"
	"math"
)

// Status is the lifecycle state of a download (ARCHITECTURE section 5).
// It mirrors domain.Status and is defined locally in the UI package until
// internal/domain lands; an adapter can map domain snapshots to this type.
type Status string

const (
	StatusQueued      Status = "queued"
	StatusProbing     Status = "probing"
	StatusDownloading Status = "downloading"
	StatusFinalizing  Status = "finalizing"
	StatusCompleted   Status = "completed"
	StatusPaused      Status = "paused"
	StatusFailed      Status = "failed"
	StatusCancelled   Status = "cancelled"
)

// ErrorCodeFileConflict is the well-known error code used when a download
// cannot proceed because the destination file already exists.
const ErrorCodeFileConflict = "file_conflict"

// DownloadSnapshot is a point-in-time copy of a download's state for display.
// It mirrors domain.DownloadSnapshot (ARCHITECTURE section 5).
//
// Only RedactedURL is carried across the UI boundary; raw URLs (which may
// contain tokens/credentials) never enter UI display structs
// (ARCHITECTURE sections 9, 12).
type DownloadSnapshot struct {
	// ID is the stable download identifier.
	ID string
	// Filename is the destination file name.
	Filename string
	// DestinationDir is the directory the file is (or will be) written to.
	DestinationDir string
	// Status is the lifecycle state.
	Status Status
	// TotalBytes is the total size, or a negative value when unknown.
	TotalBytes int64
	// DownloadedBytes is the number of bytes fetched so far.
	DownloadedBytes int64
	// SpeedBytesPerSec is the observed transfer rate.
	SpeedBytesPerSec float64
	// ETASeconds is the estimated remaining time, or negative when unknown.
	ETASeconds int64
	// RedactedURL is URL with query/userinfo redacted. Safe for display.
	RedactedURL string
	// ErrorCode is a machine-readable error code (e.g. ErrorCodeFileConflict).
	ErrorCode string
	// ErrorMessage is the sanitized, user-facing error text.
	ErrorMessage string
	// SuggestedFilename is the auto-rename suggestion when a file conflict
	// is waiting for resolution. Empty when not applicable.
	SuggestedFilename string
}

// RowViewModel carries everything a downloads-list row needs to render.
type RowViewModel struct {
	ID                string
	Filename          string
	Status            Status
	StatusText        string
	Progress          float64 // 0..1, or -1 when indeterminate
	IsIndeterminate   bool
	ProgressText      string // e.g. "12.5 MiB / 100.0 MiB"
	SpeedText         string // e.g. "1.8 MiB/s"
	ETA               string // e.g. "02m 15s"
	Source            string // redacted URL only
	ErrorMessage      string
	ErrorCode         string
	SuggestedFilename string
	TotalBytes        int64
	DownloadedBytes   int64
}

var byteUnits = []string{"B", "KiB", "MiB", "GiB", "TiB", "PiB"}

// FormatBytes renders a byte count using binary units:
// "0 B", "64.0 KiB", "12.5 MiB". Negative values return "—".
func FormatBytes(b int64) string {
	if b < 0 {
		return "—"
	}
	return formatByteValue(float64(b))
}

func formatByteValue(v float64) string {
	if v < 1024 {
		return fmt.Sprintf("%.0f B", v)
	}
	unit := 0
	for v >= 1024 && unit < len(byteUnits)-1 {
		v /= 1024
		unit++
	}
	return fmt.Sprintf("%.1f %s", v, byteUnits[unit])
}

// FormatProgressBytes renders "downloaded / total". A negative total means
// the length is unknown: "4.2 MiB / Unknown".
func FormatProgressBytes(downloaded, total int64) string {
	if total < 0 {
		return FormatBytes(downloaded) + " / Unknown"
	}
	return FormatBytes(downloaded) + " / " + FormatBytes(total)
}

// FormatSpeed renders e.g. "1.8 MiB/s", or "—" when the rate is unknown.
func FormatSpeed(bytesPerSec float64) string {
	if bytesPerSec <= 0 || math.IsNaN(bytesPerSec) || math.IsInf(bytesPerSec, 0) {
		return "—"
	}
	return formatByteValue(bytesPerSec) + "/s"
}

// FormatETA renders e.g. "02m 15s", "< 1s" when the ETA is zero, and "—"
// when unknown (negative value).
func FormatETA(etaSeconds int64) string {
	if etaSeconds < 0 {
		return "—"
	}
	if etaSeconds == 0 {
		return "< 1s"
	}
	if etaSeconds < 60 {
		return fmt.Sprintf("%ds", etaSeconds)
	}
	if hours := etaSeconds / 3600; hours > 0 {
		rest := etaSeconds % 3600
		return fmt.Sprintf("%dh %02dm %02ds", hours, rest/60, rest%60)
	}
	return fmt.Sprintf("%02dm %02ds", etaSeconds/60, etaSeconds%60)
}

// FormatStatus maps a Status to a short human label. A file-conflict error
// code takes precedence so conflicting downloads read "Conflict".
func FormatStatus(s Status, errCode string) string {
	if errCode == ErrorCodeFileConflict {
		return "Conflict"
	}
	switch s {
	case StatusQueued:
		return "Queued"
	case StatusProbing:
		return "Probing"
	case StatusDownloading:
		return "Downloading"
	case StatusFinalizing:
		return "Finalizing"
	case StatusCompleted:
		return "Completed"
	case StatusPaused:
		return "Paused"
	case StatusFailed:
		return "Failed"
	case StatusCancelled:
		return "Cancelled"
	default:
		return string(s)
	}
}

// MapSnapshotToRow converts a snapshot into its row view model. Only the
// redacted URL is kept; the raw URL is never copied into the view model.
func MapSnapshotToRow(snap DownloadSnapshot) RowViewModel {
	row := RowViewModel{
		ID:                snap.ID,
		Filename:          snap.Filename,
		Status:            snap.Status,
		StatusText:        FormatStatus(snap.Status, snap.ErrorCode),
		ProgressText:      FormatProgressBytes(snap.DownloadedBytes, snap.TotalBytes),
		SpeedText:         FormatSpeed(snap.SpeedBytesPerSec),
		ETA:               FormatETA(snap.ETASeconds),
		Source:            snap.RedactedURL,
		ErrorMessage:      snap.ErrorMessage,
		ErrorCode:         snap.ErrorCode,
		SuggestedFilename: snap.SuggestedFilename,
		TotalBytes:        snap.TotalBytes,
		DownloadedBytes:   snap.DownloadedBytes,
	}
	if snap.TotalBytes > 0 {
		p := float64(snap.DownloadedBytes) / float64(snap.TotalBytes)
		switch {
		case p < 0:
			p = 0
		case p > 1:
			p = 1
		}
		row.Progress = p
	} else {
		row.Progress = -1
		row.IsIndeterminate = true
	}
	return row
}

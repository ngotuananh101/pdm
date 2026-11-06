// Port types for Stage 1 (single-stream engine) and Stage 2 (persistence).
//
// These types are the contract between the app layer and the engine's
// StreamDownloader, and between the app layer and the storage layer.
// The domain package deliberately imports nothing beyond the standard
// library.
package domain

import (
	"context"
	"time"
)

// FinalizeMode controls how the finished temp file is promoted to its
// final destination.
type FinalizeMode string

const (
	// FinalizeNoClobber fails if the destination already exists.
	FinalizeNoClobber FinalizeMode = "no_clobber"
	// FinalizeAutoRename picks a non-conflicting filename.
	FinalizeAutoRename FinalizeMode = "auto_rename"
	// FinalizeOverwrite replaces an existing destination.
	FinalizeOverwrite FinalizeMode = "overwrite"
)

// ProgressUpdate is reported by the engine while a transfer runs.
type ProgressUpdate struct {
	DownloadedBytes  int64
	TotalBytes       int64 // -1 if unknown
	ResolvedFilename string
}

// TransferRequest describes a single-stream transfer to perform.
type TransferRequest struct {
	ID             string
	URL            string
	DestinationDir string
	Filename       string
	TempPath       string
	Mode           FinalizeMode
	OnProgress     func(ProgressUpdate)
}

// TransferResult is returned after a successful transfer and finalize.
type TransferResult struct {
	FinalPath    string
	Filename     string
	TempPath     string
	WrittenBytes int64
	TotalBytes   int64
}

// StreamDownloader transfers one file over a single HTTP stream.
type StreamDownloader interface {
	Download(ctx context.Context, req TransferRequest) (TransferResult, error)
}

// DownloadRepository persists Download entities and their queue order.
// The app layer depends only on this interface; the SQLite
// implementation lives in internal/storage/sqlite.
//
// Queue order is owned by the repository, not by the entity:
// Download has no QueuePosition field. Insert appends at the end
// (queue_position = max + 1), ListAll always returns queue order, and
// Move reorders. Update writes every persisted field except
// queue_position; Move is the only way to change it.
//
// Field mapping notes (schema 001, ARCH §5):
//   - TotalBytes -1 (unknown) maps to NULL; any value >= 0 maps to itself.
//   - Empty ETag / LastModified map to NULL.
//   - SupportsRange maps to 0/1.
//   - SpeedBytesPerSec, ETASeconds and SuggestedFilename are runtime-only
//     and are never persisted.
//
// Methods that address a missing ID (GetByID, Update, UpdateStatus,
// Delete, Move) return an error wrapping ErrDownloadNotFound.
type DownloadRepository interface {
	// Insert stores a new download, appending it at the end of the queue.
	// A zero CreatedAt / UpdatedAt is filled with the current UTC time
	// (and written back into d). Inserting an ID that already exists
	// is an error.
	Insert(ctx context.Context, d *Download) error
	// GetByID returns the download with the given ID.
	GetByID(ctx context.Context, id string) (*Download, error)
	// ListAll returns every download in queue order (queue_position).
	ListAll(ctx context.Context) ([]*Download, error)
	// Update rewrites every persisted field of d except queue_position.
	// A zero UpdatedAt is filled with the current UTC time.
	Update(ctx context.Context, d *Download) error
	// UpdateStatus changes only the status (and updated_at) of the
	// download with the given ID. A zero updatedAt is filled with the
	// current UTC time.
	UpdateStatus(ctx context.Context, id string, status Status, updatedAt time.Time) error
	// Delete removes the download row. Dependent segment rows are
	// removed by the foreign-key cascade.
	Delete(ctx context.Context, id string) error
	// Move places the download at the given 0-based position in the
	// queue order, shifting the other downloads to make room.
	// Positions outside [0, n-1] clamp to the nearest end. Moving a
	// download to the position it already occupies is a no-op.
	Move(ctx context.Context, id string, position int) error
}

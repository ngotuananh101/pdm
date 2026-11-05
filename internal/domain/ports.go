// Port types for Stage 1 (single-stream engine).
//
// These types are the contract between the app layer and the engine's
// StreamDownloader. The domain package deliberately imports nothing
// beyond the standard library.
package domain

import "context"

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

// Finalize (Stage 3, Phase 4.2).
//
// Finalize promotes a finished .part file to its destination, and is
// the single place that does so for the segmented path (the
// single-stream downloader finalizes internally today; the service
// routes the segmented path through here so both paths end with the
// same checks).
//
// Order, per docs/ARCHITECTURE.md §8:
//
//  1. The .part file must exist.
//  2. Its size must equal totalBytes when the total is known
//     (totalBytes < 0 means unknown: nothing to check). A mismatch
//     fails WITHOUT renaming, and the .part is left intact.
//  3. The file is synced (and closed) so the bytes are durable
//     before the rename.
//  4. platform.FinalizeTempFile renames it with the no-clobber
//     mechanism. Only after that returns success may the caller
//     record completed.
//
// Finalize never deletes anything and never overwrites: a rename
// failure (conflict, symlink, directory) leaves the .part exactly
// where it was.
package engine

import (
	"fmt"
	"os"

	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/platform"
)

// Finalize verifies and promotes tempPath to dir/filename according
// to mode. It returns the final path and filename on success.
func Finalize(tempPath, dir, filename string, mode domain.FinalizeMode, totalBytes int64) (finalPath, finalName string, err error) {
	fi, err := os.Stat(tempPath)
	if err != nil {
		return "", "", codeError(domain.ErrorCodeFilesystem, "finalize: stat temp file: "+err.Error(), err)
	}
	if totalBytes >= 0 && fi.Size() != totalBytes {
		return "", "", codeError(domain.ErrorCodeSizeMismatch,
			fmt.Sprintf("finalize: size mismatch: temp file has %d bytes, expected %d", fi.Size(), totalBytes),
			domain.ErrSizeMismatch)
	}
	// Sync before rename: the rename must only publish durable bytes.
	f, err := os.Open(tempPath)
	if err != nil {
		return "", "", codeError(domain.ErrorCodeFilesystem, "finalize: open temp file: "+err.Error(), err)
	}
	syncErr := f.Sync()
	closeErr := f.Close()
	if syncErr != nil {
		return "", "", codeError(domain.ErrorCodeFilesystem, "finalize: sync temp file: "+syncErr.Error(), syncErr)
	}
	if closeErr != nil {
		return "", "", codeError(domain.ErrorCodeFilesystem, "finalize: close temp file: "+closeErr.Error(), closeErr)
	}
	return platform.FinalizeTempFile(tempPath, dir, filename, mode)
}

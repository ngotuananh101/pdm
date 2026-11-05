package platform

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// EnsureDestinationDir cleans dir, creates it if needed, and verifies
// that it is a directory. It returns the cleaned path.
func EnsureDestinationDir(dir string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("%w: empty destination directory", domain.ErrInvalidDestination)
	}
	cleanDir := filepath.Clean(dir)
	// If cleanDir already exists as a non-directory, report it
	// directly instead of surfacing a raw MkdirAll error.
	if fi, err := os.Stat(cleanDir); err == nil {
		if !fi.IsDir() {
			return "", fmt.Errorf("%w: destination is not a directory: %s", domain.ErrInvalidDestination, cleanDir)
		}
		// Refuse symlinks: os.Stat follows them, so re-check with Lstat.
		if lfi, lerr := os.Lstat(cleanDir); lerr == nil && lfi.Mode()&os.ModeSymlink != 0 {
			return "", fmt.Errorf("%w: destination directory is a symlink: %s", domain.ErrSymlinkTarget, cleanDir)
		}
		return cleanDir, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("platform: stat destination dir: %w", err)
	}
	if err := os.MkdirAll(cleanDir, 0o755); err != nil {
		return "", fmt.Errorf("platform: create destination dir: %w", err)
	}
	st, err := os.Stat(cleanDir)
	if err != nil {
		return "", fmt.Errorf("platform: stat destination dir: %w", err)
	}
	if !st.IsDir() {
		return "", fmt.Errorf("%w: destination is not a directory", domain.ErrInvalidDestination)
	}
	if lfi, lerr := os.Lstat(cleanDir); lerr == nil && lfi.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: destination directory is a symlink: %s", domain.ErrSymlinkTarget, cleanDir)
	}
	return cleanDir, nil
}

// CheckExistingTarget inspects finalPath without following symlinks.
// It returns (false, false, nil) if the path does not exist,
// (true, true, nil) if it exists and is a symlink, and (true, false, nil)
// if it exists as a regular file or other non-symlink entry.
func CheckExistingTarget(finalPath string) (exists bool, isSymlink bool, err error) {
	fi, err := os.Lstat(finalPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, false, nil
		}
		return false, false, err
	}
	return true, fi.Mode()&os.ModeSymlink != 0, nil
}

// SuggestAvailableFilename returns filename if free, otherwise the
// first "stem (n)ext" candidate that does not exist in dir.
func SuggestAvailableFilename(dir, filename string) (string, error) {
	stem, ext := splitStemExt(filename)
	// Check the plain filename first.
	if _, err := os.Lstat(filepath.Join(dir, filename)); errors.Is(err, os.ErrNotExist) {
		return filename, nil
	} else if err != nil {
		return "", err
	}
	for i := 1; i <= 1000; i++ {
		suffix := fmt.Sprintf(" (%d)", i)
		candidate := truncateStemTo(stem, suffix+ext, MaxFilenameBytes) + suffix + ext
		if _, err := os.Lstat(filepath.Join(dir, candidate)); errors.Is(err, os.ErrNotExist) {
			return candidate, nil
		} else if err != nil {
			return "", err
		}
	}
	return "", fmt.Errorf("platform: no available filename for %q in %q after 1000 attempts", filename, dir)
}

// FinalizeTempFile atomically promotes tempPath to dir/filename
// according to mode. It never overwrites a symlink or a directory.
func FinalizeTempFile(tempPath, dir, filename string, mode domain.FinalizeMode) (finalPath string, finalName string, err error) {
	if err := ValidateFilename(filename); err != nil {
		return "", "", err
	}
	if dir == "" {
		return "", "", fmt.Errorf("%w: empty destination directory", domain.ErrInvalidDestination)
	}
	target := filepath.Join(dir, filename)

	// Refuse to clobber symlinks or directories regardless of mode.
	if fi, lerr := os.Lstat(target); lerr == nil {
		if fi.Mode()&os.ModeSymlink != 0 {
			return "", "", fmt.Errorf("%w: %s", domain.ErrSymlinkTarget, target)
		}
		if fi.IsDir() {
			return "", "", fmt.Errorf("%w: destination is a directory: %s", domain.ErrInvalidDestination, target)
		}
	} else if !errors.Is(lerr, os.ErrNotExist) {
		return "", "", lerr
	}

	switch mode {
	case domain.FinalizeNoClobber:
		if err := atomicRenameNoClobber(tempPath, target); err != nil {
			if errors.Is(err, domain.ErrDestinationExists) {
				suggest, serr := SuggestAvailableFilename(dir, filename)
				if serr != nil {
					return "", "", err
				}
				return "", "", &domain.FileConflictError{
					Filename:          filename,
					ExistingPath:      target,
					SuggestedFilename: suggest,
				}
			}
			return "", "", err
		}
		return target, filename, nil

	case domain.FinalizeOverwrite:
		if err := atomicRenameOverwrite(tempPath, target); err != nil {
			return "", "", err
		}
		return target, filename, nil

	case domain.FinalizeAutoRename:
		// Try candidate 0 then "stem (i)ext" for i=1..1000.
		candidates := []struct {
			name string
			path string
		}{{filename, target}} // candidate 0
		stem, ext := splitStemExt(filename)
		for i := 1; i <= 1000; i++ {
			suffix := fmt.Sprintf(" (%d)", i)
			candName := truncateStemTo(stem, suffix+ext, MaxFilenameBytes) + suffix + ext
			candidates = append(candidates, struct {
				name string
				path string
			}{candName, filepath.Join(dir, candName)})
		}
		for _, cand := range candidates {
			if err := atomicRenameNoClobber(tempPath, cand.path); err != nil {
				if errors.Is(err, domain.ErrDestinationExists) {
					continue
				}
				// A symlink race may surface as either sentinel.
				if errors.Is(err, domain.ErrSymlinkTarget) {
					continue
				}
				return "", "", err
			}
			return cand.path, cand.name, nil
		}
		return "", "", fmt.Errorf("platform: no available filename for %q in %q after 1000 attempts", filename, dir)

	default:
		return "", "", fmt.Errorf("platform: unknown finalize mode %q", string(mode))
	}
}

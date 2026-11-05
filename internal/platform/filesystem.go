package platform

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// checkPathNoSymlinks walks every component of path with Lstat and
// refuses the path if any existing component is a symlink. Components
// that do not exist yet cannot be symlinks and are skipped, so the
// check also works for paths about to be created. Only the final
// component used to be checked, which let a destination below a
// symlinked parent (e.g. <symlink-to-outside>/sub) write outside the
// intended tree.
func checkPathNoSymlinks(path string) error {
	clean := filepath.Clean(path)
	for p := clean; ; p = filepath.Dir(p) {
		if fi, err := os.Lstat(p); err == nil {
			if fi.Mode()&os.ModeSymlink != 0 {
				return fmt.Errorf("%w: path component is a symlink: %s", domain.ErrSymlinkTarget, p)
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("platform: lstat path component: %w", err)
		}
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
	}
	return nil
}

// EnsureDestinationDir cleans dir, creates it if needed, and verifies
// that it is a directory. It returns the cleaned path. It refuses the
// directory if any component of the path is a symlink.
func EnsureDestinationDir(dir string) (string, error) {
	if dir == "" {
		return "", fmt.Errorf("%w: empty destination directory", domain.ErrInvalidDestination)
	}
	cleanDir := filepath.Clean(dir)
	if err := checkPathNoSymlinks(cleanDir); err != nil {
		return "", err
	}
	// If cleanDir already exists as a non-directory, report it
	// directly instead of surfacing a raw MkdirAll error.
	if fi, err := os.Stat(cleanDir); err == nil {
		if !fi.IsDir() {
			return "", fmt.Errorf("%w: destination is not a directory: %s", domain.ErrInvalidDestination, cleanDir)
		}
		return cleanDir, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("platform: stat destination dir: %w", err)
	}
	if err := os.MkdirAll(cleanDir, 0o755); err != nil {
		return "", fmt.Errorf("platform: create destination dir: %w", err)
	}
	// Re-validate after creation: a parent could have been swapped
	// for a symlink while MkdirAll was following it.
	if err := checkPathNoSymlinks(cleanDir); err != nil {
		return "", err
	}
	st, err := os.Stat(cleanDir)
	if err != nil {
		return "", fmt.Errorf("platform: stat destination dir: %w", err)
	}
	if !st.IsDir() {
		return "", fmt.Errorf("%w: destination is not a directory", domain.ErrInvalidDestination)
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
	// Re-validate the destination directory instead of trusting that
	// the caller checked it earlier: it may have been swapped for a
	// symlink (or below a symlinked parent) in the meantime.
	if err := checkPathNoSymlinks(dir); err != nil {
		return "", "", err
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

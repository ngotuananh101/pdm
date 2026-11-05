//go:build linux

package platform

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// atomicRenameNoClobber renames src to dst only if dst does not exist.
func atomicRenameNoClobber(src, dst string) error {
	err := unix.Renameat2(unix.AT_FDCWD, src, unix.AT_FDCWD, dst, unix.RENAME_NOREPLACE)
	if err == nil {
		return nil
	}
	switch err {
	case unix.EEXIST, unix.ENOTEMPTY:
		return domain.ErrDestinationExists
	case unix.ENOSYS, unix.EINVAL:
		// Fall back to link+remove for filesystems/kernels without
		// renameat2 support.
		return linkRemoveNoClobber(src, dst)
	default:
		if errors.Is(err, unix.EEXIST) {
			return domain.ErrDestinationExists
		}
		return fmt.Errorf("platform: renameat2: %w", err)
	}
}

func linkRemoveNoClobber(src, dst string) error {
	if err := os.Link(src, dst); err != nil {
		if errors.Is(err, os.ErrExist) {
			return domain.ErrDestinationExists
		}
		if errors.Is(err, syscall.EEXIST) {
			return domain.ErrDestinationExists
		}
		return fmt.Errorf("platform: link: %w", err)
	}
	if err := os.Remove(src); err != nil {
		return fmt.Errorf("platform: remove temp after link: %w", err)
	}
	return nil
}

// atomicRenameOverwrite renames src to dst, replacing an existing
// regular file if present. Callers must have already refused symlink
// and directory targets.
func atomicRenameOverwrite(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("platform: rename: %w", err)
	}
	return nil
}

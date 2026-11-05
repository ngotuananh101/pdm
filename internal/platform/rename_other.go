//go:build !linux && !windows

package platform

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// atomicRenameNoClobber renames src to dst only if dst does not exist,
// using link(2) + remove for atomicity without renameat2.
func atomicRenameNoClobber(src, dst string) error {
	if err := os.Link(src, dst); err != nil {
		if errors.Is(err, os.ErrExist) || errors.Is(err, syscall.EEXIST) {
			return domain.ErrDestinationExists
		}
		return fmt.Errorf("platform: link: %w", err)
	}
	if err := os.Remove(src); err != nil {
		return fmt.Errorf("platform: remove temp after link: %w", err)
	}
	return nil
}

// atomicRenameOverwrite renames src to dst, replacing an existing file.
func atomicRenameOverwrite(src, dst string) error {
	if err := os.Rename(src, dst); err != nil {
		return fmt.Errorf("platform: rename: %w", err)
	}
	return nil
}

//go:build windows

package platform

import (
	"errors"
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/windows"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// atomicRenameNoClobber renames src to dst only if dst does not exist.
func atomicRenameNoClobber(src, dst string) error {
	srcPtr, err := windows.UTF16PtrFromString(src)
	if err != nil {
		return fmt.Errorf("platform: %w", err)
	}
	dstPtr, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return fmt.Errorf("platform: %w", err)
	}
	err = windows.MoveFileEx(srcPtr, dstPtr, windows.MOVEFILE_WRITE_THROUGH)
	if err == nil {
		return nil
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		switch errno {
		case windows.ERROR_ALREADY_EXISTS, windows.ERROR_FILE_EXISTS:
			return domain.ErrDestinationExists
		}
		if errors.Is(err, windows.ERROR_ALREADY_EXISTS) || errors.Is(err, windows.ERROR_FILE_EXISTS) {
			return domain.ErrDestinationExists
		}
	}
	if err == windows.ERROR_ALREADY_EXISTS || err == windows.ERROR_FILE_EXISTS {
		return domain.ErrDestinationExists
	}
	return fmt.Errorf("platform: MoveFileEx: %w", err)
}

// atomicRenameOverwrite renames src to dst, replacing an existing file.
func atomicRenameOverwrite(src, dst string) error {
	srcPtr, err := windows.UTF16PtrFromString(src)
	if err != nil {
		return fmt.Errorf("platform: %w", err)
	}
	dstPtr, err := windows.UTF16PtrFromString(dst)
	if err != nil {
		return fmt.Errorf("platform: %w", err)
	}
	if err := windows.MoveFileEx(srcPtr, dstPtr, windows.MOVEFILE_REPLACE_EXISTING|windows.MOVEFILE_WRITE_THROUGH); err != nil {
		return fmt.Errorf("platform: MoveFileEx: %w", err)
	}
	return nil
}

// Ensure the os import is referenced on Windows builds that may not use
// it elsewhere; it is used indirectly via os.ErrExist checks in tests.
var _ = os.ErrExist

// Package platform contains OS-specific helpers for PDM: filesystem paths,
// URL and filename validation, and atomic finalize primitives.
package platform

import (
	"os"
	"path/filepath"
)

// DefaultDownloadDir returns the default directory for downloads.
// It honours XDG_DOWNLOAD_DIR when that variable holds an absolute path,
// otherwise it returns the "Downloads" directory inside the user's home.
// If the home directory cannot be determined it falls back to ".".
func DefaultDownloadDir() string {
	if xdg := os.Getenv("XDG_DOWNLOAD_DIR"); xdg != "" {
		if filepath.IsAbs(xdg) {
			return xdg
		}
	}
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return "."
	}
	return filepath.Join(home, "Downloads")
}

// AppDataDir returns the application data directory for PDM, which is
// the per-user config directory joined with "pdm".
func AppDataDir() (string, error) {
	cfg, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(cfg, "pdm"), nil
}

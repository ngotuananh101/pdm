package config

import "context"

// SettingsStore persists the whole Config as one unit. The SQLite
// implementation lives in internal/storage/sqlite
// (SettingsRepository); internal/ui holds this interface so the UI can
// save settings without importing SQL (ARCH §3: only cmd/pdm composes
// concrete implementations).
type SettingsStore interface {
	// GetSettings returns the stored config, or fallback when nothing
	// has been stored yet. A corrupt stored value returns fallback
	// plus an error.
	GetSettings(ctx context.Context, fallback Config) (Config, error)
	// SaveSettings validates cfg and persists it whole, replacing any
	// previous value. It never writes an invalid config.
	SaveSettings(ctx context.Context, cfg Config) error
}

// Command pdm is the Ponta Download Manager desktop entry point. It wires
// the logger, configuration, engine, app service and Fyne UI together
// (ARCHITECTURE sections 2 and 4). Only cmd/pdm composes these concrete
// implementations; internal/ui depends only on the DownloadService
// interface (plus domain for snapshot types).
package main

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/ngotuananh101/pdm/internal/app"
	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/engine"
	"github.com/ngotuananh101/pdm/internal/platform"
	"github.com/ngotuananh101/pdm/internal/storage/sqlite"
	"github.com/ngotuananh101/pdm/internal/ui"
)

func main() {
	// Structured logger to stderr. Never log URLs: they may carry
	// tokens (ARCHITECTURE sections 9 and 12).
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)
	logger.Info("pdm starting")

	cfg := config.Default(platform.DefaultDownloadDir())
	// Load persisted settings from SQLite (Stage 2, Phase 3): a
	// missing database, a failed one-time JSON migration, or corrupt
	// stored settings all fall back to defaults with a warning, so
	// bad settings can never prevent startup. The JSON file is only
	// read by the one-time migration, never as the live store.
	//
	// db is kept in scope past this block: the same open database
	// also backs the download repository below (Phase 5) — one
	// shared connection, never opened twice.
	var settingsStore config.SettingsStore
	var db *sqlite.DB
	if dir, err := platform.AppDataDir(); err != nil {
		logger.Warn("pdm: cannot locate app data dir, using default settings", "err", err)
	} else if opened, err := sqlite.Open(filepath.Join(dir, sqlite.DefaultDBFilename)); err != nil {
		logger.Warn("pdm: cannot open database, using default settings", "err", err)
	} else {
		db = opened
		defer db.Close()
		ctx := context.Background()
		if migrated, err := sqlite.MigrateSettingsFromJSON(ctx, db.DB, dir, cfg); err != nil {
			logger.Warn("pdm: settings migration from JSON failed, using default settings", "err", err)
		} else if migrated {
			logger.Info("pdm: migrated settings from JSON to SQLite")
		}
		settingsStore = sqlite.NewSettingsRepository(db.DB)
		if loaded, err := settingsStore.GetSettings(ctx, cfg); err != nil {
			logger.Warn("pdm: using default settings", "err", err)
		} else {
			cfg = loaded
		}
	}
	// engine.NewHTTPClient never returns a nil client.
	httpClient := engine.NewHTTPClient(cfg)
	downloader := engine.NewSingleDownloader(cfg, httpClient)
	coreSvc := app.NewService(cfg, downloader)
	if db != nil {
		// The download repository wraps the same *sql.DB the
		// settings repository uses; Service.Shutdown closes it
		// after the shutdown checkpoint. A failed restore must
		// never prevent startup: the app continues with an empty
		// queue and the next startup retries recovery.
		coreSvc.SetDownloadRepository(sqlite.NewDownloadRepository(db.DB))
		// Stage 3, Phase 4.4: run downloads through the manager
		// (probe + segmented workers with resume, single-stream
		// fallback), with the segment repository it resumes from
		// and restart recovery reconciles against. Wired before
		// LoadFromRepository so recovery can verify finalizing
		// jobs.
		segRepo := sqlite.NewSegmentRepository(db.DB)
		coreSvc.SetSegmentRepository(segRepo)
		coreSvc.SetManager(engine.NewManager(cfg, httpClient, nil, segRepo))
		if err := coreSvc.LoadFromRepository(context.Background()); err != nil {
			logger.Warn("pdm: could not restore previous downloads", "err", err)
		}
	}
	svc := ui.NewAppServiceAdapterWithLogger(coreSvc, logger)
	// The settings dialog saves through the SQLite store; without a
	// database (open failed above) SaveConfig reports an error
	// instead of silently writing a JSON file the next startup
	// would ignore.
	if settingsStore != nil {
		svc.SetSettingsStore(settingsStore)
	}

	factory := func() ui.DownloadService { return svc }
	// onClose runs exactly once inside MainWindow's single Quit sequence
	// (background goroutine) after svc.Shutdown returns; it must not call
	// Shutdown again.
	uiApp := ui.NewApp("com.ponta.pdm", svc, factory, func() {
		logger.Info("pdm quit: service shutdown complete")
	})
	uiApp.Run()
}

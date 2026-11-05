// Command pdm is the Ponta Download Manager desktop entry point. It wires
// the logger, configuration, engine, app service and Fyne UI together
// (ARCHITECTURE sections 2 and 4). Only cmd/pdm composes these concrete
// implementations; internal/ui depends only on the DownloadService
// interface (plus domain for snapshot types).
package main

import (
	"log/slog"
	"os"

	"github.com/ngotuananh101/pdm/internal/app"
	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/engine"
	"github.com/ngotuananh101/pdm/internal/platform"
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
	// Load persisted settings (Phase 7, U1): a missing, corrupt,
	// or invalid settings file falls back to defaults (config.Load
	// never fails hard), so bad settings can never prevent startup.
	if dir, err := platform.AppDataDir(); err != nil {
		logger.Warn("pdm: cannot locate app data dir, using default settings", "err", err)
	} else if loaded, err := config.Load(dir, cfg); err != nil {
		logger.Warn("pdm: using default settings", "err", err)
	} else {
		cfg = loaded
	}
	// engine.NewHTTPClient never returns a nil client.
	httpClient := engine.NewHTTPClient(cfg)
	downloader := engine.NewSingleDownloader(cfg, httpClient)
	coreSvc := app.NewService(cfg, downloader)
	svc := ui.NewAppServiceAdapterWithLogger(coreSvc, logger)

	factory := func() ui.DownloadService { return svc }
	// onClose runs exactly once inside MainWindow's single Quit sequence
	// (background goroutine) after svc.Shutdown returns; it must not call
	// Shutdown again.
	uiApp := ui.NewApp("com.ponta.pdm", svc, factory, func() {
		logger.Info("pdm quit: service shutdown complete")
	})
	uiApp.Run()
}

package ui

import (
	"context"
	"errors"
	"log/slog"

	"github.com/ngotuananh101/pdm/internal/app"
	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
)

// AppServiceAdapter adapts the real app.Service (which speaks
// internal/domain and internal/config types) to the UI's DownloadService
// contract. It lives in internal/ui so cmd/pdm stays a thin wiring layer
// while UI keeps importing only domain types, which ARCHITECTURE section 3
// permits (UI -> app/domain).
type AppServiceAdapter struct {
	svc    *app.Service
	logger *slog.Logger
}

// NewAppServiceAdapter wraps a real app.Service for the UI.
func NewAppServiceAdapter(svc *app.Service) *AppServiceAdapter {
	return &AppServiceAdapter{svc: svc, logger: slog.Default()}
}

// NewAppServiceAdapterWithLogger is NewAppServiceAdapter with an explicit logger.
func NewAppServiceAdapterWithLogger(svc *app.Service, logger *slog.Logger) *AppServiceAdapter {
	if logger == nil {
		logger = slog.Default()
	}
	return &AppServiceAdapter{svc: svc, logger: logger}
}

func mapPolicy(p config.FileConflictPolicy) ConflictPolicy {
	switch p {
	case config.ConflictAsk:
		return ConflictAsk
	case config.ConflictAutoRename:
		return ConflictAutoRename
	case config.ConflictOverwrite:
		return ConflictOverwrite
	default:
		return ConflictAsk
	}
}

func mapPolicyReverse(p ConflictPolicy) config.FileConflictPolicy {
	switch p {
	case ConflictAutoRename:
		return config.ConflictAutoRename
	case ConflictOverwrite:
		return config.ConflictOverwrite
	default:
		return config.ConflictAsk
	}
}

func mapResolution(r Resolution) domain.ConflictResolution {
	switch r {
	case ResolutionAutoRename:
		return domain.ResolutionAutoRename
	case ResolutionOverwrite:
		return domain.ResolutionOverwrite
	case ResolutionCancel:
		return domain.ResolutionCancel
	default:
		return domain.ResolutionUnset
	}
}

func mapResolutionReverse(r domain.ConflictResolution) Resolution {
	switch r {
	case domain.ResolutionAutoRename:
		return ResolutionAutoRename
	case domain.ResolutionOverwrite:
		return ResolutionOverwrite
	case domain.ResolutionCancel:
		return ResolutionCancel
	default:
		return ResolutionAutoRename
	}
}

func mapStatus(s domain.Status) Status {
	switch s {
	case domain.StatusQueued:
		return StatusQueued
	case domain.StatusProbing:
		return StatusProbing
	case domain.StatusDownloading:
		return StatusDownloading
	case domain.StatusFinalizing:
		return StatusFinalizing
	case domain.StatusCompleted:
		return StatusCompleted
	case domain.StatusPaused:
		return StatusPaused
	case domain.StatusFailed:
		return StatusFailed
	case domain.StatusCancelled:
		return StatusCancelled
	default:
		return Status(string(s))
	}
}

func mapSnapshot(d domain.DownloadSnapshot) DownloadSnapshot {
	return DownloadSnapshot{
		ID:                d.ID,
		Filename:          d.Filename,
		DestinationDir:    d.DestinationDir,
		Status:            mapStatus(d.Status),
		TotalBytes:        d.TotalBytes,
		DownloadedBytes:   d.DownloadedBytes,
		SpeedBytesPerSec:  d.SpeedBytesPerSec,
		ETASeconds:        d.ETASeconds,
		RedactedURL:       d.RedactedURL,
		ErrorCode:         string(d.ErrorCode),
		ErrorMessage:      d.ErrorMessage,
		SuggestedFilename: d.SuggestedFilename,
	}
}

// AddDownload delegates to app.Service, translating domain file conflicts
// into the UI's FileConflictError.
func (a *AppServiceAdapter) AddDownload(req AddRequest) (string, error) {
	var res domain.ConflictResolution
	if req.Resolution != nil {
		res = mapResolution(*req.Resolution)
	}
	id, err := a.svc.AddDownload(app.AddRequest{
		URL:            req.URL,
		DestinationDir: req.DestinationDir,
		Filename:       req.Filename,
		Resolution:     res,
	})
	if err != nil {
		var fce *domain.FileConflictError
		if errors.As(err, &fce) {
			return "", &FileConflictError{
				Filename:          fce.Filename,
				ExistingPath:      fce.ExistingPath,
				SuggestedFilename: fce.SuggestedFilename,
			}
		}
	}
	return id, err
}

func (a *AppServiceAdapter) Cancel(id string, deleteTemp bool) error {
	return a.svc.Cancel(id, deleteTemp)
}

func (a *AppServiceAdapter) Retry(id string) error {
	return a.svc.Retry(id)
}

func (a *AppServiceAdapter) Remove(id string) error {
	return a.svc.Remove(id)
}

func (a *AppServiceAdapter) ResolveConflict(id string, res Resolution) error {
	return a.svc.ResolveConflict(id, mapResolution(res))
}

func (a *AppServiceAdapter) List() []DownloadSnapshot {
	raw := a.svc.List()
	out := make([]DownloadSnapshot, len(raw))
	for i, s := range raw {
		out[i] = mapSnapshot(s)
	}
	return out
}

func (a *AppServiceAdapter) GetConfig() Config {
	cfg := a.svc.GetConfig()
	return Config{
		DefaultDownloadDir: cfg.DefaultDownloadDir,
		ConflictPolicy:     mapPolicy(cfg.FileConflictPolicy),
		MaxActiveDownloads: cfg.MaxActiveDownloads,
	}
}

func (a *AppServiceAdapter) SaveConfig(sc ScratchConfig) error {
	cur := a.svc.GetConfig()
	cur.DefaultDownloadDir = sc.DefaultDownloadDir
	cur.FileConflictPolicy = mapPolicyReverse(sc.ConflictPolicy)
	cur.MaxActiveDownloads = sc.MaxActiveDownloads
	return a.svc.UpdateConfig(cur)
}

func (a *AppServiceAdapter) SetListener(f func()) {
	a.svc.SetListener(f)
}

func (a *AppServiceAdapter) Shutdown(ctx context.Context) error {
	return a.svc.Shutdown(ctx)
}

// Ensure AppServiceAdapter satisfies the UI contract.
var _ DownloadService = (*AppServiceAdapter)(nil)

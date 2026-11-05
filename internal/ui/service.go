package ui

import (
	"context"
	"fmt"
	"sync"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// Config mirrors the subset of internal/config.Config the UI needs. It is
// defined here so internal/ui does not depend on config internals, and
// cmd/pdm adapts the real config into it.
type Config struct {
	DefaultDownloadDir string
	ConflictPolicy     ConflictPolicy
	MaxActiveDownloads int
}

// ConflictPolicy selects how duplicate destination files are handled.
type ConflictPolicy string

const (
	ConflictAsk        ConflictPolicy = "ask"
	ConflictAutoRename ConflictPolicy = "auto_rename"
	ConflictOverwrite  ConflictPolicy = "overwrite"
)

// Resolution is the user-chosen outcome for a file conflict.
type Resolution string

const (
	ResolutionAutoRename Resolution = "auto_rename"
	ResolutionOverwrite  Resolution = "overwrite"
	ResolutionCancel     Resolution = "cancel"
)

// AddRequest is the parameter set for adding a download.
type AddRequest struct {
	URL            string
	DestinationDir string
	Filename       string
	Resolution     *Resolution
}

// FileConflictError is returned (as *FileConflictError) by AddDownload when
// ConflictPolicy is ask and the destination file already exists. The UI uses
// it to offer the 3-way resolution prompt.
type FileConflictError struct {
	Filename          string
	ExistingPath      string
	SuggestedFilename string
}

func (e *FileConflictError) Error() string {
	if e.Filename == "" {
		return "destination file already exists"
	}
	return "destination file already exists: " + e.Filename
}

// DownloadService is the contract internal/ui expects from internal/app.
// cmd/pdm wires the concrete app.Service into this interface (see
// adapter.go). The methods correspond to ARCHITECTURE sections 5 and 10.
type DownloadService interface {
	// AddDownload enqueues a new download. It returns *FileConflictError
	// when the destination already exists and no resolution was supplied.
	AddDownload(req AddRequest) (string, error)
	// Cancel stops an active download. When deleteTemp is true the
	// service also removes the .part file; the UI asks the user first
	// (ARCHITECTURE section 5).
	Cancel(id string, deleteTemp bool) error
	// Retry re-queues a failed or cancelled download.
	Retry(id string) error
	// Remove deletes the record from the list without touching any file.
	Remove(id string) error
	// ResolveConflict applies the chosen resolution to a download stuck
	// on a runtime file conflict (e.g. after Content-Disposition).
	ResolveConflict(id string, res Resolution) error
	// List returns the current snapshots sorted for display.
	List() []DownloadSnapshot
	// GetConfig returns the current settings.
	GetConfig() Config
	// SaveConfig persists the settings.
	SaveConfig(ScratchConfig) error
	// SetListener registers f, called after every state change. The
	// service must not hold locks while calling f; UI handlers must
	// re-enter the main thread with fyne.Do.
	SetListener(f func())
	// Shutdown stops the service, honoring ctx.
	Shutdown(ctx context.Context) error
}

// ServiceFactory creates a fresh service. It is an indirection seam so UI
// tests and future app wiring can supply a substitute implementation.
type ServiceFactory func() DownloadService

// ScratchConfig is the settings-update payload used by the settings dialog.
// Named separately from Config so callers must be explicit.
type ScratchConfig struct {
	DefaultDownloadDir string
	ConflictPolicy     ConflictPolicy
	MaxActiveDownloads int
}

// NoopService is an in-memory test double. It is not a production service;
// cmd/pdm wires the real app.Service via AppServiceAdapter. It exists so UI
// package tests can exercise dialogs and views without HTTP or disk.
type NoopService struct {
	mu       sync.RWMutex
	cfg      Config
	listener func()
	snaps    []DownloadSnapshot
	nextID   int
}

// NewNoopService builds a NoopService with a permissive default config.
func NewNoopService() *NoopService {
	return &NoopService{
		cfg: Config{
			DefaultDownloadDir: "~/.local/share/pdm/downloads",
			ConflictPolicy:     ConflictAsk,
			MaxActiveDownloads: 3,
		},
	}
}

func (s *NoopService) AddDownload(req AddRequest) (string, error) {
	s.mu.Lock()
	s.nextID++
	id := fmt.Sprintf("download-%d", s.nextID)
	s.snaps = append(s.snaps, DownloadSnapshot{
		ID:             id,
		Filename:       req.Filename,
		DestinationDir: req.DestinationDir,
		Status:         StatusQueued,
		TotalBytes:     -1,
		RedactedURL:    domain.RedactURL(req.URL),
	})
	s.mu.Unlock()
	s.notify()
	return id, nil
}

func (s *NoopService) Cancel(_ string, _ bool) error { s.notify(); return nil }
func (s *NoopService) Retry(_ string) error          { s.notify(); return nil }
func (s *NoopService) Remove(_ string) error         { s.notify(); return nil }
func (s *NoopService) ResolveConflict(_ string, _ Resolution) error {
	s.notify()
	return nil
}

func (s *NoopService) List() []DownloadSnapshot {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]DownloadSnapshot, len(s.snaps))
	copy(out, s.snaps)
	return out
}

func (s *NoopService) GetConfig() Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

func (s *NoopService) SaveConfig(c ScratchConfig) error {
	s.mu.Lock()
	s.cfg = Config(c)
	s.mu.Unlock()
	s.notify()
	return nil
}

func (s *NoopService) SetListener(f func()) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.listener = f
}

func (s *NoopService) Shutdown(_ context.Context) error { return nil }

func (s *NoopService) notify() {
	s.mu.RLock()
	l := s.listener
	s.mu.RUnlock()
	if l != nil {
		l()
	}
}

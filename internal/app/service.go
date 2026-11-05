// Package app owns Stage 1 download state: the job queue, the
// concurrency scheduler, per-job worker lifecycle, speed/ETA
// calculation, and listener notifications.
//
// Concurrency discipline (critical):
//   - All mutable state is guarded by Service.mu (sync.RWMutex).
//   - The registered listener is NEVER invoked while holding mu.
//     It is copied under lock, the lock is released, and only then
//     is it called.
//   - Every running job has its own cancelFunc and done channel.
//     done is closed only when the worker goroutine has completely
//     exited (the engine closes all file handles before returning),
//     which makes it safe to delete a .part file after <-done,
//     even on Windows.
package app

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/engine"
	"github.com/ngotuananh101/pdm/internal/platform"
)

// AddRequest describes a new download to enqueue.
type AddRequest struct {
	URL            string
	DestinationDir string
	Filename       string
	Resolution     domain.ConflictResolution
}

// job is the mutable per-download record owned by Service.
type job struct {
	download     domain.Download
	finalizeMode domain.FinalizeMode

	cancel     context.CancelFunc
	done       chan struct{}
	running    bool
	cancelling bool

	// Speed tracking: last observed byte count and time.
	lastBytes int64
	lastTime  time.Time
}

// Service is the Stage 1 download service.
type Service struct {
	mu         sync.RWMutex
	cfg        config.Config
	downloader domain.StreamDownloader
	jobs       map[string]*job
	order      []string
	listener   func()
	closed     bool
	running    int
}

// NewService creates a Service with the given config and downloader.
// If downloader is nil, a SingleDownloader bound to cfg is used.
// MaxActiveDownloads and FileConflictPolicy are normalized so a
// zero-valued config still yields a usable service; other zero
// fields are left as-is (the engine treats them as defaults).
func NewService(cfg config.Config, downloader domain.StreamDownloader) *Service {
	if cfg.MaxActiveDownloads <= 0 {
		cfg.MaxActiveDownloads = config.DefaultMaxActiveDownloads
	}
	if cfg.MaxActiveDownloads > 32 {
		cfg.MaxActiveDownloads = 32
	}
	if !cfg.FileConflictPolicy.IsValid() {
		cfg.FileConflictPolicy = config.ConflictAsk
	}
	if downloader == nil {
		downloader = engine.NewSingleDownloader(cfg, nil)
	}
	return &Service{
		cfg:        cfg,
		downloader: downloader,
		jobs:       make(map[string]*job),
	}
}

// New is an alias for NewService.
func New(cfg config.Config, downloader domain.StreamDownloader) *Service {
	return NewService(cfg, downloader)
}

// notify invokes the listener without holding mu. The listener is
// copied under a read lock, the lock is released, then it is called.
func (s *Service) notify() {
	s.mu.RLock()
	l := s.listener
	s.mu.RUnlock()
	if l != nil {
		l()
	}
}

// SetListener registers f, called after every state change.
func (s *Service) SetListener(f func()) {
	s.mu.Lock()
	s.listener = f
	s.mu.Unlock()
}

// GetConfig returns the current config.
func (s *Service) GetConfig() config.Config {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.cfg
}

// UpdateConfig validates cfg, stores it, wakes queued jobs if
// MaxActiveDownloads increased, and notifies the listener.
func (s *Service) UpdateConfig(cfg config.Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	s.mu.Lock()
	s.cfg = cfg
	s.tryDispatchLocked()
	s.mu.Unlock()
	s.notify()
	return nil
}

// effectiveFinalizeMode maps a resolution / policy to a finalize mode.
func effectiveFinalizeMode(res domain.ConflictResolution, policy config.FileConflictPolicy) (domain.FinalizeMode, bool) {
	// bool reports whether the caller explicitly resolved the conflict.
	switch res {
	case domain.ResolutionAutoRename:
		return domain.FinalizeAutoRename, true
	case domain.ResolutionOverwrite:
		return domain.FinalizeOverwrite, true
	}
	switch policy {
	case config.ConflictAutoRename:
		return domain.FinalizeAutoRename, true
	case config.ConflictOverwrite:
		return domain.FinalizeOverwrite, true
	default:
		return domain.FinalizeNoClobber, false
	}
}

// hasConcretePathFilename reports whether the URL path ends in a
// concrete filename segment (as opposed to a bare root or trailing
// slash, for which DeriveFilenameFromURL falls back to "download").
func hasConcretePathFilename(rawPath string) bool {
	p := strings.TrimRight(rawPath, "/")
	if p == "" {
		return false
	}
	base := path.Base(p)
	return base != "" && base != "/" && base != "." && base != ".."
}

// AddDownload validates the request, performs the ConflictAsk
// precheck, enqueues a queued job, notifies, and dispatches.
func (s *Service) AddDownload(req AddRequest) (string, error) {
	s.mu.RLock()
	closed := s.closed
	policy := s.cfg.FileConflictPolicy
	s.mu.RUnlock()
	if closed {
		return "", domain.ErrServiceClosed
	}
	if req.Resolution == domain.ResolutionCancel {
		return "", fmt.Errorf("app: invalid resolution %q for AddDownload", string(req.Resolution))
	}

	parsedURL, err := platform.ValidateHTTPURL(req.URL)
	if err != nil {
		return "", err
	}
	destDir, err := platform.EnsureDestinationDir(req.DestinationDir)
	if err != nil {
		return "", err
	}
	if req.Filename != "" {
		if err := platform.ValidateFilename(req.Filename); err != nil {
			return "", err
		}
	}

	mode, explicit := effectiveFinalizeMode(req.Resolution, policy)
	if mode == domain.FinalizeNoClobber && !explicit {
		// Immediate precheck so the UI can prompt right away.
		candidate := req.Filename
		concrete := req.Filename != ""
		if candidate == "" {
			candidate = platform.DeriveFilenameFromURL(parsedURL)
			concrete = hasConcretePathFilename(parsedURL.EscapedPath())
		}
		if concrete {
			target := filepath.Join(destDir, candidate)
			exists, isSymlink, cerr := platform.CheckExistingTarget(target)
			if cerr != nil {
				return "", cerr
			}
			if isSymlink {
				return "", fmt.Errorf("%w: %s", domain.ErrSymlinkTarget, target)
			}
			if exists {
				suggested, serr := platform.SuggestAvailableFilename(destDir, candidate)
				if serr != nil {
					suggested = ""
				}
				return "", &domain.FileConflictError{
					Filename:          candidate,
					ExistingPath:      target,
					SuggestedFilename: suggested,
				}
			}
		}
	}

	id, err := platform.NewID()
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	j := &job{
		download: domain.Download{
			ID:             id,
			URL:            req.URL,
			Filename:       req.Filename,
			DestinationDir: destDir,
			Status:         domain.StatusQueued,
			TotalBytes:     -1,
			ETASeconds:     -1,
			CreatedAt:      now,
			UpdatedAt:      now,
		},
		finalizeMode: mode,
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return "", domain.ErrServiceClosed
	}
	s.jobs[id] = j
	s.order = append(s.order, id)
	s.tryDispatchLocked()
	s.mu.Unlock()
	s.notify()
	return id, nil
}

// List returns deep-copied snapshots in insertion order, with
// RedactedURL populated and ErrorMessage sanitized.
func (s *Service) List() []domain.DownloadSnapshot {
	s.mu.RLock()
	out := make([]domain.DownloadSnapshot, 0, len(s.order))
	for _, id := range s.order {
		j := s.jobs[id]
		if j == nil {
			continue
		}
		snap := j.download.Snapshot()
		snap.ErrorMessage = domain.SanitizeErrorText(snap.ErrorMessage)
		out = append(out, snap)
	}
	s.mu.RUnlock()
	return out
}

// Cancel stops an active download. When deleteTemp is true the
// .part file is removed only after the worker goroutine has fully
// exited (<-job.done), so deletion is safe on Windows.
func (s *Service) Cancel(id string, deleteTemp bool) error {
	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	if j.download.Status == domain.StatusCancelled {
		s.mu.Unlock()
		return nil
	}
	if !j.download.Status.IsActive() {
		s.mu.Unlock()
		return fmt.Errorf("%w: cannot cancel job in status %q", domain.ErrInvalidTransition, j.download.Status)
	}
	if !j.running {
		// Queued but not yet dispatched: no worker goroutine exists,
		// so there is nothing to wait for.
		j.download.Status = domain.StatusCancelled
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		j.download.UpdatedAt = time.Now().UTC()
		tempPath := j.download.TempPath
		s.mu.Unlock()

		if deleteTemp && tempPath != "" {
			if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				s.notify()
				return err
			}
			s.mu.Lock()
			if jj, ok := s.jobs[id]; ok && jj.download.TempPath == tempPath {
				jj.download.TempPath = ""
			}
			s.mu.Unlock()
		}
		s.notify()
		return nil
	}
	j.cancelling = true
	cancel := j.cancel
	done := j.done
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}

	s.mu.Lock()
	j, ok = s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	// If the job already completed before the cancel took effect,
	// leave the completed result (and final file) untouched.
	if j.download.Status == domain.StatusCompleted {
		s.mu.Unlock()
		return nil
	}
	tempPath := ""
	if deleteTemp {
		tempPath = j.download.TempPath
	}
	s.mu.Unlock()

	if deleteTemp && tempPath != "" {
		if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}

	s.mu.Lock()
	j, ok = s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	if j.download.Status == domain.StatusCompleted {
		s.mu.Unlock()
		return nil
	}
	if deleteTemp && tempPath != "" && j.download.TempPath == tempPath {
		j.download.TempPath = ""
	}
	j.download.Status = domain.StatusCancelled
	j.download.SpeedBytesPerSec = 0
	j.download.ETASeconds = -1
	j.download.UpdatedAt = time.Now().UTC()
	s.mu.Unlock()
	s.notify()
	return nil
}

// Retry re-queues a failed or cancelled download.
func (s *Service) Retry(id string) error {
	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	if j.download.Status != domain.StatusFailed && j.download.Status != domain.StatusCancelled {
		s.mu.Unlock()
		return fmt.Errorf("%w: cannot retry job in status %q", domain.ErrInvalidTransition, j.download.Status)
	}
	if s.closed {
		s.mu.Unlock()
		return domain.ErrServiceClosed
	}
	j.download.ErrorCode = ""
	j.download.ErrorMessage = ""
	j.download.SuggestedFilename = ""
	j.download.DownloadedBytes = 0
	j.download.SpeedBytesPerSec = 0
	j.download.ETASeconds = -1
	j.download.Status = domain.StatusQueued
	j.download.UpdatedAt = time.Now().UTC()
	j.cancelling = false
	j.cancel = nil
	j.done = nil
	j.running = false
	j.lastBytes = 0
	j.lastTime = time.Time{}
	s.tryDispatchLocked()
	s.mu.Unlock()
	s.notify()
	return nil
}

// Remove deletes a non-active job record without touching final
// files on disk. It refuses active jobs.
func (s *Service) Remove(id string) error {
	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	if j.download.Status.IsActive() || j.download.Status == domain.StatusPaused {
		s.mu.Unlock()
		return fmt.Errorf("%w: cannot remove active job in status %q", domain.ErrInvalidTransition, j.download.Status)
	}
	idCopy := id
	_ = idCopy
	delete(s.jobs, id)
	_ = s.order
	for i, oid := range s.order {
		if oid == id {
			s.order = append(s.order[:i], s.order[i+1:]...)
			break
		}
	}
	s.mu.Unlock()
	s.notify()
	return nil
}

// ResolveConflict applies the chosen resolution to a job that is
// stuck on a file conflict. When the .part file is still on disk
// and the resolution is auto-rename/overwrite, it is finalized
// immediately without re-downloading.
func (s *Service) ResolveConflict(id string, res domain.ConflictResolution) error {
	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	if j.download.ErrorCode != domain.ErrorCodeFileConflict {
		s.mu.Unlock()
		return fmt.Errorf("%w: job %q is not waiting on a file conflict", domain.ErrInvalidTransition, id)
	}
	switch res {
	case domain.ResolutionCancel:
		tempPath := j.download.TempPath
		j.download.Status = domain.StatusCancelled
		j.download.ErrorCode = ""
		j.download.ErrorMessage = ""
		j.download.SuggestedFilename = ""
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		j.download.UpdatedAt = time.Now().UTC()
		s.mu.Unlock()
		if tempPath != "" {
			if err := os.Remove(tempPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
		}
		s.mu.Lock()
		if jj, ok := s.jobs[id]; ok {
			jj.download.TempPath = ""
		}
		s.mu.Unlock()
		s.notify()
		return nil
	case domain.ResolutionAutoRename, domain.ResolutionOverwrite:
		mode := domain.FinalizeAutoRename
		if res == domain.ResolutionOverwrite {
			mode = domain.FinalizeOverwrite
		}
		// Guard against concurrent ResolveConflict calls: transition to
		// finalizing under lock so a second caller sees ErrorCode != conflict.
		prevSuggested := j.download.SuggestedFilename
		tempPath := j.download.TempPath
		destDir := j.download.DestinationDir
		filename := j.download.Filename
		j.finalizeMode = mode
		j.download.Status = domain.StatusFinalizing
		j.download.ErrorCode = ""
		_ = prevSuggested
		s.mu.Unlock()

		if tempPath != "" {
			if _, err := os.Stat(tempPath); err == nil {
				finalPath, finalName, ferr := platform.FinalizeTempFile(tempPath, destDir, filename, mode)
				if ferr != nil {
					s.mu.Lock()
					if jj, ok := s.jobs[id]; ok {
						jj.download.ErrorMessage = domain.SanitizeErrorText(ferr.Error())
						var fce *domain.FileConflictError
						if errors.As(ferr, &fce) {
							jj.download.ErrorCode = domain.ErrorCodeFileConflict
							if fce.Filename != "" {
								jj.download.Filename = fce.Filename
							}
							jj.download.SuggestedFilename = fce.SuggestedFilename
						} else {
							jj.download.ErrorCode = domain.ErrorCodeFilesystem
						}
						jj.download.Status = domain.StatusFailed
						jj.download.SpeedBytesPerSec = 0
						jj.download.ETASeconds = -1
						jj.download.UpdatedAt = time.Now().UTC()
					}
					s.mu.Unlock()
					s.notify()
					return ferr
				}
				s.mu.Lock()
				if jj, ok := s.jobs[id]; ok {
					jj.download.Status = domain.StatusCompleted
					jj.download.FinalPath = finalPath
					jj.download.Filename = finalName
					jj.download.TempPath = ""
					if jj.download.TotalBytes <= 0 {
						jj.download.TotalBytes = jj.download.DownloadedBytes
					}
					jj.download.ErrorCode = ""
					jj.download.ErrorMessage = ""
					jj.download.SuggestedFilename = ""
					jj.download.SpeedBytesPerSec = 0
					jj.download.ETASeconds = -1
					jj.download.UpdatedAt = time.Now().UTC()
				}
				s.mu.Unlock()
				s.notify()
				return nil
			}
		}
		// Temp file missing: re-queue the download with the new mode.
		s.mu.Lock()
		jj, ok := s.jobs[id]
		if !ok {
			s.mu.Unlock()
			return domain.ErrDownloadNotFound
		}
		jj.download.Status = domain.StatusQueued
		jj.download.ErrorCode = ""
		jj.download.ErrorMessage = ""
		jj.download.SuggestedFilename = ""
		jj.download.DownloadedBytes = 0
		jj.download.SpeedBytesPerSec = 0
		jj.download.ETASeconds = -1
		jj.download.TempPath = ""
		jj.download.UpdatedAt = time.Now().UTC()
		jj.cancelling = false
		jj.cancel = nil
		jj.done = nil
		jj.running = false
		s.tryDispatchLocked()
		s.mu.Unlock()
		s.notify()
		return nil
	default:
		s.mu.Unlock()
		return fmt.Errorf("app: invalid conflict resolution %q", string(res))
	}
}

// tryDispatchLocked starts queued jobs while slots are available.
// Caller must hold s.mu.
func (s *Service) tryDispatchLocked() {
	if s.closed {
		return
	}
	for s.running < s.cfg.MaxActiveDownloads {
		var next *job
		for _, id := range s.order {
			j := s.jobs[id]
			if j != nil && j.download.Status == domain.StatusQueued && !j.running {
				next = j
				break
			}
		}
		if next == nil {
			return
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		next.cancel = cancel
		next.done = done
		next.cancelling = false
		next.running = true
		next.download.Status = domain.StatusProbing
		next.download.UpdatedAt = time.Now().UTC()
		s.running++
		id := next.download.ID
		go s.run(id, ctx, done)
	}
}

// onProgress updates byte counts and EWMA speed (alpha=0.3).
func (s *Service) onProgress(id string, pu domain.ProgressUpdate) {
	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	now := time.Now()
	j.download.DownloadedBytes = pu.DownloadedBytes
	j.download.TotalBytes = pu.TotalBytes
	if pu.ResolvedFilename != "" {
		j.download.Filename = pu.ResolvedFilename
	}
	if !j.lastTime.IsZero() {
		dt := now.Sub(j.lastTime).Seconds()
		delta := pu.DownloadedBytes - j.lastBytes
		if dt > 0 && delta >= 0 {
			inst := float64(delta) / dt
			if j.download.SpeedBytesPerSec == 0 {
				j.download.SpeedBytesPerSec = inst
			} else {
				j.download.SpeedBytesPerSec = 0.3*inst + 0.7*j.download.SpeedBytesPerSec
			}
		}
	}
	j.lastBytes = pu.DownloadedBytes
	j.lastTime = now
	if j.download.TotalBytes > 0 && j.download.SpeedBytesPerSec > 0 {
		remaining := float64(j.download.TotalBytes - j.download.DownloadedBytes)
		if remaining < 0 {
			remaining = 0
		}
		j.download.ETASeconds = int64(remaining / j.download.SpeedBytesPerSec)
	} else {
		j.download.ETASeconds = -1
	}
	j.download.UpdatedAt = now.UTC()
	s.mu.Unlock()
	s.notify()
}

// run executes one job. done is closed (via defer) only after the
// engine has returned and all file handles are closed.
func (s *Service) run(id string, ctx context.Context, done chan struct{}) {
	defer func() {
		s.mu.Lock()
		if s.running > 0 {
			s.running--
		}
		if jj, ok := s.jobs[id]; ok && jj.done == done {
			jj.running = false
		}
		s.tryDispatchLocked()
		s.mu.Unlock()
		close(done)
		s.notify()
	}()

	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	if j.cancelling {
		j.download.Status = domain.StatusCancelled
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		j.download.UpdatedAt = time.Now().UTC()
		s.mu.Unlock()
		return
	}
	j.download.Status = domain.StatusDownloading
	j.download.DownloadedBytes = 0
	j.download.SpeedBytesPerSec = 0
	j.download.ETASeconds = -1
	j.download.UpdatedAt = time.Now().UTC()
	j.lastBytes = 0
	j.lastTime = time.Now()
	url := j.download.URL
	destDir := j.download.DestinationDir
	filename := j.download.Filename
	mode := j.finalizeMode
	s.mu.Unlock()
	s.notify()

	res, err := s.downloader.Download(ctx, domain.TransferRequest{
		ID:             id,
		URL:            url,
		DestinationDir: destDir,
		Filename:       filename,
		Mode:           mode,
		OnProgress: func(pu domain.ProgressUpdate) {
			s.onProgress(id, pu)
		},
	})

	s.mu.Lock()
	j, ok = s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	j.download.UpdatedAt = now

	if err == nil {
		j.download.Status = domain.StatusCompleted
		j.download.FinalPath = res.FinalPath
		if res.Filename != "" {
			j.download.Filename = res.Filename
		}
		j.download.TempPath = ""
		j.download.DownloadedBytes = res.WrittenBytes
		j.download.TotalBytes = res.TotalBytes
		j.download.ErrorCode = ""
		j.download.ErrorMessage = ""
		j.download.SuggestedFilename = ""
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		s.mu.Unlock()
		s.notify()
		return
	}

	// Preserve whatever partial state the engine reported.
	if res.TempPath != "" {
		j.download.TempPath = res.TempPath
	}
	if res.Filename != "" {
		j.download.Filename = res.Filename
	}
	if res.WrittenBytes != 0 || res.TotalBytes != 0 {
		j.download.DownloadedBytes = res.WrittenBytes
		j.download.TotalBytes = res.TotalBytes
	}

	// Cancellation (via Cancel or Shutdown) takes precedence.
	if j.cancelling || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		j.download.Status = domain.StatusCancelled
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		s.mu.Unlock()
		s.notify()
		return
	}

	var fce *domain.FileConflictError
	if errors.As(err, &fce) {
		j.download.Status = domain.StatusFailed
		j.download.ErrorCode = domain.ErrorCodeFileConflict
		j.download.ErrorMessage = domain.SanitizeErrorText(err.Error())
		if fce.Filename != "" {
			j.download.Filename = fce.Filename
		}
		if j.download.Filename == "" {
			j.download.Filename = fce.Filename
		}
		j.download.SuggestedFilename = fce.SuggestedFilename
		// TempPath is intentionally preserved (see res above) so
		// ResolveConflict can finalize it without re-downloading.
		// When the engine failed before creating a .part (early
		// conflict check), record the conventional temp path so
		// ResolveConflict has a path to probe and clear.
		if j.download.TempPath == "" && j.download.Filename != "" && j.download.DestinationDir != "" {
			j.download.TempPath = filepath.Join(j.download.DestinationDir, fmt.Sprintf(".%s.%s.part", j.download.Filename, id))
		}
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		s.mu.Unlock()
		s.notify()
		return
	}

	j.download.Status = domain.StatusFailed
	j.download.ErrorCode = errorCodeFor(err)
	j.download.ErrorMessage = domain.SanitizeErrorText(err.Error())
	j.download.SpeedBytesPerSec = 0
	j.download.ETASeconds = -1
	s.mu.Unlock()
	s.notify()
}

// errorCodeFor maps an engine error to a stable domain code.
func errorCodeFor(err error) domain.ErrorCode {
	var ce *engine.CodedError
	if errors.As(err, &ce) && ce.Code != "" {
		return domain.ErrorCode(ce.Code)
	}
	switch {
	case errors.Is(err, domain.ErrSizeMismatch):
		return domain.ErrorCodeSizeMismatch
	case errors.Is(err, domain.ErrSymlinkTarget), errors.Is(err, domain.ErrDestinationExists):
		return domain.ErrorCodeFilesystem
	case errors.Is(err, domain.ErrInvalidURL), errors.Is(err, domain.ErrUnsupportedScheme),
		errors.Is(err, domain.ErrURLHasCredentials), errors.Is(err, domain.ErrInvalidFilename),
		errors.Is(err, domain.ErrInvalidDestination):
		return domain.ErrorCodeValidation
	default:
		return domain.ErrorCodeNetwork
	}
}

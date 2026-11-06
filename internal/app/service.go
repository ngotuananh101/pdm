// Package app owns Stage 1 download state: the job queue, per-job
// worker lifecycle, speed/ETA calculation, and listener notifications.
//
// The concurrency scheduler lives in internal/scheduler: it owns the
// pending queue, the global slot limit, and the per-host slot limit,
// and the service feeds it jobs through startJob. Status changes
// still go through the single setStatusLocked gate below.
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
//   - Scheduler calls (Enqueue, SetLimits, Finished) are made while
//     holding mu, because the scheduler invokes startJob
//     synchronously and startJob assumes mu is held (see startJob).
//     The scheduler never calls startJob while holding its own
//     lock, so this cannot deadlock.
package app

import (
	"context"
	"errors"
	"fmt"
	"net/url"
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
	"github.com/ngotuananh101/pdm/internal/scheduler"
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

	// pausing is true while a Pause is stopping this job's worker
	// (U3, Phase 7): Pause cancels the worker context exactly like
	// Cancel, and run() parks the job in paused instead of cancelled
	// when it sees this flag. It is cleared by run() when the worker
	// exits, or by Resume/Retry when a new generation starts, so a
	// stale flag can never park a fresh generation.
	pausing bool

	// resolving is true while a ResolveConflict finalize is in
	// flight (status finalizing, rename running outside mu).
	// resolveDone is closed when that ResolveConflict returns, so
	// Cancel and Shutdown can wait for the rename instead of
	// racing it.
	resolving   bool
	resolveDone chan struct{}

	// Speed tracking: last observed byte count and time.
	lastBytes int64
	lastTime  time.Time
}

// Service is the Stage 1 download service. Queueing and concurrency
// limits live in the scheduler (see NewService); the service keeps
// the job records, the status machine, and the worker lifecycle.
type Service struct {
	mu         sync.RWMutex
	cfg        config.Config
	downloader domain.StreamDownloader
	// prober verifies Range support and fetches validators before a
	// download starts (Stage 3, Phase 2). A nil prober skips
	// probing: the job runs single-stream with no validators.
	prober engine.RangeProber
	// manager runs downloads through probe + segmented workers with
	// resume (Stage 3, Phase 4.4). A nil manager keeps the
	// pre-Phase-4 path: the single-stream downloader directly.
	manager engine.ManagerRunner
	// segRepo holds the segment plans the manager resumes from and
	// restart recovery reconciles against (Stage 3, Phase 4). A nil
	// repo means no plan is available: the manager runs without
	// persistence and recovery cannot verify segments.
	segRepo domain.SegmentRepository
	// repo persists every status change (Phase 5.1). A nil repo
	// means the service runs RAM-only, exactly like Stage 1.
	repo     domain.DownloadRepository
	jobs     map[string]*job
	order    []string
	listener func()
	closed   bool
	// shutdownComplete reports that a Shutdown call ran to
	// completion (workers done, checkpoint synced, DB closed). A
	// later Shutdown returns nil immediately.
	shutdownComplete bool
	// restored reports that LoadFromRepository already ran; a
	// second call is a no-op.
	restored bool
	sched    *scheduler.Scheduler
}

// NewService creates a Service with the given config and downloader,
// without persistence (repo is nil). See NewServiceWithRepository.
// If downloader is nil, a SingleDownloader bound to cfg is used.
// MaxActiveDownloads and FileConflictPolicy are normalized so a
// zero-valued config still yields a usable service; other zero
// fields are left as-is (the engine treats them as defaults).
func NewService(cfg config.Config, downloader domain.StreamDownloader) *Service {
	return NewServiceWithRepository(cfg, downloader, nil)
}

// NewServiceWithRepository creates a Service that persists every
// status change through repo (see setStatusLocked). A nil repo means
// the service runs RAM-only, exactly like NewService. Prefer this
// constructor when the repository is known at construction; use
// SetDownloadRepository when it only becomes available later
// (cmd/pdm opens the database conditionally).
func NewServiceWithRepository(cfg config.Config, downloader domain.StreamDownloader, repo domain.DownloadRepository) *Service {
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
	svc := &Service{
		cfg:        cfg,
		downloader: downloader,
		repo:       repo,
		jobs:       make(map[string]*job),
	}
	svc.prober = engine.NewProber(cfg, nil)
	// The scheduler owns the pending queue and the slot limits; the
	// service feeds it jobs through startJob. A zero
	// MaxConnectionsPerHost falls back to the scheduler default (4),
	// matching the old maxConnsPerHostLocked behaviour.
	svc.sched = scheduler.New(cfg.MaxActiveDownloads, cfg.MaxConnectionsPerHost, svc.startJob)
	return svc
}

// SetDownloadRepository attaches (or replaces) the persistence
// repository. Jobs already in RAM keep working: their next status
// change inserts the missing row instead of failing (see
// persistLocked). It is safe to call with nil to detach persistence.
func (s *Service) SetDownloadRepository(repo domain.DownloadRepository) {
	s.mu.Lock()
	s.repo = repo
	s.mu.Unlock()
}

// SetProber attaches (or replaces) the range prober used before a
// download starts. A nil prober skips probing: the job runs
// single-stream with no validators, exactly like Stage 2.
func (s *Service) SetProber(p engine.RangeProber) {
	s.mu.Lock()
	s.prober = p
	s.mu.Unlock()
}

// SetManager attaches (or replaces) the download manager used by
// run() (Stage 3, Phase 4.4). A nil manager restores the direct
// single-stream path.
func (s *Service) SetManager(m engine.ManagerRunner) {
	s.mu.Lock()
	s.manager = m
	s.mu.Unlock()
}

// SetSegmentRepository attaches (or replaces) the segment plan
// repository used by the manager (resume) and by restart recovery
// (Stage 3, Phase 4). A nil repository means no segment plans are
// available.
func (s *Service) SetSegmentRepository(r domain.SegmentRepository) {
	s.mu.Lock()
	s.segRepo = r
	s.mu.Unlock()
}

// New is an alias for NewService.
func New(cfg config.Config, downloader domain.StreamDownloader) *Service {
	return NewService(cfg, downloader)
}

// startJob is the scheduler's dispatch callback: it performs the
// queued -> probing transition, installs the worker's cancel/done,
// and spawns run().
//
// It is invoked synchronously from Scheduler.Enqueue/SetLimits/
// Finished, outside the scheduler lock — and always while the
// caller holds s.mu, so it must NOT take s.mu itself. Returning
// false tells the scheduler the job could not start (paused,
// cancelled, or removed meanwhile) so the reserved slot is released
// and the next job is dispatched.
func (s *Service) startJob(id string, ctx context.Context, cancel context.CancelFunc, done chan struct{}) bool {
	j, ok := s.jobs[id]
	if !ok || j.running {
		return false
	}
	// queued -> probing is always allowed for the job selected
	// above; if it ever fails, refuse the dispatch and leave the
	// job queued for the next dispatch attempt.
	if err := s.setStatusLocked(j, domain.StatusProbing); err != nil {
		return false
	}
	j.cancel = cancel
	j.done = done
	j.cancelling = false
	j.running = true
	go s.run(id, ctx, done)
	return true
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

// setStatusLocked is the single gate for every live status change: it
// moves j to the given status only when domain.CanTransition allows
// it, and returns an error wrapping domain.ErrInvalidTransition
// otherwise, leaving the status unchanged. On success it also writes
// the job's full persisted state to the repository, so the DB always
// reflects the status the service reports; a DB failure restores the
// job exactly (status and fields) and returns an error instead of
// reporting false success. Caller must hold s.mu. Direct assignments
// to download.Status outside this helper are forbidden, except
// restart recovery in lifecycle.go, which is not a live transition
// (no worker ever ran in this process).
func (s *Service) setStatusLocked(j *job, to domain.Status) error {
	from := j.download.Status
	if !domain.CanTransition(from, to) {
		return fmt.Errorf("%w: cannot move job %q from %q to %q", domain.ErrInvalidTransition, j.download.ID, from, to)
	}
	// Snapshot the whole entity: a failed DB write restores the job
	// exactly, including fields the caller reset before this call
	// (Retry/Resume clear error state first).
	prev := j.download
	j.download.Status = to
	j.download.UpdatedAt = time.Now().UTC()
	if err := s.persistLocked(j); err != nil {
		j.download = prev
		return fmt.Errorf("app: persist status %q for job %q: %w", to, j.download.ID, err)
	}
	return nil
}

// persistLocked writes j's full persisted state to the repository.
// Caller must hold s.mu. A nil repo is a no-op: the service runs
// RAM-only. When the row does not exist yet (the job predates the
// attached repo), the row is inserted instead of failing.
func (s *Service) persistLocked(j *job) error {
	if s.repo == nil {
		return nil
	}
	if err := s.repo.Update(context.Background(), &j.download); err != nil {
		if errors.Is(err, domain.ErrDownloadNotFound) {
			return s.repo.Insert(context.Background(), &j.download)
		}
		return err
	}
	return nil
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
	// Update the scheduler limits while still holding mu: the
	// dispatch inside SetLimits runs startJob, which assumes mu
	// is held.
	s.sched.SetLimits(cfg.MaxActiveDownloads, cfg.MaxConnectionsPerHost)
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
	if s.repo != nil {
		if err := s.repo.Insert(context.Background(), &j.download); err != nil {
			s.mu.Unlock()
			return "", fmt.Errorf("app: persist new download: %w", err)
		}
	}
	s.jobs[id] = j
	s.order = append(s.order, id)
	// Enqueue while still holding mu: the dispatch inside runs
	// startJob, which assumes mu is held.
	s.sched.Enqueue(id, hostOf(req.URL))
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

// removeTempPart deletes the .part file at path. It is the single
// place that removes a .part file (A6, Phase 3): run() never deletes
// one, so a failed job's .part is removed only when the user
// explicitly chooses to delete it (Cancel with deleteTemp, or
// ResolveConflict Cancel). A file that is already gone (e.g. a
// previous delete won a race) is not an error; any other removal
// failure is reported to the caller.
func removeTempPart(path string) error {
	if path == "" {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Cancel stops an active download. When deleteTemp is true the
// .part file is removed only after the worker goroutine has fully
// exited (<-job.done), so deletion is safe on Windows.
func (s *Service) Cancel(id string, deleteTemp bool) error {
	// If a ResolveConflict is finalizing this job, wait for it to
	// finish before deciding: the resolve owns the rename, and a
	// Cancel decided mid-rename could only lose to it. Waiting
	// also serializes Cancel/ResolveConflict per job (ARCH §4).
	// The loop re-checks after each resolve, in case the job went
	// back to file_conflict and another resolve started.
	for {
		s.mu.Lock()
		j, ok := s.jobs[id]
		if !ok {
			s.mu.Unlock()
			return domain.ErrDownloadNotFound
		}
		if !j.resolving || j.resolveDone == nil {
			s.mu.Unlock()
			break
		}
		rd := j.resolveDone
		s.mu.Unlock()
		<-rd
	}

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
	// A paused job has no worker, but it can still be cancelled:
	// paused -> cancelled is allowed (ARCH section 5), so a
	// paused job is never a trap state — Cancel it (optionally
	// deleting the preserved .part) instead of having to Resume
	// it first.
	if !j.download.Status.IsActive() && j.download.Status != domain.StatusPaused {
		// Capture the status while holding mu: the error below is
		// formatted after Unlock, and run() may change the status
		// concurrently (same post-Unlock read Phase 1 fixed in
		// Remove).
		st := j.download.Status
		s.mu.Unlock()
		return fmt.Errorf("%w: cannot cancel job in status %q", domain.ErrInvalidTransition, st)
	}
	if !j.running {
		// Queued but not yet dispatched: remove it from the
		// scheduler's pending queue so it can never start, then
		// cancel it. There is no worker goroutine to wait for.
		s.sched.Stop(id)
		if err := s.setStatusLocked(j, domain.StatusCancelled); err != nil {
			// The job was removed from the scheduler's pending
			// queue above; put it back so a DB failure does not
			// leave it queued in RAM but undispatchable.
			if j.download.Status == domain.StatusQueued {
				s.sched.Enqueue(id, hostOf(j.download.URL))
			}
			s.mu.Unlock()
			return err
		}
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		j.download.UpdatedAt = time.Now().UTC()
		tempPath := j.download.TempPath
		s.mu.Unlock()

		if deleteTemp && tempPath != "" {
			if err := removeTempPart(tempPath); err != nil {
				s.notify()
				return err
			}
			s.mu.Lock()
			if jj, ok := s.jobs[id]; ok && jj.download.Status == domain.StatusCancelled && jj.download.TempPath == tempPath {
				// Only clear the .part path while the job is still
				// the cancelled generation this Cancel created: a
				// Retry in between may have re-queued the job and
				// the new worker may reuse the same temp path.
				jj.download.TempPath = ""
			}
			s.mu.Unlock()
		}
		s.notify()
		return nil
	}
	j.cancelling = true
	done := j.done
	s.mu.Unlock()

	// Cancel the worker through the scheduler: this serializes
	// with a concurrent Pause/Cancel for the same job and marks
	// the entry stopping so no new worker can start for it. The
	// done channel is closed by run()'s defer once the worker has
	// fully exited, even if the scheduler entry is already gone.
	s.sched.Stop(id)
	if done != nil {
		<-done
	}

	s.mu.Lock()
	j, ok = s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	// Generation check (A2): if a Retry has started a new generation
	// since this Cancel began (Retry resets done, then dispatch
	// installs a fresh channel), the old Cancel must not touch the
	// new job's status or its .part file. The new generation owns
	// its own lifecycle.
	if j.done != done {
		s.mu.Unlock()
		return nil
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
		if err := removeTempPart(tempPath); err != nil {
			return err
		}
	}

	s.mu.Lock()
	j, ok = s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	// Generation check again (A2): a Retry may have slipped in
	// while this Cancel was removing the .part file outside the
	// lock. Never stamp the new generation cancelled, and never
	// clear its TempPath.
	if j.done != done {
		s.mu.Unlock()
		return nil
	}
	if j.download.Status == domain.StatusCompleted {
		s.mu.Unlock()
		return nil
	}
	if deleteTemp && tempPath != "" && j.download.TempPath == tempPath {
		j.download.TempPath = ""
	}
	if j.download.Status != domain.StatusCancelled {
		// The worker may already have reached a terminal state while
		// Cancel waited for it (e.g. failed on its own): a forbidden
		// transition out of a terminal state is refused and the
		// terminal state stands.
		if err := s.setStatusLocked(j, domain.StatusCancelled); err != nil {
			s.mu.Unlock()
			s.notify()
			if errors.Is(err, domain.ErrInvalidTransition) {
				return nil
			}
			return err
		}
	}
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
		// Capture the status while holding mu: the error below is
		// formatted after Unlock, and run() may change the status
		// concurrently (same post-Unlock read Phase 1 fixed in
		// Remove; found by TestCancelRetryRace under -race).
		st := j.download.Status
		s.mu.Unlock()
		return fmt.Errorf("%w: cannot retry job in status %q", domain.ErrInvalidTransition, st)
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
	if err := s.setStatusLocked(j, domain.StatusQueued); err != nil {
		s.mu.Unlock()
		return err
	}
	j.cancelling = false
	j.pausing = false
	j.cancel = nil
	j.done = nil
	j.running = false
	j.lastBytes = 0
	j.lastTime = time.Time{}
	// Re-queue while still holding mu: the dispatch inside runs
	// startJob, which assumes mu is held.
	s.sched.Enqueue(id, hostOf(j.download.URL))
	s.mu.Unlock()
	s.notify()
	return nil
}

// Pause stops an active download and parks it in paused (U3,
// Phase 7). Like Cancel, it cancels the worker context and waits
// for the worker goroutine to exit before returning, so no worker
// is still writing when the job reports paused. Unlike Cancel, it
// keeps the .part file (TempPath is preserved) and never deletes
// anything. A queued job that has not started yet is parked
// without ever starting a worker. Resume re-queues a paused job;
// with the manager wired in (Stage 3, Phase 4.1) the next run
// resumes from the committed bytes, otherwise it downloads from
// the beginning.
func (s *Service) Pause(id string) error {
	// If a ResolveConflict is finalizing this job, wait for it to
	// finish before deciding, exactly like Cancel (ARCH section 4):
	// the resolve owns the rename, and a Pause decided mid-rename
	// could only lose to it.
	for {
		s.mu.Lock()
		j, ok := s.jobs[id]
		if !ok {
			s.mu.Unlock()
			return domain.ErrDownloadNotFound
		}
		if !j.resolving || j.resolveDone == nil {
			s.mu.Unlock()
			break
		}
		rd := j.resolveDone
		s.mu.Unlock()
		<-rd
	}

	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	if j.download.Status == domain.StatusPaused {
		s.mu.Unlock()
		return fmt.Errorf("%w: job %q is already paused", domain.ErrInvalidTransition, id)
	}
	if !j.download.Status.IsActive() {
		st := j.download.Status
		s.mu.Unlock()
		return fmt.Errorf("%w: cannot pause job in status %q", domain.ErrInvalidTransition, st)
	}
	if !j.running {
		// Queued but not yet dispatched: remove it from the
		// scheduler's pending queue so it can never start, then
		// park it. No worker goroutine exists, so there is
		// nothing to wait for.
		s.sched.Stop(id)
		if err := s.setStatusLocked(j, domain.StatusPaused); err != nil {
			// The job was removed from the scheduler's pending
			// queue above; put it back so a DB failure does not
			// leave it queued in RAM but undispatchable.
			if j.download.Status == domain.StatusQueued {
				s.sched.Enqueue(id, hostOf(j.download.URL))
			}
			s.mu.Unlock()
			return err
		}
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		j.download.UpdatedAt = time.Now().UTC()
		s.mu.Unlock()
		s.notify()
		return nil
	}
	j.pausing = true
	done := j.done
	s.mu.Unlock()

	// Stop the worker through the scheduler (see Cancel): it
	// cancels the context, serializes with a concurrent
	// Pause/Cancel for the same job, and marks the entry
	// stopping so no new worker can start for it.
	s.sched.Stop(id)
	if done != nil {
		<-done
	}

	s.mu.Lock()
	j, ok = s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	// Generation check (A2, same as Cancel): if a Retry has
	// started a new generation since this Pause began, the old
	// Pause must not touch the new job's status. The new
	// generation owns its own lifecycle.
	if j.done != done {
		s.mu.Unlock()
		return nil
	}
	// If the job completed before the pause took effect, leave
	// the completed result (and final file) untouched, like
	// Cancel does.
	if j.download.Status == domain.StatusCompleted {
		j.pausing = false
		s.mu.Unlock()
		return nil
	}
	// run() parks the job in paused itself when it sees the flag;
	// this is the normal path and the flag is already cleared
	// there.
	if j.download.Status == domain.StatusPaused {
		s.mu.Unlock()
		return nil
	}
	// Backstop: if run() left the job in a state Pause can still
	// park, park it now. A forbidden move out of a terminal state
	// is refused and that state stands.
	j.pausing = false
	if err := s.setStatusLocked(j, domain.StatusPaused); err != nil {
		s.mu.Unlock()
		s.notify()
		if errors.Is(err, domain.ErrInvalidTransition) {
			return nil
		}
		return err
	}
	j.download.SpeedBytesPerSec = 0
	j.download.ETASeconds = -1
	j.download.UpdatedAt = time.Now().UTC()
	s.mu.Unlock()
	s.notify()
	return nil
}

// Resume re-queues a paused download (U3, Phase 7). When the
// manager is wired in (Stage 3, Phase 4.1), the next run resumes
// from the committed segment bytes instead of starting over: the
// .part file and the segment plan are kept, complete segments are
// skipped, and only the remaining ranges are fetched. Without the
// manager (the pre-Phase-4 path) the next worker starts a fresh
// single-stream transfer — the engine opens the preserved .part
// with O_TRUNC, so previously downloaded bytes are fetched again.
// Resume only works from paused; failed/cancelled jobs use Retry
// instead.
func (s *Service) Resume(id string) error {
	s.mu.Lock()
	j, ok := s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return domain.ErrDownloadNotFound
	}
	if j.download.Status != domain.StatusPaused {
		st := j.download.Status
		s.mu.Unlock()
		return fmt.Errorf("%w: cannot resume job in status %q", domain.ErrInvalidTransition, st)
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
	if err := s.setStatusLocked(j, domain.StatusQueued); err != nil {
		s.mu.Unlock()
		return err
	}
	j.cancelling = false
	j.pausing = false
	j.cancel = nil
	j.done = nil
	j.running = false
	j.lastBytes = 0
	j.lastTime = time.Time{}
	// Re-queue while still holding mu: the dispatch inside runs
	// startJob, which assumes mu is held.
	s.sched.Enqueue(id, hostOf(j.download.URL))
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
		// Capture the status while holding mu: the error below is
		// formatted after Unlock, and run() may change the status
		// concurrently.
		st := j.download.Status
		s.mu.Unlock()
		return fmt.Errorf("%w: cannot remove active job in status %q", domain.ErrInvalidTransition, st)
	}
	idCopy := id
	_ = idCopy
	// A queued job may still sit in the scheduler's pending queue;
	// remove it so it can never start after its record is gone.
	// (Active/paused jobs are refused above, so there is no worker
	// to wait for here.)
	s.sched.Stop(id)
	if s.repo != nil {
		if err := s.repo.Delete(context.Background(), id); err != nil && !errors.Is(err, domain.ErrDownloadNotFound) {
			// The row stays, so the record must stay too; put a
			// queued job back in the scheduler's pending queue.
			if j.download.Status == domain.StatusQueued {
				s.sched.Enqueue(id, hostOf(j.download.URL))
			}
			s.mu.Unlock()
			return err
		}
	}
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
	// Mark the resolve in flight (A3, like run's done channel):
	// Cancel waits on resolveDone instead of racing the rename,
	// and Shutdown waits for it too. The deferred cleanup runs on
	// every return below, after the final status is recorded.
	j.resolving = true
	resolveDone := make(chan struct{})
	j.resolveDone = resolveDone
	defer func() {
		s.mu.Lock()
		j.resolving = false
		j.resolveDone = nil
		s.mu.Unlock()
		close(resolveDone)
	}()
	switch res {
	case domain.ResolutionCancel:
		tempPath := j.download.TempPath
		// failed -> cancelled is allowed (ARCH §5) only for
		// conflict resolution: the user cancels a failed
		// file_conflict job. From any other state setStatusLocked
		// refuses the transition and the job is left untouched.
		if err := s.setStatusLocked(j, domain.StatusCancelled); err != nil {
			s.mu.Unlock()
			return err
		}
		j.download.ErrorCode = ""
		j.download.ErrorMessage = ""
		j.download.SuggestedFilename = ""
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		j.download.UpdatedAt = time.Now().UTC()
		s.mu.Unlock()
		if tempPath != "" {
			if err := removeTempPart(tempPath); err != nil {
				return err
			}
		}
		s.mu.Lock()
		if jj, ok := s.jobs[id]; ok && jj.download.Status == domain.StatusCancelled && jj.download.TempPath == tempPath {
			// Only clear the .part path while the job is still the
			// cancelled generation this resolve created: a Retry
			// in between may have re-queued the job and the new
			// worker may reuse the same temp path.
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
		if err := s.setStatusLocked(j, domain.StatusFinalizing); err != nil {
			s.mu.Unlock()
			return err
		}
		j.download.ErrorCode = ""
		_ = prevSuggested
		s.mu.Unlock()

		if tempPath != "" {
			if _, err := os.Stat(tempPath); err == nil {
				finalPath, finalName, ferr := platform.FinalizeTempFile(tempPath, destDir, filename, mode)
				if ferr != nil {
					s.mu.Lock()
					if jj, ok := s.jobs[id]; ok && jj.download.Status == domain.StatusFinalizing {
						// Only record the failure while the job is
						// still the finalizing generation this
						// resolve started; otherwise leave the
						// status to whoever moved it.
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
						// finalizing -> failed is always allowed here.
						_ = s.setStatusLocked(jj, domain.StatusFailed)
						jj.download.SpeedBytesPerSec = 0
						jj.download.ETASeconds = -1
						jj.download.UpdatedAt = time.Now().UTC()
					}
					s.mu.Unlock()
					s.notify()
					return ferr
				}
				s.mu.Lock()
				if jj, ok := s.jobs[id]; ok && jj.download.Status == domain.StatusFinalizing {
					// The file is already renamed: complete only
					// while the job is still the finalizing
					// generation this resolve started. If another
					// operation moved it meanwhile, leave that
					// status untouched instead of overwriting it.
					// The outcome fields are set BEFORE the
					// completed transition, so its persist already
					// carries FinalPath (Phase 4.2: a crash between
					// the rename and completed must not lose it).
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
					// finalizing -> completed is always allowed here.
					_ = s.setStatusLocked(jj, domain.StatusCompleted)
				}
				s.mu.Unlock()
				s.notify()
				return nil
			}
		}
		// Temp file missing: the download cannot be finalized.
		// finalizing -> failed is allowed; the job waits for Retry,
		// which re-queues it with the resolved finalize mode.
		// finalizing -> queued is forbidden, so there is no dispatch
		// here.
		s.mu.Lock()
		jj, ok := s.jobs[id]
		if !ok {
			s.mu.Unlock()
			return domain.ErrDownloadNotFound
		}
		if jj.download.Status != domain.StatusFinalizing {
			// Another operation moved the job while this resolve
			// was probing for the .part file; leave its status
			// untouched instead of forcing it to failed.
			s.mu.Unlock()
			s.notify()
			return nil
		}
		jj.download.ErrorCode = ""
		jj.download.ErrorMessage = "temporary file is missing; retry to download again"
		jj.download.SuggestedFilename = ""
		jj.download.DownloadedBytes = 0
		jj.download.SpeedBytesPerSec = 0
		jj.download.ETASeconds = -1
		jj.download.TempPath = ""
		jj.cancelling = false
		jj.cancel = nil
		jj.done = nil
		jj.running = false
		// finalizing -> failed is always allowed here.
		_ = s.setStatusLocked(jj, domain.StatusFailed)
		s.mu.Unlock()
		s.notify()
		return nil
	default:
		s.mu.Unlock()
		return fmt.Errorf("app: invalid conflict resolution %q", string(res))
	}
}

// hostOf returns the lowercased hostname of rawURL with any port
// stripped, or "" when the URL cannot be parsed. Jobs with no host
// are not counted against any per-host limit.
func hostOf(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return ""
	}
	return strings.ToLower(u.Hostname())
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

// parkIfStoppingLocked parks a job whose Pause or Cancel arrived
// while it was waiting to start (before the transfer, or during the
// probe). Caller must hold s.mu. When it returns true the lock has
// been released and the caller must return from run() without
// starting the downloader; when it returns false the job is still
// probing and the caller keeps the lock.
func (s *Service) parkIfStoppingLocked(j *job) bool {
	if j.cancelling {
		if err := s.setStatusLocked(j, domain.StatusCancelled); err != nil {
			// Unreachable: probing -> cancelled is always allowed
			// for a dispatched job.
			s.mu.Unlock()
			return true
		}
		j.pausing = false
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		j.download.UpdatedAt = time.Now().UTC()
		s.mu.Unlock()
		return true
	}
	if j.pausing {
		// Pause arrived before the worker started its transfer:
		// park the job without ever calling the downloader.
		// probing -> paused is always allowed for a dispatched job.
		if err := s.setStatusLocked(j, domain.StatusPaused); err != nil {
			s.mu.Unlock()
			return true
		}
		j.pausing = false
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		j.download.UpdatedAt = time.Now().UTC()
		s.mu.Unlock()
		return true
	}
	return false
}

// applyProbeResultLocked records a successful probe's validators on
// the job and persists them through the repository (best effort: the
// downloading transition that follows persists the whole entity
// again, so a persist failure here is surfaced there instead of
// failing the probe). A weak ETag is never stored: only a strong
// ETag may serve as the If-Range validator. Caller must hold s.mu.
func (s *Service) applyProbeResultLocked(j *job, pr engine.ProbeResult) {
	j.download.ETag = engine.StrongETag(pr.ETag)
	j.download.LastModified = pr.LastModified
	j.download.SupportsRange = pr.SupportsRange
	if pr.TotalBytes >= 0 {
		j.download.TotalBytes = pr.TotalBytes
	}
	j.download.UpdatedAt = time.Now().UTC()
	_ = s.persistLocked(j)
}

// runViaManager executes one job through the download manager
// (Phase 4.4): probe, segmented workers with resume from the stored
// plan, or the single-stream fallback. It returns the transfer
// result and whether the segmented path was used — in that case the
// .part file is left for the caller to finalize (Phase 4.2); the
// single-stream fallback finalizes itself. expectedETag and
// expectedTotal are the validators recorded for the previous
// generation of this job, used to decide resume vs restart.
func (s *Service) runViaManager(ctx context.Context, mgr engine.ManagerRunner, id, url, destDir, filename, tempPath, expectedETag string, expectedTotal int64, mode domain.FinalizeMode) (domain.TransferResult, bool, error) {
	res, err := mgr.Run(ctx, engine.ManagerRequest{
		ID:                 id,
		URL:                url,
		DestinationDir:     destDir,
		Filename:           filename,
		TempPath:           tempPath,
		Mode:               mode,
		ExpectedETag:       expectedETag,
		ExpectedTotalBytes: expectedTotal,
		OnProgress: func(pu domain.ProgressUpdate) {
			s.onProgress(id, pu)
		},
	})
	out := domain.TransferResult{
		FinalPath:    res.FinalPath,
		Filename:     res.Filename,
		TempPath:     res.TempPath,
		WrittenBytes: res.WrittenBytes,
		TotalBytes:   res.TotalBytes,
	}
	return out, res.UsedSegments, err
}

// run executes one job. done is closed (via defer) only after the
// engine has returned and all file handles are closed.
func (s *Service) run(id string, ctx context.Context, done chan struct{}) {
	defer func() {
		s.mu.Lock()
		if jj, ok := s.jobs[id]; ok && jj.done == done {
			jj.running = false
		}
		// Report the worker exit to the scheduler while still
		// holding mu: the dispatch inside Finished runs startJob,
		// which assumes mu is held, so a Retry/Resume cannot slip
		// in between the slot release and the dispatch and leave
		// the job stuck queued. Finished ignores a stale
		// generation (done does not match the running entry), so
		// one release per run is exact even if a Retry started a
		// new generation meanwhile.
		s.sched.Finished(id, done)
		// A Retry/Resume may have slipped in between run()
		// recording the final status and this defer: it set the
		// job queued, but its Enqueue was refused because the
		// scheduler entry still existed. The entry is gone now,
		// so enqueue the job again — otherwise it would sit
		// queued forever with no worker. (The entry is gone, so
		// this cannot start a second worker for the old
		// generation; a Cancel/Pause of the new generation that
		// ran meanwhile has already moved it out of queued.)
		if jj, ok := s.jobs[id]; ok && jj.download.Status == domain.StatusQueued && !jj.running {
			s.sched.Enqueue(id, hostOf(jj.download.URL))
		}
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
	if s.parkIfStoppingLocked(j) {
		return
	}
	prober := s.prober
	manager := s.manager
	url := j.download.URL
	// Validators recorded for the previous generation (if any):
	// the manager compares them with its fresh probe to decide
	// resume vs restart from scratch (Phase 4.1, decision 2).
	// They are captured before this run's probe overwrites them.
	expectedETag := j.download.ETag
	expectedTotal := j.download.TotalBytes
	s.mu.Unlock()

	// Probe for Range support and validators while the job is still
	// probing. The lock is released here: Pause/Cancel must be able
	// to park the job while the probe is in flight, and the probe
	// runs on the job context, so it aborts as soon as they cancel
	// it. A probe failure never fails the job: the download falls
	// back to single-stream (plan §6), and the single-stream
	// attempt reports the real error (404, auth, network) if there
	// is one.
	var pr engine.ProbeResult
	probed := false
	if prober != nil {
		var perr error
		pr, perr = prober.Probe(ctx, url)
		probed = perr == nil
	}

	s.mu.Lock()
	j, ok = s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	if s.parkIfStoppingLocked(j) {
		return
	}
	if probed {
		s.applyProbeResultLocked(j, pr)
	}
	if err := s.setStatusLocked(j, domain.StatusDownloading); err != nil {
		// Unreachable: probing -> downloading is always allowed for
		// a dispatched job.
		s.mu.Unlock()
		return
	}
	j.download.DownloadedBytes = 0
	j.download.SpeedBytesPerSec = 0
	j.download.ETASeconds = -1
	j.download.UpdatedAt = time.Now().UTC()
	j.lastBytes = 0
	j.lastTime = time.Now()
	// url was captured before the probe above; the destination and
	// mode are captured here.
	destDir := j.download.DestinationDir
	filename := j.download.Filename
	tempPath := j.download.TempPath
	mode := j.finalizeMode
	s.mu.Unlock()
	s.notify()

	var res domain.TransferResult
	var usedSegments bool
	var err error
	if manager != nil {
		res, usedSegments, err = s.runViaManager(ctx, manager, id, url, destDir, filename, tempPath, expectedETag, expectedTotal, mode)
	} else {
		res, err = s.downloader.Download(ctx, domain.TransferRequest{
			ID:             id,
			URL:            url,
			DestinationDir: destDir,
			Filename:       filename,
			Mode:           mode,
			OnProgress: func(pu domain.ProgressUpdate) {
				s.onProgress(id, pu)
			},
		})
	}

	// A segmented download leaves the .part file for finalize
	// (Phase 4.2): verify its size and rename it here, so both
	// paths share the same completion rule. A finalize failure is
	// a download failure: the .part stays intact for retry or
	// conflict resolution, and the error flows through the usual
	// failure branches below (including file_conflict).
	if err == nil && usedSegments && res.FinalPath == "" {
		finalName := res.Filename
		if finalName == "" {
			finalName = filename
		}
		finalPath, resolvedName, ferr := engine.Finalize(res.TempPath, destDir, finalName, mode, res.TotalBytes)
		if ferr != nil {
			err = ferr
		} else {
			res.FinalPath = finalPath
			res.Filename = resolvedName
		}
	}

	s.mu.Lock()
	j, ok = s.jobs[id]
	if !ok {
		s.mu.Unlock()
		return
	}
	now := time.Now().UTC()
	j.download.UpdatedAt = now

	if err == nil {
		// downloading -> finalizing -> completed (ARCH §5). The
		// file is already finalized; record the outcome fields
		// BEFORE the finalizing transition, so the transition's
		// persist already carries FinalPath (Phase 4.2: a crash
		// between finalize and completed must not lose it). Then
		// notify while in finalizing, so observers never see
		// downloading jump straight to completed.
		j.download.FinalPath = res.FinalPath
		if res.Filename != "" {
			j.download.Filename = res.Filename
		}
		j.pausing = false
		j.download.TempPath = ""
		j.download.DownloadedBytes = res.WrittenBytes
		j.download.TotalBytes = res.TotalBytes
		j.download.ErrorCode = ""
		j.download.ErrorMessage = ""
		j.download.SuggestedFilename = ""
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		if serr := s.setStatusLocked(j, domain.StatusFinalizing); serr != nil {
			// Unreachable: no other goroutine may move a running
			// job out of downloading. Report failure rather than
			// a completion that was never recorded.
			j.download.ErrorMessage = domain.SanitizeErrorText(serr.Error())
			// downloading -> failed is always allowed here.
			_ = s.setStatusLocked(j, domain.StatusFailed)
			s.mu.Unlock()
			s.notify()
			return
		}
		s.mu.Unlock()
		s.notify()
		s.mu.Lock()
		j, ok = s.jobs[id]
		if !ok {
			s.mu.Unlock()
			return
		}
		// finalizing -> completed is always allowed here: no other
		// goroutine may transition a running job out of finalizing.
		_ = s.setStatusLocked(j, domain.StatusCompleted)
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

	// Pause (U3, Phase 7) takes precedence over the generic
	// cancellation branch below: Pause cancels the same context,
	// so ctx.Err() is non-nil here, but the job must land in
	// paused — with its .part preserved (TempPath was recorded
	// above) — not cancelled. A concurrent Cancel wins over Pause
	// (the branch below runs when cancelling is set).
	if j.pausing && !j.cancelling {
		j.pausing = false
		// downloading -> paused is always allowed here.
		_ = s.setStatusLocked(j, domain.StatusPaused)
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		s.mu.Unlock()
		s.notify()
		return
	}

	// Cancellation (via Cancel or Shutdown) takes precedence.
	if j.cancelling || ctx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		// downloading -> cancelled is always allowed here.
		_ = s.setStatusLocked(j, domain.StatusCancelled)
		j.pausing = false
		j.download.SpeedBytesPerSec = 0
		j.download.ETASeconds = -1
		s.mu.Unlock()
		s.notify()
		return
	}

	var fce *domain.FileConflictError
	if errors.As(err, &fce) {
		// downloading -> failed is always allowed here.
		_ = s.setStatusLocked(j, domain.StatusFailed)
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

	// downloading -> failed is always allowed here.
	_ = s.setStatusLocked(j, domain.StatusFailed)
	// .part lifecycle (A6, Phase 3) — single rule, owned here:
	// run() never deletes a .part file. A failure keeps the .part
	// when the error is transient (network, 5xx, rate limit) so
	// Retry can reuse it, and a terminal failure also keeps the
	// recorded path; the file is removed only when the user
	// explicitly chooses to delete it (Cancel with deleteTemp, or
	// ResolveConflict Cancel), via removeTempPart.
	// Backstop: if the engine failed after creating the temp file
	// but returned no TempPath (e.g. a Sync/Close failure), record
	// the conventional path so the file is never a silent orphan:
	// filesystem and size errors are the ones that imply a temp
	// file may exist on disk.
	code := errorCodeFor(err)
	if j.download.TempPath == "" && (code == domain.ErrorCodeFilesystem || code == domain.ErrorCodeSizeMismatch) && j.download.Filename != "" && j.download.DestinationDir != "" {
		j.download.TempPath = filepath.Join(j.download.DestinationDir, fmt.Sprintf(".%s.%s.part", j.download.Filename, id))
	}
	j.download.ErrorCode = code
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

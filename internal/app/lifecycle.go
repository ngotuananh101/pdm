package app

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// LoadFromRepository restores every persisted download into the
// service, applying restart recovery (see installRestoredLocked), and
// dispatches jobs that end up queued. It must be called once, before
// any AddDownload, and only when a repository is attached; with no
// repo it is a no-op returning nil, and a second call is a no-op.
// Terminal jobs are kept as-is so the UI can still show them. A
// per-job recovery failure is recorded and the load continues with
// the remaining jobs; the combined error is returned.
func (s *Service) LoadFromRepository(ctx context.Context) error {
	if s.repo == nil {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	downloads, err := s.repo.ListAll(ctx)
	if err != nil {
		return fmt.Errorf("app: load downloads: %w", err)
	}
	s.mu.Lock()
	if s.restored {
		s.mu.Unlock()
		return nil
	}
	if len(s.jobs) > 0 {
		s.mu.Unlock()
		return fmt.Errorf("app: LoadFromRepository called after jobs already exist")
	}
	s.restored = true
	var errs []error
	var toDispatch []string
	for _, d := range downloads {
		dispatch, rerr := s.installRestoredLocked(d)
		if rerr != nil {
			errs = append(errs, rerr)
			continue
		}
		if dispatch {
			toDispatch = append(toDispatch, d.ID)
		}
	}
	// Enqueue while still holding mu: the dispatch inside runs
	// startJob, which assumes mu is held.
	for _, id := range toDispatch {
		j := s.jobs[id]
		if j == nil {
			continue
		}
		s.sched.Enqueue(id, hostOf(j.download.URL))
	}
	s.mu.Unlock()
	s.notify()
	return errors.Join(errs...)
}

// installRestoredLocked installs one persisted download into RAM,
// applying restart recovery. Caller must hold s.mu. It returns true
// when the job ends up queued and should be dispatched. The job is
// always installed; a persistence failure is returned as an error
// (the next restart re-runs recovery from the stale row).
func (s *Service) installRestoredLocked(d *domain.Download) (bool, error) {
	if d == nil {
		return false, fmt.Errorf("app: restore nil download")
	}
	mode, _ := effectiveFinalizeMode(domain.ResolutionUnset, s.cfg.FileConflictPolicy)
	j := &job{
		download:     *d,
		finalizeMode: mode,
	}
	// Runtime-only fields are never persisted; reset them to their
	// initial values for the new process.
	j.download.SpeedBytesPerSec = 0
	j.download.ETASeconds = -1
	s.jobs[d.ID] = j
	s.order = append(s.order, d.ID)

	switch d.Status {
	case domain.StatusCompleted, domain.StatusCancelled, domain.StatusFailed, domain.StatusPaused:
		// Terminal and paused jobs are kept exactly as stored so
		// the UI can still show them. No dispatch.
		return false, nil
	case domain.StatusQueued:
		return true, nil
	case domain.StatusProbing, domain.StatusDownloading:
		return s.recoverInterruptedLocked(j)
	case domain.StatusFinalizing:
		return s.reconcileFinalizingLocked(j)
	default:
		// Unknown status (e.g. written by a newer version): keep
		// it visible but never dispatch it.
		return false, nil
	}
}

// recoverInterruptedLocked re-queues a job that was probing or
// downloading when the previous process ended (the confirmed Stage 2
// decision), or fails it when its .part file is gone although the
// persisted metadata says bytes were already downloaded. Caller must
// hold s.mu. The job restarts from scratch on dispatch (Stage 1 has
// no Range resume); the .part file, if present, is truncated by the
// engine. Nothing is deleted here.
//
// The status is assigned directly instead of going through
// setStatusLocked: probing/downloading -> queued is not a live
// transition in the state machine (ARCH §5), because no worker ever
// ran in this process — recovery re-states where the job stands, it
// does not move a running job.
func (s *Service) recoverInterruptedLocked(j *job) (bool, error) {
	d := &j.download
	if d.DownloadedBytes > 0 && !restoredPartExists(d) {
		d.Status = domain.StatusFailed
		d.ErrorCode = domain.ErrorCodeFilesystem
		d.ErrorMessage = "download interrupted: temporary file is missing; retry to download again"
		d.SpeedBytesPerSec = 0
		d.ETASeconds = -1
		d.UpdatedAt = time.Now().UTC()
		if err := s.persistLocked(j); err != nil {
			return false, fmt.Errorf("app: persist recovered job %q: %w", d.ID, err)
		}
		return false, nil
	}
	d.Status = domain.StatusQueued
	d.DownloadedBytes = 0
	d.SpeedBytesPerSec = 0
	d.ETASeconds = -1
	d.UpdatedAt = time.Now().UTC()
	if err := s.persistLocked(j); err != nil {
		return false, fmt.Errorf("app: persist recovered job %q: %w", d.ID, err)
	}
	return true, nil
}

// reconcileFinalizingLocked reconciles a job that was finalizing when
// the previous process ended (ARCH §8). In Stage 2 the outcome is
// always failed: the durable metadata cannot verify the finalized
// file — DownloadedBytes/TotalBytes only reach the DB at status
// transitions, and at the finalizing transition they are still 0/-1,
// so neither size nor validator can be checked, and ARCH §5 forbids
// marking a job completed merely because a file exists. The .part
// and destination are still inspected so the error message says
// whether the rename may have happened; nothing is deleted or
// overwritten, and Retry re-downloads from scratch.
func (s *Service) reconcileFinalizingLocked(j *job) (bool, error) {
	d := &j.download
	partGone := !restoredPartExists(d)
	finalExists := d.FinalPath != "" && fileExists(d.FinalPath)
	d.Status = domain.StatusFailed
	d.ErrorCode = domain.ErrorCodeFilesystem
	switch {
	case !partGone:
		d.ErrorMessage = "finalize did not complete: temporary file is still present; retry to download again"
	case finalExists:
		d.ErrorMessage = "could not verify the finalized file after restart: a file exists at the destination, check it before retrying"
	default:
		d.ErrorMessage = "finalize did not complete and no finished file was found; retry to download again"
	}
	d.SpeedBytesPerSec = 0
	d.ETASeconds = -1
	d.UpdatedAt = time.Now().UTC()
	if err := s.persistLocked(j); err != nil {
		return false, fmt.Errorf("app: persist recovered job %q: %w", d.ID, err)
	}
	return false, nil
}

// fileExists reports whether path names an existing file.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// restoredTempPath returns the .part path for a restored download:
// the recorded TempPath when present, else the conventional engine
// path (".<filename>.<id>.part" in the destination dir, see
// engine/single.go). It returns "" when neither can be derived.
func restoredTempPath(d *domain.Download) string {
	if d.TempPath != "" {
		return d.TempPath
	}
	if d.Filename != "" && d.DestinationDir != "" {
		return filepath.Join(d.DestinationDir, fmt.Sprintf(".%s.%s.part", d.Filename, d.ID))
	}
	return ""
}

// restoredPartExists reports whether the .part file for a restored
// download is on disk. A path that cannot be derived counts as
// missing: recovery must not assume a file it cannot locate.
func restoredPartExists(d *domain.Download) bool {
	p := restoredTempPath(d)
	if p == "" {
		return false
	}
	return fileExists(p)
}

// Shutdown marks the service closed, cancels all active job
// contexts, and waits for every active worker goroutine to exit.
// It returns ctx.Err() if the context deadline expires first.
// Shutdown is idempotent: repeated calls are safe.
//
// Shutdown order (ARCH §10): stop accepting jobs, cancel workers,
// wait with the caller's deadline, checkpoint the synced offsets,
// close the database, exit. If the deadline expires or the
// checkpoint fails, the error is returned instead of claiming a
// clean shutdown; the next startup falls back to the previous
// checkpoint. A timed-out Shutdown does not close the database, so a
// later Shutdown can still persist the final state.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.shutdownComplete {
		s.mu.Unlock()
		return nil
	}
	if !s.closed {
		s.closed = true
		for _, j := range s.jobs {
			if j.running && j.cancel != nil {
				j.cancel()
			}
		}
	}
	// Stop the scheduler as well: no further job may dispatch,
	// and its running entries are cancelled (cancelling twice —
	// here via j.cancel and there via the entry — is harmless).
	s.sched.Close()
	// Collect done channels of running jobs while holding the lock.
	var dones []chan struct{}
	for _, j := range s.jobs {
		if j.done != nil {
			dones = append(dones, j.done)
		}
		// A ResolveConflict in flight is not a running worker
		// (running == false while it finalizes), but its rename
		// must finish before the process exits: wait for its done
		// channel in the same deadline as the workers (A3).
		if j.resolveDone != nil {
			dones = append(dones, j.resolveDone)
		}
	}
	s.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	// Wait for all workers, or for the context to expire.
	for _, d := range dones {
		select {
		case <-d:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	// Checkpoint: persist the full current state of every job —
	// bytes, paths and error fields that changed in RAM since the
	// last status transition — so the next startup recovers from
	// this checkpoint. On failure, still close the database
	// (best-effort cleanup) but report the checkpoint error.
	if err := s.checkpointState(); err != nil {
		_ = s.closeRepoDB()
		return err
	}
	if err := s.closeRepoDB(); err != nil {
		return fmt.Errorf("app: close database: %w", err)
	}
	s.mu.Lock()
	s.shutdownComplete = true
	s.mu.Unlock()
	return nil
}

// checkpointState persists the full current state of every job to
// the repository (best-effort per job; the first error is reported).
// A nil repo is a no-op.
func (s *Service) checkpointState() error {
	if s.repo == nil {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var firstErr error
	for _, id := range s.order {
		j := s.jobs[id]
		if j == nil {
			continue
		}
		if err := s.persistLocked(j); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("app: checkpoint job %q: %w", id, err)
		}
	}
	return firstErr
}

// closeRepoDB closes the database behind the repository, when the
// repository owns a closable handle (the SQLite repository closes
// its shared *sql.DB; the settings repository shares the same
// handle, which is fine — the app is exiting). A nil repo, or a repo
// without a Close method, is a no-op.
func (s *Service) closeRepoDB() error {
	if s.repo == nil {
		return nil
	}
	closer, ok := s.repo.(io.Closer)
	if !ok {
		return nil
	}
	return closer.Close()
}

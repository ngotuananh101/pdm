// Download manager (Stage 3, Phase 3.3).
//
// The manager runs one download job standalone (it is NOT wired into
// the service/scheduler yet — that is Phase 4.4):
//
//  1. Probe the URL for Range support and validators.
//  2. When Range is verified and the size is known, split the file
//     into segments, create the .part file, persist the segment plan
//     (and its validators, via the plan) to storage BEFORE any worker
//     starts, then run up to SegmentsPerDownload workers; when a
//     segment finishes, its worker takes the next pending segment.
//  3. Otherwise fall back to the single-stream downloader.
//
// Worker errors are retried a bounded number of times (retry.go). A
// 200 or 416 from a worker means the resource changed mid-download
// (decisions 2 and 3): all workers stop, the URL is re-probed, and
// the job restarts from scratch with the new probe — or falls back
// to single-stream when Range is gone. The stale .part file and the
// stale plan are removed before any restart, so bytes from two
// resource versions are never mixed.
//
// The manager does NOT finalize: the segmented path leaves the
// .part file in place for Phase 4.2 (finalize) to verify and rename.
// The single-stream fallback still finalizes immediately, as
// SingleDownloader does today; Phase 4 unifies the two paths at the
// service layer.
//
// Resume (Phase 4.1): when the segment repository already holds a
// plan for this download ID that covers the probed size and the
// fresh probe agrees with the caller's expected validators, the run
// resumes from the plan: complete segments are never re-requested,
// a segment left running by a previous session continues from its
// committed bytes, and the .part file is opened without O_TRUNC. A
// plan that does not cover the size, or whose resource changed
// (validator/total mismatch), is discarded with its .part file and
// the job restarts from scratch (decision 2).
//
// Segments per download: config.SegmentsPerDownload when positive,
// otherwise DefaultSegmentsPerDownload (4). The setting is already
// present in config (default 4, validated 1-16); Phase 5 wires it
// into the Settings UI.
package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/platform"
)

// ManagerRunner is the interface the app service uses to run a
// download through the manager (Phase 4.4). Manager satisfies it.
type ManagerRunner interface {
	Run(ctx context.Context, req ManagerRequest) (ManagerResult, error)
}

var _ ManagerRunner = (*Manager)(nil)

// Manager runs one download job: probe, segmented workers, or the
// single-stream fallback.
type Manager struct {
	cfg     config.Config
	client  *http.Client
	prober  RangeProber
	segRepo domain.SegmentRepository // may be nil: no plan persistence
	single  *SingleDownloader

	// sleep waits out a retry backoff. It defaults to time.Sleep;
	// tests override it to observe waits without waiting in real time.
	sleep func(time.Duration)
}

// NewManager creates a manager using the given config, client, prober
// and segment repository. A nil client gets the production client
// from NewHTTPClient; a nil prober gets a NewProber on that client;
// a nil segRepo disables plan persistence (in-memory run).
func NewManager(cfg config.Config, client *http.Client, prober RangeProber, segRepo domain.SegmentRepository) *Manager {
	if client == nil {
		client = NewHTTPClient(cfg)
	}
	if prober == nil {
		prober = NewProber(cfg, client)
	}
	return &Manager{
		cfg:     cfg,
		client:  client,
		prober:  prober,
		segRepo: segRepo,
		single:  NewSingleDownloader(cfg, client),
	}
}

// ManagerRequest describes one download job for the manager.
type ManagerRequest struct {
	ID             string
	URL            string
	DestinationDir string
	Filename       string
	TempPath       string
	Mode           domain.FinalizeMode
	OnProgress     func(domain.ProgressUpdate)
	// ExpectedETag / ExpectedTotalBytes are the validators the
	// caller recorded for a previous generation of this job (e.g.
	// before a Pause). When set, a stored segment plan is only
	// resumed when the fresh probe agrees with them; a mismatch
	// means the resource changed and the plan is discarded
	// (decision 2). Empty/<=0 means "no prior information": any
	// stored plan that covers the probed size may be resumed.
	ExpectedETag       string
	ExpectedTotalBytes int64
}

// ManagerResult is the outcome of a manager run. FinalPath is set
// only by the single-stream fallback (which finalizes immediately);
// the segmented path leaves the .part file for Phase 4 finalize.
type ManagerResult struct {
	FinalPath    string
	Filename     string
	TempPath     string
	WrittenBytes int64
	TotalBytes   int64
	UsedSegments bool
}

// Run executes one download job.
func (m *Manager) Run(ctx context.Context, req ManagerRequest) (ManagerResult, error) {
	if err := ctx.Err(); err != nil {
		return ManagerResult{}, err
	}
	if req.ID == "" {
		return ManagerResult{}, codeError(domain.ErrorCodeValidation, "manager: download ID must not be empty", nil)
	}
	parsedURL, err := platform.ValidateHTTPURL(req.URL)
	if err != nil {
		return ManagerResult{}, err
	}
	destDir, err := platform.EnsureDestinationDir(req.DestinationDir)
	if err != nil {
		return ManagerResult{}, err
	}
	filename := req.Filename
	if filename == "" {
		filename = platform.DeriveFilenameFromURL(parsedURL)
	} else if verr := platform.ValidateFilename(filename); verr != nil {
		return ManagerResult{}, verr
	}
	tempPath := req.TempPath
	if tempPath == "" {
		tempPath = filepath.Join(destDir, fmt.Sprintf(".%s.%s.part", filename, req.ID))
	}
	mode := req.Mode
	if mode == "" {
		mode = domain.FinalizeNoClobber
	}
	req.Mode = mode

	probe, err := m.prober.Probe(ctx, parsedURL.String())
	if err != nil {
		if ctx.Err() != nil {
			return ManagerResult{}, ctx.Err()
		}
		// Probe failure: fall back to single-stream (plan §6 risk:
		// a server that rejects the ranged probe may still serve a
		// plain GET; the single-stream attempt surfaces the real
		// error when it cannot).
		return m.runSingle(ctx, req, destDir, filename, tempPath)
	}
	if !probe.SupportsRange || probe.TotalBytes <= 0 {
		return m.runSingle(ctx, req, destDir, filename, tempPath)
	}
	// Resume (Phase 4.1): reuse a stored plan when it covers the
	// probed size and the fresh probe agrees with the validators
	// the caller recorded for the previous generation. A stale
	// plan is discarded with its .part file, never resumed.
	var resume []domain.Segment
	if m.segRepo != nil {
		if plan, err := m.segRepo.GetByDownload(ctx, req.ID); err == nil && len(plan) > 0 {
			if planCovers(plan, probe.TotalBytes) && resumeValidatorMatches(req, probe) {
				resume = make([]domain.Segment, len(plan))
				for i, s := range plan {
					resume[i] = *s
				}
			} else {
				_ = m.segRepo.DeleteByDownload(ctx, req.ID)
				_ = os.Remove(tempPath)
			}
		}
	}
	return m.runSegmented(ctx, req, parsedURL, destDir, filename, tempPath, probe, false, resume)
}

// planCovers reports whether plan covers exactly [0, totalBytes-1]
// with contiguous, well-formed segments in index order, each with a
// committed count inside its range and a known status.
func planCovers(plan []*domain.Segment, totalBytes int64) bool {
	if len(plan) == 0 || totalBytes <= 0 {
		return false
	}
	var expect int64
	for i, s := range plan {
		if s == nil || s.Index != i || s.StartOffset != expect {
			return false
		}
		if s.EndOffset < s.StartOffset || s.CommittedBytes < 0 || s.CommittedBytes > s.Size() {
			return false
		}
		switch s.Status {
		case domain.SegmentPending, domain.SegmentRunning, domain.SegmentComplete:
		default:
			return false
		}
		expect = s.EndOffset + 1
	}
	return expect == totalBytes
}

// resumeValidatorMatches reports whether the fresh probe agrees with
// the validators the caller recorded for a previous generation. An
// empty expected ETag (or a non-positive expected total) means the
// caller recorded nothing: any covering plan may be resumed.
func resumeValidatorMatches(req ManagerRequest, probe ProbeResult) bool {
	if req.ExpectedTotalBytes > 0 && req.ExpectedTotalBytes != probe.TotalBytes {
		return false
	}
	if req.ExpectedETag != "" && req.ExpectedETag != probe.ETag {
		return false
	}
	return true
}

// writtenSoFar sums the in-memory committed bytes of segments: the
// bytes this run (and any resumed previous run) has written, used to
// report progress on error returns.
func writtenSoFar(segments []domain.Segment) int64 {
	var total int64
	for i := range segments {
		total += segments[i].CommittedBytes
	}
	return total
}

// runSingle delegates to the single-stream downloader. A stored
// segment plan does not describe a single-stream transfer, so it is
// discarded first: a later retry must not resume from a plan whose
// bytes came from a different transfer shape.
func (m *Manager) runSingle(ctx context.Context, req ManagerRequest, destDir, filename, tempPath string) (ManagerResult, error) {
	if m.segRepo != nil {
		_ = m.segRepo.DeleteByDownload(ctx, req.ID)
	}
	res, err := m.single.Download(ctx, domain.TransferRequest{
		ID:             req.ID,
		URL:            req.URL,
		DestinationDir: destDir,
		Filename:       filename,
		TempPath:       tempPath,
		Mode:           req.Mode,
		OnProgress:     req.OnProgress,
	})
	if err != nil {
		return ManagerResult{TempPath: tempPath, Filename: filename}, err
	}
	return ManagerResult{
		FinalPath:    res.FinalPath,
		Filename:     res.Filename,
		TempPath:     res.TempPath,
		WrittenBytes: res.WrittenBytes,
		TotalBytes:   res.TotalBytes,
		UsedSegments: false,
	}, nil
}

// runSegmented downloads probe.TotalBytes with segment workers.
// restarted is true when this is the single allowed restart after a
// validator/size change; a second 200/416 fails the job instead of
// looping forever. resume, when non-empty, is a stored plan to
// resume from (Phase 4.1): its committed bytes are kept and only
// the remaining ranges are fetched.
func (m *Manager) runSegmented(ctx context.Context, req ManagerRequest, parsedURL *url.URL, destDir, filename, tempPath string, probe ProbeResult, restarted bool, resume []domain.Segment) (ManagerResult, error) {
	maxSegs := m.cfg.SegmentsPerDownload
	if maxSegs <= 0 {
		maxSegs = config.DefaultSegmentsPerDownload
	}
	minSeg := m.cfg.MinSegmentSize
	if minSeg <= 0 {
		minSeg = config.DefaultMinSegmentSize
	}

	// Refuse a symlink .part path before opening (same rule as single.go).
	if fi, err := os.Lstat(tempPath); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		return ManagerResult{}, fmt.Errorf("%w: %s", domain.ErrSymlinkTarget, tempPath)
	}

	resuming := len(resume) > 0
	var segments []domain.Segment
	if resuming {
		segments = resume
		for i := range segments {
			// A segment left running by a previous session is
			// pending again from its committed bytes; the worker
			// re-marks it running when it starts.
			if segments[i].Status == domain.SegmentRunning {
				segments[i].Status = domain.SegmentPending
			}
		}
		// Resume needs the .part file the plan describes: without
		// it (or with fewer bytes than the plan claims committed)
		// there is nothing safe to resume from, so drop the plan
		// and restart from scratch below.
		if fi, err := os.Stat(tempPath); err != nil || fi.Size() < writtenSoFar(segments) {
			if m.segRepo != nil {
				_ = m.segRepo.DeleteByDownload(ctx, req.ID)
			}
			_ = os.Remove(tempPath)
			resuming = false
		}
	}
	var f *os.File
	var err error
	if resuming {
		// Resume: open WITHOUT O_TRUNC, so committed bytes stay.
		f, err = os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE, 0o600)
	} else {
		segments = domain.SplitSegments(probe.TotalBytes, maxSegs, minSeg)
		for i := range segments {
			segments[i].DownloadID = req.ID
		}
		// Fresh run: truncate any stale .part from an earlier attempt.
		f, err = os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	}
	if err != nil {
		return ManagerResult{}, codeError(domain.ErrorCodeFilesystem, "manager: open temp file: "+err.Error(), err)
	}
	defer f.Close()

	if !resuming {
		// Persist the plan (segments + validators) BEFORE any worker
		// starts, so a crash mid-download can resume from storage.
		// Delete any stale plan first: a discarded resume plan (or
		// a retry after Cancel with deleteTemp) would otherwise
		// collide with the fresh insert.
		if m.segRepo != nil {
			_ = m.segRepo.DeleteByDownload(ctx, req.ID)
			if err := m.segRepo.InsertPlan(ctx, req.ID, segments); err != nil {
				return ManagerResult{}, codeError(domain.ErrorCodeFilesystem, "manager: store segment plan: "+err.Error(), err)
			}
		}
	}

	checkpointer := NewCheckpointer(f, m.segRepo, req.ID)

	// Progress reporting: aggregate committed bytes across segments,
	// throttled like the single-stream engine. On resume the counter
	// starts at the already-committed bytes, so progress reports
	// the total downloaded, not just this run's remainder.
	progress := &segmentProgress{total: probe.TotalBytes, filename: filename, onProgress: req.OnProgress, interval: m.progressInterval(), done: writtenSoFar(segments)}

	jobCtx, jobCancel := context.WithCancel(ctx)
	defer jobCancel()

	// Periodic checkpointing; a checkpoint error stops the job.
	cpCtx, cpCancel := context.WithCancel(jobCtx)
	defer cpCancel()
	var firstErr error
	var errMu sync.Mutex
	setErr := func(err error) {
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}
	go func() {
		if err := checkpointer.Run(cpCtx, m.checkpointInterval()); err != nil && !errors.Is(err, context.Canceled) {
			setErr(err)
			jobCancel()
		}
	}()

	// Worker pool: at most maxSegs workers; each takes the next
	// pending segment when its current one finishes.
	workers := len(segments)
	if workers > maxSegs {
		workers = maxSegs
	}
	jobs := make(chan int)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				if jobCtx.Err() != nil {
					return
				}
				if err := m.downloadSegmentWithRetry(jobCtx, f, checkpointer, progress, &segments[idx], req.URL, probe); err != nil {
					setErr(err)
					jobCancel()
					return
				}
			}
		}()
	}
feed:
	for i := range segments {
		if segments[i].CommittedBytes >= segments[i].Size() {
			// Already complete (resumed plan): never re-requested.
			continue
		}
		select {
		case jobs <- i:
		case <-jobCtx.Done():
			break feed
		}
	}
	close(jobs)
	wg.Wait()
	cpCancel()

	if firstErr != nil {
		if ctx.Err() != nil {
			return ManagerResult{TempPath: tempPath, Filename: filename, TotalBytes: probe.TotalBytes, WrittenBytes: writtenSoFar(segments)}, ctx.Err()
		}
		if (errors.Is(firstErr, ErrRangeIgnored) || errors.Is(firstErr, ErrRangeNotSatisfiable)) && !restarted {
			// Close the .part handle first: the restart removes
			// the file, which fails on Windows while it is open.
			// (The deferred Close below then no-ops.)
			_ = f.Close()
			return m.restartAfterValidatorChange(ctx, req, parsedURL, destDir, filename, tempPath)
		}
		return ManagerResult{TempPath: tempPath, Filename: filename, TotalBytes: probe.TotalBytes, WrittenBytes: writtenSoFar(segments)}, firstErr
	}

	// Final checkpoint: sync the file and publish every remaining
	// dirty offset before reporting success.
	if err := checkpointer.Checkpoint(ctx); err != nil {
		return ManagerResult{TempPath: tempPath, Filename: filename, TotalBytes: probe.TotalBytes, WrittenBytes: writtenSoFar(segments)}, err
	}
	written := writtenSoFar(segments)
	if written != probe.TotalBytes {
		return ManagerResult{TempPath: tempPath, Filename: filename, TotalBytes: probe.TotalBytes, WrittenBytes: written},
			codeError(domain.ErrorCodeSizeMismatch,
				fmt.Sprintf("manager: size mismatch: wrote %d, expected %d", written, probe.TotalBytes),
				domain.ErrSizeMismatch)
	}
	progress.emit(written, true)
	return ManagerResult{
		Filename:     filename,
		TempPath:     tempPath,
		WrittenBytes: written,
		TotalBytes:   probe.TotalBytes,
		UsedSegments: true,
	}, nil
}

// restartAfterValidatorChange handles a 200/416 mid-download: stop,
// re-probe, and restart from scratch with the new probe — or fall
// back to single-stream when Range is gone. The stale .part file and
// plan are removed first, so bytes from two resource versions are
// never mixed (decisions 2 and 3). The caller closes the .part file
// handle before calling this, so the remove works on Windows too.
func (m *Manager) restartAfterValidatorChange(ctx context.Context, req ManagerRequest, parsedURL *url.URL, destDir, filename, tempPath string) (ManagerResult, error) {
	newProbe, err := m.prober.Probe(ctx, parsedURL.String())
	if err != nil {
		if ctx.Err() != nil {
			return ManagerResult{}, ctx.Err()
		}
		return ManagerResult{TempPath: tempPath, Filename: filename}, err
	}
	// The old .part and plan belong to the old resource version.
	_ = os.Remove(tempPath)
	if m.segRepo != nil {
		_ = m.segRepo.DeleteByDownload(ctx, req.ID)
	}
	if !newProbe.SupportsRange || newProbe.TotalBytes <= 0 {
		return m.runSingle(ctx, req, destDir, filename, tempPath)
	}
	return m.runSegmented(ctx, req, parsedURL, destDir, filename, tempPath, newProbe, true, nil)
}

// downloadSegmentWithRetry downloads one segment with bounded
// retries for transient failures. A 200/416 is terminal for the
// segment (the manager restarts the whole job); other terminal
// errors fail immediately.
func (m *Manager) downloadSegmentWithRetry(ctx context.Context, f *os.File, cp *Checkpointer, progress *segmentProgress, seg *domain.Segment, url string, probe ProbeResult) error {
	maxAttempts := m.cfg.MaxRetries
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	worker := NewSegmentWorker(m.cfg, m.client)
	if m.segRepo != nil {
		if err := m.segRepo.MarkStatus(ctx, seg.DownloadID, seg.Index, domain.SegmentRunning); err != nil {
			return err
		}
	}
	lastReported := seg.CommittedBytes
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		task := SegmentTask{
			Segment:      *seg,
			URL:          url,
			TotalBytes:   probe.TotalBytes,
			ETag:         probe.ETag,
			LastModified: probe.LastModified,
		}
		committed, err := worker.DownloadSegment(ctx, f, task, func(c int64) {
			cp.NoteWritten(seg.Index, c)
			progress.add(c - lastReported)
			lastReported = c
		})
		seg.CommittedBytes = committed
		if err == nil {
			if m.segRepo != nil {
				if err := m.segRepo.MarkStatus(ctx, seg.DownloadID, seg.Index, domain.SegmentComplete); err != nil {
					return err
				}
			}
			// Persist this segment's final bytes durably now, so
			// a crash cannot lose a finished segment.
			if err := cp.Checkpoint(ctx); err != nil {
				return err
			}
			return nil
		}
		if errors.Is(err, ErrRangeIgnored) || errors.Is(err, ErrRangeNotSatisfiable) {
			return err
		}
		if !isRetryableError(err) || attempt == maxAttempts-1 {
			return err
		}
		if err := m.sleepCtx(ctx, retryDelay(attempt, err)); err != nil {
			return err
		}
	}
	return errors.New("manager: unreachable retry state")
}

// sleepCtx waits dur (via m.sleep when set, else time.Sleep) unless
// ctx is cancelled first. Mirrors SingleDownloader.sleepCtx.
func (m *Manager) sleepCtx(ctx context.Context, dur time.Duration) error {
	if dur <= 0 {
		return ctx.Err()
	}
	if m.sleep != nil {
		m.sleep(dur)
		return ctx.Err()
	}
	t := time.NewTimer(dur)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

func (m *Manager) checkpointInterval() time.Duration {
	if m.cfg.CheckpointInterval <= 0 {
		return config.DefaultCheckpointInterval
	}
	return m.cfg.CheckpointInterval
}

func (m *Manager) progressInterval() time.Duration {
	if m.cfg.ProgressInterval <= 0 {
		return config.DefaultProgressInterval
	}
	return m.cfg.ProgressInterval
}

// segmentProgress aggregates committed bytes across segments and
// emits throttled progress updates.
type segmentProgress struct {
	total      int64
	filename   string
	onProgress func(domain.ProgressUpdate)
	interval   time.Duration

	mu       sync.Mutex
	done     int64
	lastEmit time.Time
}

func (p *segmentProgress) add(delta int64) {
	if p.onProgress == nil {
		return
	}
	p.mu.Lock()
	p.done += delta
	done := p.done
	now := time.Now()
	throttled := now.Sub(p.lastEmit) < p.interval && done < p.total
	p.mu.Unlock()
	if throttled {
		return
	}
	p.emit(done, false)
}

func (p *segmentProgress) emit(done int64, force bool) {
	if p.onProgress == nil {
		return
	}
	p.mu.Lock()
	if !force && time.Since(p.lastEmit) < p.interval && done < p.total {
		p.mu.Unlock()
		return
	}
	p.lastEmit = time.Now()
	p.mu.Unlock()
	p.onProgress(domain.ProgressUpdate{
		DownloadedBytes:  done,
		TotalBytes:       p.total,
		ResolvedFilename: p.filename,
	})
}

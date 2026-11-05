package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/platform"
)

// SingleDownloader implements domain.StreamDownloader for single-stream GETs.
type SingleDownloader struct {
	cfg    config.Config
	client *http.Client

	// sleep waits out a retry backoff. It defaults to time.Sleep;
	// tests override it to observe waits without waiting in real time.
	sleep func(time.Duration)
}

// NewSingleDownloader creates a downloader using the given config and client.
func NewSingleDownloader(cfg config.Config, client *http.Client) *SingleDownloader {
	if client == nil {
		client = NewHTTPClient(cfg)
	}
	return &SingleDownloader{cfg: cfg, client: client}
}

// Download performs a single-stream GET, writes to a temp file, and
// finalizes it according to req.Mode, with bounded retries for
// transient failures (A4, Phase 6). At most cfg.MaxRetries attempts
// are made in total (at least one); only network errors, 429/503 and
// other 5xx are retried, with exponential backoff plus jitter, and a
// Retry-After header is honored as a minimum wait. Auth failures,
// 404s, size mismatches, filesystem errors and cancellations fail
// immediately. Each attempt restarts from scratch (Stage 1 has no
// resume) and re-opens the .part file with O_TRUNC.
func (d *SingleDownloader) Download(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	maxAttempts := d.cfg.MaxRetries
	if maxAttempts < 1 {
		maxAttempts = 1
	}
	var lastRes domain.TransferResult
	var lastErr error
	for attempt := 0; attempt < maxAttempts; attempt++ {
		res, err := d.downloadOnce(ctx, req)
		if err == nil {
			return res, nil
		}
		lastRes, lastErr = res, err
		if !isRetryableError(err) || attempt == maxAttempts-1 {
			return res, err
		}
		if err := d.sleepCtx(ctx, retryDelay(attempt, err)); err != nil {
			return lastRes, err
		}
	}
	return lastRes, lastErr
}

// sleepCtx waits dur (via d.sleep when set, else time.Sleep) unless
// ctx is cancelled first.
func (d *SingleDownloader) sleepCtx(ctx context.Context, dur time.Duration) error {
	if dur <= 0 {
		return ctx.Err()
	}
	if d.sleep != nil {
		d.sleep(dur)
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

// downloadOnce performs a single attempt of Download: one GET,
// written to a temp file and finalized according to req.Mode.
//
// Stall handling: a stall-aware context (context.WithCancelCause) is
// created and a resettable timer (time.AfterFunc) fires after
// cfg.StallTimeout of no progress, canceling with a CodedError cause.
// The timer is reset on every successful read > 0 and always Stop()ed
// via defer so no timers or goroutines leak.
func (d *SingleDownloader) downloadOnce(ctx context.Context, req domain.TransferRequest) (domain.TransferResult, error) {
	// Validate URL.
	parsedURL, err := platform.ValidateHTTPURL(req.URL)
	if err != nil {
		return domain.TransferResult{}, err
	}

	// Validate destination directory.
	destDir, err := platform.EnsureDestinationDir(req.DestinationDir)
	if err != nil {
		return domain.TransferResult{}, err
	}

	// Build GET request with context.
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedURL.String(), nil)
	if err != nil {
		return domain.TransferResult{}, codeError(domain.ErrorCodeNetwork, "build request: "+err.Error(), err)
	}
	httpReq.Header.Set("Accept-Encoding", "identity")
	httpReq.Header.Set("User-Agent", "PDM/0.1")

	// Stall-aware context.
	stallTimeout := d.cfg.StallTimeout
	if stallTimeout <= 0 {
		stallTimeout = config.DefaultStallTimeout
	}
	stallErr := codeError(domain.ErrorCodeNetwork, "download stalled: no progress after "+stallTimeout.String(), nil)
	reqCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	stallTimer := time.AfterFunc(stallTimeout, func() {
		cancel(stallErr)
	})
	defer stallTimer.Stop()

	// Attach the stall-aware context to the request.
	httpReq = httpReq.WithContext(reqCtx)

	resp, err := d.client.Do(httpReq)
	if err != nil {
		// If the stall timer fired, prefer the stall error.
		if cause := context.Cause(reqCtx); cause == stallErr {
			return domain.TransferResult{}, cause
		}
		// If the parent context was cancelled, surface that.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return domain.TransferResult{}, ctxErr
		}
		return domain.TransferResult{}, codeError(domain.ErrorCodeNetwork, "execute request: "+err.Error(), err)
	}
	defer resp.Body.Close()

	// Check HTTP status: only 200 OK is accepted.
	if resp.StatusCode != http.StatusOK {
		code := domain.ErrorCodeHTTPStatus
		switch resp.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			code = domain.ErrorCodeHTTPAuth
		case http.StatusNotFound, http.StatusGone:
			code = domain.ErrorCodeHTTPNotFound
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			code = domain.ErrorCodeHTTPRateLimit
		default:
			if resp.StatusCode >= 500 {
				code = domain.ErrorCodeHTTPServer
			}
		}
		msg := fmt.Sprintf("HTTP %d %s", resp.StatusCode, http.StatusText(resp.StatusCode))
		ce := codeError(code, msg, nil)
		// A Retry-After on a retryable status is honored as a
		// minimum wait before the next attempt (A4).
		ce.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
		return domain.TransferResult{}, ce
	}

	// Resolve effective filename.
	var filename string
	if req.Filename != "" {
		if verr := platform.ValidateFilename(req.Filename); verr != nil {
			return domain.TransferResult{}, verr
		}
		filename = req.Filename
	} else if cd := platform.ParseContentDispositionFilename(resp.Header.Get("Content-Disposition")); cd != "" {
		// ParseContentDispositionFilename already sanitizes traversal.
		filename = cd
	} else {
		// Fall back to deriving from the final URL after redirects.
		finalURL := parsedURL
		if resp.Request != nil && resp.Request.URL != nil {
			finalURL = resp.Request.URL
		}
		filename = platform.DeriveFilenameFromURL(finalURL)
	}

	// Early conflict check BEFORE writing gigabytes.
	if req.Mode == domain.FinalizeNoClobber {
		target := filepath.Join(destDir, filename)
		exists, isSymlink, err := platform.CheckExistingTarget(target)
		if err != nil {
			return domain.TransferResult{}, err
		}
		if isSymlink {
			return domain.TransferResult{}, fmt.Errorf("%w: %s", domain.ErrSymlinkTarget, target)
		}
		if exists {
			suggest, serr := platform.SuggestAvailableFilename(destDir, filename)
			if serr != nil {
				suggest = ""
			}
			return domain.TransferResult{}, &domain.FileConflictError{
				Filename:          filename,
				ExistingPath:      target,
				SuggestedFilename: suggest,
			}
		}
	}

	// Determine temp path.
	tempPath := req.TempPath
	if tempPath == "" {
		tempPath = filepath.Join(destDir, fmt.Sprintf(".%s.%s.part", filename, req.ID))
	}

	// Refuse symlink temp path before opening.
	if fi, err := os.Lstat(tempPath); err == nil && fi.Mode()&os.ModeSymlink != 0 {
		return domain.TransferResult{}, fmt.Errorf("%w: %s", domain.ErrSymlinkTarget, tempPath)
	}
	// Open temp file.
	tempFile, err := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return domain.TransferResult{}, codeError(domain.ErrorCodeFilesystem, "open temp file: "+err.Error(), err)
	}

	// All error paths below must close tempFile BEFORE returning,
	// so the caller can delete the .part file on Windows.
	bufSize := d.cfg.BufferSize
	if bufSize <= 0 {
		bufSize = config.DefaultBufferSize
	}
	buf := make([]byte, bufSize)
	totalBytes := resp.ContentLength
	totalWritten := int64(0)
	emit := func() {
		if req.OnProgress != nil {
			req.OnProgress(domain.ProgressUpdate{
				DownloadedBytes:  totalWritten,
				TotalBytes:       totalBytes,
				ResolvedFilename: filename,
			})
		}
	}
	emit()

	progressInterval := d.cfg.ProgressInterval
	if progressInterval <= 0 {
		progressInterval = config.DefaultProgressInterval
	}
	lastEmit := time.Now()

	var readErr error
	var writeErr error
	for {
		n, rerr := resp.Body.Read(buf)
		if n > 0 {
			// Reset stall timer on progress.
			if !stallTimer.Stop() {
				// Timer already fired; drain nothing, just proceed to
				// observe cancellation on next read/loop. Best-effort reset.
			}
			stallTimer.Reset(stallTimeout)

			if _, werr := tempFile.Write(buf[:n]); werr != nil {
				// Track the write failure separately: a failed
				// temp-file write is a disk error, not a network
				// error (A6, Phase 3).
				writeErr = werr
				readErr = werr
				break
			}
			totalWritten += int64(n)

			// Abort early if we already exceeded the declared length.
			if totalBytes >= 0 && totalWritten > totalBytes {
				readErr = domain.ErrSizeMismatch
				break
			}

			now := time.Now()
			if now.Sub(lastEmit) >= progressInterval {
				emit()
				lastEmit = now
			}
		}
		if rerr != nil {
			readErr = rerr
			break
		}
	}

	// Emit final progress if we made it without a read error other than EOF.
	if errors.Is(readErr, io.EOF) {
		emit()
	}

	if readErr != nil && !errors.Is(readErr, io.EOF) {
		// Always close the temp file so the OS releases the handle.
		_ = tempFile.Close()
		// Stall takes precedence over generic cancellation.
		if cause := context.Cause(reqCtx); cause == stallErr {
			return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, cause
		}
		// Parent/tool cancellation.
		if ctx.Err() != nil {
			return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, ctx.Err()
		}
		if reqCtx.Err() != nil {
			// Cancelled by stall or elsewhere.
			if cause := context.Cause(reqCtx); cause != nil {
				return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, cause
			}
			return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, reqCtx.Err()
		}
		// If we overshot Content-Length, treat as size mismatch.
		// The .part is kept and its path reported: deleting it is
		// the service's job (single rule, Phase 3), not the
		// engine's.
		if errors.Is(readErr, domain.ErrSizeMismatch) {
			return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, codeError(domain.ErrorCodeSizeMismatch, "downloaded more bytes than Content-Length", domain.ErrSizeMismatch)
		}
		// A failed temp-file write is a disk problem (ENOSPC, EIO,
		// ...), not a network problem: report filesystem_error so
		// the caller does not retry it as a network blip.
		if writeErr != nil {
			return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, codeError(domain.ErrorCodeFilesystem, "write temp file: "+writeErr.Error(), writeErr)
		}
		// The body ended before Content-Length was satisfied: the
		// transfer is short, not broken mid-stream. Only when a
		// length was declared; without one there is nothing to
		// mismatch against, so it stays a network error below.
		if totalBytes >= 0 && errors.Is(readErr, io.ErrUnexpectedEOF) {
			return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, codeError(domain.ErrorCodeSizeMismatch, fmt.Sprintf("size mismatch: wrote %d, expected %d", totalWritten, totalBytes), domain.ErrSizeMismatch)
		}
		// Other read errors.
		return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, codeError(domain.ErrorCodeNetwork, "transfer: "+readErr.Error(), readErr)
	}

	// Verify exact size. The .part is kept and its path reported
	// (single cleanup rule, Phase 3): the service decides whether
	// to keep it for retry or delete it on the user's choice.
	if totalBytes >= 0 && totalWritten != totalBytes {
		_ = tempFile.Close()
		return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, codeError(domain.ErrorCodeSizeMismatch, fmt.Sprintf("size mismatch: wrote %d, expected %d", totalWritten, totalBytes), domain.ErrSizeMismatch)
	}

	// Durability: sync and close, checking both errors before rename.
	// Both returns carry TempPath (A6, Phase 3): the .part stays on
	// disk and the caller must be able to find it to retry or
	// delete it.
	if err := tempFile.Sync(); err != nil {
		_ = tempFile.Close()
		return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, codeError(domain.ErrorCodeFilesystem, "sync temp file: "+err.Error(), err)
	}
	if err := tempFile.Close(); err != nil {
		return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, codeError(domain.ErrorCodeFilesystem, "close temp file: "+err.Error(), err)
	}

	// Finalize.
	finalPath, finalName, ferr := platform.FinalizeTempFile(tempPath, destDir, filename, req.Mode)
	if ferr != nil {
		// If a mid-download race created the target, FinalizeTempFile
		// returns *FileConflictError and keeps tempPath intact.
		// Return it alongside TransferResult so app.Service can
		// preserve TempPath for user resolution.
		var fce *domain.FileConflictError
		if errors.As(ferr, &fce) {
			return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, ferr
		}
		return domain.TransferResult{TempPath: tempPath, Filename: filename, WrittenBytes: totalWritten, TotalBytes: totalBytes}, ferr
	}

	return domain.TransferResult{
		FinalPath:    finalPath,
		Filename:     finalName,
		TempPath:     tempPath,
		WrittenBytes: totalWritten,
		TotalBytes:   totalBytes,
	}, nil
}

// Ensure SingleDownloader satisfies domain.StreamDownloader at compile time.
var _ domain.StreamDownloader = (*SingleDownloader)(nil)

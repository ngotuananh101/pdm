// Segment worker (Stage 3, Phase 3.1).
//
// A worker downloads exactly the uncommitted remainder of one
// segment: one ranged GET with Range + If-Range, Accept-Encoding:
// identity, 64 KiB bounded reads, WriteAt at the matching file
// offset, and cancellation via context.Context.
//
// Only a 206 whose Content-Range matches the requested range is
// accepted, and only after the header check — no body byte is ever
// written on a mismatch. A 200 (decision 3: the server ignored Range
// or the validator changed) is an ErrRangeIgnored error, never a
// silent write of the full body into the segment. A 416 is an
// ErrRangeNotSatisfiable error: the caller must re-check size and
// validator before deciding what to do next.
package engine

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/platform"
)

// ErrRangeIgnored reports that the server answered a ranged request
// with 200: it ignored Range, or the If-Range validator no longer
// matches (the resource changed). The caller must never write the
// 200 body into the segment; it re-probes and restarts instead
// (decision 3).
var ErrRangeIgnored = errors.New("server returned 200 for a ranged request: range ignored or validator changed")

// ErrRangeNotSatisfiable reports a 416: the requested range is no
// longer satisfiable, so the caller must re-check size and validator
// before deciding what to do next.
var ErrRangeNotSatisfiable = errors.New("server returned 416: range not satisfiable, size or validator changed")

// SegmentWorker downloads one segment's remaining bytes.
type SegmentWorker struct {
	cfg    config.Config
	client *http.Client
}

// NewSegmentWorker creates a worker using the given config and client.
// A nil client gets the production client from NewHTTPClient.
func NewSegmentWorker(cfg config.Config, client *http.Client) *SegmentWorker {
	if client == nil {
		client = NewHTTPClient(cfg)
	}
	return &SegmentWorker{cfg: cfg, client: client}
}

// SegmentTask is one worker job: the segment to download, the URL,
// the expected total size (-1 when unknown), and the validators from
// the probe.
type SegmentTask struct {
	Segment      domain.Segment
	URL          string
	TotalBytes   int64 // -1 if unknown
	ETag         string
	LastModified string
}

// ifRangeValue returns the If-Range header value for a task: a strong
// ETag first; otherwise Last-Modified when it parses as a valid HTTP
// date, which is the condition under which it qualifies as a strong
// validator for If-Range (RFC 9110 §13.1.2). It returns "" when
// neither validator qualifies, in which case no If-Range is sent.
func ifRangeValue(etag, lastModified string) string {
	if v := StrongETag(etag); v != "" {
		return v
	}
	if strings.TrimSpace(lastModified) != "" {
		if _, err := http.ParseTime(lastModified); err == nil {
			return lastModified
		}
	}
	return ""
}

// DownloadSegment downloads the not-yet-committed remainder of
// task.Segment into f at the matching file offsets. onProgress, when
// non-nil, is called after every write with the segment's committed
// byte count (relative to the segment start). The returned value is
// that same committed count after this attempt, so a retry resumes
// from exactly where this attempt stopped.
//
// A fully committed segment is a no-op success.
func (w *SegmentWorker) DownloadSegment(ctx context.Context, f *os.File, task SegmentTask, onProgress func(committed int64)) (int64, error) {
	seg := task.Segment
	if seg.Remaining() <= 0 {
		return seg.CommittedBytes, nil
	}
	start := seg.StartOffset + seg.CommittedBytes
	end := seg.EndOffset
	remaining := seg.Remaining()

	parsedURL, err := platform.ValidateHTTPURL(task.URL)
	if err != nil {
		return seg.CommittedBytes, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedURL.String(), nil)
	if err != nil {
		return seg.CommittedBytes, codeError(domain.ErrorCodeNetwork, "worker: build request: "+err.Error(), err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	if v := ifRangeValue(task.ETag, task.LastModified); v != "" {
		req.Header.Set("If-Range", v)
	}
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", "PDM/0.1")

	resp, err := w.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return seg.CommittedBytes, ctx.Err()
		}
		return seg.CommittedBytes, codeError(domain.ErrorCodeNetwork, "worker: execute request: "+err.Error(), err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusPartialContent:
		// Checked below, before any body byte is read.
	case http.StatusOK:
		return seg.CommittedBytes, codeError(domain.ErrorCodeHTTPStatus,
			"worker: server returned 200 for ranged request bytes="+fmt.Sprintf("%d-%d", start, end), ErrRangeIgnored)
	case http.StatusRequestedRangeNotSatisfiable:
		return seg.CommittedBytes, codeError(domain.ErrorCodeSizeMismatch,
			"worker: server returned 416 for ranged request bytes="+fmt.Sprintf("%d-%d", start, end), ErrRangeNotSatisfiable)
	default:
		return seg.CommittedBytes, probeStatusError(resp)
	}

	// Validate Content-Range BEFORE reading the body: a 206 for the
	// wrong range must not write a single byte.
	if err := checkWorkerContentRange(resp.Header.Get("Content-Range"), start, end, task.TotalBytes); err != nil {
		return seg.CommittedBytes, err
	}

	bufSize := w.cfg.BufferSize
	if bufSize <= 0 {
		bufSize = config.DefaultBufferSize
	}
	buf := make([]byte, bufSize)
	// LimitReader caps the read at exactly the remaining range, so a
	// server that sends more than the range cannot overflow it.
	limited := io.LimitReader(resp.Body, remaining)
	written := int64(0)
	for {
		n, rerr := limited.Read(buf)
		if n > 0 {
			if _, werr := f.WriteAt(buf[:n], start+written); werr != nil {
				return seg.CommittedBytes + written, codeError(domain.ErrorCodeFilesystem, "worker: write segment: "+werr.Error(), werr)
			}
			written += int64(n)
			if onProgress != nil {
				onProgress(seg.CommittedBytes + written)
			}
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			if ctx.Err() != nil {
				return seg.CommittedBytes + written, ctx.Err()
			}
			return seg.CommittedBytes + written, codeError(domain.ErrorCodeNetwork, "worker: transfer: "+rerr.Error(), rerr)
		}
	}

	if written != remaining {
		return seg.CommittedBytes + written, codeError(domain.ErrorCodeSizeMismatch,
			fmt.Sprintf("worker: size mismatch: wrote %d of %d bytes for segment %d", written, remaining, seg.Index),
			domain.ErrSizeMismatch)
	}
	return seg.CommittedBytes + written, nil
}

// checkWorkerContentRange validates "bytes <start>-<end>/<total>"
// against the requested range. total must match the expected total
// when both are known; "*" accepts an unknown total.
func checkWorkerContentRange(v string, start, end, total int64) error {
	fail := func(msg string) error {
		return codeError(domain.ErrorCodeSizeMismatch, "worker: "+msg, domain.ErrSizeMismatch)
	}
	const prefix = "bytes "
	if !strings.HasPrefix(v, prefix) {
		return fail("missing or malformed Content-Range")
	}
	rest := strings.TrimPrefix(v, prefix)
	slash := strings.Index(rest, "/")
	if slash < 0 {
		return fail("malformed Content-Range")
	}
	rangePart, totalPart := rest[:slash], rest[slash+1:]
	dash := strings.Index(rangePart, "-")
	if dash < 0 {
		return fail("malformed Content-Range")
	}
	gotStart, err1 := strconv.ParseInt(rangePart[:dash], 10, 64)
	gotEnd, err2 := strconv.ParseInt(rangePart[dash+1:], 10, 64)
	if err1 != nil || err2 != nil || gotStart != start || gotEnd != end {
		return fail(fmt.Sprintf("Content-Range %q does not match requested range bytes=%d-%d", v, start, end))
	}
	if totalPart == "*" {
		return nil
	}
	gotTotal, err := strconv.ParseInt(totalPart, 10, 64)
	if err != nil {
		return fail("malformed Content-Range total")
	}
	if total >= 0 && gotTotal != total {
		return fail(fmt.Sprintf("Content-Range total %d does not match expected total %d", gotTotal, total))
	}
	return nil
}

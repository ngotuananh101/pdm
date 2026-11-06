// Range probing (Stage 3, Phase 2.1).
//
// Probe verifies Range support with a real ranged GET: GET with
// Range: bytes=0-0 and Accept-Encoding: identity. Only a 206 whose
// Content-Range matches the requested range proves Range support; a
// 200 means the server ignored Range, and a 206 with a missing or
// mismatched Content-Range is an error, never a silent mis-record.
// The probe body is always closed without being read.
//
// No HEAD request is sent: HEAD (and Accept-Ranges) is not evidence
// of Range support, and the ranged GET answers everything HEAD
// would — size, validators, and whether Range is really honored.
// Skipping HEAD also keeps a server that answers HEAD with 405 from
// breaking the probe.
//
// Validators come from the final response after redirects, so a
// redirect target's ETag/Last-Modified is what gets stored. Redirect
// targets are validated by the client's CheckRedirect (SSRF guard in
// client.go), and Accept-Encoding: identity is preserved across
// redirects there.
package engine

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/platform"
)

// ProbeResult is the outcome of probing one URL for Range support.
//
// TotalBytes is -1 when the size is unknown (200 with chunked
// encoding, or 206 with Content-Range "bytes 0-0/*"). ETag holds a
// strong ETag only: it is "" when the server sent none or a weak one
// (see StrongETag). LastModified is stored verbatim as received;
// whether it qualifies as a strong validator is decided by the
// worker that later uses it in If-Range (Phase 3).
type ProbeResult struct {
	TotalBytes    int64 // -1 if unknown
	ETag          string
	LastModified  string
	SupportsRange bool
}

// RangeProber probes a URL for Range support and validators.
type RangeProber interface {
	Probe(ctx context.Context, url string) (ProbeResult, error)
}

// Prober is the HTTP implementation of RangeProber.
type Prober struct {
	cfg    config.Config
	client *http.Client
}

// NewProber creates a prober using the given config and client. A nil
// client gets the production client from NewHTTPClient.
func NewProber(cfg config.Config, client *http.Client) *Prober {
	if client == nil {
		client = NewHTTPClient(cfg)
	}
	return &Prober{cfg: cfg, client: client}
}

// Ensure Prober satisfies RangeProber at compile time.
var _ RangeProber = (*Prober)(nil)

// StrongETag returns etag unchanged when it is a strong validator
// and "" when it is weak (W/ prefix) or empty, so a weak ETag is
// never stored as the If-Range validator.
func StrongETag(etag string) string {
	etag = strings.TrimSpace(etag)
	if etag == "" || strings.HasPrefix(etag, "W/") {
		return ""
	}
	return etag
}

// Probe verifies Range support for rawURL with a single ranged GET.
// A probe failure (network error, unexpected status, malformed
// Content-Range) is an error: the caller decides whether to fall
// back to a single-stream download.
func (p *Prober) Probe(ctx context.Context, rawURL string) (ProbeResult, error) {
	parsedURL, err := platform.ValidateHTTPURL(rawURL)
	if err != nil {
		return ProbeResult{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, parsedURL.String(), nil)
	if err != nil {
		return ProbeResult{}, codeError(domain.ErrorCodeNetwork, "build probe request: "+err.Error(), err)
	}
	req.Header.Set("Range", "bytes=0-0")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("User-Agent", "PDM/0.1")

	resp, err := p.client.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ProbeResult{}, ctx.Err()
		}
		return ProbeResult{}, codeError(domain.ErrorCodeNetwork, "probe request: "+err.Error(), err)
	}
	defer resp.Body.Close()

	// Validators come from the final response (after redirects).
	result := ProbeResult{
		TotalBytes:   -1,
		ETag:         StrongETag(resp.Header.Get("ETag")),
		LastModified: resp.Header.Get("Last-Modified"),
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		total, err := parseProbeContentRange(resp.Header.Get("Content-Range"))
		if err != nil {
			return ProbeResult{}, err
		}
		result.TotalBytes = total
		result.SupportsRange = true
		return result, nil
	case http.StatusOK:
		// The server ignored Range: no Range support, but the
		// response still carries the resource size and validators.
		if resp.ContentLength >= 0 {
			result.TotalBytes = resp.ContentLength
		}
		return result, nil
	default:
		return ProbeResult{}, probeStatusError(resp)
	}
}

// parseProbeContentRange parses "bytes 0-0/<total>" (or
// "bytes 0-0/*") and returns the total, or -1 for "*". Anything else
// is an error: a 206 whose Content-Range does not match the
// requested range proves nothing about the resource and must never
// be trusted.
func parseProbeContentRange(v string) (int64, error) {
	fail := func(msg string) (int64, error) {
		return 0, codeError(domain.ErrorCodeSizeMismatch, "probe: "+msg, nil)
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
	start, err1 := strconv.ParseInt(rangePart[:dash], 10, 64)
	end, err2 := strconv.ParseInt(rangePart[dash+1:], 10, 64)
	if err1 != nil || err2 != nil || start != 0 || end != 0 {
		return fail("Content-Range does not match requested range bytes=0-0")
	}
	if totalPart == "*" {
		return -1, nil
	}
	total, err := strconv.ParseInt(totalPart, 10, 64)
	if err != nil || total <= end {
		return fail("Content-Range total does not cover requested range")
	}
	return total, nil
}

// probeStatusError maps a non-200/206 probe status to a CodedError,
// with the same status classification as the single-stream engine.
func probeStatusError(resp *http.Response) error {
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
	ce := codeError(code, fmt.Sprintf("probe: HTTP %d %s", resp.StatusCode, http.StatusText(resp.StatusCode)), nil)
	ce.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	return ce
}

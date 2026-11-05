// Retry helpers for the single-stream engine (A4, Phase 6).
package engine

import (
	"context"
	"errors"
	"math/rand/v2"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// Retry backoff bounds: the first backoff is retryBaseBackoff,
// doubling per failed attempt up to retryMaxBackoff. Jitter spreads
// the wait uniformly over [backoff/2, backoff] so many clients do not
// retry in lockstep.
const (
	retryBaseBackoff = 200 * time.Millisecond
	retryMaxBackoff  = 5 * time.Second
)

// parseRetryAfter parses a Retry-After header value: either a delay in
// seconds or an HTTP date. It returns 0 for an empty, malformed, or
// already-past value.
func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if secs, err := strconv.Atoi(value); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(value); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}

// isRetryableError reports whether err is a transient failure worth
// retrying: network errors, 429/503 (rate limit) and other 5xx. Auth
// failures, 404s, size mismatches, filesystem errors, validation
// errors, file conflicts and context cancellation are terminal and
// must never be retried.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	var ce *CodedError
	if errors.As(err, &ce) {
		switch domain.ErrorCode(ce.Code) {
		case domain.ErrorCodeNetwork, domain.ErrorCodeHTTPRateLimit, domain.ErrorCodeHTTPServer:
			return true
		}
	}
	return false
}

// retryDelay computes how long to wait before attempt number attempt
// (0-based: 0 is the wait after the first failure). The exponential
// backoff is jittered, and a Retry-After carried by the error is
// honored as a minimum: the wait never goes below it.
func retryDelay(attempt int, err error) time.Duration {
	backoff := retryBaseBackoff << attempt
	if backoff > retryMaxBackoff || backoff <= 0 {
		backoff = retryMaxBackoff
	}
	// Uniform jitter over [backoff/2, backoff].
	half := backoff / 2
	wait := half + time.Duration(rand.Int64N(int64(half)+1))
	var ce *CodedError
	if errors.As(err, &ce) && ce.RetryAfter > wait {
		return ce.RetryAfter
	}
	return wait
}

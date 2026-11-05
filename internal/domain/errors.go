// Well-known error codes surfaced as Download.ErrorCode. They are stable
// strings suitable for display and for mapping to user-friendly messages;
// the accompanying ErrorMessage is free text and must already be sanitized
// (see SanitizeErrorText).
package domain

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ErrorCode is a stable, machine-readable error category.
type ErrorCode string

const (
	ErrorCodeFileConflict  ErrorCode = "file_conflict"
	ErrorCodeNetwork       ErrorCode = "network_error"
	ErrorCodeHTTPAuth      ErrorCode = "http_auth"
	ErrorCodeHTTPNotFound  ErrorCode = "http_not_found"
	ErrorCodeHTTPRateLimit ErrorCode = "http_rate_limit"
	ErrorCodeHTTPServer    ErrorCode = "http_server"
	ErrorCodeHTTPStatus    ErrorCode = "http_status"
	ErrorCodeSizeMismatch  ErrorCode = "size_mismatch"
	ErrorCodeFilesystem    ErrorCode = "filesystem_error"
	ErrorCodeValidation    ErrorCode = "validation_error"
)

// Sentinel errors returned by the app/engine layers.
var (
	ErrInvalidURL         = errors.New("invalid URL")
	ErrUnsupportedScheme  = errors.New("unsupported URL scheme")
	ErrURLHasCredentials  = errors.New("URL must not contain credentials")
	ErrInvalidFilename    = errors.New("invalid filename")
	ErrInvalidDestination = errors.New("invalid destination")
	ErrDestinationExists  = errors.New("destination already exists")
	ErrSymlinkTarget      = errors.New("destination resolves through a symlink")
	ErrInvalidTransition  = errors.New("invalid status transition")
	ErrDownloadNotFound   = errors.New("download not found")
	ErrSizeMismatch       = errors.New("downloaded size does not match expected size")
	ErrServiceClosed      = errors.New("service is closed")
)

// FileConflictError reports that the destination file already exists.
// It wraps ErrDestinationExists so callers can match with errors.Is,
// and carries the suggested alternative filename for auto-rename flows.
type FileConflictError struct {
	Filename          string
	ExistingPath      string
	SuggestedFilename string
}

// Error implements the error interface.
func (e *FileConflictError) Error() string {
	if e == nil {
		return ErrDestinationExists.Error()
	}
	if e.ExistingPath != "" {
		return fmt.Sprintf("%s: %s", ErrDestinationExists, e.ExistingPath)
	}
	return ErrDestinationExists.Error()
}

// Unwrap returns ErrDestinationExists, so
// errors.Is(err, ErrDestinationExists) is true for a FileConflictError.
func (e *FileConflictError) Unwrap() error {
	return ErrDestinationExists
}

// RedactURL returns a log/UI-safe rendering of raw containing only
// scheme://host/path, plus "?<redacted>" when a query string was present
// (even an empty one after '?'). Userinfo (user:pass@) and fragments are
// always stripped.
//
// If raw cannot be parsed, a best-effort fallback truncates at '?' or '#'
// and removes anything before '@' after '://', so malformed URLs never
// leak credentials or query/fragment secrets either.
func RedactURL(raw string) string {
	if raw == "" {
		return ""
	}
	parsed, err := url.Parse(raw)
	if err == nil {
		var b strings.Builder
		if parsed.Scheme != "" {
			b.WriteString(parsed.Scheme)
			b.WriteString("://")
		}
		// parsed.Host never includes userinfo.
		if parsed.Host != "" {
			b.WriteString(parsed.Host)
		}
		b.WriteString(parsed.Path)
		if parsed.RawQuery != "" || strings.Contains(raw, "?") {
			b.WriteString("?<redacted>")
		}
		return b.String()
	}
	return redactMalformedURL(raw)
}

// redactMalformedURL is the fallback used by RedactURL when url.Parse
// fails. It never returns credentials or query/fragment content.
func redactMalformedURL(raw string) string {
	s := raw

	// Truncate at the first '?' or '#', remembering whether a query was
	// present at all (including an empty one after '?').
	hasQuery := false
	if idx := strings.IndexAny(s, "?#"); idx >= 0 {
		hasQuery = s[idx] == '?'
		// A '?' appearing after '#' is part of the fragment, not a query.
		if s[idx] == '#' {
			hasQuery = strings.Contains(s[idx:], "?") && strings.Index(s, "?") > idx
			// '?' before '#' would already have been the terminator, so
			// if the terminator is '#', any '?' is actually inside the
			// fragment. However, url.Parse failures rarely hinge on this;
			// be conservative only about secrets, which we drop anyway.
			// The presence flag merely decides whether to append the
			// redacted-query marker.
			hasQuery = false
			if q := strings.Index(s, "?"); q >= 0 && q < idx {
				hasQuery = true
			}
		}
		s = s[:idx]
	}

	// Strip userinfo: anything between "://" and the last '@' after it.
	if idx := strings.Index(s, "://"); idx >= 0 {
		rest := s[idx+3:]
		if at := strings.LastIndex(rest, "@"); at >= 0 {
			s = s[:idx+3] + rest[at+1:]
		}
	} else if at := strings.LastIndex(s, "@"); at >= 0 {
		// No "://" separator; drop everything up to the last '@' so a
		// bare "user:pass@host" shape cannot leak credentials either.
		// Keep the part after '@' only.
		s = s[at+1:]
	}

	if hasQuery {
		s += "?<redacted>"
	}
	return s
}

// SanitizeErrorText replaces every http:// or https:// URL appearing in
// msg (for example inside a *url.Error produced by net/http) with its
// redacted form, so tokens or passwords embedded in URLs never reach the
// UI or the logs.
func SanitizeErrorText(msg string) string {
	if msg == "" {
		return msg
	}
	var b strings.Builder
	rest := msg
	for {
		// Find the earliest http:// or https:// occurrence.
		next := -1
		prefixLen := 0
		for _, p := range []string{"https://", "http://"} {
			if i := strings.Index(rest, p); i >= 0 && (next < 0 || i < next) {
				next = i
				prefixLen = len(p)
			}
		}
		if next < 0 {
			b.WriteString(rest)
			break
		}
		b.WriteString(rest[:next])

		// Extract the candidate URL: read until whitespace or a quote.
		candidate := rest[next:]
		end := strings.IndexAny(candidate, " \t\r\n\"'")
		var rawURL, tail string
		if end < 0 {
			rawURL = candidate
			tail = ""
		} else {
			rawURL = candidate[:end]
			tail = candidate[end:]
		}
		b.WriteString(RedactURL(rawURL))
		rest = tail
		_ = prefixLen
	}
	return b.String()
}

package platform

import (
	"fmt"
	"mime"
	"net/url"
	"path"
	"strings"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// ValidateHTTPURL checks raw is an http or https URL that is safe to
// fetch: it must be free of ASCII control characters, must use an
// http/https scheme, must have a non-empty host, and must not embed
// credentials.
func ValidateHTTPURL(raw string) (*url.URL, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil, fmt.Errorf("%w: empty URL", domain.ErrInvalidURL)
	}
	for i := 0; i < len(trimmed); i++ {
		if c := trimmed[i]; c <= 0x1F || c == 0x7F {
			return nil, fmt.Errorf("%w: URL contains control characters", domain.ErrInvalidURL)
		}
	}
	u, err := url.Parse(trimmed)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrInvalidURL, err)
	}
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		return nil, fmt.Errorf("%w: %q", domain.ErrUnsupportedScheme, u.Scheme)
	}
	if u.Host == "" || u.Hostname() == "" {
		return nil, fmt.Errorf("%w: URL has no host", domain.ErrInvalidURL)
	}
	if strings.ContainsAny(u.Hostname(), " \t\r\n") {
		return nil, fmt.Errorf("%w: URL host contains whitespace", domain.ErrInvalidURL)
	}
	if u.User != nil {
		return nil, fmt.Errorf("%w: URL contains credentials", domain.ErrURLHasCredentials)
	}
	// Normalize scheme to lowercase for downstream comparisons.
	u.Scheme = scheme
	return u, nil
}

// DeriveFilenameFromURL returns a sanitized filename derived from the
// last non-empty path segment of u, falling back to "download".
func DeriveFilenameFromURL(u *url.URL) string {
	if u == nil {
		return "download"
	}
	p := u.EscapedPath()
	// Trim trailing slashes so "/dir/file.txt/" still resolves.
	p = strings.TrimRight(p, "/")
	base := path.Base(p)
	if base == "" || base == "/" || base == "." {
		return "download"
	}
	// Unescape percent-encoded sequences safely; fall back to raw.
	if unescaped, err := url.PathUnescape(base); err == nil {
		base = unescaped
	}
	if base == "" || base == "." || base == ".." {
		return "download"
	}
	// Strip any residual path components (encoded slashes etc.).
	base = path.Base(base)
	sanitized := SanitizeFilename(base)
	if sanitized == "" {
		return "download"
	}
	return sanitized
}

// ParseContentDispositionFilename extracts a sanitized filename from a
// Content-Disposition header value, or "" if none is present or safe.
func ParseContentDispositionFilename(header string) string {
	if strings.TrimSpace(header) == "" {
		return ""
	}
	// Try the standard parser first.
	if _, params, err := mime.ParseMediaType(header); err == nil {
		if raw, ok := params["filename"]; ok && raw != "" {
			// mime.ParseMediaType returns the raw value; parameters may
			// still be quoted or contain RFC 5987 separators.
			// Also check for RFC 5987 filename* which mime does not
			// always expose as params["filename"].
			if decoded := decodeRFC5987(params); decoded != "" {
				return safeCDName(decoded)
			}
			return safeCDName(raw)
		}
		if decoded := decodeRFC5987(params); decoded != "" {
			return safeCDName(decoded)
		}
		return ""
	}
	// Fallback: mime.ParseMediaType failed (malformed header). Scan
	// manually for filename= or filename*= tokens.
	raw := extractFilenameParamFallback(header)
	if raw == "" {
		return ""
	}
	return safeCDName(raw)
}

// decodeRFC5987 looks for an RFC 5987 encoded filename* value in params.
func decodeRFC5987(params map[string]string) string {
	for k, v := range params {
		if strings.EqualFold(k, "filename*") || strings.HasSuffix(strings.ToLower(k), "filename*") {
			if decoded := decode5987Value(v); decoded != "" {
				return decoded
			}
		}
	}
	return ""
}

// decode5987Value decodes an RFC 5987 encoded string of the form
// charset'lang'value. Only UTF-8 is supported.
func decode5987Value(v string) string {
	v = strings.Trim(v, "\"")
	parts := strings.SplitN(v, "'", 3)
	if len(parts) != 3 {
		// Not RFC 5987; try plain percent decoding.
		if decoded, err := url.PathUnescape(strings.Trim(v, "\"")); err == nil && decoded != v {
			return decoded
		}
		return v
	}
	charset := strings.ToLower(parts[0])
	encoded := parts[2]
	if charset != "" && charset != "utf-8" {
		// We cannot decode arbitrary charsets; use the raw value.
		return encoded
	}
	if decoded, err := url.PathUnescape(encoded); err == nil {
		return decoded
	}
	return encoded
}

// extractFilenameParamFallback scans header for filename= or filename*=
func extractFilenameParamFallback(header string) string {
	lower := strings.ToLower(header)
	// Look for filename*= first (RFC 5987).
	idx := strings.Index(lower, "filename*=")
	if idx >= 0 {
		val := strings.TrimSpace(header[idx+len("filename*="):])
		// Strip trailing ;params or whitespace.
		if semi := strings.Index(val, ";"); semi >= 0 {
			val = val[:semi]
		}
		val = strings.TrimSpace(val)
		return strings.Trim(val, "\"")
	}
	idx = strings.Index(lower, "filename=")
	if idx < 0 {
		return ""
	}
	val := strings.TrimSpace(header[idx+len("filename="):])
	if strings.HasPrefix(val, "\"") {
		if end := strings.Index(val[1:], "\""); end >= 0 {
			return val[1 : 1+end]
		}
		return strings.Trim(val, "\"")
	}
	if semi := strings.Index(val, ";"); semi >= 0 {
		val = val[:semi]
	}
	return strings.TrimSpace(val)
}

// safeCDName converts an untrusted Content-Disposition filename into a
// validated name, or "" if it cannot be made safe. Crucially it applies
// filepath.Base semantics for both '/' and '\\' so a Windows path from
// a server cannot leak traversal segments.
func safeCDName(raw string) string {
	// Normalize backslashes to forward slashes then take Base, so both
	// "..\\..\\evil.exe" and "../../evil.exe" collapse to "evil.exe".
	normalized := strings.ReplaceAll(raw, "\\", "/")
	base := path.Base(normalized)
	base = strings.TrimSpace(base)
	if base == "" || base == "." || base == "/" || base == ".." {
		return ""
	}
	// Validate via SanitizeFilename but reject if sanitization made it
	// unrecognizable (e.g. contained only forbidden chars). Sanitize
	// always yields a non-empty valid name, so we only accept it when
	// the raw basename had at least one safe character.
	if base == "download" && raw != "download" {
		// Check whether the raw name was meaningfully different —
		// if sanitize produced the generic fallback from garbage,
		// treat as absent. However, SanitizeFilename maps empty/CON
		// etc. appropriately; we accept its output as long as the
		// raw basename was non-empty.
		return SanitizeFilename(base)
	}
	sanitized := SanitizeFilename(base)
	if sanitized == "" {
		return ""
	}
	return sanitized
}

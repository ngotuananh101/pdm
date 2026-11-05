// Package engine implements single-stream HTTP downloading.
package engine

import (
	"crypto/tls"
	"net"
	"net/http"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/platform"
)

// CodedError is an error with a stable machine-readable code and a
// sanitized message. The sanitized message never contains raw query
// tokens, credentials, or fragments from URLs.
type CodedError struct {
	Code    string
	Message string
	Cause   error

	// RetryAfter is the minimum wait before retrying, parsed from a
	// Retry-After header (seconds or HTTP date). Zero when the error
	// carries no such instruction.
	RetryAfter time.Duration
}

// Error returns the sanitized message. It always routes through
// domain.SanitizeErrorText so tokens embedded in URLs never leak.
func (e *CodedError) Error() string {
	if e == nil {
		return string(domain.ErrorCodeNetwork)
	}
	msg := e.Message
	if msg == "" && e.Cause != nil {
		msg = e.Cause.Error()
	}
	if msg == "" {
		msg = e.Code
	}
	return domain.SanitizeErrorText(msg)
}

// Unwrap returns the underlying cause, if any.
func (e *CodedError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Cause
}

// codeError creates a CodedError with the given code, wrapping cause.
func codeError(code domain.ErrorCode, msg string, cause error) *CodedError {
	return &CodedError{Code: string(code), Message: msg, Cause: cause}
}

// NewHTTPClient builds an http.Client with conservative TLS and
// connection settings for the download engine.
func NewHTTPClient(cfg config.Config) *http.Client {
	maxConns := cfg.MaxConnectionsPerHost
	if maxConns <= 0 {
		maxConns = config.DefaultMaxConnectionsPerHost
	}
	transport := &http.Transport{
		TLSClientConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
		},
		DisableCompression:    true,
		MaxConnsPerHost:       maxConns,
		MaxIdleConnsPerHost:   maxConns,
		DialContext:           (&net.Dialer{Timeout: 15 * time.Second}).DialContext,
		TLSHandshakeTimeout:   15 * time.Second,
		ResponseHeaderTimeout: 30 * time.Second,
		IdleConnTimeout:       90 * time.Second,
	}

	return &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return codeError(domain.ErrorCodeNetwork, "too many redirects", nil)
			}
			// Redirect targets must be public: a public origin
			// redirecting to loopback/private/link-local would be an
			// SSRF vector (P1). ValidatePublicHTTPURL also re-checks
			// scheme/credentials, so it subsumes the ValidateHTTPURL
			// call it replaces.
			if _, err := platform.ValidatePublicHTTPURL(req.URL.String()); err != nil {
				return err
			}
			// Preserve Accept-Encoding: identity across redirects.
			req.Header.Set("Accept-Encoding", "identity")
			// Strip sensitive headers on cross-host redirects.
			if len(via) > 0 {
				prev := via[len(via)-1]
				if prev.URL.Host != req.URL.Host {
					req.Header.Del("Authorization")
					req.Header.Del("Cookie")
				}
			}
			return nil
		},
	}
}

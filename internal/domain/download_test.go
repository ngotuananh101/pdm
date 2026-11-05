package domain

import (
	"errors"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestStatusValues(t *testing.T) {
	cases := map[Status]string{
		StatusQueued:      "queued",
		StatusProbing:     "probing",
		StatusDownloading: "downloading",
		StatusFinalizing:  "finalizing",
		StatusCompleted:   "completed",
		StatusPaused:      "paused",
		StatusCancelled:   "cancelled",
		StatusFailed:      "failed",
	}
	for s, want := range cases {
		if string(s) != want {
			t.Errorf("Status = %q, want %q", string(s), want)
		}
	}
}

func TestIsActiveAndTerminal(t *testing.T) {
	active := []Status{StatusQueued, StatusProbing, StatusDownloading, StatusFinalizing}
	for _, s := range active {
		if !s.IsActive() {
			t.Errorf("%s: IsActive() = false, want true", s)
		}
		if s.IsTerminal() {
			t.Errorf("%s: IsTerminal() = true, want false", s)
		}
	}
	terminal := []Status{StatusCompleted, StatusCancelled, StatusFailed}
	for _, s := range terminal {
		if s.IsTerminal() != true {
			t.Errorf("%s: IsTerminal() = false, want true", s)
		}
		if s.IsActive() {
			t.Errorf("%s: IsActive() = true, want false", s)
		}
	}
	// Unknown statuses are neither active nor terminal.
	var zero Status
	if zero.IsActive() || zero.IsTerminal() {
		t.Errorf("empty status: active=%v terminal=%v, want both false", zero.IsActive(), zero.IsTerminal())
	}
	if Status("bogus").IsActive() || Status("bogus").IsTerminal() {
		t.Errorf("bogus status reported as active/terminal")
	}
}

func TestCanTransitionAllowed(t *testing.T) {
	allowed := [][2]Status{
		{StatusQueued, StatusProbing},
		{StatusQueued, StatusPaused},
		{StatusQueued, StatusCancelled},
		{StatusQueued, StatusFailed},
		{StatusProbing, StatusDownloading},
		{StatusProbing, StatusPaused},
		{StatusProbing, StatusCancelled},
		{StatusProbing, StatusFailed},
		{StatusDownloading, StatusFinalizing},
		{StatusDownloading, StatusPaused},
		{StatusDownloading, StatusCancelled},
		{StatusDownloading, StatusFailed},
		{StatusFinalizing, StatusCompleted},
		{StatusFinalizing, StatusFailed},
		{StatusPaused, StatusQueued},
		{StatusPaused, StatusCancelled},
		{StatusFailed, StatusQueued},
		{StatusFailed, StatusCancelled},
		{StatusFailed, StatusFinalizing},
		{StatusCancelled, StatusQueued},
	}
	for _, p := range allowed {
		if !CanTransition(p[0], p[1]) {
			t.Errorf("CanTransition(%s, %s) = false, want true", p[0], p[1])
		}
	}
}

func TestCanTransitionForbidden(t *testing.T) {
	forbidden := [][2]Status{
		// Backwards jumps.
		{StatusDownloading, StatusProbing},
		{StatusFinalizing, StatusDownloading},
		{StatusCompleted, StatusQueued},
		{StatusCompleted, StatusPaused},
		{StatusPaused, StatusDownloading},
		{StatusPaused, StatusCompleted},
		{StatusFailed, StatusDownloading},
		{StatusCancelled, StatusDownloading},
		// Self-transitions are not listed in the diagram.
		{StatusQueued, StatusQueued},
		{StatusDownloading, StatusDownloading},
		{StatusFinalizing, StatusFinalizing},
		{StatusPaused, StatusPaused},
		{StatusFailed, StatusFailed},
		{StatusCancelled, StatusCancelled},
		{StatusCompleted, StatusCompleted},
		// Skipping stages.
		{StatusQueued, StatusDownloading},
		{StatusQueued, StatusFinalizing},
		{StatusQueued, StatusCompleted},
		{StatusProbing, StatusFinalizing},
		{StatusDownloading, StatusCompleted},
		{StatusDownloading, StatusQueued},
		{StatusFinalizing, StatusPaused},
		{StatusFinalizing, StatusCancelled},
		{StatusFinalizing, StatusQueued},
		{StatusFailed, StatusPaused},
		{StatusCancelled, StatusPaused},
		{StatusCancelled, StatusFailed},
		{StatusPaused, StatusFailed},
		{StatusPaused, StatusDownloading},
		// Unknown statuses.
		{Status(""), StatusQueued},
		{StatusQueued, Status("")},
		{Status("bogus"), StatusQueued},
	}
	for _, p := range forbidden {
		if CanTransition(p[0], p[1]) {
			t.Errorf("CanTransition(%s, %s) = true, want false", p[0], p[1])
		}
	}
}

func TestConflictResolutionValues(t *testing.T) {
	if ResolutionUnset != "" {
		t.Errorf("ResolutionUnset = %q, want empty", ResolutionUnset)
	}
	if ResolutionAutoRename != "auto_rename" {
		t.Errorf("ResolutionAutoRename = %q", ResolutionAutoRename)
	}
	if ResolutionOverwrite != "overwrite" {
		t.Errorf("ResolutionOverwrite = %q", ResolutionOverwrite)
	}
	if ResolutionCancel != "cancel" {
		t.Errorf("ResolutionCancel = %q", ResolutionCancel)
	}
}

func TestErrorCodeValues(t *testing.T) {
	cases := map[ErrorCode]string{
		ErrorCodeFileConflict:  "file_conflict",
		ErrorCodeNetwork:       "network_error",
		ErrorCodeHTTPAuth:      "http_auth",
		ErrorCodeHTTPNotFound:  "http_not_found",
		ErrorCodeHTTPRateLimit: "http_rate_limit",
		ErrorCodeHTTPServer:    "http_server",
		ErrorCodeHTTPStatus:    "http_status",
		ErrorCodeSizeMismatch:  "size_mismatch",
		ErrorCodeFilesystem:    "filesystem_error",
		ErrorCodeValidation:    "validation_error",
	}
	for code, want := range cases {
		if string(code) != want {
			t.Errorf("ErrorCode = %q, want %q", string(code), want)
		}
	}
}

func TestRedactURL(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want string
	}{
		{"plain", "https://example.com/path/file.bin", "https://example.com/path/file.bin"},
		{"userpass", "https://user:pass@example.com/f", "https://example.com/f"},
		{"user only", "https://user@example.com/f", "https://example.com/f"},
		{"encoded userinfo", "https://us%40er:p%40ss@example.com/f", "https://example.com/f"},
		{"query token", "https://example.com/f?token=abc123&x=1", "https://example.com/f?<redacted>"},
		{"empty query marker", "https://example.com/f?", "https://example.com/f?<redacted>"},
		{"fragment", "https://example.com/f#section", "https://example.com/f"},
		{"query and fragment", "https://example.com/f?token=s#frag", "https://example.com/f?<redacted>"},
		{"http", "http://example.com/a/b", "http://example.com/a/b"},
		{"port", "https://example.com:8443/f", "https://example.com:8443/f"},
		{"root path empty", "https://example.com", "https://example.com"},
		{"empty", "", ""},
		{"userinfo query fragment", "https://u:p@example.com/f?t=1#z", "https://example.com/f?<redacted>"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := RedactURL(tc.raw)
			if got != tc.want {
				t.Errorf("RedactURL(%q) = %q, want %q", tc.raw, got, tc.want)
			}
			if strings.Contains(got, "pass") || strings.Contains(got, "token=abc") {
				t.Errorf("RedactURL(%q) leaked secrets: %q", tc.raw, got)
			}
		})
	}
}

func TestRedactURLMalformed(t *testing.T) {
	// Malformed inputs must never leak credentials or query/fragment content.
	cases := []struct {
		raw  string
		want string
	}{
		{"https://user:secretpass@example.com/path?token=xyz#frag", "https://example.com/path?<redacted>"},
		{"https://user:secret@example.com/path", "https://example.com/path"},
		{"http://u:p@host.example/x?y=1#z", "http://host.example/x?<redacted>"},
		{"https://user:pass@bad host/path?tok=1", "https://bad host/path?<redacted>"},
	}
	for _, tc := range cases {
		got := RedactURL(tc.raw)
		if got != tc.want {
			t.Errorf("RedactURL(%q) = %q, want %q", tc.raw, got, tc.want)
		}
		for _, secret := range []string{"secret", "pass", "token=xyz", "tok=1", "#frag"} {
			if strings.Contains(got, secret) {
				t.Errorf("RedactURL(%q) = %q leaked %q", tc.raw, got, secret)
			}
		}
		if strings.Count(got, "@") != 0 {
			t.Errorf("RedactURL(%q) = %q still contains '@'", tc.raw, got)
		}
	}
}

func TestSanitizeErrorText(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"url.Error style",
			`Get "https://user:pass@example.com/file?token=abc": dial tcp: lookup failed`,
			`Get "https://example.com/file?<redacted>": dial tcp: lookup failed`},
		{"plain url",
			"failed to fetch https://example.com/a?sig=zzz from server",
			"failed to fetch https://example.com/a?<redacted> from server"},
		{"no url", "connection refused", "connection refused"},
		{"http", "err http://u:p@host.com/x?q=1 done", "err http://host.com/x?<redacted> done"},
		{"multiple", "a https://h1.com/x?t=1 b http://h2.com/y#z c", "a https://h1.com/x?<redacted> b http://h2.com/y c"},
		{"empty", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SanitizeErrorText(tc.in)
			if got != tc.want {
				t.Errorf("SanitizeErrorText(%q) = %q, want %q", tc.in, got, tc.want)
			}
		})
	}
}

func TestSanitizeErrorTextURLErrorInstance(t *testing.T) {
	raw := `https://user:pass@example.com/file.bin?token=supersecret`
	ue := &url.Error{Op: "Get", URL: raw, Err: errors.New("connection reset")}
	got := SanitizeErrorText(ue.Error())
	if strings.Contains(got, "pass") || strings.Contains(got, "supersecret") || strings.Contains(got, "user:") {
		t.Fatalf("SanitizeErrorText leaked secrets: %q", got)
	}
	want := `Get "https://example.com/file.bin?<redacted>": connection reset`
	if got != want {
		t.Errorf("SanitizeErrorText(url.Error) = %q, want %q", got, want)
	}
}

func TestFileConflictErrorIsAndAs(t *testing.T) {
	err := &FileConflictError{
		Filename:          "file.bin",
		ExistingPath:      "/tmp/file.bin",
		SuggestedFilename: "file (1).bin",
	}
	if !errors.Is(err, ErrDestinationExists) {
		t.Errorf("errors.Is(err, ErrDestinationExists) = false")
	}
	var target *FileConflictError
	if !errors.As(err, &target) {
		t.Fatalf("errors.As did not match *FileConflictError")
	}
	if target.Filename != "file.bin" || target.ExistingPath != "/tmp/file.bin" || target.SuggestedFilename != "file (1).bin" {
		t.Errorf("fields not preserved: %+v", target)
	}
	if err.Error() == "" {
		t.Errorf("Error() returned empty string")
	}
	if !strings.Contains(err.Error(), "/tmp/file.bin") {
		t.Errorf("Error() = %q, want it to mention ExistingPath", err.Error())
	}
	// Unwrap chain: errors.Is through a wrapped value still works.
	wrapped := errors.Join(errors.New("boom"), err)
	if !errors.Is(wrapped, ErrDestinationExists) {
		t.Errorf("errors.Is(wrapped, ErrDestinationExists) = false")
	}
	var nilErr *FileConflictError
	if nilErr.Error() == "" {
		t.Errorf("nil FileConflictError.Error() empty")
	}
}

func TestDownloadSnapshot(t *testing.T) {
	now := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	d := &Download{
		ID:                "id-1",
		URL:               "https://user:pass@example.com/f.bin?token=abc",
		Filename:          "f.bin",
		DestinationDir:    "/tmp/dest",
		TempPath:          "/tmp/dest/f.bin.part",
		FinalPath:         "/tmp/dest/f.bin",
		Status:            StatusDownloading,
		TotalBytes:        -1,
		DownloadedBytes:   512,
		SpeedBytesPerSec:  1024.5,
		ETASeconds:        -1,
		ErrorCode:         ErrorCodeNetwork,
		ErrorMessage:      "boom",
		SuggestedFilename: "f (1).bin",
		CreatedAt:         now,
		UpdatedAt:         now.Add(time.Minute),
	}
	snap := d.Snapshot()
	if snap.RedactedURL != "https://example.com/f.bin?<redacted>" {
		t.Errorf("RedactedURL = %q", snap.RedactedURL)
	}
	// Snapshot must be a copy: mutating the entity must not affect it.
	d.Status = StatusCompleted
	d.DownloadedBytes = 9999
	d.Filename = "changed.bin"
	if snap.Status != StatusDownloading || snap.DownloadedBytes != 512 || snap.Filename != "f.bin" {
		t.Errorf("snapshot aliases entity: %+v", snap)
	}
	if snap.TotalBytes != -1 || snap.ETASeconds != -1 {
		t.Errorf("unknown sentinels not preserved: %+v", snap)
	}
	// Nil receiver.
	var nilD *Download
	zero := nilD.Snapshot()
	if zero.RedactedURL != "" || zero.ID != "" {
		t.Errorf("nil snapshot = %+v, want zero", zero)
	}
	// Value semantics: snapshot itself can be copied freely.
	_ = snap
}

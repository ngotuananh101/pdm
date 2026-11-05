package platform

import (
	"errors"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// ---------- ValidatePublicHTTPURL (P1 SSRF guard) ----------

func TestValidatePublicHTTPURL_RejectsNonPublicHosts(t *testing.T) {
	for _, raw := range []string{
		"http://127.0.0.1/",
		"http://127.0.0.1:8080/file.zip",
		"http://localhost/",
		"http://localhost:8080/file.zip",
		"http://10.0.0.5/",
		"http://172.16.0.1/",
		"http://192.168.1.1/",
		"http://169.254.169.254/",
		"http://169.254.169.254/latest/meta-data/",
		"http://[::1]/",
		"http://[::1]:8080/file.zip",
		"http://0.0.0.0/",
	} {
		_, err := ValidatePublicHTTPURL(raw)
		if err == nil {
			t.Errorf("ValidatePublicHTTPURL(%q) = nil, want rejection", raw)
			continue
		}
		if !errors.Is(err, domain.ErrNonPublicURL) {
			t.Errorf("ValidatePublicHTTPURL(%q) error = %v, want errors.Is ErrNonPublicURL", raw, err)
		}
	}
}

func TestValidatePublicHTTPURL_AcceptsPublic(t *testing.T) {
	// IP literals: no DNS involved, so these cases are hermetic.
	for _, raw := range []string{
		"https://93.184.216.34/file.zip",
		"http://8.8.8.8/file.zip",
	} {
		if _, err := ValidatePublicHTTPURL(raw); err != nil {
			t.Errorf("ValidatePublicHTTPURL(%q) = %v, want nil", raw, err)
		}
	}
	// Hostname resolving to a public address (needs DNS).
	if _, err := ValidatePublicHTTPURL("https://example.com/file.zip"); err != nil {
		t.Errorf("ValidatePublicHTTPURL(%q) = %v, want nil", "https://example.com/file.zip", err)
	}
}

func TestValidatePublicHTTPURL_StillRejectsSyntaxErrors(t *testing.T) {
	cases := []struct {
		raw  string
		want error
	}{
		{"ftp://example.com/f", domain.ErrUnsupportedScheme},
		{"http://user:pass@example.com/", domain.ErrURLHasCredentials},
		{"", domain.ErrInvalidURL},
		{"http:///path", domain.ErrInvalidURL},
	}
	for _, c := range cases {
		_, err := ValidatePublicHTTPURL(c.raw)
		if err == nil {
			t.Errorf("ValidatePublicHTTPURL(%q) = nil, want %v", c.raw, c.want)
			continue
		}
		if !errors.Is(err, c.want) {
			t.Errorf("ValidatePublicHTTPURL(%q) error = %v, want %v", c.raw, err, c.want)
		}
	}
}

// TestValidateHTTPURL_AcceptsLoopback_Contract documents the settled
// contract (definition B): ValidateHTTPURL checks syntax, scheme and
// credentials only, and deliberately still accepts loopback/private
// hosts. That is not a bug: the SSRF guard lives in
// ValidatePublicHTTPURL, which only the UI add dialog and the engine
// redirect check call. The engine and its httptest-based tests rely on
// loopback URLs going through ValidateHTTPURL.
func TestValidateHTTPURL_AcceptsLoopback_Contract(t *testing.T) {
	for _, raw := range []string{
		"http://127.0.0.1/file.bin",
		"http://localhost:8080/file.bin",
		"http://192.168.1.1/file.bin",
	} {
		if _, err := ValidateHTTPURL(raw); err != nil {
			t.Errorf("ValidateHTTPURL(%q) = %v, want nil (loopback/private are accepted by design; see ValidatePublicHTTPURL)", raw, err)
		}
	}
}

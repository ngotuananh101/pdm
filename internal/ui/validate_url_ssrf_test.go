package ui

import (
	"errors"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// TestValidateURLRejectsNonPublicHosts (P1): the add dialog must refuse
// loopback/private/link-local hosts before AddDownload is called;
// submit() surfaces the error through showError.
func TestValidateURLRejectsNonPublicHosts(t *testing.T) {
	for _, raw := range []string{
		"http://127.0.0.1/file.zip",
		"http://localhost/file.zip",
		"http://10.0.0.5/file.zip",
		"http://192.168.1.1/file.zip",
		"http://169.254.169.254/latest/meta-data/",
		"http://[::1]/file.zip",
	} {
		err := validateURL(raw)
		if err == nil {
			t.Errorf("validateURL(%q) = nil, want rejection", raw)
			continue
		}
		if !errors.Is(err, domain.ErrNonPublicURL) {
			t.Errorf("validateURL(%q) error = %v, want errors.Is ErrNonPublicURL", raw, err)
		}
	}
}

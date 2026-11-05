package engine

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// TestCheckRedirect_BlocksLoopbackTarget (P1): a redirect to a loopback
// host must be refused by the engine client's CheckRedirect. Both
// httptest servers listen on 127.0.0.1; that is the point — the redirect
// target is loopback, so the test asserts refusal, not a successful
// download.
func TestCheckRedirect_BlocksLoopbackTarget(t *testing.T) {
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("secret"))
	}))
	defer target.Close()

	origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/file.bin", http.StatusFound)
	}))
	defer origin.Close()

	client := NewHTTPClient(testConfig())
	_, err := client.Get(origin.URL + "/redir")
	if err == nil {
		t.Fatal("expected error for redirect to loopback, got nil")
	}
	if !errors.Is(err, domain.ErrNonPublicURL) {
		t.Fatalf("err = %v, want errors.Is ErrNonPublicURL", err)
	}
}

// TestCheckRedirect_AllowsPublicTarget checks the allow path of the same
// CheckRedirect without any network: a redirect to a public IP literal
// must pass.
func TestCheckRedirect_AllowsPublicTarget(t *testing.T) {
	client := NewHTTPClient(testConfig())
	req := httptest.NewRequest(http.MethodGet, "http://93.184.216.34/file.bin", nil)
	via := []*http.Request{httptest.NewRequest(http.MethodGet, "http://example.com/old", nil)}
	if err := client.CheckRedirect(req, via); err != nil {
		t.Fatalf("CheckRedirect(public target) = %v, want nil", err)
	}
}

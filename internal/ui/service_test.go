package ui

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// fakeService is a hand-rolled DownloadService for isolated parser and
// adapter tests. It records every call so tests can assert the exact
// request objects the UI built.
type fakeService struct {
	mu            sync.Mutex
	adds          []AddRequest
	lastConfig    Config
	resolveCalls  []Resolution
	cancelCalls   []bool
	retryCalls    int
	removeCalls   int
	pauseCalls    int
	resumeCalls   int
	shutdownCalls atomic.Int32
	listener      func()
	// addErr / resolveErr / pauseErr / resumeErr are returned by the
	// matching method when set, so tests can drive failure paths.
	addErr     error
	resolveErr error
	pauseErr   error
	resumeErr  error
}

func (f *fakeService) AddDownload(req AddRequest) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.adds = append(f.adds, req)
	if f.addErr != nil {
		return "", f.addErr
	}
	return "fake-id", nil
}

func (f *fakeService) Cancel(_ string, deleteTemp bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelCalls = append(f.cancelCalls, deleteTemp)
	return nil
}

func (f *fakeService) Retry(_ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.retryCalls++
	return nil
}

func (f *fakeService) Remove(_ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.removeCalls++
	return nil
}

func (f *fakeService) ResolveConflict(_ string, res Resolution) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resolveCalls = append(f.resolveCalls, res)
	return f.resolveErr
}

func (f *fakeService) Pause(_ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pauseCalls++
	return f.pauseErr
}

func (f *fakeService) Resume(_ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resumeCalls++
	return f.resumeErr
}

func (f *fakeService) List() []DownloadSnapshot { return nil }

func (f *fakeService) GetConfig() Config {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastConfig
}

func (f *fakeService) SaveConfig(c ScratchConfig) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastConfig = Config(c)
	return nil
}

func (f *fakeService) SetListener(fn func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listener = fn
}

func (f *fakeService) Shutdown(_ context.Context) error {
	f.shutdownCalls.Add(1)
	return nil
}

func TestAddDialogValidationRejectsNothingExtra(t *testing.T) {
	// validateURL / validateFilename are the doors; submit() rejects
	// before touching svc. These tests assert the doors without Fyne.
	if err := validateURL("http://user:pass@example.com/"); err == nil {
		t.Error("credentials accepted")
	}
	if err := validateFilename("../../evil.exe"); err == nil {
		t.Error("traversal accepted")
	}
}

func TestRedactedURLNeverLeaksTokens(t *testing.T) {
	cases := []string{
		"https://example.com/file.zip?token=SUPER_SECRET&password=hunter2",
		"http://user:pass@example.com/file.zip",
		"http://user:pass@127.0.0.1:8080/a?key=abc#sec",
	}
	for _, raw := range cases {
		redacted := domain.RedactURL(raw)
		for _, secret := range []string{"pass", "SUPER_SECRET", "hunter2", "user", "token", "password", "key=abc", "sec"} {
			if strings.Contains(redacted, secret) {
				t.Errorf("RedactURL(%q) = %q leaked %q", raw, redacted, secret)
			}
		}
		if strings.Contains(redacted, "@") {
			t.Errorf("RedactURL(%q) = %q still contains @", raw, redacted)
		}
	}
}

// TestNoopServicePauseResume (Phase 7, U3): the test double parks an
// active snapshot in paused and re-queues it, and refuses both
// operations in the wrong states.
func TestNoopServicePauseResume(t *testing.T) {
	svc := NewNoopService()
	id, err := svc.AddDownload(AddRequest{URL: "http://example.com/f.bin", DestinationDir: "/tmp", Filename: "f.bin"})
	if err != nil {
		t.Fatal(err)
	}
	if err := svc.Resume(id); err == nil {
		t.Error("Resume on a queued download should fail")
	}
	if err := svc.Pause(id); err != nil {
		t.Fatalf("Pause = %v", err)
	}
	if got := svc.List()[0].Status; got != StatusPaused {
		t.Fatalf("Status = %q, want paused", got)
	}
	if err := svc.Pause(id); err == nil {
		t.Error("second Pause should fail")
	}
	if err := svc.Resume(id); err != nil {
		t.Fatalf("Resume = %v", err)
	}
	if got := svc.List()[0].Status; got != StatusQueued {
		t.Fatalf("Status = %q, want queued", got)
	}
	if err := svc.Pause("no-such-id"); !errors.Is(err, domain.ErrDownloadNotFound) {
		t.Fatalf("Pause unknown id = %v, want ErrDownloadNotFound", err)
	}
}

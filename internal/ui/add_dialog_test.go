// Tests for the Add dialog conflict-retry path (Phase 7, U2).
//
// A second AddDownload failure (e.g. the suggested name was just
// taken) must surface the error in the add dialog and leave the
// dialog open — never close it as if the download had been created.
package ui

import (
	"errors"
	"strings"
	"testing"

	"fyne.io/fyne/v2/test"
	"fyne.io/fyne/v2/widget"
)

func newTestAddDialog(t *testing.T, svc DownloadService) *addDialog {
	t.Helper()
	a := test.NewApp()
	t.Cleanup(a.Quit)
	w := test.NewWindow(widget.NewLabel(""))
	t.Cleanup(w.Close)
	return newAddDialog(w, svc, Config{
		DefaultDownloadDir: "/tmp",
		ConflictPolicy:     ConflictAsk,
		MaxActiveDownloads: 3,
	})
}

func TestResolveConflictAddFailureShowsError(t *testing.T) {
	svc := &fakeService{addErr: errors.New("destination already exists")}
	ad := newTestAddDialog(t, svc)

	if ad.resolveConflictAdd(ResolutionAutoRename, "http://example.com/f.bin", "/tmp", "f.bin") {
		t.Fatal("resolveConflictAdd should return false when AddDownload fails")
	}
	if len(svc.adds) != 1 {
		t.Fatalf("adds = %d, want 1 attempt", len(svc.adds))
	}
	if svc.adds[0].Resolution == nil || *svc.adds[0].Resolution != ResolutionAutoRename {
		t.Fatalf("attempt resolution = %v, want auto_rename", svc.adds[0].Resolution)
	}
	if ad.errLabel.Hidden {
		t.Error("error label should be visible after a failed retry")
	}
	if !strings.Contains(ad.errLabel.Text, "destination already exists") {
		t.Errorf("error label = %q, want it to show the AddDownload error", ad.errLabel.Text)
	}
}

func TestResolveConflictAddSuccess(t *testing.T) {
	svc := &fakeService{}
	ad := newTestAddDialog(t, svc)

	if !ad.resolveConflictAdd(ResolutionOverwrite, "http://example.com/f.bin", "/tmp", "f.bin") {
		t.Fatal("resolveConflictAdd should return true when AddDownload succeeds")
	}
	if len(svc.adds) != 1 {
		t.Fatalf("adds = %d, want 1 attempt", len(svc.adds))
	}
	if svc.adds[0].Resolution == nil || *svc.adds[0].Resolution != ResolutionOverwrite {
		t.Fatalf("attempt resolution = %v, want overwrite", svc.adds[0].Resolution)
	}
	if !ad.errLabel.Hidden {
		t.Errorf("error label should be hidden after success, got %q", ad.errLabel.Text)
	}
}

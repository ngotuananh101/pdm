// Tests for the downloads-list action buttons (Stage 3, Phase 5.3, TDD).
//
// Resume is a real resume since Phase 4.1, so a paused row must offer a
// plain "Resume" button — no "restarts from start" wording. Pause keeps
// its label; only Resume changes.
package ui

import (
	"strings"
	"testing"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/test"
	"fyne.io/fyne/v2/widget"
)

// rowButtonTexts renders one row through updateRowContents and returns
// the texts of the action buttons it shows for that status.
func rowButtonTexts(t *testing.T, r RowViewModel) []string {
	t.Helper()
	a := test.NewApp()
	t.Cleanup(a.Quit)
	obj := newRowTemplate()
	noop := func(string) {}
	updateRowContents(obj, r, noop, noop, noop, noop, noop, noop)
	root, ok := obj.(*fyne.Container)
	if !ok || len(root.Objects) == 0 {
		t.Fatal("row template root is not a container")
	}
	body, ok := root.Objects[0].(*fyne.Container)
	if !ok || len(body.Objects) < 6 {
		t.Fatal("row template body is missing the actions container")
	}
	actions, ok := body.Objects[5].(*fyne.Container)
	if !ok {
		t.Fatal("row template actions is not a container")
	}
	var texts []string
	for _, o := range actions.Objects {
		if b, ok := o.(*widget.Button); ok {
			texts = append(texts, b.Text)
		}
	}
	return texts
}

func TestPausedRowResumeButtonLabel(t *testing.T) {
	texts := rowButtonTexts(t, RowViewModel{ID: "d1", Filename: "f.bin", Status: StatusPaused, StatusText: "Paused"})
	found := false
	for _, txt := range texts {
		if strings.Contains(txt, "restarts") || strings.Contains(txt, "from start") || strings.Contains(txt, "tải lại") {
			t.Errorf("paused row button %q still says Resume restarts from the beginning", txt)
		}
		if txt == "Resume" {
			found = true
		}
	}
	if !found {
		t.Errorf("paused row buttons = %v, want a plain \"Resume\" button", texts)
	}
}

func TestActiveRowPauseButtonUnchanged(t *testing.T) {
	texts := rowButtonTexts(t, RowViewModel{ID: "d1", Filename: "f.bin", Status: StatusDownloading, StatusText: "Downloading"})
	found := false
	for _, txt := range texts {
		if txt == "Pause" {
			found = true
		}
	}
	if !found {
		t.Errorf("active row buttons = %v, want a \"Pause\" button", texts)
	}
}

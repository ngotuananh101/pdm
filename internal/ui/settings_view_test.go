// Tests for the Settings dialog segments field (Stage 3, Phase 5.2, TDD).
//
// The dialog must show the current "Số đoạn tối đa mỗi lượt tải" value,
// save a valid value through SaveConfig, and show an error without saving
// when the value is not a number between 1 and 16.
package ui

import (
	"strings"
	"testing"

	"fyne.io/fyne/v2/test"
	"fyne.io/fyne/v2/widget"
)

func newTestSettingsDialog(t *testing.T, svc DownloadService) *settingsDialog {
	t.Helper()
	a := test.NewApp()
	t.Cleanup(a.Quit)
	w := test.NewWindow(widget.NewLabel(""))
	t.Cleanup(w.Close)
	d := newSettingsDialog(w, svc)
	d.build()
	return d
}

func TestSettingsDialogShowsSegmentsEntry(t *testing.T) {
	svc := &fakeService{}
	svc.lastConfig = Config{
		DefaultDownloadDir:  "/tmp",
		ConflictPolicy:      ConflictAsk,
		MaxActiveDownloads:  3,
		SegmentsPerDownload: 4,
	}
	d := newTestSettingsDialog(t, svc)

	if d.segmentsEntry.Text != "4" {
		t.Errorf("segments entry = %q, want %q (current config value)", d.segmentsEntry.Text, "4")
	}
	found := false
	for _, item := range d.form.Items {
		if item.Text == "Số đoạn tối đa mỗi lượt tải (1..16)" {
			found = true
		}
	}
	if !found {
		t.Error("settings form has no \"Số đoạn tối đa mỗi lượt tải (1..16)\" item")
	}
}

func TestSettingsDialogSaveSegments(t *testing.T) {
	svc := &fakeService{}
	svc.lastConfig = Config{
		DefaultDownloadDir:  "/tmp",
		ConflictPolicy:      ConflictAsk,
		MaxActiveDownloads:  3,
		SegmentsPerDownload: 4,
	}
	d := newTestSettingsDialog(t, svc)

	d.segmentsEntry.SetText("8")
	if !d.save() {
		t.Fatalf("save() = false, want true (error label: %q)", d.errLabel.Text)
	}
	if svc.lastConfig.SegmentsPerDownload != 8 {
		t.Errorf("saved SegmentsPerDownload = %d, want 8", svc.lastConfig.SegmentsPerDownload)
	}
	// The other settings still save alongside the new field.
	if svc.lastConfig.MaxActiveDownloads != 3 || svc.lastConfig.DefaultDownloadDir != "/tmp" {
		t.Errorf("saved config = %+v, want the other settings unchanged", svc.lastConfig)
	}
}

func TestSettingsDialogInvalidSegmentsShowsErrorAndDoesNotSave(t *testing.T) {
	for _, bad := range []string{"0", "17", "-1", "abc", "", "1.5"} {
		svc := &fakeService{}
		svc.lastConfig = Config{
			DefaultDownloadDir:  "/tmp",
			ConflictPolicy:      ConflictAsk,
			MaxActiveDownloads:  3,
			SegmentsPerDownload: 4,
		}
		d := newTestSettingsDialog(t, svc)

		d.segmentsEntry.SetText(bad)
		if d.save() {
			t.Errorf("save() with segments %q = true, want false", bad)
		}
		if d.errLabel.Hidden {
			t.Errorf("save() with segments %q should show the error label", bad)
		}
		if !strings.Contains(d.errLabel.Text, "Segments per download") {
			t.Errorf("error label = %q, want it to mention segments per download", d.errLabel.Text)
		}
		if svc.lastConfig.SegmentsPerDownload != 4 {
			t.Errorf("invalid save with segments %q persisted SegmentsPerDownload = %d, want unchanged 4", bad, svc.lastConfig.SegmentsPerDownload)
		}
	}
}

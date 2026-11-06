package ui

import (
	"strconv"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// conflictPolicyLabel maps a stored policy to its settings label.
func conflictPolicyLabel(p ConflictPolicy) string {
	switch p {
	case ConflictAutoRename:
		return "Auto-rename (e.g. file (1).ext)"
	case ConflictOverwrite:
		return "Overwrite existing file"
	default:
		return "Ask every time (Default)"
	}
}

// segmentsLabel is the settings form label for the max-segments field
// (Stage 3, Phase 5.2: user-configurable, default 4, range 1..16).
const segmentsLabel = "Số đoạn tối đa mỗi lượt tải (1..16)"

// settingsDialog builds the Settings dialog (ARCHITECTURE section 10).
// The widgets are built by build() and kept as fields so tests can drive
// the dialog (prefill, save, error display) without opening a window.
type settingsDialog struct {
	parent fyne.Window
	svc    DownloadService

	dirEntry      *widget.Entry
	policySelect  *widget.Select
	maxEntry      *widget.Entry
	segmentsEntry *widget.Entry
	errLabel      *widget.Label
	form          *widget.Form
	dlg           dialog.Dialog
}

// newSettingsDialog creates the dialog.
func newSettingsDialog(parent fyne.Window, svc DownloadService) *settingsDialog {
	return &settingsDialog{parent: parent, svc: svc}
}

// build constructs the form widgets from the current config.
func (s *settingsDialog) build() {
	cfg := s.svc.GetConfig()

	s.dirEntry = widget.NewEntry()
	s.dirEntry.SetText(cfg.DefaultDownloadDir)
	browse := widget.NewButton("Browse...", func() {
		dialog.ShowFolderOpen(func(folder fyne.ListableURI, err error) {
			if err != nil || folder == nil {
				return
			}
			s.dirEntry.SetText(folder.Path())
		}, s.parent)
	})

	s.policySelect = widget.NewSelect([]string{
		"Ask every time (Default)",
		"Auto-rename (e.g. file (1).ext)",
		"Overwrite existing file",
	}, func(string) {})
	s.policySelect.SetSelected(conflictPolicyLabel(cfg.ConflictPolicy))

	s.maxEntry = widget.NewEntry()
	s.maxEntry.SetText(strconv.Itoa(cfg.MaxActiveDownloads))

	s.segmentsEntry = widget.NewEntry()
	s.segmentsEntry.SetText(strconv.Itoa(cfg.SegmentsPerDownload))

	s.errLabel = widget.NewLabel("")
	s.errLabel.Wrapping = fyne.TextWrapWord
	s.errLabel.Hide()

	s.form = widget.NewForm(
		widget.NewFormItem("Default download directory", container.NewBorder(nil, nil, nil, browse, s.dirEntry)),
		widget.NewFormItem("Duplicate file policy", s.policySelect),
		widget.NewFormItem("Max active downloads (1..32)", s.maxEntry),
		widget.NewFormItem(segmentsLabel, s.segmentsEntry),
	)
}

// save validates the entries and persists them via svc.SaveConfig. It
// returns false — leaving the error visible and the dialog open — when a
// value is invalid or the save fails, true when the settings were saved.
func (s *settingsDialog) save() bool {
	max, err := strconv.Atoi(strings.TrimSpace(s.maxEntry.Text))
	if err != nil || max < 1 || max > 32 {
		s.errLabel.SetText(domain.SanitizeErrorText("Max active downloads must be a number between 1 and 32."))
		s.errLabel.Show()
		return false
	}
	segments, err := strconv.Atoi(strings.TrimSpace(s.segmentsEntry.Text))
	if err != nil || segments < 1 || segments > 16 {
		s.errLabel.SetText(domain.SanitizeErrorText("Segments per download must be a number between 1 and 16."))
		s.errLabel.Show()
		return false
	}
	if strings.TrimSpace(s.dirEntry.Text) == "" {
		s.errLabel.SetText(domain.SanitizeErrorText("Default download directory is required."))
		s.errLabel.Show()
		return false
	}
	if err := s.svc.SaveConfig(ScratchConfig{
		DefaultDownloadDir:  strings.TrimSpace(s.dirEntry.Text),
		ConflictPolicy:      policyFromLabel(s.policySelect.Selected),
		MaxActiveDownloads:  max,
		SegmentsPerDownload: segments,
	}); err != nil {
		s.errLabel.SetText(domain.SanitizeErrorText(err.Error()))
		s.errLabel.Show()
		return false
	}
	s.errLabel.Hide()
	return true
}

// show opens the modal with current values.
func (s *settingsDialog) show() {
	s.build()
	content := container.NewVBox(s.form, s.errLabel)

	saveBtn := widget.NewButton("Save", func() {
		if s.save() && s.dlg != nil {
			s.dlg.Hide()
		}
	})
	s.dlg = dialog.NewCustom("Settings", "Dismiss", content, s.parent)
	if cd, ok := s.dlg.(*dialog.CustomDialog); ok {
		cd.SetButtons([]fyne.CanvasObject{saveBtn, widget.NewButton("Cancel", func() {
			s.dlg.Hide()
		})})
	}
	s.dlg.Resize(fyne.NewSize(560, 340))
	s.dlg.Show()
}

func policyFromLabel(label string) ConflictPolicy {
	switch label {
	case "Auto-rename (e.g. file (1).ext)":
		return ConflictAutoRename
	case "Overwrite existing file":
		return ConflictOverwrite
	default:
		return ConflictAsk
	}
}

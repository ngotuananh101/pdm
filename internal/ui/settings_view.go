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

// settingsDialog builds the Settings dialog (ARCHITECTURE section 10).
type settingsDialog struct {
	parent fyne.Window
	svc    DownloadService
}

// newSettingsDialog creates the dialog.
func newSettingsDialog(parent fyne.Window, svc DownloadService) *settingsDialog {
	return &settingsDialog{parent: parent, svc: svc}
}

// show opens the modal with current values.
func (s *settingsDialog) show() {
	cfg := s.svc.GetConfig()

	dirEntry := widget.NewEntry()
	dirEntry.SetText(cfg.DefaultDownloadDir)
	browse := widget.NewButton("Browse...", func() {
		dialog.ShowFolderOpen(func(folder fyne.ListableURI, err error) {
			if err != nil || folder == nil {
				return
			}
			dirEntry.SetText(folder.Path())
		}, s.parent)
	})

	policySelect := widget.NewSelect([]string{
		"Ask every time (Default)",
		"Auto-rename (e.g. file (1).ext)",
		"Overwrite existing file",
	}, func(string) {})
	policySelect.SetSelected(conflictPolicyLabel(cfg.ConflictPolicy))

	maxEntry := widget.NewEntry()
	maxEntry.SetText(strconv.Itoa(cfg.MaxActiveDownloads))

	errLabel := widget.NewLabel("")
	errLabel.Wrapping = fyne.TextWrapWord
	errLabel.Hide()

	form := widget.NewForm(
		widget.NewFormItem("Default download directory", container.NewBorder(nil, nil, nil, browse, dirEntry)),
		widget.NewFormItem("Duplicate file policy", policySelect),
		widget.NewFormItem("Max active downloads (1..32)", maxEntry),
	)
	content := container.NewVBox(form, errLabel)

	var d dialog.Dialog
	saveBtn := widget.NewButton("Save", func() {
		max, err := strconv.Atoi(strings.TrimSpace(maxEntry.Text))
		if err != nil || max < 1 || max > 32 {
			errLabel.SetText(domain.SanitizeErrorText("Max active downloads must be a number between 1 and 32."))
			errLabel.Show()
			return
		}
		if strings.TrimSpace(dirEntry.Text) == "" {
			errLabel.SetText(domain.SanitizeErrorText("Default download directory is required."))
			errLabel.Show()
			return
		}
		if err := s.svc.SaveConfig(ScratchConfig{
			DefaultDownloadDir: strings.TrimSpace(dirEntry.Text),
			ConflictPolicy:     policyFromLabel(policySelect.Selected),
			MaxActiveDownloads: max,
		}); err != nil {
			errLabel.SetText(domain.SanitizeErrorText(err.Error()))
			errLabel.Show()
			return
		}
		errLabel.Hide()
		if d != nil {
			d.Hide()
		}
	})
	d = dialog.NewCustom("Settings", "Dismiss", content, s.parent)
	if cd, ok := d.(*dialog.CustomDialog); ok {
		cd.SetButtons([]fyne.CanvasObject{saveBtn, widget.NewButton("Cancel", func() {
			d.Hide()
		})})
	}
	d.Resize(fyne.NewSize(560, 340))
	d.Show()
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

package ui

import (
	"errors"
	"fmt"
	"strings"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"

	"github.com/ngotuananh101/pdm/internal/domain"
	"github.com/ngotuananh101/pdm/internal/platform"
)

// addDialog builds the "Add download" dialog (ARCHITECTURE section 10).
// Validation happens inline before submit; a file conflict opens the 3-way
// resolution prompt instead of failing silently.
type addDialog struct {
	parent fyne.Window
	svc    DownloadService
	cfg    Config

	urlEntry  *widget.Entry
	dirEntry  *widget.Entry
	nameEntry *widget.Entry
	errLabel  *widget.Label

	// dialog is kept so Add/Cancel can Hide() it. CustomDialog has no
	// confirm hook, so every button must close it explicitly.
	dlg dialog.Dialog
}

// newAddDialog creates the dialog with entries pre-filled from cfg.
func newAddDialog(parent fyne.Window, svc DownloadService, cfg Config) *addDialog {
	ad := &addDialog{parent: parent, svc: svc, cfg: cfg}
	ad.urlEntry = widget.NewEntry()
	ad.urlEntry.SetPlaceHolder("https://example.com/file.zip")
	ad.dirEntry = widget.NewEntry()
	ad.dirEntry.SetText(cfg.DefaultDownloadDir)
	ad.nameEntry = widget.NewEntry()
	ad.nameEntry.SetPlaceHolder("Leave blank to detect automatically")
	ad.errLabel = widget.NewLabel("")
	ad.errLabel.Wrapping = fyne.TextWrapWord
	ad.errLabel.Hide()
	return ad
}

// show opens the modal dialog.
func (ad *addDialog) show() {
	browse := widget.NewButton("Browse...", func() {
		dialog.ShowFolderOpen(func(folder fyne.ListableURI, err error) {
			if err != nil || folder == nil {
				return
			}
			ad.dirEntry.SetText(folder.Path())
		}, ad.parent)
	})
	form := widget.NewForm(
		widget.NewFormItem("URL", ad.urlEntry),
		widget.NewFormItem("Destination directory", container.NewBorder(nil, nil, nil, browse, ad.dirEntry)),
		widget.NewFormItem("Filename (optional)", ad.nameEntry),
	)
	content := container.NewVBox(form, ad.errLabel)

	// d is assigned after button construction captures it by reference;
	// callbacks run only after Show(), by which time d is set.
	var d dialog.Dialog
	addBtn := widget.NewButton("Add", func() {
		if ad.submit() {
			if d != nil {
				d.Hide()
			}
		}
	})
	d = dialog.NewCustom("Add Download", "Dismiss", content, ad.parent)
	if cd, ok := d.(*dialog.CustomDialog); ok {
		cd.SetButtons([]fyne.CanvasObject{addBtn, widget.NewButton("Cancel", func() {
			d.Hide()
		})})
	}
	ad.dlg = d
	d.Resize(fyne.NewSize(560, 320))
	d.Show()
}

// submit validates input and calls AddDownload. Returns true on success.
func (ad *addDialog) submit() bool {
	url := strings.TrimSpace(ad.urlEntry.Text)
	dir := strings.TrimSpace(ad.dirEntry.Text)
	name := strings.TrimSpace(ad.nameEntry.Text)

	if err := validateURL(url); err != nil {
		ad.showError(err.Error())
		return false
	}
	if name != "" {
		if err := platform.ValidateFilename(name); err != nil {
			ad.showError(err.Error())
			return false
		}
	}
	if dir == "" {
		ad.showError("Destination directory is required.")
		return false
	}

	_, err := ad.svc.AddDownload(AddRequest{URL: url, DestinationDir: dir, Filename: name})
	if err == nil {
		ad.errLabel.Hide()
		return true
	}

	var conflictErr *FileConflictError
	if errors.As(err, &conflictErr) {
		ad.showConflictPrompt(conflictErr, url, dir, name)
		return false
	}
	ad.showError(err.Error())
	return false
}

// showConflictPrompt offers auto-rename / overwrite / cancel. On cancel the
// add dialog is reopened without touching any file.
func (ad *addDialog) showConflictPrompt(conflictErr *FileConflictError, url, dir, name string) {
	suggested := conflictErr.SuggestedFilename
	if suggested == "" {
		suggested = conflictErr.Filename + " (1)"
	}
	msg := fmt.Sprintf("“%s” already exists in the destination directory.\n\nHow should PDM handle this conflict?", conflictErr.Filename)

	var d dialog.Dialog
	buttons := container.NewHBox(
		widget.NewButton("Auto-rename ("+suggested+")", func() {
			res := ResolutionAutoRename
			_, _ = ad.svc.AddDownload(AddRequest{URL: url, DestinationDir: dir, Filename: name, Resolution: &res})
			ad.errLabel.Hide()
			if d != nil {
				d.Hide()
			}
			if ad.dlg != nil {
				ad.dlg.Hide()
			}
		}),
		widget.NewButton("Overwrite existing file", func() {
			res := ResolutionOverwrite
			_, _ = ad.svc.AddDownload(AddRequest{URL: url, DestinationDir: dir, Filename: name, Resolution: &res})
			ad.errLabel.Hide()
			if d != nil {
				d.Hide()
			}
			if ad.dlg != nil {
				ad.dlg.Hide()
			}
		}),
		widget.NewButton("Cancel", func() {
			if d != nil {
				d.Hide()
			}
			// Return to the add dialog without overwriting anything.
			ad.show()
		}),
	)
	content := container.NewVBox(widget.NewLabel(msg), buttons)
	d = dialog.NewCustom("File Exists", "Dismiss", content, ad.parent)
	d.Show()
}

// showRuntimeConflictPrompt handles a mid-download file_conflict notification
// (e.g. after Content-Disposition resolution), offering the same 3-way
// resolution via svc.ResolveConflict.
func showRuntimeConflictPrompt(parent fyne.Window, svc DownloadService, snap DownloadSnapshot) {
	suggested := snap.SuggestedFilename
	if suggested == "" {
		suggested = snap.Filename + " (1)"
	}
	msg := fmt.Sprintf("“%s” already exists in the destination directory.\n\nHow should PDM handle this conflict?", snap.Filename)
	id := snap.ID

	var d dialog.Dialog
	buttons := container.NewHBox(
		widget.NewButton("Auto-rename ("+suggested+")", func() {
			_ = svc.ResolveConflict(id, ResolutionAutoRename)
			if d != nil {
				d.Hide()
			}
		}),
		widget.NewButton("Overwrite existing file", func() {
			_ = svc.ResolveConflict(id, ResolutionOverwrite)
			if d != nil {
				d.Hide()
			}
		}),
		widget.NewButton("Cancel", func() {
			if d != nil {
				d.Hide()
			}
		}),
	)
	content := container.NewVBox(widget.NewLabel(msg), buttons)
	d = dialog.NewCustom("File Exists", "Dismiss", content, parent)
	d.Show()
}

func (ad *addDialog) showError(msg string) {
	ad.errLabel.SetText(domain.SanitizeErrorText(msg))
	ad.errLabel.Show()
}

// validateURL checks that url is an absolute http/https URL free of
// credentials, control characters, and malformed hosts, and that its
// host is public (P1 SSRF guard). It delegates to
// platform.ValidatePublicHTTPURL, which includes the
// platform.ValidateHTTPURL syntax checks, so the UI enforces exactly
// the same policy as the engine's redirect check.
func validateURL(url string) error {
	_, err := platform.ValidatePublicHTTPURL(url)
	return err
}

// validateFilename wraps platform.ValidateFilename for the optional custom
// filename field.
//
//nolint:revive
func validateFilename(name string) error {
	return platform.ValidateFilename(name)
}

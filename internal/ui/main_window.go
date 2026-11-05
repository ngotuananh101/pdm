package ui

import (
	"context"
	"sync"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/widget"
)

// MainWindow is the root window: toolbar, downloads list, summary bar and
// the add/settings dialogs (ARCHITECTURE section 10).
type MainWindow struct {
	window           fyne.Window
	app              fyne.App
	svc              DownloadService
	view             *downloadsView
	mu               sync.Mutex // guards pendingConflicts only; never held during fyne.Do
	pendingConflicts map[string]bool

	// quitOnce guards the single idempotent Quit sequence so neither the
	// toolbar Quit button nor the window close intercept can trigger two
	// Shutdown calls.
	quitOnce     sync.Once
	quitStarted  chan struct{}
	shutdownDone chan struct{}
	statusLabel  *widget.Label

	addBtn      *widget.Button
	settingsBtn *widget.Button
	quitBtn     *widget.Button

	logger logger
}

// logger is a minimal logging surface so tests can pass a no-op.
type logger interface {
	Warn(msg string, args ...any)
}

// NewMainWindow builds the window and wires service listener notifications.
// The onClose parameter is kept for signature compatibility but shutdown is
// owned by MainWindow's single Quit sequence; onClose is invoked exactly
// once inside that background goroutine before quitting.
func NewMainWindow(a fyne.App, svc DownloadService, _ ServiceFactory, onClose func()) *MainWindow {
	mw := &MainWindow{
		window:           a.NewWindow("Ponta Download Manager"),
		app:              a,
		svc:              svc,
		pendingConflicts: make(map[string]bool),
		quitStarted:      make(chan struct{}),
		shutdownDone:     make(chan struct{}),
		logger:           defaultLogger{},
	}
	mw.view = newDownloadsView(svc, mw.handleCancel, mw.handleResolve, mw.handleRetry, mw.handleRemove)
	mw.window.Resize(fyne.NewSize(900, 600))

	addBtn := widget.NewButton("Add URL", func() {
		dialogWindow := &addDialog{
			parent:    mw.window,
			svc:       svc,
			cfg:       svc.GetConfig(),
			urlEntry:  widget.NewEntry(),
			dirEntry:  widget.NewEntry(),
			nameEntry: widget.NewEntry(),
			errLabel:  widget.NewLabel(""),
		}
		if err := buildAddDialogForm(dialogWindow); err != nil {
			dialog.ShowError(err, mw.window)
		}
	})
	settingsBtn := widget.NewButton("Settings", func() {
		newSettingsDialog(mw.window, svc).show()
	})
	quitBtn := widget.NewButton("Quit", func() {
		mw.requestQuit(onClose)
	})
	mw.addBtn = addBtn
	mw.settingsBtn = settingsBtn
	mw.quitBtn = quitBtn
	mw.statusLabel = widget.NewLabel("")

	toolbar := container.NewHBox(addBtn, settingsBtn, quitBtn)
	header := container.NewVBox(toolbar, widget.NewSeparator())
	// Footer holds the shutdown status so users see progress when Quit
	// is pressed and shutdown is in flight.
	footer := container.NewVBox(mw.statusLabel, widget.NewSeparator())
	content := container.NewBorder(header, footer, nil, nil, mw.view.content())
	mw.window.SetContent(content)

	// Background service notifications arrive off the Fyne main thread.
	// We call svc.List() exactly once per notification on the callback
	// thread and hand the snapshot slice to the view inside fyne.Do, so
	// the view never re-reads state that could have changed between the
	// check and the render (no stale-snapshot race). We never hold mw.mu
	// while doing UI work (ARCHITECTURE sections 2 and 10).
	svc.SetListener(func() {
		snaps := svc.List()
		fyne.Do(func() {
			mw.view.refreshWithSnapshots(snaps)
			// Surface runtime conflicts exactly once per download.
			for _, s := range snaps {
				if s.ErrorCode == ErrorCodeFileConflict {
					mw.maybePromptConflict(s)
				}
			}
		})
	})

	mw.window.SetCloseIntercept(func() {
		mw.requestQuit(onClose)
	})
	return mw
}

// requestQuit starts the single Quit sequence. It never blocks the Fyne
// main thread: Shutdown runs in a background goroutine with a 5s timeout,
// and a.Quit() is invoked via fyne.Do once the service has stopped. The
// window is never Hide()n without a tray, so there is no zombie process.
func (mw *MainWindow) requestQuit(onClose func()) {
	mw.quitOnce.Do(func() {
		close(mw.quitStarted)
		// Disable interactive controls and surface progress. These
		// mutations are safe off the Fyne main thread (Fyne widgets
		// guard their own state) and are wrapped in fyne.Do so that
		// non-UI goroutine callers and headless test apps both work.
		fyne.Do(func() {
			if mw.addBtn != nil {
				mw.addBtn.Disable()
			}
			if mw.settingsBtn != nil {
				mw.settingsBtn.Disable()
			}
			if mw.quitBtn != nil {
				mw.quitBtn.Disable()
			}
			if mw.statusLabel != nil {
				mw.statusLabel.SetText("Shutting down...")
			}
		})
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err := mw.svc.Shutdown(ctx)
			cancel()
			if err != nil {
				mw.logger.Warn("shutdown failed", "err", err)
			}
			if onClose != nil {
				onClose()
			}
			close(mw.shutdownDone)
			fyne.Do(func() {
				// a.Quit() ends ShowAndRun; closing after shutdown
				// keeps the window visible until workers exit.
				mw.window.Close()
				if mw.app != nil {
					mw.app.Quit()
				}
			})
		}()
	})
}

// MaybeShutdownDone is a test hook: returns when Shutdown has completed.
func (mw *MainWindow) MaybeShutdownDone() <-chan struct{} { return mw.shutdownDone }

// QuitStarted is a test hook: closes when Quit begins.
func (mw *MainWindow) QuitStarted() <-chan struct{} { return mw.quitStarted }

// maybePromptConflict shows the runtime conflict prompt once per download.
// It runs on the Fyne main thread (inside fyne.Do).
func (mw *MainWindow) maybePromptConflict(snap DownloadSnapshot) {
	mw.mu.Lock()
	seen := mw.pendingConflicts[snap.ID]
	if !seen {
		mw.pendingConflicts[snap.ID] = true
	}
	mw.mu.Unlock()
	if seen {
		return
	}
	showRuntimeConflictPrompt(mw.window, mw.svc, snap)
}

// handleCancel asks whether to also delete the .part file (ARCHITECTURE
// section 5) before calling Cancel.
func (mw *MainWindow) handleCancel(id string) {
	content := container.NewVBox(widget.NewLabel("Cancel this download?"))
	var d dialog.Dialog
	buttons := container.NewHBox(
		widget.NewButton("Cancel & Delete Temp File", func() {
			_ = mw.svc.Cancel(id, true)
			d.Hide()
		}),
		widget.NewButton("Cancel & Keep Temp File", func() {
			_ = mw.svc.Cancel(id, false)
			d.Hide()
		}),
		widget.NewButton("Keep downloading", func() {
			d.Hide()
		}),
	)
	d = dialog.NewCustom("Cancel download?", "Dismiss", container.NewVBox(content, buttons), mw.window)
	d.Show()
}

func (mw *MainWindow) handleResolve(id string) {
	for _, snap := range mw.svc.List() {
		if snap.ID == id {
			showRuntimeConflictPrompt(mw.window, mw.svc, snap)
			return
		}
	}
}

func (mw *MainWindow) handleRetry(id string) {
	_ = mw.svc.Retry(id)
}

func (mw *MainWindow) handleRemove(id string) {
	_ = mw.svc.Remove(id)
}

// buildAddDialogForm initializes the addDialog entries and shows it.
func buildAddDialogForm(ad *addDialog) error {
	cfg := ad.svc.GetConfig()
	if ad.urlEntry == nil {
		ad.urlEntry = widget.NewEntry()
	}
	ad.urlEntry.SetPlaceHolder("https://example.com/file.zip")
	if ad.dirEntry == nil {
		ad.dirEntry = widget.NewEntry()
	}
	ad.dirEntry.SetText(cfg.DefaultDownloadDir)
	if ad.nameEntry == nil {
		ad.nameEntry = widget.NewEntry()
	}
	ad.nameEntry.SetPlaceHolder("Leave blank to detect automatically")
	if ad.errLabel == nil {
		ad.errLabel = widget.NewLabel("")
		ad.errLabel.Wrapping = fyne.TextWrapWord
		ad.errLabel.Hide()
	}
	ad.cfg = cfg
	ad.show()
	return nil
}

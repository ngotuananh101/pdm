package ui

import (
	"context"
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
)

// App owns the Fyne application handle and the root window. It is the only
// place in the UI package that creates the Fyne app (app.NewWithID) so
// cmd/pdm stays a thin wiring layer.
type App struct {
	a   fyne.App
	win *MainWindow
	// onClose is invoked exactly once inside MainWindow's background
	// shutdown goroutine, after svc.Shutdown returns and before Quit.
	// It is nil when no handler was supplied (unit tests).
	onClose func()
}

// NewApp creates the Fyne app with the given application ID and builds the
// download service and main window. onClose is invoked when the window is
// closed so main.go can release resources with a bounded context; it runs
// inside MainWindow's single Quit sequence, never synchronously on the
// Fyne main thread. Passing nil is safe.
func NewApp(id string, svc DownloadService, factory ServiceFactory, onClose func()) *App {
	a := app.NewWithID(id)
	ui := &App{a: a, win: NewMainWindow(a, svc, factory, onClose), onClose: onClose}
	return ui
}

// Run shows the main window and starts the Fyne event loop. It blocks until
// Quit is called from MainWindow's shutdown goroutine (via window.Close
// after svc.Shutdown completes).
func (u *App) Run() {
	u.win.window.ShowAndRun()
}

// Quit requests Fyne to quit. It is called from MainWindow's background
// goroutine after Shutdown completes; calling it directly from a button
// handler would bypass Shutdown, so prefer MainWindow.requestQuit.
func (u *App) Quit() {
	u.a.Quit()
}

// Window returns the main fyne window.
func (u *App) Window() fyne.Window {
	return u.win.window
}

// Shutdown asks the service to stop, honoring ctx cancellation. It is kept
// for tests and for programmatic shutdown; the normal quit path goes
// through MainWindow.requestQuit.
func (u *App) Shutdown(ctx context.Context) error {
	return u.win.svc.Shutdown(ctx)
}

// Service returns the download service backing this UI.
func (u *App) Service() DownloadService {
	return u.win.svc
}

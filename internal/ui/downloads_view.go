package ui

import (
	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
)

// isActive reports whether a download is still running and therefore
// cancellable.
func isActive(r RowViewModel) bool {
	switch r.Status {
	case StatusQueued, StatusProbing, StatusDownloading, StatusFinalizing:
		return true
	default:
		return false
	}
}

// isTerminalFailure reports whether a download can be retried or removed.
func isTerminalFailure(r RowViewModel) bool {
	return r.Status == StatusFailed || r.Status == StatusCancelled
}

// formatStats renders the per-row stats line. FormatSpeed already appends
// "/s" (or "—"), so it must not be doubled.
func formatStats(r RowViewModel) string {
	return r.ProgressText + "  •  " + r.SpeedText + "  •  ETA " + r.ETA
}

// newRowTemplate builds the row widget tree. The actions container is
// repopulated with the buttons relevant to the current status on every
// update, so the template stays generic.
func newRowTemplate() fyne.CanvasObject {
	filename := widget.NewLabelWithStyle("", fyne.TextAlignLeading, fyne.TextStyle{Bold: true})
	status := widget.NewLabel("")
	progress := widget.NewProgressBar()
	stats := widget.NewLabel("")
	source := widget.NewLabel("")
	errLabel := widget.NewLabel("")
	errLabel.Wrapping = fyne.TextWrapWord
	actions := container.NewHBox()
	top := container.NewBorder(nil, nil, container.NewVBox(status), filename)
	body := container.NewVBox(
		top,
		progress,
		stats,
		source,
		errLabel,
		actions,
	)
	return container.NewVBox(body)
}

// updateRowContents refreshes a row's widgets from its view model. It is
// called on the Fyne main thread (inside fyne.Do).
func updateRowContents(obj fyne.CanvasObject, r RowViewModel, onCancel, onResolve, onRetry, onRemove, onPause, onResume func(string)) {
	root, ok := obj.(*fyne.Container)
	if !ok || len(root.Objects) == 0 {
		return
	}
	body, ok := root.Objects[0].(*fyne.Container)
	if !ok || len(body.Objects) < 6 {
		return
	}
	topBox, _ := body.Objects[0].(*fyne.Container)
	var filename, status *widget.Label
	if topBox != nil && len(topBox.Objects) >= 2 {
		filename, _ = topBox.Objects[1].(*widget.Label)
		status, _ = topBox.Objects[0].(*widget.Label)
	}
	progress, _ := body.Objects[1].(*widget.ProgressBar)
	stats, _ := body.Objects[2].(*widget.Label)
	source, _ := body.Objects[3].(*widget.Label)
	errLabel, _ := body.Objects[4].(*widget.Label)
	actions, _ := body.Objects[5].(*fyne.Container)
	if filename == nil || status == nil || progress == nil || stats == nil || source == nil || errLabel == nil || actions == nil {
		return
	}

	filename.SetText(r.Filename)
	status.SetText(r.StatusText)
	if r.IsIndeterminate {
		progress.SetValue(0)
		progress.SetValue(0.5)
	} else {
		progress.SetValue(r.Progress)
	}
	stats.SetText(formatStats(r))
	source.SetText(r.Source)
	source.Truncation = fyne.TextTruncateEllipsis
	if r.ErrorMessage != "" {
		errLabel.SetText(r.ErrorMessage)
		errLabel.Show()
	} else {
		errLabel.Hide()
	}
	body.Refresh()

	// Rebuild the contextual action buttons for the current status.
	actions.RemoveAll()
	id := r.ID
	switch {
	case isActive(r):
		pause := widget.NewButton("Pause", func() { onPause(id) })
		btn := widget.NewButton("Cancel", func() { onCancel(id) })
		actions.Add(pause)
		actions.Add(btn)
	case r.Status == StatusPaused:
		// The label states the Stage 1 semantics: Resume restarts
		// the download from the beginning (no Range resume yet).
		resume := widget.NewButton("Resume (restarts from start)", func() { onResume(id) })
		btn := widget.NewButton("Cancel", func() { onCancel(id) })
		actions.Add(resume)
		actions.Add(btn)
	case r.ErrorCode == ErrorCodeFileConflict:
		btn := widget.NewButton("Resolve Conflict", func() { onResolve(id) })
		actions.Add(btn)
	case r.Status == StatusCompleted:
		btn := widget.NewButton("Remove from list", func() { onRemove(id) })
		actions.Add(btn)
	case isTerminalFailure(r):
		retry := widget.NewButton("Retry", func() { onRetry(id) })
		remove := widget.NewButton("Remove", func() { onRemove(id) })
		actions.Add(retry)
		actions.Add(remove)
	}
	actions.Refresh()
}

// downloadsView owns the virtualized downloads list.
type downloadsView struct {
	svc       DownloadService
	list      *widget.List
	empty     *widget.Label
	summary   *widget.Label
	rows      []RowViewModel
	onCancel  func(string)
	onResolve func(string)
	onRetry   func(string)
	onRemove  func(string)
	onPause   func(string)
	onResume  func(string)
}

// newDownloadsView builds the list and empty state. Button callbacks are
// stored so updateRowContents can wire them per row.
func newDownloadsView(svc DownloadService, onCancel, onResolve, onRetry, onRemove, onPause, onResume func(string)) *downloadsView {
	dv := &downloadsView{
		svc:       svc,
		onCancel:  onCancel,
		onResolve: onResolve,
		onRetry:   onRetry,
		onRemove:  onRemove,
		onPause:   onPause,
		onResume:  onResume,
	}
	dv.list = widget.NewList(
		func() int { return len(dv.rows) },
		newRowTemplate,
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			updateRowContents(obj, dv.rows[id], dv.onCancel, dv.onResolve, dv.onRetry, dv.onRemove, dv.onPause, dv.onResume)
		},
	)
	dv.empty = widget.NewLabelWithStyle(
		"No downloads yet.\nClick “Add URL” to start a download.",
		fyne.TextAlignCenter,
		fyne.TextStyle{},
	)
	dv.summary = widget.NewLabel("")
	dv.refresh()
	return dv
}

// content returns the widget tree for this view.
func (dv *downloadsView) content() fyne.CanvasObject {
	top := container.NewVBox(dv.empty, dv.summary)
	return container.NewBorder(top, nil, nil, nil, dv.list)
}

// refresh rebuilds the row cache from the service and updates the list.
// Must run on the Fyne main thread.
func (dv *downloadsView) refresh() {
	dv.refreshWithSnapshots(dv.svc.List())
}

// refreshWithSnapshots rebuilds the row cache from a caller-supplied
// snapshot slice, which must have been obtained once by the caller (see
// MainWindow's listener). Splitting refresh from the svc.List() call lets
// MainWindow capture snapshots a single time outside fyne.Do and pass them
// in, eliminating the stale-snapshot race where the model changed between
// capture and render.
func (dv *downloadsView) refreshWithSnapshots(snaps []DownloadSnapshot) {
	dv.rows = make([]RowViewModel, len(snaps))
	for i, s := range snaps {
		dv.rows[i] = MapSnapshotToRow(s)
	}
	dv.list.Refresh()
	if len(dv.rows) == 0 {
		dv.empty.Show()
		dv.list.Hide()
	} else {
		dv.empty.Hide()
		dv.list.Show()
	}
	dv.summary.SetText(Summarize(snaps))
}

// Summarize renders the "active / queued / paused / completed"
// status bar text.
func Summarize(snaps []DownloadSnapshot) string {
	var active, queued, paused, completed, failed int
	for _, s := range snaps {
		switch s.Status {
		case StatusDownloading, StatusProbing, StatusFinalizing:
			active++
		case StatusQueued:
			queued++
		case StatusPaused:
			paused++
		case StatusCompleted:
			completed++
		case StatusFailed:
			failed++
		}
	}
	return "Active: " + itoa(active) + "   Queued: " + itoa(queued) +
		"   Paused: " + itoa(paused) + "   Completed: " + itoa(completed) + "   Failed: " + itoa(failed)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	digits := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	out := ""
	for n > 0 {
		out = digits[n%10] + out
		n /= 10
	}
	return out
}

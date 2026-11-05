package ui

import (
	"math"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

func TestFormatBytes(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{0, "0 B"},
		{1, "1 B"},
		{512, "512 B"},
		{1023, "1023 B"},
		{1024, "1.0 KiB"},
		{64 * 1024, "64.0 KiB"},
		{1536, "1.5 KiB"},
		{12*1024*1024 + 512*1024, "12.5 MiB"},
		{1024 * 1024 * 1024, "1.0 GiB"},
		{2*1024*1024*1024 + 512*1024*1024, "2.5 GiB"},
		{-1, "—"},
		{-1024, "—"},
	}
	for _, tc := range tests {
		if got := FormatBytes(tc.in); got != tc.want {
			t.Errorf("FormatBytes(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFormatBytesNegative(t *testing.T) {
	if got := FormatBytes(-5); got != "—" {
		t.Errorf("FormatBytes(-5) = %q, want %q", got, "—")
	}
}

func TestFormatProgressBytesKnownTotal(t *testing.T) {
	got := FormatProgressBytes(12*1024*1024+512*1024, 100*1024*1024)
	want := "12.5 MiB / 100.0 MiB"
	if got != want {
		t.Errorf("FormatProgressBytes known = %q, want %q", got, want)
	}
}

func TestFormatProgressBytesUnknownTotal(t *testing.T) {
	for _, total := range []int64{-1, -1000} {
		got := FormatProgressBytes(4*1024*1024+200*1024, total)
		want := "4.2 MiB / Unknown"
		if got != want {
			t.Errorf("FormatProgressBytes(%d) = %q, want %q", total, got, want)
		}
	}
}

func TestFormatProgressBytesZeroDownloadedUnknownTotal(t *testing.T) {
	got := FormatProgressBytes(0, -1)
	want := "0 B / Unknown"
	if got != want {
		t.Errorf("FormatProgressBytes(0, -1) = %q, want %q", got, want)
	}
}

func TestFormatSpeed(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{float64(1024), "1.0 KiB/s"},
		{1.8 * 1024 * 1024, "1.8 MiB/s"},
		{512, "512 B/s"},
		{0, "—"},
		{-1, "—"},
		{-100, "—"},
		{math.NaN(), "—"},
		{math.Inf(1), "—"},
		{math.Inf(-1), "—"},
	}
	for _, tc := range tests {
		if got := FormatSpeed(tc.in); got != tc.want {
			t.Errorf("FormatSpeed(%v) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFormatStatsNoDoubleSlashS(t *testing.T) {
	// formatStats must render SpeedText verbatim; FormatSpeed already
	// appends "/s" (or "—"), so the rendered line must never contain
	// "/s/s".
	for _, speed := range []float64{512, 1024, 1.8 * 1024 * 1024, 0, -1} {
		row := RowViewModel{
			ProgressText: "1.0 KiB / 2.0 KiB",
			SpeedText:    FormatSpeed(speed),
			ETA:          "02m 15s",
		}
		line := formatStats(row)
		if strings.Contains(line, "/s/s") {
			t.Errorf("formatStats with speed %v = %q contains doubled /s", speed, line)
		}
		// Explicit expectation for a known speed.
		if speed == 1024 {
			want := "1.0 KiB / 2.0 KiB  •  1.0 KiB/s  •  ETA 02m 15s"
			if line != want {
				t.Errorf("formatStats(1024) = %q, want %q", line, want)
			}
		}
		if speed == 0 {
			want := "1.0 KiB / 2.0 KiB  •  —  •  ETA 02m 15s"
			if line != want {
				t.Errorf("formatStats(0) = %q, want %q", line, want)
			}
		}
	}
}

func TestFormatETA(t *testing.T) {
	tests := []struct {
		in   int64
		want string
	}{
		{-1, "—"},
		{-3600, "·"},
		{0, "< 1s"},
		{1, "1s"},
		{45, "45s"},
		{59, "59s"},
		{60, "01m 00s"},
		{135, "02m 15s"},
		{3600, "1h 00m 00s"},
		{3725, "1h 02m 05s"},
	}
	// Fix the -3600 case: negative always renders as "—".
	tests[1].want = "—"
	for _, tc := range tests {
		if got := FormatETA(tc.in); got != tc.want {
			t.Errorf("FormatETA(%d) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestFormatStatus(t *testing.T) {
	tests := []struct {
		s    Status
		code string
		want string
	}{
		{StatusQueued, "", "Queued"},
		{StatusProbing, "", "Probing"},
		{StatusDownloading, "", "Downloading"},
		{StatusFinalizing, "", "Finalizing"},
		{StatusCompleted, "", "Completed"},
		{StatusPaused, "", "Paused"},
		{StatusFailed, "", "Failed"},
		{StatusCancelled, "", "Cancelled"},
		{Status("unknown_status"), "", "unknown_status"},
		{StatusFailed, ErrorCodeFileConflict, "Conflict"},
		{StatusDownloading, ErrorCodeFileConflict, "Conflict"},
		{StatusFailed, "network_error", "Failed"},
	}
	for _, tc := range tests {
		if got := FormatStatus(tc.s, tc.code); got != tc.want {
			t.Errorf("FormatStatus(%q, %q) = %q, want %q", tc.s, tc.code, got, tc.want)
		}
	}
}

func TestMapSnapshotToRowDeterminate(t *testing.T) {
	snap := DownloadSnapshot{
		ID:               "id-1",
		Filename:         "sample.zip",
		DestinationDir:   "/tmp",
		Status:           StatusDownloading,
		TotalBytes:       100 * 1024 * 1024,
		DownloadedBytes:  25 * 1024 * 1024,
		SpeedBytesPerSec: 1.8 * 1024 * 1024,
		ETASeconds:       135,
		RedactedURL:      "https://example.com/sample.zip",
	}
	row := MapSnapshotToRow(snap)
	if row.Filename != "sample.zip" {
		t.Errorf("Filename = %q", row.Filename)
	}
	if row.StatusText != "Downloading" {
		t.Errorf("StatusText = %q", row.StatusText)
	}
	if row.IsIndeterminate {
		t.Error("expected determinate progress")
	}
	if math.Abs(row.Progress-0.25) > 1e-9 {
		t.Errorf("Progress = %v, want 0.25", row.Progress)
	}
	if row.ProgressText != "25.0 MiB / 100.0 MiB" {
		t.Errorf("ProgressText = %q", row.ProgressText)
	}
	if row.SpeedText != "1.8 MiB/s" {
		t.Errorf("SpeedText = %q", row.SpeedText)
	}
	if row.ETA != "02m 15s" {
		t.Errorf("ETA = %q", row.ETA)
	}
	if row.Source != "https://example.com/sample.zip" {
		t.Errorf("Source = %q, want redacted URL", row.Source)
	}
}

func TestMapSnapshotToRowIndeterminate(t *testing.T) {
	for _, tc := range []struct {
		total        int64
		wantProgress string
	}{
		{-1, "1.0 KiB / Unknown"},
		{0, "1.0 KiB / 0 B"},
	} {
		snap := DownloadSnapshot{
			ID:              "id-2",
			Filename:        "stream.bin",
			Status:          StatusDownloading,
			TotalBytes:      tc.total,
			DownloadedBytes: 1024,
			ETASeconds:      -1,
			RedactedURL:     "https://example.com/stream.bin",
		}
		row := MapSnapshotToRow(snap)
		if !row.IsIndeterminate {
			t.Errorf("total %d: expected indeterminate", tc.total)
		}
		if row.Progress != -1 {
			t.Errorf("total %d: Progress = %v, want -1", tc.total, row.Progress)
		}
		if row.ProgressText != tc.wantProgress {
			t.Errorf("total %d: ProgressText = %q, want %q", tc.total, row.ProgressText, tc.wantProgress)
		}
		if row.ETA != "—" {
			t.Errorf("ETA = %q, want —", row.ETA)
		}
	}
}

func TestMapSnapshotToRowClampsProgress(t *testing.T) {
	snap := DownloadSnapshot{
		Status:          StatusDownloading,
		TotalBytes:      100,
		DownloadedBytes: 250,
		RedactedURL:     "https://example.com/x",
	}
	row := MapSnapshotToRow(snap)
	if row.Progress != 1 {
		t.Errorf("clamped Progress = %v, want 1", row.Progress)
	}
}

func TestMapSnapshotToRowNeverLeaksRawURL(t *testing.T) {
	secret := "https://example.com/file.zip?token=SUPER_SECRET&password=hunter2"
	snap := DownloadSnapshot{
		ID:                "id-secret",
		Filename:          "file.zip",
		Status:            StatusFailed,
		RedactedURL:       "https://example.com/file.zip",
		ErrorCode:         "network_error",
		ErrorMessage:      "connection reset",
		SuggestedFilename: "file (1).zip",
	}
	row := MapSnapshotToRow(snap)
	leaked := row.Source == secret || containsSecret(row.Source)
	if leaked {
		t.Errorf("Source leaked raw URL: %q", row.Source)
	}
	if row.Source != "https://example.com/file.zip" {
		t.Errorf("Source = %q, want redacted URL", row.Source)
	}
	if row.ErrorMessage != "connection reset" {
		t.Errorf("ErrorMessage = %q", row.ErrorMessage)
	}
	if row.StatusText != "Failed" {
		t.Errorf("StatusText = %q", row.StatusText)
	}
	if row.SuggestedFilename != "file (1).zip" {
		t.Errorf("SuggestedFilename = %q", row.SuggestedFilename)
	}
}

func TestMapSnapshotToRowConflict(t *testing.T) {
	snap := DownloadSnapshot{
		ID:                "id-conflict",
		Filename:          "photo.jpg",
		Status:            StatusFailed,
		ErrorCode:         ErrorCodeFileConflict,
		ErrorMessage:      "file already exists",
		SuggestedFilename: "photo (1).jpg",
		TotalBytes:        1000,
		RedactedURL:       "https://example.com/photo.jpg",
	}
	row := MapSnapshotToRow(snap)
	if row.StatusText != "Conflict" {
		t.Errorf("StatusText = %q, want Conflict", row.StatusText)
	}
	if row.ErrorCode != ErrorCodeFileConflict {
		t.Errorf("ErrorCode = %q", row.ErrorCode)
	}
	if row.SuggestedFilename != "photo (1).jpg" {
		t.Errorf("SuggestedFilename = %q", row.SuggestedFilename)
	}
}

func TestMapSnapshotToRowEmpty(t *testing.T) {
	row := MapSnapshotToRow(DownloadSnapshot{})
	if row.ID != "" || row.Filename != "" || row.Source != "" {
		t.Errorf("expected zero fields, got %+v", row)
	}
	if !row.IsIndeterminate {
		t.Error("zero total should be indeterminate")
	}
	if row.SpeedText != "—" {
		t.Errorf("SpeedText = %q", row.SpeedText)
	}
}

func TestValidateURLRejectsCredentials(t *testing.T) {
	rejected := []string{
		"http://user:pass@example.com/file.zip",
		"http://user@example.com/file.zip",
		"https://admin:secret@example.com/",
		"http://user:pass@127.0.0.1:8080/a",
	}
	for _, raw := range rejected {
		err := validateURL(raw)
		if err == nil {
			t.Errorf("validateURL(%q) = nil, want rejection", raw)
			continue
		}
		if !strings.Contains(strings.ToLower(err.Error()), "credential") {
			t.Errorf("validateURL(%q) error %v does not mention credentials", raw, err)
		}
	}
}

func TestValidateURLRejectsMalformed(t *testing.T) {
	rejected := []string{
		"",
		"   ",
		"example.com/file.zip",
		"ftp://example.com/file.zip",
		"javascript:alert(1)",
		"http:///path",
		"https:///path",
		"http://",
		"http://exam ple.com/",
		"https://exa\tmple.com/",
		"http://example.com/\x01",
		"http://example.com/\x7f",
		"http://exam\x01ple.com/",
		"\x01http://example.com/",
	}
	for _, raw := range rejected {
		if err := validateURL(raw); err == nil {
			t.Errorf("validateURL(%q) = nil, want rejection", raw)
		}
	}
}

func TestValidateURLAcceptsValid(t *testing.T) {
	for _, raw := range []string{
		"https://example.com/file.zip",
		"http://example.com/a/b.iso",
		"HTTP://Example.COM/X",
		"https://example.com:8443/path?q=1#frag",
		"  https://example.com/trimmed  ",
	} {
		if err := validateURL(raw); err != nil {
			t.Errorf("validateURL(%q) = %v, want nil", raw, err)
		}
	}
}

func TestValidateFilenameRejects(t *testing.T) {
	rejected := []string{
		"",
		".",
		"..",
		"../../evil.exe",
		"..\\..\\evil.exe",
		"a/b",
		"a\\b",
		"/etc/passwd",
		`C:\evil`,
		"CON",
		"CON.txt",
		"con",
		"con.txt",
		"PRN",
		"AUX",
		"NUL",
		"aux.tar.gz",
		"COM1",
		"com9.txt",
		"LPT1",
		"file.txt.",
		"file.txt..",
		" file.txt",
		"file.txt ",
		"file\x00.txt",
		"file\x01.txt",
		"file\x7f.txt",
		"<file>.txt",
		"file>.txt",
		`file".txt`,
		"file|.txt",
		"file?.txt",
		"file*.txt",
		"file:.txt",
		"\xff\xfe invalid utf8",
		strings.Repeat("a", 201),
	}
	for _, name := range rejected {
		if err := validateFilename(name); err == nil {
			t.Errorf("validateFilename(%q) = nil, want rejection", name)
		}
	}
}

func TestValidateFilenameAccepts(t *testing.T) {
	for _, name := range []string{
		"file.txt", "a", "archive.tar.gz", "my file.pdf", "UPPER.TXT",
		"résumé.pdf", "a..b", ".hidden", "file (1).txt",
		strings.Repeat("a", 200),
	} {
		if err := validateFilename(name); err != nil {
			t.Errorf("validateFilename(%q) = %v, want nil", name, err)
		}
	}
}

func TestNoopServiceIDsAreWellFormed(t *testing.T) {
	svc := NewNoopService()
	id1, _ := svc.AddDownload(AddRequest{URL: "https://example.com/a", DestinationDir: "/tmp", Filename: "a"})
	id2, _ := svc.AddDownload(AddRequest{URL: "https://example.com/b", DestinationDir: "/tmp", Filename: "b"})
	if id1 != "download-1" || id2 != "download-2" {
		t.Fatalf("IDs = %q, %q, want download-1, download-2", id1, id2)
	}
}

func TestNoopServiceRedactedURLNeverLeaks(t *testing.T) {
	svc := NewNoopService()
	raw := "http://user:pass@example.com/file.zip?token=SECRET&password=hunter2#frag"
	id, _ := svc.AddDownload(AddRequest{URL: raw, DestinationDir: "/tmp", Filename: "file.zip"})
	snaps := svc.List()
	if len(snaps) != 1 {
		t.Fatalf("List() len = %d, want 1", len(snaps))
	}
	snap := snaps[0]
	if snap.ID != id {
		t.Errorf("ID = %q, want %q", snap.ID, id)
	}
	redacted := snap.RedactedURL
	if redacted == raw {
		t.Errorf("RedactedURL leaked raw URL verbatim")
	}
	for _, secret := range []string{"pass", "SECRET", "hunter2", "frag", "user", "token", "password"} {
		if strings.Contains(redacted, secret) {
			t.Errorf("RedactedURL %q leaked %q", redacted, secret)
		}
	}
	// domain.RedactURL keeps scheme://host/path only.
	want := domain.RedactURL(raw)
	if redacted != want {
		t.Errorf("RedactedURL = %q, want domain.RedactURL %q", redacted, want)
	}
}

func TestNoopServiceConcurrentOpsRace(t *testing.T) {
	svc := NewNoopService()
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			url := "http://user:pass@example.com/" + string(rune('a'+n%26)) + "?token=T123"
			id, _ := svc.AddDownload(AddRequest{URL: url, DestinationDir: "/tmp", Filename: "f.txt"})
			_ = svc.List()
			_ = svc.GetConfig()
			_ = svc.Cancel(id, true)
			_ = svc.Retry(id)
			_ = svc.Remove(id)
			_ = svc.SaveConfig(ScratchConfig{DefaultDownloadDir: "/tmp", ConflictPolicy: ConflictAsk, MaxActiveDownloads: 3})
			_ = svc.Shutdown(nil)
		}(i)
	}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.SetListener(func() {
				_ = svc.List()
			})
		}()
	}
	wg.Wait()
	// Every AddDownload that ran must have produced a distinct id.
	if len(svc.List()) != 50 {
		t.Errorf("List() len = %d, want 50", len(svc.List()))
	}
}

func TestNoopServiceListenerNotCalledUnderLock(t *testing.T) {
	// The listener re-enters the service (List/GetConfig); if notify held
	// the write lock this would deadlock.
	svc := NewNoopService()
	done := make(chan struct{})
	svc.SetListener(func() {
		_ = svc.List()
		_ = svc.GetConfig()
		close(done)
	})
	go func() {
		_, _ = svc.AddDownload(AddRequest{URL: "https://example.com/a", DestinationDir: "/tmp", Filename: "a"})
	}()
	select {
	case <-done:
		// listener ran; lock was free.
	default:
		// Give the goroutine time if the scheduler delayed it.
		<-done
	}
}

func TestSummarizeCounts(t *testing.T) {
	snaps := []DownloadSnapshot{
		{Status: StatusDownloading},
		{Status: StatusProbing},
		{Status: StatusQueued},
		{Status: StatusPaused},
		{Status: StatusCompleted},
		{Status: StatusFailed},
		{Status: StatusCancelled},
	}
	got := Summarize(snaps)
	want := "Active: 2   Queued: 1   Paused: 1   Completed: 1   Failed: 1"
	if got != want {
		t.Errorf("Summarize = %q, want %q", got, want)
	}
}

func containsSecret(s string) bool {
	return len(s) > 0 && (s == "https://example.com/file.zip?token=SUPER_SECRET&password=hunter2")
}

func TestDownloadSnapshotHasNoRawURLField(t *testing.T) {
	typ := reflect.TypeOf(DownloadSnapshot{})
	if _, ok := typ.FieldByName("URL"); ok {
		t.Fatal("ui.DownloadSnapshot must not expose a raw URL field; only RedactedURL may cross the UI boundary")
	}
	if _, ok := typ.FieldByName("RedactedURL"); !ok {
		t.Fatal("ui.DownloadSnapshot must expose RedactedURL")
	}

	mapped := mapSnapshot(domain.DownloadSnapshot{
		ID:          "id-boundary",
		Filename:    "archive.zip",
		URL:         "https://example.com/archive.zip?token=SUPER_SECRET_TOKEN",
		RedactedURL: "https://example.com/archive.zip",
		Status:      domain.StatusQueued,
	})
	if strings.Contains(mapped.RedactedURL, "SUPER_SECRET_TOKEN") {
		t.Fatalf("mapSnapshot leaked raw query token into RedactedURL: %q", mapped.RedactedURL)
	}
}

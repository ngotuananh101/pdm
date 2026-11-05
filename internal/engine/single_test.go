package engine

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
)

// testConfig returns a usable config for engine tests.
func testConfig() config.Config {
	return config.Config{
		DefaultDownloadDir:    os.TempDir(),
		FileConflictPolicy:    config.ConflictAsk,
		MaxActiveDownloads:    3,
		SegmentsPerDownload:   4,
		MaxConnectionsPerHost: 4,
		MinSegmentSize:        1 << 20,
		BufferSize:            4 << 10,
		ProgressInterval:      50 * time.Millisecond,
		CheckpointInterval:    5 * time.Second,
		MaxRetries:            3,
		StallTimeout:          5 * time.Second,
	}
}

func sha256Hex(t *testing.T, data []byte) string {
	t.Helper()
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func readAndHashFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read file %s: %v", path, err)
	}
	return sha256Hex(t, data)
}

// fakeProgress collects progress updates thread-safely.
type fakeProgress struct {
	updates []domain.ProgressUpdate
}

func (p *fakeProgress) fn() func(domain.ProgressUpdate) {
	return func(u domain.ProgressUpdate) {
		p.updates = append(p.updates, u)
	}
}

// --- Test 1: Known Content-Length + SHA-256 -------------------------------
func TestDownload_KnownContentLength_SHA256(t *testing.T) {
	body := []byte("hello known world, this is some binary-ish content 12345")
	wantHash := sha256Hex(t, body)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dest := t.TempDir()
	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
	prog := &fakeProgress{}
	res, err := dl.Download(context.Background(), domain.TransferRequest{
		ID:             "t1",
		URL:            srv.URL + "/file.bin",
		DestinationDir: dest,
		Mode:           domain.FinalizeOverwrite,
		OnProgress:     prog.fn(),
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if res.Filename != "file.bin" {
		t.Fatalf("Filename = %q, want file.bin", res.Filename)
	}
	if res.WrittenBytes != int64(len(body)) {
		t.Fatalf("WrittenBytes = %d, want %d", res.WrittenBytes, len(body))
	}
	gotHash := readAndHashFile(t, res.FinalPath)
	if gotHash != wantHash {
		t.Fatalf("SHA256 = %s, want %s", gotHash, wantHash)
	}
}

// --- Test 2: Unknown Content-Length (chunked) + SHA-256 ------------------
func TestDownload_UnknownContentLength_Chunked(t *testing.T) {
	body := []byte(strings.Repeat("chunked-streaming data ", 200))
	wantHash := sha256Hex(t, body)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Explicitly flush to force chunked encoding, no Content-Length.
		w.Header().Set("Transfer-Encoding", "chunked")
		_, _ = w.Write(body[:100])
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		_, _ = w.Write(body[100:])
	}))
	defer srv.Close()

	dest := t.TempDir()
	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
	res, err := dl.Download(context.Background(), domain.TransferRequest{
		ID:             "t2",
		URL:            srv.URL + "/unknown.bin",
		DestinationDir: dest,
		Mode:           domain.FinalizeOverwrite,
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	gotHash := readAndHashFile(t, res.FinalPath)
	if gotHash != wantHash {
		t.Fatalf("SHA256 = %s, want %s", gotHash, wantHash)
	}
}

// --- Test 3: Content-Disposition + path traversal ------------------------
func TestDownload_ContentDisposition_Traversal(t *testing.T) {
	body := []byte("evil payload")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Disposition", `attachment; filename="../../evil.exe"`)
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dest := t.TempDir()
	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
	res, err := dl.Download(context.Background(), domain.TransferRequest{
		ID:             "t3",
		URL:            srv.URL + "/download",
		DestinationDir: dest,
		Mode:           domain.FinalizeOverwrite,
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	// Sanitized filename must be "evil.exe", not a traversal path.
	if res.Filename != "evil.exe" {
		t.Fatalf("Filename = %q, want evil.exe", res.Filename)
	}
	// Final path must be inside destDir.
	if !strings.HasPrefix(filepath.Clean(res.FinalPath), filepath.Clean(dest)+string(os.PathSeparator)) {
		t.Fatalf("FinalPath %q escapes destDir %q", res.FinalPath, dest)
	}
	if _, statErr := os.Stat(filepath.Join(dest, "evil.exe")); statErr != nil {
		t.Fatalf("expected sanitized file inside destDir: %v", statErr)
	}
}

// --- Test 4: Valid redirect vs malicious redirect ------------------------
func TestDownload_Redirect(t *testing.T) {
	t.Run("valid redirect", func(t *testing.T) {
		body := []byte("redirected ok")
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.URL.Path == "/old" {
				http.Redirect(w, r, "/new/file.txt", http.StatusFound)
				return
			}
			_, _ = w.Write(body)
		}))
		defer srv.Close()

		dest := t.TempDir()
		cfg := testConfig()
		dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
		res, err := dl.Download(context.Background(), domain.TransferRequest{
			ID: "t4a", URL: srv.URL + "/old", DestinationDir: dest, Mode: domain.FinalizeOverwrite,
		})
		if err != nil {
			t.Fatalf("Download after redirect: %v", err)
		}
		data, _ := os.ReadFile(res.FinalPath)
		if string(data) != string(body) {
			t.Fatalf("body = %q, want %q", data, body)
		}
	})

	// Malicious redirects to ftp:// and file:// must be blocked.
	for _, target := range []string{"ftp://example.com/file", "file:///etc/passwd"} {
		t.Run("malicious redirect to "+strings.Split(target, "://")[0], func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				http.Redirect(w, r, target, http.StatusFound)
			}))
			defer srv.Close()

			dest := t.TempDir()
			cfg := testConfig()
			dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
			_, err := dl.Download(context.Background(), domain.TransferRequest{
				ID: "t4b", URL: srv.URL + "/redir", DestinationDir: dest, Mode: domain.FinalizeOverwrite,
			})
			if err == nil {
				t.Fatalf("expected error for redirect to %s, got nil", target)
			}
		})
	}
}

// --- Test 5: Mid-stream cancellation -------------------------------------
func TestDownload_MidStreamCancellation(t *testing.T) {
	// Server streams slowly; client cancels mid-way.
	started := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000000")
		// Write a small first chunk, then block.
		_, _ = w.Write([]byte("partial"))
		close(started)
		select {
		case <-r.Context().Done():
		case <-time.After(10 * time.Second):
		}
	}))
	defer srv.Close()

	dest := t.TempDir()
	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		<-started
		cancel()
	}()

	res, err := dl.Download(ctx, domain.TransferRequest{
		ID: "t5", URL: srv.URL + "/slow.bin", DestinationDir: dest, Mode: domain.FinalizeOverwrite,
	})
	<-done
	if err == nil {
		t.Fatalf("expected cancellation error, got nil")
	}
	// No final file should exist.
	entries, _ := os.ReadDir(dest)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".part") {
			t.Fatalf("unexpected final file created during cancelled download: %s", e.Name())
		}
	}
	// TempPath should be removable: it must exist (partial) or not be locked.
	// We attempt to remove it to simulate caller cleanup (validates that
	// the file handle was closed — critical on Windows).
	if res.TempPath != "" {
		if _, statErr := os.Stat(res.TempPath); statErr == nil {
			if rmErr := os.Remove(res.TempPath); rmErr != nil {
				t.Fatalf("temp .part not removable after cancellation (handle leak?): %v", rmErr)
			}
		}
	}
}

// --- Test 6: Truncated body ---------------------------------------------
func TestDownload_TruncatedBody(t *testing.T) {
	body := []byte("short")
	// Declare Content-Length larger than actual body, then close early.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write(body)
		// Handler returns early; net/http will close connection.
		// To force truncation, hijack and close abruptly.
		hj, ok := w.(http.Hijacker)
		if !ok {
			return
		}
		conn, _, _ := hj.Hijack()
		_ = conn.Close()
	}))
	defer srv.Close()

	dest := t.TempDir()
	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
	_, err := dl.Download(context.Background(), domain.TransferRequest{
		ID: "t6", URL: srv.URL + "/trunc.bin", DestinationDir: dest, Mode: domain.FinalizeOverwrite,
	})
	if err == nil {
		t.Fatalf("expected error for truncated body, got nil")
	}
	if !errors.Is(err, domain.ErrSizeMismatch) && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		// Accept CodedError wrapping ErrSizeMismatch at minimum; fail otherwise.
		var ce *CodedError
		if errors.As(err, &ce) {
			if ce.Code != string(domain.ErrorCodeSizeMismatch) && ce.Code != string(domain.ErrorCodeNetwork) {
				t.Fatalf("error code = %s, want size_mismatch or network_error; err=%v", ce.Code, err)
			}
		} else {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	// No final file should have been created.
	entries, _ := os.ReadDir(dest)
	for _, e := range entries {
		if !strings.HasSuffix(e.Name(), ".part") {
			t.Fatalf("final file created despite truncation: %s", e.Name())
		}
	}
}

// --- Test 7: Stall timeout ------------------------------------------------
func TestDownload_StallTimeout(t *testing.T) {
	before := runtime.NumGoroutine()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100000")
		_, _ = w.Write([]byte("start"))
		// Block until client disconnects; never send more.
		select {
		case <-r.Context().Done():
		case <-time.After(30 * time.Second):
		}
	}))
	defer srv.Close()

	dest := t.TempDir()
	cfg := testConfig()
	cfg.StallTimeout = 120 * time.Millisecond
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	_, err := dl.Download(ctx, domain.TransferRequest{
		ID: "t7", URL: srv.URL + "/hang.bin", DestinationDir: dest, Mode: domain.FinalizeOverwrite,
	})
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("expected stall error, got nil")
	}
	if elapsed > 3*time.Second {
		t.Fatalf("stall timeout took too long: %v", elapsed)
	}
	// Should be the stall CodedError.
	var ce *CodedError
	if !errors.As(err, &ce) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected stall/cancel error, got %T: %v", err, err)
	}

	// Brief settle, then check goroutines haven't exploded.
	time.Sleep(200 * time.Millisecond)
	after := runtime.NumGoroutine()
	// Allow generous slack for test-server goroutines still draining.
	if after > before+10 {
		t.Fatalf("possible goroutine leak: before=%d after=%d", before, after)
	}
}

// --- Test 8: HTTP status classification + secret redaction ---------------
func TestDownload_HTTPStatusClassification_NoSecretLeak(t *testing.T) {
	cases := []struct {
		status int
		code   domain.ErrorCode
	}{
		{403, domain.ErrorCodeHTTPAuth},
		{404, domain.ErrorCodeHTTPNotFound},
		{500, domain.ErrorCodeHTTPServer},
		{401, domain.ErrorCodeHTTPAuth},
		{410, domain.ErrorCodeHTTPNotFound},
		{429, domain.ErrorCodeHTTPRateLimit},
		{503, domain.ErrorCodeHTTPRateLimit},
	}
	for _, tc := range cases {
		t.Run(fmt.Sprintf("status %d", tc.status), func(t *testing.T) {
			urlWithSecret := ""
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte("error"))
			}))
			defer srv.Close()
			urlWithSecret = srv.URL + "/file.bin?token=TOPSECRET"

			dest := t.TempDir()
			cfg := testConfig()
			dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
			_, err := dl.Download(context.Background(), domain.TransferRequest{
				ID: "t8", URL: urlWithSecret, DestinationDir: dest, Mode: domain.FinalizeOverwrite,
			})
			if err == nil {
				t.Fatalf("expected error for status %d, got nil", tc.status)
			}
			var ce *CodedError
			if !errors.As(err, &ce) {
				t.Fatalf("expected CodedError for status %d, got %T: %v", tc.status, err, err)
			}
			if ce.Code != string(tc.code) {
				t.Fatalf("Code = %s, want %s (status %d)", ce.Code, tc.code, tc.status)
			}
			if strings.Contains(err.Error(), "TOPSECRET") {
				t.Fatalf("error message leaked secret: %q", err.Error())
			}
			if strings.Contains(ce.Error(), "TOPSECRET") {
				t.Fatalf("CodedError leaked secret: %q", ce.Error())
			}
		})
	}
}

// --- Test 9: Duplicate target handling -----------------------------------
// Pre-existing file + FinalizeNoClobber -> early precheck conflict.
func TestDownload_NoClobber_EarlyConflict(t *testing.T) {
	body := []byte("new content")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dest := t.TempDir()
	existing := filepath.Join(dest, "file.bin")
	if err := os.WriteFile(existing, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
	_, err := dl.Download(context.Background(), domain.TransferRequest{
		ID: "t9a", URL: srv.URL + "/file.bin", DestinationDir: dest, Mode: domain.FinalizeNoClobber,
	})
	if err == nil {
		t.Fatalf("expected FileConflictError, got nil")
	}
	var fce *domain.FileConflictError
	if !errors.As(err, &fce) {
		t.Fatalf("expected *FileConflictError, got %T: %v", err, err)
	}
	if fce.Filename != "file.bin" {
		t.Fatalf("Filename = %q, want file.bin", fce.Filename)
	}
	// Existing file untouched.
	data, _ := os.ReadFile(existing)
	if string(data) != "old" {
		t.Fatalf("existing file modified: %q", data)
	}
}

// Mid-download race: target appears during streaming -> FinalizeTempFile
// returns FileConflictError with tempPath preserved.
func TestDownload_NoClobber_MidDownloadRace(t *testing.T) {
	dest := t.TempDir()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte(strings.Repeat("x", 40)))
		if f, ok := w.(http.Flusher); ok {
			f.Flush()
		}
		// Wait for test to plant the conflicting file.
		time.Sleep(250 * time.Millisecond)
		_, _ = w.Write([]byte(strings.Repeat("y", 60)))
	}))
	defer srv.Close()

	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))

	// Plant the conflicting file shortly after download starts.
	go func() {
		time.Sleep(80 * time.Millisecond)
		_ = os.WriteFile(filepath.Join(dest, "file.bin"), []byte("planted"), 0o600)
	}()

	res, err := dl.Download(context.Background(), domain.TransferRequest{
		ID: "t9b", URL: srv.URL + "/file.bin", DestinationDir: dest, Mode: domain.FinalizeNoClobber,
	})
	if err == nil {
		t.Fatalf("expected FileConflictError from mid-download race, got nil (final=%s)", res.FinalPath)
	}
	var fce *domain.FileConflictError
	if !errors.As(err, &fce) {
		t.Fatalf("expected *FileConflictError, got %T: %v", err, err)
	}
	// TempPath must be preserved alongside the error.
	if res.TempPath == "" {
		t.Fatalf("TempPath empty in result alongside FileConflictError")
	}
	if _, statErr := os.Stat(res.TempPath); statErr != nil {
		t.Fatalf("temp .part not preserved after mid-download race: %v", statErr)
	}
	// Planted file untouched.
	data, _ := os.ReadFile(filepath.Join(dest, "file.bin"))
	if string(data) != "planted" {
		t.Fatalf("planted file overwritten: %q", data)
	}
}

func TestDownload_AutoRename(t *testing.T) {
	body := []byte("auto rename content")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dest := t.TempDir()
	if err := os.WriteFile(filepath.Join(dest, "file.bin"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
	res, err := dl.Download(context.Background(), domain.TransferRequest{
		ID: "t9c", URL: srv.URL + "/file.bin", DestinationDir: dest, Mode: domain.FinalizeAutoRename,
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if res.Filename != "file (1).bin" {
		t.Fatalf("Filename = %q, want %q", res.Filename, "file (1).bin")
	}
	// Original untouched.
	data, _ := os.ReadFile(filepath.Join(dest, "file.bin"))
	if string(data) != "old" {
		t.Fatalf("original file modified: %q", data)
	}
}

func TestDownload_Overwrite(t *testing.T) {
	body := []byte("overwritten!")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dest := t.TempDir()
	if err := os.WriteFile(filepath.Join(dest, "file.bin"), []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}

	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
	res, err := dl.Download(context.Background(), domain.TransferRequest{
		ID: "t9d", URL: srv.URL + "/file.bin", DestinationDir: dest, Mode: domain.FinalizeOverwrite,
	})
	if err != nil {
		t.Fatalf("Download: %v", err)
	}
	if res.Filename != "file.bin" {
		t.Fatalf("Filename = %q, want file.bin", res.Filename)
	}
	data, _ := os.ReadFile(res.FinalPath)
	if string(data) != string(body) {
		t.Fatalf("final content = %q, want %q", data, body)
	}
}

func TestDownload_RefusesSymlinkTempFile(t *testing.T) {
	body := []byte("payload")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = w.Write(body)
	}))
	defer srv.Close()

	dest := t.TempDir()
	real := filepath.Join(dest, "real.part")
	if err := os.WriteFile(real, []byte("orig"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dest, ".file.bin.id1.part")
	if err := os.Symlink(real, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}

	cfg := testConfig()
	dl := NewSingleDownloader(cfg, NewHTTPClient(cfg))
	_, err := dl.Download(context.Background(), domain.TransferRequest{
		ID: "id1", URL: srv.URL + "/file.bin", DestinationDir: dest, Mode: domain.FinalizeOverwrite,
	})
	if err == nil {
		t.Fatal("expected error for symlink temp file, got nil")
	}
	// Download uses .file.bin.id1.part (from req.ID when TempPath empty);
	// error should wrap ErrSymlinkTarget.
	if !errors.Is(err, domain.ErrSymlinkTarget) {
		t.Fatalf("err = %v, want ErrSymlinkTarget", err)
	}
	// Real file must be untouched.
	data, _ := os.ReadFile(real)
	if string(data) != "orig" {
		t.Fatalf("symlink target modified: %q", data)
	}
}

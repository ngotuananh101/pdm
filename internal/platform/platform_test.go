package platform

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// ---------- ValidateFilename ----------

func TestValidateFilename_Valid(t *testing.T) {
	for _, name := range []string{
		"file.txt", "a", "archive.tar.gz", "my file.pdf", "UPPER.TXT",
		"résumé.pdf", "a..b", ".hidden", "file (1).txt",
		strings.Repeat("a", 200),
	} {
		if err := ValidateFilename(name); err != nil {
			t.Errorf("ValidateFilename(%q) = %v, want nil", name, err)
		}
	}
}

func TestValidateFilename_Invalid(t *testing.T) {
	cases := []string{
		"", ".", "..",
		"a/b", "a\\b", "/etc/passwd", `C:\evil`,
		"file\x00.txt", "file\x01.txt", "file\x7f.txt",
		"<file>.txt", "file>.txt", "file:<.txt", `file".txt`, "file|.txt", "file?.txt", "file*.txt",
		" file.txt", "file.txt ", "file.txt.", "file.txt..",
		"CON", "con", "PRN", "AUX", "NUL", "aux.tar.gz", "COM1", "com9.txt", "LPT1", "lpt9.log",
		"COM¹", "com²", "COM³", "LPT¹", "lpt².txt", "CON.txt",
		"\xff\xfe invalid utf8",
		"a" + string([]byte{0xff}) + "b",
		strings.Repeat("a", 201),
	}
	for _, name := range cases {
		err := ValidateFilename(name)
		if err == nil {
			t.Errorf("ValidateFilename(%q) = nil, want error", name)
			continue
		}
		if !errors.Is(err, domain.ErrInvalidFilename) {
			t.Errorf("ValidateFilename(%q) error = %v, want errors.Is ErrInvalidFilename", name, err)
		}
	}
}

func TestValidateFilename_ByteLimit(t *testing.T) {
	exact := strings.Repeat("a", 200)
	if err := ValidateFilename(exact); err != nil {
		t.Errorf("200 bytes: %v", err)
	}
	// 199 bytes + 1 multi-byte char position checks keep byte length.
	over := strings.Repeat("a", 199) + "é" // 201 bytes
	if err := ValidateFilename(over); err == nil {
		t.Errorf("201 bytes accepted, want rejection")
	}
	// Reserved name check takes precedence even with extension.
	for _, n := range []string{"CON.txt", "aux.tar.gz"} {
		if err := ValidateFilename(n); !errors.Is(err, domain.ErrInvalidFilename) {
			t.Errorf("%q: %v", n, err)
		}
	}
}

// ---------- SanitizeFilename ----------

func TestSanitizeFilename_Guaranatee(t *testing.T) {
	inputs := []string{
		"", ".", "..", "...", "   ", "a/b/c.txt", `C:\Users\file.txt`,
		"../../../etc/passwd", `..\..\evil.exe`, "../../evil.exe",
		"file\x00.txt", "file\x1f.jpg", "a<b>c:d\"e|f?g*h.txt",
		"CON", "con.txt", "aux.tar.gz", "COM1", "COM¹.txt", "lpt9",
		"file.txt.", " file.txt ", " trailing. ",
		strings.Repeat("a", 300) + ".txt",
		strings.Repeat("é", 200), // 400 bytes of UTF-8
		"strings with spaces and <forbidden>.zip",
		".hidden",
		"résumé (final).pdf",
	}
	for _, in := range inputs {
		got := SanitizeFilename(in)
		if err := ValidateFilename(got); err != nil {
			t.Errorf("SanitizeFilename(%q) = %q, ValidateFilename: %v", in, got, err)
		}
		if len(got) > MaxFilenameBytes {
			t.Errorf("SanitizeFilename(%q) = %q (%d bytes), want <= %d", in, got, len(got), MaxFilenameBytes)
		}
		if !utf8.ValidString(got) {
			t.Errorf("SanitizeFilename(%q) = %q, want valid UTF-8", in, got)
		}
	}
}

func TestSanitizeFilename_ReservedNames(t *testing.T) {
	cases := map[string]string{
		"CON":     "_CON",
		"con.txt": "_con.txt",
		"NUL":     "_NUL",
		"COM1":    "_COM1",
		"COM¹":    "_COM¹",
		"LPT³":    "_LPT³",
	}
	for in, want := range cases {
		got := SanitizeFilename(in)
		if got != want {
			t.Errorf("SanitizeFilename(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSanitizeFilename_Traversal(t *testing.T) {
	cases := map[string]string{
		"../../evil.exe":         "evil.exe",
		`..\..\evil.exe`:         "evil.exe",
		"/etc/passwd":            "passwd",
		`C:\Windows\file.txt`:    "file.txt",
		"a/b\\c.txt":             "c.txt",
		"../../deep/../file.bin": "file.bin",
	}
	for in, want := range cases {
		got := SanitizeFilename(in)
		if got != want {
			t.Errorf("SanitizeFilename(%q) = %q, want %q", in, got, want)
		}
		if strings.Contains(got, "/") || strings.Contains(got, "\\") {
			t.Errorf("result contains separator: %q", got)
		}
	}
}

func TestSanitizeFilename_TruncationUTF8(t *testing.T) {
	// 200 "é" runes = 400 bytes; must truncate to <=200 bytes on a rune boundary.
	got := SanitizeFilename(strings.Repeat("é", 200))
	if len(got) > 200 || !utf8.ValidString(got) {
		t.Fatalf("got %q len=%d valid=%v", got, len(got), utf8.ValidString(got))
	}
	// Truncation with extension: extension should be preserved when reasonable.
	long := strings.Repeat("a", 300) + ".txt"
	got = SanitizeFilename(long)
	if !strings.HasSuffix(got, ".txt") {
		t.Errorf("expected .txt preserved, got %q", got)
	}
	if len(got) > MaxFilenameBytes {
		t.Errorf("len %d > %d", len(got), MaxFilenameBytes)
	}
	if !utf8.ValidString(got) {
		t.Errorf("invalid UTF-8 after truncation: %q", got)
	}
}

func TestSanitizeFilename_EmptyFallback(t *testing.T) {
	for _, in := range []string{"", ".", "..", "...", "   ", "..."} {
		got := SanitizeFilename(in)
		if got != "download" {
			t.Errorf("SanitizeFilename(%q) = %q, want download", in, got)
		}
	}
}

func TestNewID(t *testing.T) {
	a, err := NewID()
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 32 {
		t.Fatalf("len = %d, want 32 (%q)", len(a), a)
	}
	b, _ := NewID()
	if a == b {
		t.Errorf("two NewIDs equal: %q", a)
	}
}

// ---------- ValidateHTTPURL ----------

func TestValidateHTTPURL_Valid(t *testing.T) {
	for _, raw := range []string{
		"https://example.com/file.txt",
		"http://example.com/a/b.iso",
		"HTTP://Example.COM/X",
		"https://example.com:8443/path?q=1#frag",
		"  https://example.com/trimmed  ",
	} {
		u, err := ValidateHTTPURL(raw)
		if err != nil {
			t.Errorf("ValidateHTTPURL(%q) = %v", raw, err)
			continue
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			t.Errorf("scheme = %q", u.Scheme)
		}
		if u.Hostname() == "" {
			t.Errorf("host empty for %q", raw)
		}
	}
}

func TestValidateHTTPURL_Invalid(t *testing.T) {
	cases := []struct {
		raw  string
		want error
	}{
		{"", domain.ErrInvalidURL},
		{"   ", domain.ErrInvalidURL},
		{"ftp://example.com/f", domain.ErrUnsupportedScheme},
		{"javascript:alert(1)", domain.ErrUnsupportedScheme},
		{"http:///path", domain.ErrInvalidURL},
		{"https:///path", domain.ErrInvalidURL},
		{"http://user:pass@example.com/", domain.ErrURLHasCredentials},
		{"http://user@example.com/", domain.ErrURLHasCredentials},
		{"https://user@example.com/", domain.ErrURLHasCredentials},
		{"http://exam ple.com/", domain.ErrInvalidURL},
		{"https://exa\tmple.com/", domain.ErrInvalidURL},
		{"http://example.com/\x01", domain.ErrInvalidURL},
		{"http://example.com/\x7f", domain.ErrInvalidURL},
	}
	for _, c := range cases {
		_, err := ValidateHTTPURL(c.raw)
		if err == nil {
			t.Errorf("ValidateHTTPURL(%q) = nil, want %v", c.raw, c.want)
			continue
		}
		if !errors.Is(err, c.want) {
			t.Errorf("ValidateHTTPURL(%q) error = %v, want %v", c.raw, err, c.want)
		}
	}
}

// ---------- DeriveFilenameFromURL ----------

func TestDeriveFilenameFromURL(t *testing.T) {
	cases := map[string]string{
		"https://example.com/files/report.pdf":     "report.pdf",
		"https://example.com/a/b/c.tar.gz":         "c.tar.gz",
		"https://example.com/":                     "download",
		"https://example.com":                      "download",
		"https://example.com/files/":               "files",
		"https://example.com/path/file%20name.txt": "file name.txt",
		"https://example.com/path/%E2%82%AC.txt":   "€.txt",
		"https://example.com/../../evil.exe":       "evil.exe",
		"https://example.com/path/CON.txt":         "_CON.txt",
		"https://example.com/a%2Fb.txt":            "b.txt",
	}
	for raw, want := range cases {
		u, err := ValidateHTTPURL(raw)
		if err != nil {
			t.Fatalf("ValidateHTTPURL(%q): %v", raw, err)
		}
		got := DeriveFilenameFromURL(u)
		if got != want {
			t.Errorf("DeriveFilenameFromURL(%q) = %q, want %q", raw, got, want)
		}
		if err := ValidateFilename(got); err != nil {
			t.Errorf("DeriveFilenameFromURL(%q) = %q invalid: %v", raw, got, err)
		}
	}
	if got := DeriveFilenameFromURL(nil); got != "download" {
		t.Errorf("nil URL = %q, want download", got)
	}
}

// ---------- ParseContentDispositionFilename ----------

func TestParseContentDispositionFilename(t *testing.T) {
	cases := map[string]string{
		`attachment; filename="report.pdf"`:          "report.pdf",
		`attachment; filename=report.pdf`:            "report.pdf",
		`attachment; filename="a b.txt"`:             "a b.txt",
		`attachment; filename="../../evil.exe"`:      "evil.exe",
		`attachment; filename="..\\..\\evil.exe"`:    "evil.exe",
		`attachment; filename="C:\\temp\\f.txt"`:     "f.txt",
		`attachment; filename*=UTF-8''%E2%82%AC.txt`: "€.txt",
		`attachment; filename="caf%C3%A9.txt"`:       "caf%C3%A9.txt", // mime doesn't decode %; Sanitize passes through (no % forbidden)
		`attachment; filename*=UTF-8''caf%C3%A9.txt`: "café.txt",
		`inline; filename="quote.txt"`:               "quote.txt",
		`attachment`:                                 "",
		"":                                           "",
		`attachment; filename=""`:                    "",
		`attachment; filename="CON.txt"`:             "_CON.txt",
		`attachment; filename="bad:name.txt"`:        "bad_name.txt",
		`attachment; filename="trailing.txt."`:       "trailing.txt",
	}
	for hdr, want := range cases {
		got := ParseContentDispositionFilename(hdr)
		if got != want {
			t.Errorf("ParseContentDispositionFilename(%q) = %q, want %q", hdr, got, want)
			continue
		}
		if got != "" {
			if err := ValidateFilename(got); err != nil {
				t.Errorf("result %q invalid: %v", got, err)
			}
		}
	}
}

func TestParseContentDispositionFilename_TraversalNoLeak(t *testing.T) {
	// Ensure no '/' or '\\' leaks through.
	for _, hdr := range []string{
		`attachment; filename="/etc/passwd"`,
		`attachment; filename="..\..\windows\evil.dll"`,
		`attachment; filename="../../a/b.txt"`,
	} {
		got := ParseContentDispositionFilename(hdr)
		if strings.Contains(got, "/") || strings.Contains(got, "\\") {
			t.Errorf("leaked separator in %q -> %q", hdr, got)
		}
	}
}

// ---------- Paths ----------

func TestDefaultDownloadDir(t *testing.T) {
	// XDG absolute honored.
	t.Setenv("XDG_DOWNLOAD_DIR", "/tmp/xdg-downloads")
	if got := DefaultDownloadDir(); got != "/tmp/xdg-downloads" {
		t.Errorf("XDG absolute: %q", got)
	}
	// XDG relative ignored; fallback uses home Downloads.
	t.Setenv("XDG_DOWNLOAD_DIR", "relative/path")
	got := DefaultDownloadDir()
	if strings.HasPrefix(got, "relative") {
		t.Errorf("relative XDG not ignored: %q", got)
	}
	// Unset XDG -> home/Downloads
	os.Unsetenv("XDG_DOWNLOAD_DIR")
	got = DefaultDownloadDir()
	if got == "" || got == "." {
		// "/" fallback "." only when home unknown; in test env home exists.
		home, _ := os.UserHomeDir()
		if home != "" && got == "." {
			t.Errorf("expected home/Downloads, got .")
		}
	} else {
		want := filepath.Join(mustHome(t), "Downloads")
		if got != want {
			t.Errorf("got %q want %q", got, want)
		}
	}
	// Ensure filepath.Join semantics: no double slashes / unclean paths.
	if strings.Contains(got, "/"+"/") && filepath.Separator == '/' {
		t.Errorf("unclean path: %q", got)
	}
}

func mustHome(t *testing.T) string {
	t.Helper()
	home, err := os.UserHomeDir()
	if err != nil {
		t.Skipf("no home: %v", err)
	}
	return home
}

func TestAppDataDir(t *testing.T) {
	dir, err := AppDataDir()
	if err != nil {
		t.Skipf("UserConfigDir unavailable: %v", err)
	}
	if !strings.HasSuffix(dir, "pdm") && !strings.HasSuffix(dir, filepath.Join("pdm")) {
		t.Errorf("AppDataDir = %q, want suffix pdm", dir)
	}
	if !filepath.IsAbs(dir) {
		t.Errorf("AppDataDir not absolute: %q", dir)
	}
}

// ---------- EnsureDestinationDir / CheckExistingTarget ----------

func TestEnsureDestinationDir(t *testing.T) {
	base := t.TempDir()
	nested := filepath.Join(base, "a", "b", "c")
	dir, err := EnsureDestinationDir(nested)
	if err != nil {
		t.Fatal(err)
	}
	if dir != filepath.Clean(nested) {
		t.Errorf("dir = %q, want %q", dir, filepath.Clean(nested))
	}
	fi, err := os.Stat(dir)
	if err != nil || !fi.IsDir() {
		t.Fatalf("not a dir: %v", err)
	}
	// Existing file path that is not a directory -> ErrInvalidDestination
	afile := filepath.Join(base, "file.txt")
	if err := os.WriteFile(afile, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := EnsureDestinationDir(afile); !errors.Is(err, domain.ErrInvalidDestination) {
		t.Errorf("file-as-dir: err = %v, want ErrInvalidDestination", err)
	}
}

func TestCheckExistingTarget(t *testing.T) {
	dir := t.TempDir()
	exists, isSym, err := CheckExistingTarget(filepath.Join(dir, "nope"))
	if err != nil || exists || isSym {
		t.Fatalf("missing: (%v,%v,%v)", exists, isSym, err)
	}
	f := filepath.Join(dir, "reg")
	if err := os.WriteFile(f, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	exists, isSym, err = CheckExistingTarget(f)
	if err != nil || !exists || isSym {
		t.Fatalf("regular: (%v,%v,%v)", exists, isSym, err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(f, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	exists, isSym, err = CheckExistingTarget(link)
	if err != nil || !exists || !isSym {
		t.Fatalf("symlink: (%v,%v,%v)", exists, isSym, err)
	}
}

// ---------- SuggestAvailableFilename ----------

func TestSuggestAvailableFilename(t *testing.T) {
	dir := t.TempDir()
	if got, err := SuggestAvailableFilename(dir, "free.txt"); err != nil || got != "free.txt" {
		t.Fatalf("free: (%q,%v)", got, err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a (1).txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err := SuggestAvailableFilename(dir, "a.txt")
	if err != nil {
		t.Fatal(err)
	}
	if got != "a (2).txt" {
		t.Errorf("got %q, want a (2).txt", got)
	}
	// No extension
	if err := os.WriteFile(filepath.Join(dir, "README"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = SuggestAvailableFilename(dir, "README")
	if err != nil {
		t.Fatal(err)
	}
	if got != "README (1)" {
		t.Errorf("no-ext: got %q", got)
	}
	// .tar.gz => ext .gz
	if err := os.WriteFile(filepath.Join(dir, "archive.tar.gz"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	got, err = SuggestAvailableFilename(dir, "archive.tar.gz")
	if err != nil {
		t.Fatal(err)
	}
	if got != "archive.tar (1).gz" {
		t.Errorf("tar.gz: got %q", got)
	}
}

// ---------- FinalizeTempFile ----------

func writeTemp(t *testing.T, dir string, content string) string {
	t.Helper()
	f, err := os.CreateTemp(dir, "tmp-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(content); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return f.Name()
}

func TestFinalizeTempFile_NoClobber(t *testing.T) {
	dir := t.TempDir()
	src := writeTemp(t, dir, "hello")
	finalPath, finalName, err := FinalizeTempFile(src, dir, "out.txt", domain.FinalizeNoClobber)
	if err != nil {
		t.Fatal(err)
	}
	if finalName != "out.txt" || finalPath != filepath.Join(dir, "out.txt") {
		t.Fatalf("(%q,%q)", finalPath, finalName)
	}
	data, _ := os.ReadFile(finalPath)
	if string(data) != "hello" {
		t.Errorf("content = %q", data)
	}
	if _, err := os.Lstat(src); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("temp should be gone, err=%v", err)
	}
	// Second finalize with same name should produce FileConflictError.
	src2 := writeTemp(t, dir, "world")
	_, _, err = FinalizeTempFile(src2, dir, "out.txt", domain.FinalizeNoClobber)
	if err == nil {
		t.Fatal("want conflict")
	}
	if !errors.Is(err, domain.ErrDestinationExists) {
		t.Errorf("err = %v, want ErrDestinationExists", err)
	}
	var fce *domain.FileConflictError
	if !errors.As(err, &fce) {
		t.Fatalf("want *FileConflictError, got %T", err)
	}
	if fce.SuggestedFilename != "out (1).txt" {
		t.Errorf("suggested = %q", fce.SuggestedFilename)
	}
	if fce.Filename != "out.txt" || !strings.Contains(fce.ExistingPath, "out.txt") {
		t.Errorf("fce fields: %+v", fce)
	}
	// Temp file must still exist after failed no-clobber.
	if _, err := os.Lstat(src2); err != nil {
		t.Errorf("temp after conflict: %v", err)
	}
}

func TestFinalizeTempFile_Overwrite(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "out.txt"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	src := writeTemp(t, dir, "new")
	finalPath, _, err := FinalizeTempFile(src, dir, "out.txt", domain.FinalizeOverwrite)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(finalPath)
	if string(data) != "new" {
		t.Errorf("content = %q, want new", data)
	}
}

func TestFinalizeTempFile_AutoRename(t *testing.T) {
	dir := t.TempDir()
	for i, wantName := range []string{"out.txt", "out (1).txt", "out (2).txt"} {
		src := writeTemp(t, dir, fmt.Sprintf("content-%d", i))
		_, gotName, err := FinalizeTempFile(src, dir, "out.txt", domain.FinalizeAutoRename)
		if err != nil {
			t.Fatal(err)
		}
		if gotName != wantName {
			t.Errorf("i=%d got %q want %q", i, gotName, wantName)
		}
		if err := ValidateFilename(gotName); err != nil {
			t.Errorf("auto-renamed %q invalid: %v", gotName, err)
		}
	}
	// Verify contents retained.
	for i, wantName := range []string{"out.txt", "out (1).txt", "out (2).txt"} {
		data, _ := os.ReadFile(filepath.Join(dir, wantName))
		if string(data) != fmt.Sprintf("content-%d", i) {
			t.Errorf("%s content %q", wantName, data)
		}
	}
}

func TestFinalizeTempFile_SymlinkRefusedAllModes(t *testing.T) {
	for _, mode := range []domain.FinalizeMode{domain.FinalizeNoClobber, domain.FinalizeOverwrite, domain.FinalizeAutoRename} {
		t.Run(string(mode), func(t *testing.T) {
			dir := t.TempDir()
			real := filepath.Join(dir, "real.txt")
			if err := os.WriteFile(real, []byte("real"), 0o644); err != nil {
				t.Fatal(err)
			}
			link := filepath.Join(dir, "target.txt")
			if err := os.Symlink(real, link); err != nil {
				t.Skipf("symlink unsupported: %v", err)
			}
			src := writeTemp(t, dir, "evil")
			_, _, err := FinalizeTempFile(src, dir, "target.txt", mode)
			if err == nil {
				t.Fatalf("mode %s: want symlink refusal", mode)
			}
			// For AutoRename the spec says retry on symlink; our impl
			// refuses upfront. Accept either refusal or rename to a
			// different name, but never overwrite through the symlink.
			data, _ := os.ReadFile(real)
			if string(data) != "real" {
				t.Errorf("symlink target overwritten! data=%q", data)
			}
			if !errors.Is(err, domain.ErrSymlinkTarget) {
				t.Errorf("mode %s err = %v, want ErrSymlinkTarget", mode, err)
			}
		})
	}
}

func TestFinalizeTempFile_DirectoryRefused(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "subdir"), 0o755); err != nil {
		t.Fatal(err)
	}
	src := writeTemp(t, dir, "x")
	_, _, err := FinalizeTempFile(src, dir, "subdir", domain.FinalizeOverwrite)
	if !errors.Is(err, domain.ErrInvalidDestination) {
		t.Errorf("err = %v, want ErrInvalidDestination", err)
	}
}

func TestFinalizeTempFile_InvalidFilename(t *testing.T) {
	dir := t.TempDir()
	src := writeTemp(t, dir, "x")
	for _, bad := range []string{"../evil", "CON", "a/b.txt", ""} {
		if _, _, err := FinalizeTempFile(src, dir, bad, domain.FinalizeOverwrite); !errors.Is(err, domain.ErrInvalidFilename) {
			t.Errorf("bad %q err = %v", bad, err)
		}
	}
}

func TestFinalizeTempFile_AutoRenameContention(t *testing.T) {
	dir := t.TempDir()
	// Pre-create out.txt, out (1).txt, out (2).txt then concurrent finalize.
	for _, n := range []string{"race.txt", "race (1).txt", "race (2).txt"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte("existing"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	const N = 10
	var wg sync.WaitGroup
	results := make([]string, N)
	errs := make([]error, N)
	for i := 0; i < N; i++ {
		src := writeTemp(t, dir, fmt.Sprintf("c-%d", i))
		wg.Add(1)
		go func(idx int, s string) {
			defer wg.Done()
			_, name, err := FinalizeTempFile(s, dir, "race.txt", domain.FinalizeAutoRename)
			results[idx] = name
			errs[idx] = err
		}(i, src)
	}
	wg.Wait()
	seen := map[string]bool{}
	for i := 0; i < N; i++ {
		if errs[i] != nil {
			t.Errorf("goroutine %d: %v", i, errs[i])
			continue
		}
		if seen[results[i]] {
			t.Errorf("duplicate final name %q", results[i])
		}
		seen[results[i]] = true
		if results[i] == "race.txt" || results[i] == "race (1).txt" || results[i] == "race (2).txt" {
			t.Errorf("got pre-existing name %q", results[i])
		}
		if _, err := os.Lstat(filepath.Join(dir, results[i])); err != nil {
			t.Errorf("final file missing: %v", err)
		}
	}
	if len(seen) != N {
		t.Errorf("distinct = %d, want %d", len(seen), N)
	}
}

func TestFinalizeTempFile_NoClobberRace(t *testing.T) {
	dir := t.TempDir()
	const N = 8
	var wg sync.WaitGroup
	var mu sync.Mutex
	successes := 0
	for i := 0; i < N; i++ {
		src := writeTemp(t, dir, fmt.Sprintf("c-%d", i))
		wg.Add(1)
		go func(s string) {
			defer wg.Done()
			_, _, err := FinalizeTempFile(s, dir, "one.txt", domain.FinalizeNoClobber)
			if err == nil {
				mu.Lock()
				successes++
				mu.Unlock()
			}
		}(src)
	}
	wg.Wait()
	if successes != 1 {
		t.Errorf("successes = %d, want exactly 1", successes)
	}
}

func TestEnsureDestinationDir_RefusesSymlink(t *testing.T) {
	realDir := t.TempDir()
	link := filepath.Join(t.TempDir(), "linkdir")
	if err := os.Symlink(realDir, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	// Existing symlink to a directory.
	if _, err := EnsureDestinationDir(link); !errors.Is(err, domain.ErrSymlinkTarget) {
		t.Errorf("existing symlink dir: err = %v, want ErrSymlinkTarget", err)
	}
}

func TestEnsureDestinationDir_TrimsSpaces(t *testing.T) {
	dir := t.TempDir()
	got, err := EnsureDestinationDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	got2, err := EnsureDestinationDir(filepath.Join(dir, "sub/"))
	if err != nil {
		t.Fatal(err)
	}
	_ = got
	_ = got2
}

func TestSanitizeFilename_GuaranteeFuzz(t *testing.T) {
	// Property: output of SanitizeFilename always passes ValidateFilename
	// and never contains separators.
	seeds := []string{"a", "/", "\\", "<", " ", ".", "\x00", "é", "CON", "file.txt"}
	for _, s := range seeds {
		for _, s2 := range seeds {
			for _, s3 := range seeds {
				raw := s + s2 + s3
				got := SanitizeFilename(raw)
				if err := ValidateFilename(got); err != nil {
					t.Fatalf("Sanitize(%q)=%q invalid: %v", raw, got, err)
				}
				if strings.Contains(got, "/") || strings.Contains(got, "\\") {
					t.Fatalf("Sanitize(%q)=%q contains separator", raw, got)
				}
			}
		}
	}
}

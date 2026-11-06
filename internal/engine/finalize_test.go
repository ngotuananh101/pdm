// Finalize tests (Stage 3, Phase 4.2, TDD).
//
// Finalize promotes a finished .part file to its destination: it
// syncs the file, verifies its size against the expected total, and
// renames it with the no-clobber mechanism. A size mismatch or a
// rename failure must fail the job WITHOUT renaming, and the .part
// file must stay intact for retry/conflict resolution. completed is
// only ever recorded by the caller, after Finalize succeeds.
package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

func TestFinalize_SizeMatchesRenamesAndHashes(t *testing.T) {
	content := []byte("finalize-me-0123456789")
	dir := t.TempDir()
	partPath := filepath.Join(dir, ".file.bin.f1.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	finalPath, finalName, err := Finalize(partPath, dir, "file.bin", domain.FinalizeNoClobber, int64(len(content)))
	if err != nil {
		t.Fatalf("Finalize: %v", err)
	}
	if finalPath != filepath.Join(dir, "file.bin") {
		t.Fatalf("FinalPath = %q, want %q", finalPath, filepath.Join(dir, "file.bin"))
	}
	if finalName != "file.bin" {
		t.Fatalf("FinalName = %q, want %q", finalName, "file.bin")
	}
	if _, err := os.Stat(partPath); !os.IsNotExist(err) {
		t.Fatalf(".part still exists after finalize (err = %v)", err)
	}
	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatal(err)
	}
	want := sha256.Sum256(content)
	if got := sha256.Sum256(data); got != want {
		t.Fatalf("final SHA-256 = %s, want %s", hex.EncodeToString(got[:]), hex.EncodeToString(want[:]))
	}
}

func TestFinalize_SizeMismatchFailsWithoutRename(t *testing.T) {
	content := []byte("short")
	dir := t.TempDir()
	partPath := filepath.Join(dir, ".file.bin.f2.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}

	_, _, err := Finalize(partPath, dir, "file.bin", domain.FinalizeNoClobber, 100)
	if err == nil {
		t.Fatal("Finalize with wrong size: expected error, got nil")
	}
	if !isSizeMismatch(err) {
		t.Fatalf("Finalize error = %v, want a size mismatch error", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "file.bin")); !os.IsNotExist(err) {
		t.Fatal("destination file appeared after a failed finalize")
	}
	// The .part is untouched, so the job can be retried.
	if got, err := os.ReadFile(partPath); err != nil || string(got) != string(content) {
		t.Fatalf(".part after failed finalize = %q (err %v), want the original bytes", got, err)
	}
}

func TestFinalize_RenameConflictFailsAndKeepsPart(t *testing.T) {
	content := []byte("conflict-bytes")
	dir := t.TempDir()
	partPath := filepath.Join(dir, ".file.bin.f3.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	// A file already occupies the destination; NoClobber must refuse.
	if err := os.WriteFile(filepath.Join(dir, "file.bin"), []byte("existing"), 0o644); err != nil {
		t.Fatal(err)
	}

	_, _, err := Finalize(partPath, dir, "file.bin", domain.FinalizeNoClobber, int64(len(content)))
	if err == nil {
		t.Fatal("Finalize onto an existing destination: expected error, got nil")
	}
	if got, err := os.ReadFile(partPath); err != nil || string(got) != string(content) {
		t.Fatalf(".part after failed finalize = %q (err %v), want the original bytes", got, err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "file.bin")); err != nil || string(got) != "existing" {
		t.Fatalf("destination after failed finalize = %q (err %v), want the pre-existing file untouched", got, err)
	}
}

func TestFinalize_MissingPartFails(t *testing.T) {
	dir := t.TempDir()
	_, _, err := Finalize(filepath.Join(dir, ".missing.part"), dir, "file.bin", domain.FinalizeNoClobber, 10)
	if err == nil {
		t.Fatal("Finalize with a missing .part: expected error, got nil")
	}
}

func TestFinalize_UnknownTotalSkipsSizeCheck(t *testing.T) {
	// A single-stream download with no declared length (total -1)
	// has nothing to verify against; finalize still renames.
	content := []byte("unknown-length")
	dir := t.TempDir()
	partPath := filepath.Join(dir, ".file.bin.f5.part")
	if err := os.WriteFile(partPath, content, 0o600); err != nil {
		t.Fatal(err)
	}
	finalPath, _, err := Finalize(partPath, dir, "file.bin", domain.FinalizeNoClobber, -1)
	if err != nil {
		t.Fatalf("Finalize with unknown total: %v", err)
	}
	if got, err := os.ReadFile(finalPath); err != nil || string(got) != string(content) {
		t.Fatalf("final file = %q (err %v), want the original bytes", got, err)
	}
}

// isSizeMismatch reports whether err is (or wraps) a size mismatch.
func isSizeMismatch(err error) bool {
	for err != nil {
		if err == domain.ErrSizeMismatch {
			return true
		}
		type unwrapper interface{ Unwrap() error }
		u, ok := err.(unwrapper)
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

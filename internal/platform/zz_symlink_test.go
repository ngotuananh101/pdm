package platform

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestSymlinkDestDir(t *testing.T) {
	base := t.TempDir()
	real := t.TempDir()
	link := filepath.Join(base, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Skip(err)
	}
	cleaned, err := EnsureDestinationDir(link)
	fmt.Printf("cleaned=%q err=%v\n", cleaned, err)
	if err == nil {
		// attempt to write a file inside
		f := filepath.Join(cleaned, "evil.txt")
		if werr := os.WriteFile(f, []byte("x"), 0o644); werr != nil {
			t.Fatal(werr)
		}
		if _, err := os.Stat(filepath.Join(real, "evil.txt")); err == nil {
			t.Fatalf("wrote through symlink to real dir!")
		}
	}
}

// Snapshot coverage for the Stage 3 validator fields added in Phase 2
// (decision A): DownloadSnapshot must carry ETag / LastModified /
// SupportsRange like every other persisted field.
package domain

import (
	"testing"
	"time"
)

func TestDownloadSnapshotCarriesValidators(t *testing.T) {
	now := time.Date(2026, 11, 6, 8, 0, 0, 0, time.UTC)
	d := &Download{
		ID:            "id-1",
		URL:           "https://example.com/f.bin",
		Status:        StatusQueued,
		TotalBytes:    -1,
		ETag:          `"abc123"`,
		LastModified:  "Wed, 05 Nov 2026 12:00:00 GMT",
		SupportsRange: true,
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	snap := d.Snapshot()
	if snap.ETag != `"abc123"` {
		t.Errorf("snapshot ETag = %q, want %q", snap.ETag, `"abc123"`)
	}
	if snap.LastModified != "Wed, 05 Nov 2026 12:00:00 GMT" {
		t.Errorf("snapshot LastModified = %q", snap.LastModified)
	}
	if !snap.SupportsRange {
		t.Errorf("snapshot SupportsRange = false, want true")
	}
	// Snapshot is a copy: mutating the entity must not affect it.
	d.ETag = `"changed"`
	d.SupportsRange = false
	if snap.ETag != `"abc123"` || !snap.SupportsRange {
		t.Errorf("snapshot aliases entity validators: %+v", snap)
	}
}

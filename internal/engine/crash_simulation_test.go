// Crash simulation tests, Stage 3 level (Phase 6.2, TDD).
//
// These tests simulate kills at the points ARCH §13 lists — after a
// write before the sync, after the sync before the DB commit, after
// the commit, and around finalize — plus the disk-full / write-error
// class. Every case must leave the destination file untouched, never
// join bytes from different versions, and land the output byte-for-byte
// (SHA-256) equal to the fixture. The checkpoint-order cases at the
// Checkpointer level are already covered by checkpoint_test.go
// (CommitsOnlySyncedBytes, SyncErrorStopsJobWithoutAdvancing,
// CommitErrorKeepsDirtyForRetry); the tests here cover the same crash
// points end to end through the manager and the worker.
package engine

import (
	"context"
	"errors"
	"fmt"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// TestManager_ResumeAtMultipleOffsets seeds a stored segment plan at
// several committed offsets (one segment complete, half the file,
// three quarters, a half-done middle or last segment, a mixed plan)
// and runs the manager over each: only the uncommitted remainder may
// be requested, complete segments are never re-requested, and the
// finished .part must hash exactly like the fixture.
func TestManager_ResumeAtMultipleOffsets(t *testing.T) {
	segSize := int64(managerTestSize / 4)
	cases := []struct {
		name      string
		committed []int64
	}{
		{"first segment complete", []int64{segSize, 0, 0, 0}},
		{"first half complete", []int64{segSize, segSize, 0, 0}},
		{"first three complete", []int64{segSize, segSize, segSize, 0}},
		{"middle segment half", []int64{0, segSize / 2, 0, 0}},
		{"last segment half", []int64{0, 0, 0, segSize / 2}},
		{"mixed complete and half", []int64{segSize, 0, segSize / 2, 0}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ms, srv, m, repo := newManagerTest(t, "range", managerTestSize)
			defer srv.Close()
			wantHash := sha256Hex(t, ms.content)

			dest := t.TempDir()
			segs := domain.SplitSegments(managerTestSize, 4, 1024)
			for i := range segs {
				segs[i].CommittedBytes = tc.committed[i]
				switch {
				case tc.committed[i] == segs[i].Size():
					segs[i].Status = domain.SegmentComplete
				case tc.committed[i] > 0:
					segs[i].Status = domain.SegmentRunning
				default:
					segs[i].Status = domain.SegmentPending
				}
			}
			seedPartialPart(t, ms, repo, "r-off", dest, segs)

			res, err := m.Run(context.Background(), managerReq("r-off", srv.URL+"/file.bin", dest))
			if err != nil {
				t.Fatalf("Run (resume): %v", err)
			}
			if !res.UsedSegments {
				t.Fatal("UsedSegments = false, want true")
			}
			if got := sha256File(t, res.TempPath); got != wantHash {
				t.Fatalf(".part SHA-256 = %s, want %s", got, wantHash)
			}

			// Exactly the uncommitted remainder was requested:
			// a complete segment's full range never appears, and
			// a partial segment is requested once, from its
			// committed offset — never from its start.
			ranges := ms.rangedRequests()
			wantRequests := 0
			for i, s := range segs {
				full := fmt.Sprintf("bytes=%d-%d", s.StartOffset, s.EndOffset)
				if tc.committed[i] == s.Size() {
					if n := ranges[full]; n != 0 {
						t.Fatalf("complete segment %d requested %d times, want 0", i, n)
					}
					continue
				}
				wantRequests++
				remainder := fmt.Sprintf("bytes=%d-%d", s.StartOffset+tc.committed[i], s.EndOffset)
				if n := ranges[remainder]; n != 1 {
					t.Fatalf("segment %d remainder %q requested %d times, want 1", i, remainder, n)
				}
				if tc.committed[i] > 0 {
					if n := ranges[full]; n != 0 {
						t.Fatalf("half-done segment %d requested from its start %d times, want 0", i, n)
					}
				}
			}
			if len(ranges) != wantRequests {
				t.Fatalf("ranged requests = %v, want %d ranges (one per incomplete segment)", ranges, wantRequests)
			}

			stored, err := repo.GetByDownload(context.Background(), "r-off")
			if err != nil {
				t.Fatalf("GetByDownload: %v", err)
			}
			for i, s := range stored {
				if s.Status != domain.SegmentComplete {
					t.Fatalf("segment[%d].Status = %q, want complete", i, s.Status)
				}
				if s.CommittedBytes != s.Size() {
					t.Fatalf("segment[%d].CommittedBytes = %d, want %d", i, s.CommittedBytes, s.Size())
				}
			}
		})
	}
}

// TestManager_CommitFailureKeepsNothingCommitted simulates a kill
// after the file sync but before the DB commit, end to end: every
// UpdateCommitted fails while the workers still write bytes, so the
// run must fail and storage must trust nothing (committed stays 0).
// Once storage recovers, a fresh run re-fetches everything the first
// run wrote — the unrecorded bytes are never trusted — and still
// produces the exact fixture bytes.
func TestManager_CommitFailureKeepsNothingCommitted(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "range", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	req := managerReq("m-cf", srv.URL+"/file.bin", dest)

	repo.failUpdate = true
	if _, err := m.Run(context.Background(), req); err == nil {
		t.Fatal("Run with failing segment commits: expected error, got nil")
	}
	if got := repo.committedTotal("m-cf"); got != 0 {
		t.Fatalf("committed total after failed commits = %d, want 0 (storage must not claim bytes the DB never recorded)", got)
	}

	repo.failUpdate = false
	res, err := m.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("Run after storage recovers: %v", err)
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s", got, wantHash)
	}
	if got := repo.committedTotal("m-cf"); got != managerTestSize {
		t.Fatalf("committed total after recovery run = %d, want %d", got, managerTestSize)
	}
}

// TestManager_FullDiskWriteFailsFilesystemError simulates a full disk
// (ENOSPC via /dev/full) on the segmented path: the run must fail
// with a filesystem error, finalize nothing, and leave no destination
// file behind.
func TestManager_FullDiskWriteFailsFilesystemError(t *testing.T) {
	if _, err := os.Stat("/dev/full"); err != nil {
		t.Skipf("/dev/full unavailable: %v", err)
	}
	_, srv, m, _ := newManagerTest(t, "range", managerTestSize)
	defer srv.Close()

	dest := t.TempDir()
	req := managerReq("m-full", srv.URL+"/file.bin", dest)
	req.TempPath = "/dev/full"
	res, err := m.Run(context.Background(), req)
	if err == nil {
		t.Fatal("Run writing to a full disk: expected error, got nil")
	}
	var ce *CodedError
	if !errors.As(err, &ce) {
		t.Fatalf("expected CodedError, got %T: %v", err, err)
	}
	if ce.Code != string(domain.ErrorCodeFilesystem) {
		t.Fatalf("error code = %s, want filesystem_error; err=%v", ce.Code, err)
	}
	if res.FinalPath != "" {
		t.Fatalf("FinalPath = %q, want empty (nothing may finalize on a full disk)", res.FinalPath)
	}
	if _, err := os.Stat(filepath.Join(dest, "file.bin")); !os.IsNotExist(err) {
		t.Fatal("destination file appeared after a failed full-disk download")
	}
}

// TestWorker_WriteErrorFailsWithoutProgress simulates a write failure
// (read-only file handle: the write-permission class) inside a segment
// worker: the download must fail with a filesystem error, report zero
// committed bytes, never call onProgress, and leave the file exactly
// as it was — no partial byte is ever treated as downloaded.
func TestWorker_WriteErrorFailsWithoutProgress(t *testing.T) {
	ws := newWorkerServer("range")
	srv := httptest.NewServer(ws)
	defer srv.Close()

	path := filepath.Join(t.TempDir(), "part.bin")
	if err := os.WriteFile(path, make([]byte, workerTestSize), 0o600); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	progressCalls := 0
	cfg := testConfig()
	w := NewSegmentWorker(cfg, NewHTTPClient(cfg))
	seg := domain.Segment{Index: 0, StartOffset: 0, EndOffset: 49, Status: domain.SegmentPending}
	committed, err := w.DownloadSegment(context.Background(), f, workerTask(seg, srv.URL+"/file.bin"), func(c int64) {
		progressCalls++
	})
	if err == nil {
		t.Fatal("DownloadSegment with unwritable file: expected error, got nil")
	}
	var ce *CodedError
	if !errors.As(err, &ce) {
		t.Fatalf("expected CodedError, got %T: %v", err, err)
	}
	if ce.Code != string(domain.ErrorCodeFilesystem) {
		t.Fatalf("error code = %s, want filesystem_error; err=%v", ce.Code, err)
	}
	if committed != 0 {
		t.Fatalf("committed = %d, want 0 (a failed write commits nothing)", committed)
	}
	if progressCalls != 0 {
		t.Fatalf("onProgress called %d times, want 0 (no byte was written)", progressCalls)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range data {
		if b != 0 {
			t.Fatalf("byte[%d] = %d, want 0 (file must be unchanged after a failed write)", i, b)
		}
	}
}

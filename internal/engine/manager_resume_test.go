// Manager resume tests (Stage 3, Phase 4.1, TDD).
//
// A second Run for the same download ID must resume from the segment
// plan already stored in the repository instead of downloading from
// scratch: complete segments are never re-requested, a segment that
// was running in the previous session resumes from its committed
// bytes, and only the remaining bytes are fetched. The .part file
// must not be truncated on resume.
package engine

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/ngotuananh101/pdm/internal/domain"
)

// seedPartialPart creates the .part file for id in dest with the
// correct server bytes for the committed regions of segs, and stores
// the plan in repo. The content comes from ms.content.
func seedPartialPart(t *testing.T, ms *managerServer, repo *fakeSegmentRepo, id, dest string, segs []domain.Segment) string {
	t.Helper()
	if err := repo.InsertPlan(context.Background(), id, segs); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}
	partPath := filepath.Join(dest, ".file.bin."+id+".part")
	f, err := os.OpenFile(partPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		t.Fatalf("create .part: %v", err)
	}
	for _, s := range segs {
		if s.CommittedBytes <= 0 {
			continue
		}
		if _, err := f.WriteAt(ms.content[s.StartOffset:s.StartOffset+s.CommittedBytes], s.StartOffset); err != nil {
			t.Fatalf("seed .part segment %d: %v", s.Index, err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatalf("close .part: %v", err)
	}
	return partPath
}

func TestManager_ResumeFromCommittedPlan(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "range", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	// 100000 bytes / 4 segments of 25000: seg0 complete, seg1 half
	// (and still marked running from the previous session), seg2/3
	// untouched.
	segs := domain.SplitSegments(managerTestSize, 4, 1024)
	segs[0].CommittedBytes = segs[0].Size()
	segs[0].Status = domain.SegmentComplete
	segs[1].CommittedBytes = segs[1].Size() / 2
	segs[1].Status = domain.SegmentRunning
	seedPartialPart(t, ms, repo, "r1", dest, segs)

	res, err := m.Run(context.Background(), managerReq("r1", srv.URL+"/file.bin", dest))
	if err != nil {
		t.Fatalf("Run (resume): %v", err)
	}
	if !res.UsedSegments {
		t.Fatal("UsedSegments = false, want true")
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s", got, wantHash)
	}

	// Only the remaining bytes were requested: the complete segment
	// was never re-requested, and the half-done segment was
	// requested from its committed offset, not from its start.
	ranges := ms.rangedRequests()
	if n := ranges["bytes=0-24999"]; n != 0 {
		t.Fatalf("complete segment requested %d times, want 0 (must not re-download)", n)
	}
	if n := ranges["bytes=25000-49999"]; n != 0 {
		t.Fatalf("half-done segment requested from its start %d times, want 0 (must resume from committed bytes)", n)
	}
	if n := ranges["bytes=37500-49999"]; n != 1 {
		t.Fatalf("half-done segment remainder requested %d times, want 1", n)
	}
	if n := ranges["bytes=50000-74999"]; n != 1 {
		t.Fatalf("pending segment 2 requested %d times, want 1", n)
	}
	if n := ranges["bytes=75000-99999"]; n != 1 {
		t.Fatalf("pending segment 3 requested %d times, want 1", n)
	}

	stored, err := repo.GetByDownload(context.Background(), "r1")
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
}

func TestManager_ResumeAllSegmentsCompleteSkipsDownload(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "range", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	segs := domain.SplitSegments(managerTestSize, 4, 1024)
	for i := range segs {
		segs[i].CommittedBytes = segs[i].Size()
		segs[i].Status = domain.SegmentComplete
	}
	seedPartialPart(t, ms, repo, "r2", dest, segs)

	res, err := m.Run(context.Background(), managerReq("r2", srv.URL+"/file.bin", dest))
	if err != nil {
		t.Fatalf("Run (resume, all complete): %v", err)
	}
	if !res.UsedSegments {
		t.Fatal("UsedSegments = false, want true")
	}
	if res.WrittenBytes != managerTestSize || res.TotalBytes != managerTestSize {
		t.Fatalf("WrittenBytes/TotalBytes = %d/%d, want %d/%d", res.WrittenBytes, res.TotalBytes, managerTestSize, managerTestSize)
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s", got, wantHash)
	}
	if ranges := ms.rangedRequests(); len(ranges) != 0 {
		t.Fatalf("ranged requests = %v, want none (every segment already complete)", ranges)
	}
}

func TestManager_ResumeValidatorChangedRestartsFromScratch(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "range", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	segs := domain.SplitSegments(managerTestSize, 4, 1024)
	segs[0].CommittedBytes = segs[0].Size()
	segs[0].Status = domain.SegmentComplete
	segs[1].CommittedBytes = segs[1].Size() / 2
	segs[1].Status = domain.SegmentRunning
	seedPartialPart(t, ms, repo, "r3", dest, segs)

	// The stored validator ("old") no longer matches the server's
	// current ETag ("mv1"): the committed bytes belong to a different
	// resource version and must be discarded, not resumed.
	req := managerReq("r3", srv.URL+"/file.bin", dest)
	req.ExpectedETag = `"old"`
	req.ExpectedTotalBytes = managerTestSize
	res, err := m.Run(context.Background(), req)
	if err != nil {
		t.Fatalf("Run (validator changed): %v", err)
	}
	if !res.UsedSegments {
		t.Fatal("UsedSegments = false, want true")
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s (restart must not mix old-version bytes)", got, wantHash)
	}
	ranges := ms.rangedRequests()
	if n := ranges["bytes=0-24999"]; n != 1 {
		t.Fatalf("complete segment requested %d times, want 1 (stale plan discarded, full re-download)", n)
	}
	if n := ranges["bytes=25000-49999"]; n != 1 {
		t.Fatalf("half-done segment requested %d times, want 1 (restart from its start)", n)
	}
	if repo.deleteCalls < 1 {
		t.Fatalf("DeleteByDownload calls = %d, want >= 1 (stale plan removed before restart)", repo.deleteCalls)
	}
}

func TestManager_ResumeMissingPartRestartsFromScratch(t *testing.T) {
	ms, srv, m, repo := newManagerTest(t, "range", managerTestSize)
	defer srv.Close()
	wantHash := sha256Hex(t, ms.content)

	dest := t.TempDir()
	// A plan with committed bytes but no .part file on disk (the
	// file was deleted): there is nothing to resume from, so the
	// job restarts from scratch with a fresh plan.
	segs := domain.SplitSegments(managerTestSize, 4, 1024)
	segs[0].CommittedBytes = segs[0].Size()
	segs[0].Status = domain.SegmentComplete
	if err := repo.InsertPlan(context.Background(), "r4", segs); err != nil {
		t.Fatalf("InsertPlan: %v", err)
	}

	res, err := m.Run(context.Background(), managerReq("r4", srv.URL+"/file.bin", dest))
	if err != nil {
		t.Fatalf("Run (missing .part): %v", err)
	}
	if !res.UsedSegments {
		t.Fatal("UsedSegments = false, want true")
	}
	if got := sha256File(t, res.TempPath); got != wantHash {
		t.Fatalf(".part SHA-256 = %s, want %s", got, wantHash)
	}
	ranges := ms.rangedRequests()
	if n := ranges["bytes=0-24999"]; n != 1 {
		t.Fatalf("segment 0 requested %d times, want 1 (no .part to resume from)", n)
	}
}

// Tests for the segment entity and SplitSegments (Stage 3, Phase 1.2, TDD).
package domain

import "testing"

// assertCoverage checks that segs are disjoint, ordered, and cover
// exactly [0, total-1] with no overlap and no gap.
func assertCoverage(t *testing.T, segs []Segment, total int64) {
	t.Helper()
	var covered int64
	for i, s := range segs {
		if s.Index != i {
			t.Errorf("segment[%d].Index = %d, want %d", i, s.Index, i)
		}
		if s.StartOffset != covered {
			t.Errorf("segment[%d].StartOffset = %d, want %d (gap or overlap)", i, s.StartOffset, covered)
		}
		if s.EndOffset < s.StartOffset {
			t.Errorf("segment[%d] has empty range [%d, %d]", i, s.StartOffset, s.EndOffset)
		}
		if s.Status != SegmentPending {
			t.Errorf("segment[%d].Status = %q, want %q", i, s.Status, SegmentPending)
		}
		if s.CommittedBytes != 0 {
			t.Errorf("segment[%d].CommittedBytes = %d, want 0", i, s.CommittedBytes)
		}
		covered = s.EndOffset + 1
	}
	if covered != total {
		t.Errorf("segments cover [0, %d), want [0, %d)", covered, total)
	}
}

func TestSplitSegmentsZeroBytes(t *testing.T) {
	if got := SplitSegments(0, 4, 1<<20); len(got) != 0 {
		t.Errorf("SplitSegments(0, 4, 1MiB) = %d segments, want 0", len(got))
	}
	if got := SplitSegments(-5, 4, 1<<20); len(got) != 0 {
		t.Errorf("SplitSegments(-5, 4, 1MiB) = %d segments, want 0", len(got))
	}
}

func TestSplitSegmentsSmallFileFewerThanWorkers(t *testing.T) {
	// 3 bytes, 4 workers, min 1 byte: one byte per segment, 3 segments.
	segs := SplitSegments(3, 4, 1)
	if len(segs) != 3 {
		t.Fatalf("SplitSegments(3, 4, 1) = %d segments, want 3", len(segs))
	}
	want := [][2]int64{{0, 0}, {1, 1}, {2, 2}}
	for i, w := range want {
		if segs[i].StartOffset != w[0] || segs[i].EndOffset != w[1] {
			t.Errorf("segment[%d] = [%d, %d], want [%d, %d]",
				i, segs[i].StartOffset, segs[i].EndOffset, w[0], w[1])
		}
	}
	assertCoverage(t, segs, 3)
}

func TestSplitSegmentsRemainderGoesToLast(t *testing.T) {
	// 10 bytes over 4 segments: base 2, remainder 2 goes to the last.
	segs := SplitSegments(10, 4, 1)
	if len(segs) != 4 {
		t.Fatalf("SplitSegments(10, 4, 1) = %d segments, want 4", len(segs))
	}
	want := [][2]int64{{0, 1}, {2, 3}, {4, 5}, {6, 9}}
	for i, w := range want {
		if segs[i].StartOffset != w[0] || segs[i].EndOffset != w[1] {
			t.Errorf("segment[%d] = [%d, %d], want [%d, %d]",
				i, segs[i].StartOffset, segs[i].EndOffset, w[0], w[1])
		}
	}
	assertCoverage(t, segs, 10)
}

func TestSplitSegmentsLargeFileFourSegments(t *testing.T) {
	segs := SplitSegments(100, 4, 1)
	if len(segs) != 4 {
		t.Fatalf("SplitSegments(100, 4, 1) = %d segments, want 4", len(segs))
	}
	want := [][2]int64{{0, 24}, {25, 49}, {50, 74}, {75, 99}}
	for i, w := range want {
		if segs[i].StartOffset != w[0] || segs[i].EndOffset != w[1] {
			t.Errorf("segment[%d] = [%d, %d], want [%d, %d]",
				i, segs[i].StartOffset, segs[i].EndOffset, w[0], w[1])
		}
	}
	assertCoverage(t, segs, 100)
}

func TestSplitSegmentsMergesBelowMinSegmentSize(t *testing.T) {
	// 10 bytes, min 3: at most 3 segments of >= 3 bytes; remainder to last.
	segs := SplitSegments(10, 4, 3)
	if len(segs) != 3 {
		t.Fatalf("SplitSegments(10, 4, 3) = %d segments, want 3", len(segs))
	}
	want := [][2]int64{{0, 2}, {3, 5}, {6, 9}}
	for i, w := range want {
		if segs[i].StartOffset != w[0] || segs[i].EndOffset != w[1] {
			t.Errorf("segment[%d] = [%d, %d], want [%d, %d]",
				i, segs[i].StartOffset, segs[i].EndOffset, w[0], w[1])
		}
	}
	assertCoverage(t, segs, 10)
}

func TestSplitSegmentsSmallerThanMinIsOneSegment(t *testing.T) {
	segs := SplitSegments(100, 4, 1<<20)
	if len(segs) != 1 {
		t.Fatalf("SplitSegments(100, 4, 1MiB) = %d segments, want 1", len(segs))
	}
	if segs[0].StartOffset != 0 || segs[0].EndOffset != 99 {
		t.Errorf("segment[0] = [%d, %d], want [0, 99]",
			segs[0].StartOffset, segs[0].EndOffset)
	}
	assertCoverage(t, segs, 100)
}

func TestSplitSegmentsSingleWorker(t *testing.T) {
	segs := SplitSegments(1000, 1, 1)
	if len(segs) != 1 {
		t.Fatalf("SplitSegments(1000, 1, 1) = %d segments, want 1", len(segs))
	}
	if segs[0].StartOffset != 0 || segs[0].EndOffset != 999 {
		t.Errorf("segment[0] = [%d, %d], want [0, 999]",
			segs[0].StartOffset, segs[0].EndOffset)
	}
	assertCoverage(t, segs, 1000)
}

func TestSplitSegmentsLargeFileExactCoverage(t *testing.T) {
	// 10 MiB, min 1 MiB, max 4: 4 segments, exact coverage, no overlap.
	const total = 10 << 20
	segs := SplitSegments(total, 4, 1<<20)
	if len(segs) != 4 {
		t.Fatalf("SplitSegments(10MiB, 4, 1MiB) = %d segments, want 4", len(segs))
	}
	assertCoverage(t, segs, total)
	for _, s := range segs {
		if s.Size() < 1<<20 {
			t.Errorf("segment[%d].Size() = %d, want >= 1MiB", s.Index, s.Size())
		}
	}
}

func TestSegmentSizeAndRemaining(t *testing.T) {
	s := Segment{StartOffset: 10, EndOffset: 19, CommittedBytes: 4}
	if s.Size() != 10 {
		t.Errorf("Size() = %d, want 10", s.Size())
	}
	if s.Remaining() != 6 {
		t.Errorf("Remaining() = %d, want 6", s.Remaining())
	}
}

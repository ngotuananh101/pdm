// Segment model and splitting (Stage 3, Phase 1.2).
//
// A segment is one byte range of a download, end-inclusive per
// docs/ARCHITECTURE.md §5, downloaded by one worker. The domain package
// stays free of external dependencies (stdlib only).
package domain

// SegmentStatus is the lifecycle state of one segment.
type SegmentStatus string

const (
	// SegmentPending has not started (or was reset from running on resume).
	SegmentPending SegmentStatus = "pending"
	// SegmentRunning is currently being downloaded by a worker.
	SegmentRunning SegmentStatus = "running"
	// SegmentComplete has all its bytes committed (committed == size).
	SegmentComplete SegmentStatus = "complete"
)

// Segment is one byte range of a download job.
//
// StartOffset and EndOffset are inclusive byte offsets into the file:
// the segment covers [StartOffset, EndOffset]. CommittedBytes counts
// bytes from the start of the segment that are durably written and
// recorded in storage; it never decreases (see the segment repository).
// DownloadID is empty for segments returned by SplitSegments; the
// caller (or the segment repository) fills it in for the owning job.
type Segment struct {
	DownloadID     string
	Index          int
	StartOffset    int64
	EndOffset      int64 // inclusive
	CommittedBytes int64
	Status         SegmentStatus
}

// Size returns the total byte count of the segment (End - Start + 1).
func (s Segment) Size() int64 {
	return s.EndOffset - s.StartOffset + 1
}

// Remaining returns how many bytes of the segment are not yet committed.
func (s Segment) Remaining() int64 {
	return s.Size() - s.CommittedBytes
}

// SplitSegments splits a file of totalBytes bytes into at most
// maxSegments segments, each at least minSegmentSize bytes where
// possible, covering exactly [0, totalBytes-1] with no overlap and no
// gap. The remainder of the division goes to the last segment.
//
//   - totalBytes <= 0 yields no segments.
//   - maxSegments < 1 is treated as 1; minSegmentSize < 1 as 1.
//   - A file smaller than the worker count yields fewer segments: the
//     count is capped by totalBytes/minSegmentSize (integer division),
//     so segments never fall below minSegmentSize except when the whole
//     file is smaller than it, in which case there is a single segment.
func SplitSegments(totalBytes int64, maxSegments int, minSegmentSize int64) []Segment {
	if totalBytes <= 0 {
		return nil
	}
	if maxSegments < 1 {
		maxSegments = 1
	}
	if minSegmentSize < 1 {
		minSegmentSize = 1
	}
	count := int(totalBytes / minSegmentSize)
	if count < 1 {
		count = 1
	}
	if count > maxSegments {
		count = maxSegments
	}
	base := totalBytes / int64(count)
	rem := totalBytes % int64(count)

	segs := make([]Segment, 0, count)
	var start int64
	for i := 0; i < count; i++ {
		size := base
		if i == count-1 {
			size += rem
		}
		segs = append(segs, Segment{
			Index:       i,
			StartOffset: start,
			EndOffset:   start + size - 1,
			Status:      SegmentPending,
		})
		start += size
	}
	return segs
}

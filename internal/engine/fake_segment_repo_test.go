// Fake in-memory SegmentRepository for engine tests (Stage 3, Phase 3).
// It mirrors the SQLite repo's monotonic rule: committed_bytes never
// regresses, and values past the segment end are rejected.
package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/ngotuananh101/pdm/internal/domain"
)

type fakeSegmentRepo struct {
	mu       sync.Mutex
	segments map[string][]*domain.Segment

	insertPlans int
	deleteCalls int

	// failUpdate, when true, makes UpdateCommitted fail (to simulate
	// a DB commit failure). Tests flip it back to let a later
	// checkpoint succeed.
	failUpdate bool
}

func newFakeSegmentRepo() *fakeSegmentRepo {
	return &fakeSegmentRepo{segments: map[string][]*domain.Segment{}}
}

func (r *fakeSegmentRepo) InsertPlan(ctx context.Context, downloadID string, segments []domain.Segment) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if downloadID == "" {
		return fmt.Errorf("fake: InsertPlan called with empty download ID")
	}
	if len(segments) == 0 {
		return fmt.Errorf("fake: InsertPlan %q: empty segment plan", downloadID)
	}
	r.insertPlans++
	stored := make([]*domain.Segment, len(segments))
	for i, s := range segments {
		c := s
		c.DownloadID = downloadID
		stored[i] = &c
	}
	r.segments[downloadID] = stored
	return nil
}

func (r *fakeSegmentRepo) GetByDownload(ctx context.Context, downloadID string) ([]*domain.Segment, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := []*domain.Segment{}
	for _, s := range r.segments[downloadID] {
		c := *s
		out = append(out, &c)
	}
	return out, nil
}

func (r *fakeSegmentRepo) UpdateCommitted(ctx context.Context, downloadID string, index int, committedBytes int64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.failUpdate {
		return fmt.Errorf("fake: UpdateCommitted %q[%d]: injected failure", downloadID, index)
	}
	segs := r.segments[downloadID]
	if index < 0 || index >= len(segs) {
		return fmt.Errorf("fake: segment %q[%d]: %w", downloadID, index, domain.ErrSegmentNotFound)
	}
	s := segs[index]
	if committedBytes < s.CommittedBytes {
		return fmt.Errorf("fake: UpdateCommitted %q[%d]: %d < stored %d: %w",
			downloadID, index, committedBytes, s.CommittedBytes, domain.ErrCommittedBytesRegression)
	}
	if committedBytes > s.Size() {
		return fmt.Errorf("fake: UpdateCommitted %q[%d]: %d past segment end %d",
			downloadID, index, committedBytes, s.Size())
	}
	s.CommittedBytes = committedBytes
	return nil
}

func (r *fakeSegmentRepo) MarkStatus(ctx context.Context, downloadID string, index int, status domain.SegmentStatus) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	segs := r.segments[downloadID]
	if index < 0 || index >= len(segs) {
		return fmt.Errorf("fake: segment %q[%d]: %w", downloadID, index, domain.ErrSegmentNotFound)
	}
	segs[index].Status = status
	return nil
}

func (r *fakeSegmentRepo) DeleteByDownload(ctx context.Context, downloadID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.deleteCalls++
	delete(r.segments, downloadID)
	return nil
}

// committedTotal returns the sum of committed bytes over all segments
// of one download. Used by tests to observe checkpoint progress.
func (r *fakeSegmentRepo) committedTotal(downloadID string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	var total int64
	for _, s := range r.segments[downloadID] {
		total += s.CommittedBytes
	}
	return total
}

var _ domain.SegmentRepository = (*fakeSegmentRepo)(nil)

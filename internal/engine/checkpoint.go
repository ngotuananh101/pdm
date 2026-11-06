// Checkpointing (Stage 3, Phase 3.2).
//
// Checkpoint order per docs/ARCHITECTURE.md §8:
//
//  1. Snapshot the dirty per-segment offsets (writes racing the
//     snapshot stay dirty and are picked up by the next checkpoint).
//  2. Sync the file. The sync covers every byte written up to the
//     sync call, which is at least the snapshotted bytes, so storage
//     can never claim bytes that are not yet durable.
//  3. Record the snapshotted offsets in storage (one transaction per
//     segment through SegmentRepository.UpdateCommitted; each of
//     those is itself transactional and monotonic).
//  4. Only then drop the snapshotted entries from the dirty set —
//     the checkpoint is published.
//
// If the sync fails, storage is untouched. If a storage write fails,
// the dirty entries that were not published are retained, so the
// next checkpoint retries them. A checkpoint error must stop the
// job: the caller (manager) cancels the download on any error from
// Checkpoint or Run.
//
// Note on batch atomicity: the plan asks for the offsets to land in
// a single SQLite transaction. SegmentRepository has no batch-write
// method, and adding one would mean changing the storage package,
// which is outside Phase 3 scope. Per-segment UpdateCommitted calls
// are the safe approximation: each call is transactional and only
// ever advances already-synced offsets, so a crash mid-checkpoint
// can leave some segments published and others dirty, but never a
// segment claiming unsynced bytes. A batch method can replace this
// loop later without changing the ordering guarantees here.
package engine

import (
	"context"
	"errors"
	"os"
	"sync"
	"time"

	"github.com/ngotuananh101/pdm/internal/config"
	"github.com/ngotuananh101/pdm/internal/domain"
)

// fileSyncer is the minimal sync surface of the .part file. *os.File
// satisfies it; tests substitute a failing or counting syncer.
type fileSyncer interface {
	Sync() error
}

// Checkpointer tracks per-segment bytes written to the .part file but
// not yet durably recorded, and publishes them in ARCH §8 order.
type Checkpointer struct {
	syncer     fileSyncer
	repo       domain.SegmentRepository // may be nil: sync-only mode
	downloadID string

	mu    sync.Mutex
	dirty map[int]int64 // segment index -> highest committed bytes noted
	// published remembers the highest value successfully recorded
	// in storage per segment, so a stale note can never ask storage
	// to regress.
	published map[int]int64
	// publishMu serializes the publish section of Checkpoint
	// (filter + record loop). Two concurrent Checkpoints — the
	// periodic ticker and the per-segment completion checkpoint —
	// must not interleave their publishes: the filter has to see
	// every value the other call already recorded, so a stale
	// snapshot is skipped instead of tripping the repository's
	// monotonic check. The file Sync stays outside this mutex.
	publishMu sync.Mutex
}

// NewCheckpointer creates a checkpointer for the .part file f. repo
// may be nil, in which case Checkpoint only syncs the file.
func NewCheckpointer(f *os.File, repo domain.SegmentRepository, downloadID string) *Checkpointer {
	return newCheckpointer(f, repo, downloadID)
}

// NewCheckpointerWithSyncer is NewCheckpointer with an injectable
// syncer, for tests that need sync to fail or be counted.
func NewCheckpointerWithSyncer(syncer fileSyncer, repo domain.SegmentRepository, downloadID string) *Checkpointer {
	return newCheckpointer(syncer, repo, downloadID)
}

func newCheckpointer(syncer fileSyncer, repo domain.SegmentRepository, downloadID string) *Checkpointer {
	return &Checkpointer{
		syncer:     syncer,
		repo:       repo,
		downloadID: downloadID,
		dirty:      map[int]int64{},
		published:  map[int]int64{},
	}
}

// NoteWritten records that segment index has committedBytes durable
// in memory (bytes written to the file, not yet checkpointed). Only
// the highest value per segment is kept, and a value at or below the
// already-published high-water mark is ignored: committed progress
// never regresses.
func (c *Checkpointer) NoteWritten(index int, committedBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if committedBytes <= c.published[index] {
		return
	}
	if committedBytes > c.dirty[index] {
		c.dirty[index] = committedBytes
	}
}

// Two Checkpoint calls can run concurrently — the periodic ticker
// and the per-segment completion checkpoint — so a snapshot can go
// stale while its Sync is in flight: another call may publish a
// newer value for the same segment first. Publishing the stale value
// would trip the repository's monotonic check and wrongly fail the
// job, so stale entries are skipped: the filter below runs under
// publishMu (which serializes the whole publish section, so the
// filter always sees what the other call already recorded), the
// published high-water mark advances immediately after each
// successful record (not only at the end), and an UpdateCommitted
// that still reports a regression already covered by the high-water
// mark is skipped rather than treated as a failure. Skipping is
// safe: the newer value was itself synced before it was published,
// so storage already holds at least the skipped amount.
func (c *Checkpointer) Checkpoint(ctx context.Context) error {
	c.mu.Lock()
	snapshot := make(map[int]int64, len(c.dirty))
	for idx, committed := range c.dirty {
		snapshot[idx] = committed
	}
	c.mu.Unlock()
	if len(snapshot) == 0 {
		return nil
	}

	// Sync first: storage must never advance past durable data.
	if err := c.syncer.Sync(); err != nil {
		return codeError(domain.ErrorCodeFilesystem, "checkpoint: sync: "+err.Error(), err)
	}

	// Serialize the publish section: filter + record loop run
	// atomically against other Checkpoint calls.
	c.publishMu.Lock()
	defer c.publishMu.Unlock()

	// Drop snapshot entries a concurrent Checkpoint already
	// published at or above this value.
	c.mu.Lock()
	for idx, committed := range snapshot {
		if committed <= c.published[idx] {
			delete(snapshot, idx)
		}
	}
	c.mu.Unlock()
	if len(snapshot) == 0 {
		return nil
	}

	published := make([]int, 0, len(snapshot))
	if c.repo != nil {
		for idx, committed := range snapshot {
			if err := c.repo.UpdateCommitted(ctx, c.downloadID, idx, committed); err != nil {
				// A concurrent Checkpoint may have published a
				// newer value for this segment after the filter
				// above: if storage already holds at least this
				// much, the entry is stale, not a real failure.
				c.mu.Lock()
				already := c.published[idx] >= committed
				c.mu.Unlock()
				if already && errors.Is(err, domain.ErrCommittedBytesRegression) {
					published = append(published, idx)
					continue
				}
				// Keep the unpublished entries dirty for the next
				// checkpoint; only the published ones are dropped.
				c.dropPublished(published, snapshot)
				return codeError(domain.ErrorCodeFilesystem, "checkpoint: record committed bytes: "+err.Error(), err)
			}
			// Advance the high-water mark immediately, so a
			// concurrent Checkpoint filtering or failing right
			// now sees this value instead of a stale one.
			c.mu.Lock()
			if committed > c.published[idx] {
				c.published[idx] = committed
			}
			c.mu.Unlock()
			published = append(published, idx)
		}
	} else {
		for idx := range snapshot {
			published = append(published, idx)
		}
	}
	c.dropPublished(published, snapshot)
	return nil
}

// dropPublished removes the published entries from the dirty set,
// unless a worker advanced the value past the snapshot meanwhile —
// in that case the newer value stays dirty for the next checkpoint.
// It also advances the published high-water mark, so a stale note
// can never regress storage.
func (c *Checkpointer) dropPublished(published []int, snapshot map[int]int64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, idx := range published {
		if snapshot[idx] > c.published[idx] {
			c.published[idx] = snapshot[idx]
		}
		if c.dirty[idx] <= snapshot[idx] {
			delete(c.dirty, idx)
		}
	}
}

// Run checkpoints every interval until ctx is done. A checkpoint
// error stops the loop and is returned, so the caller can stop the
// job; a context cancellation returns the context error.
func (c *Checkpointer) Run(ctx context.Context, interval time.Duration) error {
	if interval <= 0 {
		interval = config.DefaultCheckpointInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := c.Checkpoint(ctx); err != nil {
				return err
			}
		}
	}
}

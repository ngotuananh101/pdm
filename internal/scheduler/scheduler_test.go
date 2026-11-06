// Scheduler tests (Phase 4, Task 4.2): the scheduler owns the running
// state — pending queue, global slots, per-host slots — and serializes
// Pause/Resume/Cancel per job ID. The fake worker below mirrors what
// Service.run does: it blocks until released or its context is
// cancelled, then reports Finished and closes done.
package scheduler_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngotuananh101/pdm/internal/scheduler"
)

// harness is a fake service around a Scheduler: start records the job
// and spawns a fake worker goroutine that runs until the test releases
// it or its context is cancelled.
type harness struct {
	t          *testing.T
	sch        *scheduler.Scheduler
	mu         sync.Mutex
	started    []string
	startCount int32
	current    int32
	peak       int32
	release    map[string]chan struct{}
}

func newHarness(t *testing.T, maxActive, maxPerHost int) *harness {
	t.Helper()
	h := &harness{t: t, release: map[string]chan struct{}{}}
	start := func(id string, ctx context.Context, cancel context.CancelFunc, done chan struct{}) bool {
		atomic.AddInt32(&h.startCount, 1)
		h.mu.Lock()
		h.started = append(h.started, id)
		// Fresh release channel per start: a restarted job must not
		// inherit the closed channel of its previous generation.
		rel := make(chan struct{})
		h.release[id] = rel
		h.mu.Unlock()
		cur := atomic.AddInt32(&h.current, 1)
		for {
			p := atomic.LoadInt32(&h.peak)
			if cur <= p || atomic.CompareAndSwapInt32(&h.peak, p, cur) {
				break
			}
		}
		go func() {
			select {
			case <-rel:
			case <-ctx.Done():
			}
			// Count as stopped BEFORE reporting Finished, mirroring
			// Service.run's defer (slot released, then dispatch):
			// Finished may start a new worker synchronously.
			atomic.AddInt32(&h.current, -1)
			h.sch.Finished(id, done)
			close(done)
		}()
		return true
	}
	h.sch = scheduler.New(maxActive, maxPerHost, start)
	return h
}

func (h *harness) waitStarted(n int, what string) {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		h.mu.Lock()
		got := len(h.started)
		h.mu.Unlock()
		if got >= n {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	h.t.Fatalf("timeout waiting for %s: started %d, want >= %d", what, len(h.started), n)
}

func (h *harness) starts() int {
	return int(atomic.LoadInt32(&h.startCount))
}

func (h *harness) peakLoad() int {
	return int(atomic.LoadInt32(&h.peak))
}

func (h *harness) startedIDs() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	out := make([]string, len(h.started))
	copy(out, h.started)
	return out
}

func (h *harness) releaseJob(id string) {
	h.mu.Lock()
	rel, ok := h.release[id]
	h.mu.Unlock()
	if !ok {
		h.t.Fatalf("releaseJob(%q): job never started", id)
	}
	select {
	case <-rel:
		// Already released.
	default:
		close(rel)
	}
}

// TestSchedulerRespectsMaxActive: 4 jobs at once run at most 3
// concurrently (ARCH §7 default); the 4th starts only after a slot frees.
func TestSchedulerRespectsMaxActive(t *testing.T) {
	h := newHarness(t, 3, 10)
	for i := 0; i < 4; i++ {
		h.sch.Enqueue(fmt.Sprintf("job-%d", i), fmt.Sprintf("host-%d.example", i))
	}
	h.waitStarted(3, "3 jobs to start")
	time.Sleep(50 * time.Millisecond)
	if got := h.starts(); got != 3 {
		t.Fatalf("starts = %d, want 3 while all slots are busy", got)
	}
	if got := h.peakLoad(); got > 3 {
		t.Fatalf("peak concurrent = %d, want <= 3", got)
	}
	h.releaseJob("job-0")
	h.waitStarted(4, "4th job to start after a slot frees")
	if got := h.peakLoad(); got > 3 {
		t.Fatalf("peak concurrent = %d, want <= 3", got)
	}
}

// TestSchedulerRespectsMaxPerHost: 5 jobs to the same host run at most
// 4 concurrently — the per-host slot counts every request including
// the probe phase, because the slot is held from dispatch until the
// worker reports Finished.
func TestSchedulerRespectsMaxPerHost(t *testing.T) {
	h := newHarness(t, 10, 4)
	for i := 0; i < 5; i++ {
		h.sch.Enqueue(fmt.Sprintf("job-%d", i), "example.com")
	}
	h.waitStarted(4, "4 jobs to start")
	time.Sleep(50 * time.Millisecond)
	if got := h.starts(); got != 4 {
		t.Fatalf("starts = %d, want 4 while the host is full", got)
	}
	if got := h.peakLoad(); got > 4 {
		t.Fatalf("peak concurrent on one host = %d, want <= 4", got)
	}
	h.releaseJob("job-0")
	h.waitStarted(5, "5th job to start after a host slot frees")
	if got := h.peakLoad(); got > 4 {
		t.Fatalf("peak concurrent on one host = %d, want <= 4", got)
	}
}

// TestSchedulerSkipsFullHostForOtherHost: a queued job whose host is
// full must not head-of-line block a later job for another host
// (same scan semantics as Stage 1 tryDispatchLocked).
func TestSchedulerSkipsFullHostForOtherHost(t *testing.T) {
	h := newHarness(t, 3, 1)
	h.sch.Enqueue("a", "h1.example")
	h.sch.Enqueue("b", "h1.example")
	h.sch.Enqueue("c", "h2.example")
	h.waitStarted(2, "a and c to start")
	time.Sleep(50 * time.Millisecond)
	if got := h.starts(); got != 2 {
		t.Fatalf("starts = %d, want 2 (a and c; b waits on its host)", got)
	}
	for _, id := range h.startedIDs() {
		if id == "b" {
			t.Fatalf("job b started while its host slot was full")
		}
	}
	h.releaseJob("a")
	h.waitStarted(3, "b to start after a frees its host slot")
}

// TestSchedulerPauseResumeNoDuplicateWorker: Stop (Pause) then Enqueue
// (Resume) for the same job must never run two worker groups for it.
// While the old worker is still exiting, a second Stop waits on the
// same done channel and an Enqueue is refused; after the worker exits,
// Resume starts exactly one new worker.
func TestSchedulerPauseResumeNoDuplicateWorker(t *testing.T) {
	h := newHarness(t, 3, 4)
	h.sch.Enqueue("job-1", "example.com")
	h.waitStarted(1, "job-1 to start")

	done, ok := h.sch.Stop("job-1")
	if !ok || done == nil {
		t.Fatalf("Stop(running) = (%v, %v), want non-nil done, true", done, ok)
	}
	// A second Stop while the worker is exiting serializes on the
	// same worker exit instead of cancelling or starting anything new.
	done2, ok2 := h.sch.Stop("job-1")
	if !ok2 || done2 != done {
		t.Fatalf("second Stop = (%v, %v), want the same done channel", done2, ok2)
	}
	// Enqueue while the old worker is stopping must not start a
	// second worker group for the same job.
	h.sch.Enqueue("job-1", "example.com")
	time.Sleep(50 * time.Millisecond)
	if got := h.starts(); got != 1 {
		t.Fatalf("starts = %d, want 1 while the old worker is stopping", got)
	}

	<-done                                // Old worker exited; its slot is released.
	h.sch.Enqueue("job-1", "example.com") // Resume.
	h.waitStarted(2, "job-1 to restart after resume")
	if got := h.starts(); got != 2 {
		t.Fatalf("starts = %d, want exactly 2 (one per generation)", got)
	}
}

// TestSchedulerDuplicateEnqueueStartsOnce: enqueueing the same job
// twice while it runs starts one worker, not two.
func TestSchedulerDuplicateEnqueueStartsOnce(t *testing.T) {
	h := newHarness(t, 3, 4)
	h.sch.Enqueue("job-1", "example.com")
	h.sch.Enqueue("job-1", "example.com")
	h.waitStarted(1, "job-1 to start")
	time.Sleep(50 * time.Millisecond)
	if got := h.starts(); got != 1 {
		t.Fatalf("starts = %d, want 1", got)
	}
}

// TestSchedulerStopQueuedJobNeverStarts: stopping a job that is still
// queued removes it from the pending queue, so it never starts even
// after a slot frees.
func TestSchedulerStopQueuedJobNeverStarts(t *testing.T) {
	h := newHarness(t, 1, 4)
	h.sch.Enqueue("a", "h1.example")
	h.sch.Enqueue("b", "h2.example")
	h.waitStarted(1, "a to start")
	done, ok := h.sch.Stop("b")
	if !ok || done != nil {
		t.Fatalf("Stop(queued) = (%v, %v), want (nil, true)", done, ok)
	}
	h.releaseJob("a")
	time.Sleep(100 * time.Millisecond)
	if got := h.starts(); got != 1 {
		t.Fatalf("starts = %d, want 1: stopped queued job b must never start", got)
	}
}

// TestSchedulerSetLimitsWakesQueued: raising the global limit
// dispatches jobs that were waiting for a slot (UpdateConfig path).
func TestSchedulerSetLimitsWakesQueued(t *testing.T) {
	h := newHarness(t, 1, 4)
	h.sch.Enqueue("a", "h1.example")
	h.sch.Enqueue("b", "h2.example")
	h.waitStarted(1, "a to start")
	time.Sleep(50 * time.Millisecond)
	if got := h.starts(); got != 1 {
		t.Fatalf("starts = %d, want 1 before the limit is raised", got)
	}
	h.sch.SetLimits(2, 4)
	h.waitStarted(2, "b to start after the limit is raised")
}

// TestSchedulerRefusedStartReleasesSlot: when start reports that a job
// could not start (e.g. it was paused or removed meanwhile), the
// scheduler releases the reserved slot and dispatches the next job
// instead of leaking the slot.
func TestSchedulerRefusedStartReleasesSlot(t *testing.T) {
	var sch *scheduler.Scheduler
	var mu sync.Mutex
	var started []string
	start := func(id string, ctx context.Context, cancel context.CancelFunc, done chan struct{}) bool {
		if id == "bad" {
			return false
		}
		mu.Lock()
		started = append(started, id)
		mu.Unlock()
		go func() {
			sch.Finished(id, done)
			close(done)
		}()
		return true
	}
	sch = scheduler.New(1, 4, start)
	sch.Enqueue("bad", "h1.example")
	sch.Enqueue("good", "h2.example")
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := len(started)
		mu.Unlock()
		if got >= 1 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(started) != 1 || started[0] != "good" {
		t.Fatalf("started = %v, want [good]: refused job must release its slot", started)
	}
}

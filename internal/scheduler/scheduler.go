// Package scheduler owns the running state of download jobs: the
// pending queue, the global slot limit, the per-host slot limit, and
// the registry of running workers.
//
// Boundaries (ARCH §3, §4): the scheduler knows nothing about the
// engine, Fyne, or SQLite. It receives work through the start callback
// supplied by the app service, which performs the status transition
// and spawns the worker goroutine. The scheduler only decides *when*
// a job may start and *which* worker runs for a job.
//
// Concurrency discipline:
//   - All scheduler state is guarded by Scheduler.mu.
//   - The start callback is NEVER invoked while holding mu: dispatch
//     collects the jobs to start under lock, releases the lock, and
//     only then calls start. This lets the service take its own lock
//     inside start without deadlocking.
//   - A job's slot (global + per-host) is reserved at dispatch and
//     released only when the worker reports Finished, so the probe
//     phase counts against the per-host limit exactly like Stage 1
//     (ARCH §7: per-host slots count every request, including probe).
//   - Pause/Resume/Cancel are serialized per job ID: Stop marks a
//     running entry stopping and returns its done channel, so a second
//     Stop for the same job waits on the same worker exit instead of
//     starting or cancelling anything new; Enqueue refuses a job that
//     is running or stopping, so no second worker group can start
//     for the same job.
package scheduler

import (
	"context"
	"sync"
)

// Defaults mirror ARCH §7 (and the config defaults).
const (
	DefaultMaxActive  = 3
	DefaultMaxPerHost = 4
)

// StartFunc is called (outside the scheduler lock) when a job is
// dispatched. It must perform the queued -> probing transition and
// spawn the worker goroutine, which reports back via Finished and
// closes done when it has fully exited. Returning false tells the
// scheduler the job could not start (e.g. it was paused or removed
// meanwhile); the scheduler then releases the reserved slot and
// dispatches the next job.
//
// The call is synchronous from Enqueue, SetLimits, or Finished, so
// it runs with whatever locks the caller of those methods holds.
// The app service calls them while holding its own lock, and its
// start implementation relies on that lock being held — it must not
// try to take it again.
type StartFunc func(id string, ctx context.Context, cancel context.CancelFunc, done chan struct{}) bool

// pendingJob is a job waiting for a slot, in enqueue order.
type pendingJob struct {
	id   string
	host string
}

// dispatch is a job whose slot is reserved and whose worker is about
// to be started (outside the lock).
type dispatch struct {
	id     string
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
}

// entry is a running (or stopping) worker.
type entry struct {
	cancel   context.CancelFunc
	done     chan struct{}
	host     string
	stopping bool
}

// Scheduler owns the running state of jobs. The zero value is not
// usable; use New.
type Scheduler struct {
	mu         sync.Mutex
	maxActive  int
	maxPerHost int
	start      StartFunc

	pending     []pendingJob
	running     map[string]*entry
	hostRunning map[string]int
	active      int
	closed      bool
}

// New creates a Scheduler. Limits <= 0 fall back to the ARCH §7
// defaults. start must not be nil.
func New(maxActive, maxPerHost int, start StartFunc) *Scheduler {
	if start == nil {
		panic("scheduler: start callback must not be nil")
	}
	if maxActive <= 0 {
		maxActive = DefaultMaxActive
	}
	if maxPerHost <= 0 {
		maxPerHost = DefaultMaxPerHost
	}
	return &Scheduler{
		maxActive:   maxActive,
		maxPerHost:  maxPerHost,
		start:       start,
		running:     make(map[string]*entry),
		hostRunning: make(map[string]int),
	}
}

// Enqueue adds a job to the pending queue (in order) and dispatches
// while slots are free. Enqueueing a job that is already pending,
// running, or stopping is a no-op, so no second worker group can
// start for the same job. Enqueueing after Close is a no-op.
func (s *Scheduler) Enqueue(id, host string) {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	for _, p := range s.pending {
		if p.id == id {
			s.mu.Unlock()
			return
		}
	}
	if _, ok := s.running[id]; ok {
		s.mu.Unlock()
		return
	}
	s.pending = append(s.pending, pendingJob{id: id, host: host})
	toStart := s.dispatchLocked()
	s.mu.Unlock()
	s.startAll(toStart)
}

// SetLimits updates the slot limits and dispatches jobs that were
// waiting for a slot. Limits <= 0 fall back to the defaults.
func (s *Scheduler) SetLimits(maxActive, maxPerHost int) {
	if maxActive <= 0 {
		maxActive = DefaultMaxActive
	}
	if maxPerHost <= 0 {
		maxPerHost = DefaultMaxPerHost
	}
	s.mu.Lock()
	s.maxActive = maxActive
	s.maxPerHost = maxPerHost
	toStart := s.dispatchLocked()
	s.mu.Unlock()
	s.startAll(toStart)
}

// Stop removes a job from the scheduler's control so Pause/Cancel can
// park it. It returns (done, true) for a running job: the worker's
// context is cancelled (once) and done is closed when the worker has
// fully exited, so the caller waits on it before recording the final
// status. A second Stop for the same job while it is stopping returns
// the same done channel. It returns (nil, true) for a job that is
// still queued: the job is removed from the pending queue and there
// is no worker to wait for. It returns (nil, false) for an unknown job.
func (s *Scheduler) Stop(id string) (done chan struct{}, ok bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ent, running := s.running[id]; running {
		if !ent.stopping {
			ent.stopping = true
			ent.cancel()
		}
		return ent.done, true
	}
	for i, p := range s.pending {
		if p.id == id {
			s.pending = append(s.pending[:i], s.pending[i+1:]...)
			return nil, true
		}
	}
	return nil, false
}

// Finished releases a worker's slot (global + per-host) and dispatches
// the next waiting job. It must be called by the worker's defer before
// done is closed, so a waiter on done (Pause/Cancel) sees the slot
// already freed. A Finished for an unknown job, or for a stale
// generation (done does not match the running entry), is ignored.
func (s *Scheduler) Finished(id string, done chan struct{}) {
	s.mu.Lock()
	ent, ok := s.running[id]
	if !ok || ent.done != done {
		s.mu.Unlock()
		return
	}
	delete(s.running, id)
	s.active--
	if ent.host != "" && s.hostRunning[ent.host] > 0 {
		s.hostRunning[ent.host]--
	}
	toStart := s.dispatchLocked()
	s.mu.Unlock()
	s.startAll(toStart)
}

// Close marks the scheduler closed so no further job is dispatched,
// and cancels every running worker. It does not wait: callers wait on
// the workers' done channels themselves (via Stop, or the done
// channels they already hold).
func (s *Scheduler) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	for _, ent := range s.running {
		if !ent.stopping {
			ent.stopping = true
			ent.cancel()
		}
	}
}

// dispatchLocked starts queued jobs while slots are available.
// Caller must hold s.mu. It returns the jobs to start; the caller
// starts them after releasing the lock.
//
// A job is dispatched only when both a global slot and a per-host
// slot are free. The scan skips queued jobs whose host is full and
// dispatches a later job for another host instead of head-of-line
// blocking behind a capped host; a job whose host is full simply
// stays queued until a slot for that host frees up.
func (s *Scheduler) dispatchLocked() []dispatch {
	if s.closed {
		return nil
	}
	var toStart []dispatch
	for s.active < s.maxActive {
		idx := -1
		for i, p := range s.pending {
			if p.host != "" && s.hostRunning[p.host] >= s.maxPerHost {
				continue
			}
			idx = i
			break
		}
		if idx < 0 {
			break
		}
		p := s.pending[idx]
		s.pending = append(s.pending[:idx], s.pending[idx+1:]...)
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		s.running[p.id] = &entry{cancel: cancel, done: done, host: p.host}
		s.active++
		if p.host != "" {
			s.hostRunning[p.host]++
		}
		toStart = append(toStart, dispatch{id: p.id, ctx: ctx, cancel: cancel, done: done})
	}
	return toStart
}

// startAll invokes the start callback for each dispatched job, outside
// the scheduler lock. A job whose start is refused releases its
// reserved slot, which may free a dispatch for another waiting job.
func (s *Scheduler) startAll(toStart []dispatch) {
	for len(toStart) > 0 {
		d := toStart[0]
		toStart = toStart[1:]
		if s.start(d.id, d.ctx, d.cancel, d.done) {
			continue
		}
		s.mu.Lock()
		ent, ok := s.running[d.id]
		if ok && ent.done == d.done {
			delete(s.running, d.id)
			s.active--
			if ent.host != "" && s.hostRunning[ent.host] > 0 {
				s.hostRunning[ent.host]--
			}
			toStart = append(toStart, s.dispatchLocked()...)
		}
		s.mu.Unlock()
	}
}

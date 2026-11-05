package app

import (
	"context"
)

// Shutdown marks the service closed, cancels all active job
// contexts, and waits for every active worker goroutine to exit.
// It returns ctx.Err() if the context deadline expires first.
// Shutdown is idempotent: repeated calls are safe.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.closed {
		s.closed = true
		for _, j := range s.jobs {
			if j.running && j.cancel != nil {
				j.cancel()
			}
		}
	}
	// Collect done channels of running jobs while holding the lock.
	var dones []chan struct{}
	for _, j := range s.jobs {
		if j.done != nil {
			dones = append(dones, j.done)
		}
		// A ResolveConflict in flight is not a running worker
		// (running == false while it finalizes), but its rename
		// must finish before the process exits: wait for its done
		// channel in the same deadline as the workers (A3).
		if j.resolveDone != nil {
			dones = append(dones, j.resolveDone)
		}
	}
	s.mu.Unlock()

	if ctx == nil {
		ctx = context.Background()
	}
	// Wait for all workers, or for the context to expire.
	for _, d := range dones {
		select {
		case <-d:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

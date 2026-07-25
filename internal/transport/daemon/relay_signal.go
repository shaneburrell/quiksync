package daemon

import (
	"context"
	"sync"
)

// Process-local wakeup registry for mid-hop relay. Notify and Wait must hit the
// same daemon process; the mid store remains the source of truth for data.
var (
	relayMu      sync.Mutex
	relayWaiters = map[string][]chan struct{}{}
	relayPending = map[string]struct{}{}
)

func relayNormJob(jobID string) string {
	if jobID == "" {
		return "default"
	}
	return jobID
}

func relayWake(jobID string) {
	jobID = relayNormJob(jobID)
	relayMu.Lock()
	waiters := relayWaiters[jobID]
	delete(relayWaiters, jobID)
	if len(waiters) == 0 {
		relayPending[jobID] = struct{}{}
		relayMu.Unlock()
		return
	}
	delete(relayPending, jobID)
	relayMu.Unlock()
	for _, ch := range waiters {
		close(ch)
	}
}

func relayWaitJob(ctx context.Context, jobID string) error {
	jobID = relayNormJob(jobID)
	relayMu.Lock()
	if _, ok := relayPending[jobID]; ok {
		delete(relayPending, jobID)
		relayMu.Unlock()
		return nil
	}
	ch := make(chan struct{})
	relayWaiters[jobID] = append(relayWaiters[jobID], ch)
	relayMu.Unlock()

	defer func() {
		relayMu.Lock()
		defer relayMu.Unlock()
		list := relayWaiters[jobID]
		for i, w := range list {
			if w == ch {
				relayWaiters[jobID] = append(list[:i], list[i+1:]...)
				break
			}
		}
		if len(relayWaiters[jobID]) == 0 {
			delete(relayWaiters, jobID)
		}
	}()

	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

package daemon

import (
	"context"
	"fmt"
	"sync"
	"time"
)

const (
	relayMaxJobIDLen      = 256
	relayMaxWaitersPerJob = 64
	relayMaxPendingJobs   = 1024
)

// relayWaitMax bounds how long MsgRelayWait blocks. Matches quic MaxIdleTimeout.
// Overridable in tests.
var relayWaitMax = 5 * time.Minute

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

func validateRelayJobID(jobID string) error {
	jobID = relayNormJob(jobID)
	if len(jobID) > relayMaxJobIDLen {
		return fmt.Errorf("relay job id too long (max %d)", relayMaxJobIDLen)
	}
	return nil
}

func relayWake(jobID string) {
	jobID = relayNormJob(jobID)
	if len(jobID) > relayMaxJobIDLen {
		return
	}
	relayMu.Lock()
	waiters := relayWaiters[jobID]
	delete(relayWaiters, jobID)
	if len(waiters) == 0 {
		_, already := relayPending[jobID]
		if already || len(relayPending) < relayMaxPendingJobs {
			relayPending[jobID] = struct{}{}
		}
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
	if err := validateRelayJobID(jobID); err != nil {
		return err
	}
	jobID = relayNormJob(jobID)

	deadline := relayWaitMax
	if deadline > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, deadline)
		defer cancel()
	}

	relayMu.Lock()
	if _, ok := relayPending[jobID]; ok {
		delete(relayPending, jobID)
		relayMu.Unlock()
		return nil
	}
	if len(relayWaiters[jobID]) >= relayMaxWaitersPerJob {
		relayMu.Unlock()
		return fmt.Errorf("relay wait: too many waiters for job")
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

package daemon

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestRelayWaitTimeout(t *testing.T) {
	old := relayWaitMax
	relayWaitMax = 30 * time.Millisecond
	defer func() { relayWaitMax = old }()

	job := "timeout-job-" + t.Name()
	err := relayWaitJob(context.Background(), job)
	if err == nil || !strings.Contains(err.Error(), "deadline") && err != context.DeadlineExceeded {
		t.Fatalf("want deadline exceeded, got %v", err)
	}
}

func TestRelayJobIDTooLong(t *testing.T) {
	long := strings.Repeat("a", relayMaxJobIDLen+1)
	if err := validateRelayJobID(long); err == nil {
		t.Fatal("expected reject")
	}
	if err := relayWaitJob(context.Background(), long); err == nil {
		t.Fatal("wait should reject long id")
	}
}

func TestRelayWaitersCap(t *testing.T) {
	old := relayWaitMax
	relayWaitMax = 2 * time.Second
	defer func() { relayWaitMax = old }()

	job := "cap-job-" + t.Name()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := make(chan struct{}, relayMaxWaitersPerJob)
	errCh := make(chan error, relayMaxWaitersPerJob+1)
	for i := 0; i < relayMaxWaitersPerJob; i++ {
		go func() {
			started <- struct{}{}
			errCh <- relayWaitJob(ctx, job)
		}()
	}
	for i := 0; i < relayMaxWaitersPerJob; i++ {
		select {
		case <-started:
		case <-time.After(time.Second):
			t.Fatal("waiters did not start")
		}
	}
	// Give waiters time to register.
	time.Sleep(20 * time.Millisecond)
	if err := relayWaitJob(ctx, job); err == nil || !strings.Contains(err.Error(), "too many waiters") {
		t.Fatalf("expected waiter cap, got %v", err)
	}
	cancel()
	for i := 0; i < relayMaxWaitersPerJob; i++ {
		select {
		case <-errCh:
		case <-time.After(2 * time.Second):
			t.Fatal("waiter hang")
		}
	}
}

func TestRelayWaitNotifyStillWorks(t *testing.T) {
	job := "ok-job-" + t.Name()
	done := make(chan error, 1)
	go func() {
		done <- relayWaitJob(context.Background(), job)
	}()
	time.Sleep(20 * time.Millisecond)
	relayWake(job)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("not woken")
	}
}

func TestRelayWakePendingAndIgnoresLongID(t *testing.T) {
	if got := relayNormJob(""); got != "default" {
		t.Fatalf("norm empty: %q", got)
	}
	long := strings.Repeat("z", relayMaxJobIDLen+1)
	relayWake(long) // must not panic or store

	job := "pending-" + t.Name()
	relayWake(job) // no waiters → pending
	if err := relayWaitJob(context.Background(), job); err != nil {
		t.Fatalf("pending wait: %v", err)
	}
}

func TestRelayPendingCap(t *testing.T) {
	relayMu.Lock()
	oldPending := relayPending
	relayPending = map[string]struct{}{}
	for i := 0; i < relayMaxPendingJobs; i++ {
		relayPending[fmt.Sprintf("fill-%d", i)] = struct{}{}
	}
	relayMu.Unlock()
	defer func() {
		relayMu.Lock()
		relayPending = oldPending
		relayMu.Unlock()
	}()

	// At cap with a new job and no waiters: wake should not grow the map.
	relayWake("overflow-" + t.Name())
	relayMu.Lock()
	_, stored := relayPending["overflow-"+t.Name()]
	n := len(relayPending)
	relayMu.Unlock()
	if stored {
		t.Fatal("pending overflow should be dropped")
	}
	if n != relayMaxPendingJobs {
		t.Fatalf("pending size=%d want %d", n, relayMaxPendingJobs)
	}
}

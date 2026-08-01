package daemon

import (
	"context"
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

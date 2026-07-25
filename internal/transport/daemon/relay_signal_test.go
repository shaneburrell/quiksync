package daemon

import (
	"context"
	"testing"
	"time"
)

func TestRelayWakeUnblocksWait(t *testing.T) {
	job := "wake-test-1"
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- relayWaitJob(ctx, job) }()

	time.Sleep(50 * time.Millisecond)
	relayWake(job)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for wake")
	}
}

func TestRelayPendingNotifyBeforeWait(t *testing.T) {
	job := "wake-test-2"
	relayWake(job)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := relayWaitJob(ctx, job); err != nil {
		t.Fatalf("expected pending notify to satisfy wait: %v", err)
	}
}

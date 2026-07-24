package transfer

import (
	"context"
	"testing"
	"time"
)

func TestLimiterDisabled(t *testing.T) {
	l := NewLimiter(0)
	start := time.Now()
	if err := l.Wait(context.Background(), 1<<20); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) > 50*time.Millisecond {
		t.Fatal("disabled limiter should not sleep")
	}
}

func TestLimiterEnforcesRate(t *testing.T) {
	const rate = 100_000 // 100 KB/s, burst = rate
	l := NewLimiter(rate)
	start := time.Now()
	// Request 2.5x burst so at least ~1.5x rate must refill (~1.5s).
	if err := l.Wait(context.Background(), 250_000); err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	min := 1200 * time.Millisecond
	if elapsed < min {
		t.Fatalf("elapsed %v too fast, want >= %v", elapsed, min)
	}
}

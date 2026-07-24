package transfer

import (
	"context"
	"sync"
	"time"
)

// Limiter is a simple shared token-bucket style bandwidth limiter.
// Wait blocks until n bytes may be transferred under the configured rate.
type Limiter struct {
	mu       sync.Mutex
	rate     float64 // bytes per second
	tokens   float64
	last     time.Time
	capacity float64
}

// NewLimiter returns a limiter for rate bytes/sec. rate <= 0 disables limiting.
func NewLimiter(rate int64) *Limiter {
	if rate <= 0 {
		return &Limiter{rate: 0}
	}
	r := float64(rate)
	// Allow a small burst equal to 1 second of rate; Wait handles n > capacity.
	return &Limiter{
		rate:     r,
		tokens:   r,
		last:     time.Now(),
		capacity: r,
	}
}

// Wait blocks until n bytes are available under the rate limit.
func (l *Limiter) Wait(ctx context.Context, n int) error {
	if l == nil || l.rate <= 0 || n <= 0 {
		return nil
	}
	remaining := float64(n)
	for remaining > 0 {
		if err := ctx.Err(); err != nil {
			return err
		}
		l.mu.Lock()
		now := time.Now()
		elapsed := now.Sub(l.last).Seconds()
		if elapsed > 0 {
			l.tokens += elapsed * l.rate
			if l.tokens > l.capacity {
				l.tokens = l.capacity
			}
			l.last = now
		}
		if l.tokens <= 0 {
			sleep := time.Duration((1.0 / l.rate) * float64(time.Second))
			if sleep < time.Millisecond {
				sleep = time.Millisecond
			}
			l.mu.Unlock()
			timer := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				timer.Stop()
				return ctx.Err()
			case <-timer.C:
			}
			continue
		}
		take := remaining
		if take > l.tokens {
			take = l.tokens
		}
		l.tokens -= take
		remaining -= take
		l.mu.Unlock()
	}
	return nil
}

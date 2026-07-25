package autotune

import (
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/compress"
)

func TestObserveDisabledPassthrough(t *testing.T) {
	tu := New(Config{Enabled: false, Compress: compress.CodecNone, Streams: 2}, "h")
	tu.profile.Streams = 2
	p := tu.Observe(Sample{BytesVerified: 100, BytesWired: 80, Elapsed: time.Second, CPUPercent: 10})
	if p.Streams != 2 {
		t.Fatalf("disabled should not retune: %+v", p)
	}
}

func TestObserveStepsAndRevert(t *testing.T) {
	tu := New(Config{Enabled: true, Compress: compress.CodecAuto, Streams: 0}, "h")
	tu.profile.Streams = 2
	tu.profile.Window = 8
	tu.profile.FrameSize = 32 * 1024
	tu.profile.Compress = compress.CodecNone
	tu.started = time.Now().Add(-60 * time.Second) // keep rolling goodput sane
	tu.bytes = 0
	tu.prev = tu.profile
	tu.lastTune = time.Now().Add(-3 * time.Second)
	// Observe increments step before switch: start at 2 so first tune is case 0.
	tu.step = 2

	// case 0: bump streams
	p := tu.Observe(Sample{
		BytesVerified: 60_000, Elapsed: time.Second,
		CPUPercent: 10, ErrorRate: 0, CompressRatio: 1.5, RTTMs: 5,
	})
	if p.Streams < 3 {
		t.Fatalf("expected stream bump, got %d", p.Streams)
	}

	tu.lastTune = time.Now().Add(-3 * time.Second)
	// case 1: grow frame on low RTT
	before := tu.profile.FrameSize
	p = tu.Observe(Sample{
		BytesVerified: 60_000, Elapsed: time.Second,
		CPUPercent: 10, ErrorRate: 0, RTTMs: 5,
	})
	if p.FrameSize <= before {
		t.Fatalf("expected frame grow, before=%d after=%d", before, p.FrameSize)
	}

	tu.lastTune = time.Now().Add(-3 * time.Second)
	// case 2: enable lz4 on compressible
	p = tu.Observe(Sample{
		BytesVerified: 60_000, Elapsed: time.Second,
		CPUPercent: 10, ErrorRate: 0, CompressRatio: 1.5, RTTMs: 5,
	})
	if p.Compress != compress.CodecLZ4 {
		t.Fatalf("expected lz4, got %s", p.Compress)
	}

	tu.lastTune = time.Now().Add(-3 * time.Second)
	tu.mu.Lock()
	tu.bytes = 50_000_000
	tu.started = time.Now().Add(-time.Second) // rolling goodput ~50MB/s
	tu.prev = Profile{Streams: 2, Window: 8, FrameSize: 32 * 1024, Compress: compress.CodecNone, CDCAvg: tu.cdcPin}
	wantStreams := tu.prev.Streams
	tu.mu.Unlock()
	// Regression: low instantaneous goodput reverts to prev
	p = tu.Observe(Sample{
		BytesVerified: 100, Elapsed: 10 * time.Second,
		CPUPercent: 10, ErrorRate: 0, RTTMs: 5,
	})
	if p.Streams != wantStreams {
		t.Fatalf("expected revert to prev streams=%d, got %d", wantStreams, p.Streams)
	}
}

func TestObserveThrottleInterval(t *testing.T) {
	tu := New(Config{Enabled: true, Compress: compress.CodecAuto}, "h")
	tu.profile.Streams = 4
	tu.lastTune = time.Now()
	p := tu.Observe(Sample{BytesVerified: 1, Elapsed: time.Millisecond, CPUPercent: 1})
	if p.Streams != 4 {
		t.Fatalf("should throttle retune within 2s")
	}
}

func TestObserveShrinkFrameOnHighRTT(t *testing.T) {
	tu := New(Config{Enabled: true, Compress: compress.CodecNone}, "h")
	tu.profile.FrameSize = 64 * 1024
	tu.started = time.Now().Add(-60 * time.Second)
	tu.bytes = 0
	tu.lastTune = time.Now().Add(-3 * time.Second)
	tu.step = 0 // next Observe → case 1
	p := tu.Observe(Sample{
		BytesVerified: 60_000, Elapsed: time.Second,
		RTTMs: 200, CPUPercent: 10,
	})
	if p.FrameSize >= 64*1024 {
		t.Fatalf("expected frame shrink on high RTT, got %d", p.FrameSize)
	}
}

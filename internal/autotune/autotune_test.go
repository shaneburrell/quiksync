package autotune

import (
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/compress"
)

func TestProbePrefersStreamsOnHighRTT(t *testing.T) {
	tu := New(Config{Enabled: true, Compress: compress.CodecAuto}, "host")
	sample := []byte(strings.Repeat("abcDEF123\n", 2000))
	p := tu.Probe(sample, 120)
	if p.Streams < 4 {
		t.Fatalf("expected higher streams on high RTT, got %d", p.Streams)
	}
}

func TestProbeDisablesCompressOnIncompressible(t *testing.T) {
	tu := New(Config{Enabled: true, Compress: compress.CodecAuto}, "host")
	// High-entropy sample (not patterned) so codecs should not win.
	sample := make([]byte, 32*1024)
	for i := range sample {
		sample[i] = byte((i*1103515245 + 12345) >> 16)
	}
	// Mix with a second chaotic pass so LZ4/zstd cannot find runs.
	for i := range sample {
		sample[i] ^= byte((i*214013 + 2531011) >> 8)
	}
	p := tu.Probe(sample, 10)
	if p.Compress != compress.CodecNone {
		t.Fatalf("expected none for incompressible, got %s", p.Compress)
	}
}

func TestProbeEnablesCompressOnText(t *testing.T) {
	tu := New(Config{Enabled: true, Compress: compress.CodecAuto}, "host")
	sample := []byte(strings.Repeat("the quick brown fox jumps over the lazy dog\n", 2000))
	p := tu.Probe(sample, 10)
	if p.Compress == compress.CodecNone {
		t.Fatalf("expected compression for text")
	}
}

func TestObserveShedsOnErrors(t *testing.T) {
	tu := New(Config{Enabled: true, Compress: compress.CodecZstd, Streams: 8}, "host")
	tu.profile.Streams = 8
	tu.profile.Compress = compress.CodecZstd
	p := tu.Observe(Sample{BytesVerified: 1000, Elapsed: time.Second, ErrorRate: 0.2, CPUPercent: 50})
	if p.Streams >= 8 {
		t.Fatalf("expected stream shed, got %d", p.Streams)
	}
	if p.Compress == compress.CodecZstd {
		t.Fatalf("expected compress downgrade")
	}
}

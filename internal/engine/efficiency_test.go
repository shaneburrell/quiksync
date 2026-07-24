//go:build efficiency

package engine_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
)

// TestEfficiencyReport generates a soak/efficiency report under testdata/artifacts/.
func TestEfficiencyReport(t *testing.T) {
	art := filepath.Join("..", "..", "testdata", "artifacts")
	if err := os.MkdirAll(art, 0o755); err != nil {
		t.Fatal(err)
	}
	soak := filepath.Join(art, "soak")
	_ = os.RemoveAll(soak)
	src := filepath.Join(soak, "src")
	dst := filepath.Join(soak, "dst")
	if err := os.MkdirAll(src, 0o755); err != nil {
		t.Fatal(err)
	}

	text := bytes.Repeat([]byte("efficiency soak line with repeated text content\n"), 80_000) // ~4MiB
	writeFile(t, filepath.Join(src, "text.log"), text)
	rand := make([]byte, 2<<20)
	for i := range rand {
		rand[i] = byte(i * 13)
	}
	writeFile(t, filepath.Join(src, "rand.bin"), rand)

	var report bytes.Buffer
	fmt.Fprintf(&report, "# QuikSync efficiency report\n\nGenerated: %s\n\n", time.Now().UTC().Format(time.RFC3339))

	// Full copy goodput
	_ = os.RemoveAll(dst)
	start := time.Now()
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst,
		Tune: autotune.Config{Enabled: true, Compress: compress.CodecAuto, Streams: 0},
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	goodput := float64(stats.BytesPayload) / elapsed.Seconds() / (1 << 20)
	fmt.Fprintf(&report, "## Full copy (auto)\n- files=%d payload=%d bytes\n- elapsed=%s\n- goodput=%.2f MiB/s\n\n",
		stats.FilesCopied, stats.BytesPayload, elapsed, goodput)

	// Append delta
	appended := append(append([]byte{}, text...), bytes.Repeat([]byte("APPEND\n"), 100)...)
	writeFile(t, filepath.Join(src, "text.log"), appended)
	start = time.Now()
	stats2, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Checksum: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1, ChunkAvg: 16 * 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed = time.Since(start)
	fmt.Fprintf(&report, "## Append delta\n- payload=%d reused_chunks=%d sent_chunks=%d\n- elapsed=%s\n\n",
		stats2.BytesPayload, stats2.ChunksReused, stats2.ChunksSent, elapsed)
	if stats2.BytesPayload > int64(len(appended))/2 {
		t.Fatalf("delta gate failed: payload %d > 50%% of %d", stats2.BytesPayload, len(appended))
	}

	// Compress comparison on text-only tree
	for _, codec := range []compress.Codec{compress.CodecNone, compress.CodecLZ4, compress.CodecZstd} {
		d := filepath.Join(soak, "dst-"+codec.String())
		_ = os.RemoveAll(d)
		start = time.Now()
		st, err := engine.Run(context.Background(), engine.Config{
			Source: src, Dest: d,
			Tune: autotune.Config{Enabled: false, Compress: codec, Streams: 2, ChunkAvg: 64 * 1024},
		})
		if err != nil {
			t.Fatal(err)
		}
		elapsed = time.Since(start)
		fmt.Fprintf(&report, "## Compress %s\n- payload=%d elapsed=%s goodput=%.2f MiB/s\n\n",
			codec, st.BytesPayload, elapsed, float64(st.BytesPayload)/elapsed.Seconds()/(1<<20))
	}

	// bwlimit soft gate
	dlim := filepath.Join(soak, "dst-bw")
	_ = os.RemoveAll(dlim)
	smallSrc := filepath.Join(soak, "small-src")
	_ = os.MkdirAll(smallSrc, 0o755)
	writeFile(t, filepath.Join(smallSrc, "blob.bin"), bytes.Repeat([]byte("Z"), 300_000))
	const rate = 80_000
	start = time.Now()
	stLim, err := engine.Run(context.Background(), engine.Config{
		Source: smallSrc, Dest: dlim, BandwidthLimit: rate,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed = time.Since(start)
	// Soft gate: allow burst + scheduler jitter (expect ≥ ~70% of ideal time).
	min := time.Duration(float64(stLim.BytesPayload) / float64(rate) * 0.7 * float64(time.Second))
	fmt.Fprintf(&report, "## Bandwidth limit (%d B/s)\n- payload=%d elapsed=%s min_expected≈%s\n\n",
		rate, stLim.BytesPayload, elapsed, min)
	if elapsed < min {
		t.Fatalf("bwlimit gate failed: elapsed %v < %v", elapsed, min)
	}

	out := filepath.Join(art, "efficiency-report.md")
	if err := os.WriteFile(out, report.Bytes(), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("wrote %s", out)
}

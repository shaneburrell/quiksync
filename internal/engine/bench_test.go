package engine_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
)

func BenchmarkLocalCopy(b *testing.B) {
	src := b.TempDir()
	writeFile(b, filepath.Join(src, "a.txt"), bytes.Repeat([]byte("x"), 200_000))
	writeFile(b, filepath.Join(src, "b.txt"), bytes.Repeat([]byte("y"), 200_000))
	tune := autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 2}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst := b.TempDir()
		if _, err := engine.Run(context.Background(), engine.Config{Source: src, Dest: dst, Tune: tune}); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDeltaAppend(b *testing.B) {
	src := b.TempDir()
	dst := b.TempDir()
	base := bytes.Repeat([]byte("delta-bench-line\n"), 50_000)
	writeFile(b, filepath.Join(src, "doc.txt"), base)
	tune := autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1, ChunkAvg: 16 * 1024}
	if _, err := engine.Run(context.Background(), engine.Config{Source: src, Dest: dst, Tune: tune}); err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		appended := append(append([]byte{}, base...), []byte("tail\n")...)
		writeFile(b, filepath.Join(src, "doc.txt"), appended)
		if _, err := engine.Run(context.Background(), engine.Config{
			Source: src, Dest: dst, Checksum: true, Tune: tune,
		}); err != nil {
			b.Fatal(err)
		}
		// restore for next iter
		writeFile(b, filepath.Join(src, "doc.txt"), base)
		_ = os.Remove(filepath.Join(dst, "doc.txt"))
		if _, err := engine.Run(context.Background(), engine.Config{Source: src, Dest: dst, Tune: tune}); err != nil {
			b.Fatal(err)
		}
	}
}


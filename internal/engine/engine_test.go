package engine_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
)

func TestLocalCopyAndVerify(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), bytes.Repeat([]byte("hello world\n"), 1000))
	writeFile(t, filepath.Join(src, "sub", "b.bin"), bytes.Repeat([]byte{1, 2, 3, 4}, 8000))

	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src,
		Dest:   dst,
		Resume: true,
		Tune:   autotune.Config{Enabled: true, Compress: compress.CodecAuto},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied != 2 {
		t.Fatalf("copied %d", stats.FilesCopied)
	}
	mismatches, err := engine.Verify(context.Background(), src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) != 0 {
		t.Fatalf("mismatches: %v", mismatches)
	}

	// Second run should skip.
	stats2, err := engine.Run(context.Background(), engine.Config{
		Source: src,
		Dest:   dst,
		Resume: true,
		Tune:   autotune.Config{Enabled: false, Compress: compress.CodecNone},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats2.FilesSkipped < 1 {
		t.Fatalf("expected skips, got %+v", stats2)
	}
}

func TestResumeAfterPartial(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeFile(t, filepath.Join(src, "big.dat"), bytes.Repeat([]byte("x"), 200000))

	_, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Resume: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt dest and ensure re-copy fixes it.
	if err := os.WriteFile(filepath.Join(dst, "big.dat"), []byte("torn"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Resume: true, Checksum: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	mismatches, err := engine.Verify(context.Background(), src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) != 0 {
		t.Fatalf("mismatches after repair: %v", mismatches)
	}
}

func TestSyncDelete(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeFile(t, filepath.Join(src, "keep.txt"), []byte("keep"))
	writeFile(t, filepath.Join(dst, "gone.txt"), []byte("gone"))
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, SyncMode: true, Delete: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesDeleted != 1 {
		t.Fatalf("deleted %d", stats.FilesDeleted)
	}
	if _, err := os.Stat(filepath.Join(dst, "gone.txt")); !os.IsNotExist(err) {
		t.Fatalf("expected gone.txt removed")
	}
}

func TestStableWindow(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	path := filepath.Join(src, "hot.log")
	writeFile(t, path, []byte("data"))
	// Make mtime "now"
	now := time.Now()
	_ = os.Chtimes(path, now, now)

	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, StableWindow: time.Hour, SkipUnstable: true, MaxFileAttempts: 1,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err == nil {
		t.Fatal("expected non-zero exit when unstable files fail")
	}
	if stats.FilesCopied != 0 || stats.FilesFailed != 1 {
		t.Fatalf("expected failed unstable file, got %+v", stats)
	}
}

func TestCompressRoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("compressible text line\n"), 500)
	for _, codec := range []compress.Codec{compress.CodecNone, compress.CodecLZ4, compress.CodecZstd} {
		used, enc, err := compress.Encode(codec, data)
		if err != nil {
			t.Fatal(err)
		}
		out, err := compress.Decode(used, enc, len(data))
		if err != nil {
			t.Fatalf("codec %s: %v", codec, err)
		}
		if !bytes.Equal(out, data) {
			t.Fatalf("roundtrip mismatch %s", codec)
		}
		if chunk.Sum(out) != chunk.Sum(data) {
			t.Fatalf("digest mismatch %s", codec)
		}
	}
}

func TestTwoPassStreamCopyAndDelta(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	// ~2 MiB forces multiple CDC chunks; two-pass must not require KeepData on pass 1.
	data := bytes.Repeat([]byte("two-pass-stream-ABCDEFGHIJKLMNOP\n"), 64*1024)
	writeFile(t, filepath.Join(src, "big.bin"), data)

	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Resume: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1, ChunkAvg: 64 * 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied != 1 {
		t.Fatalf("copied=%d", stats.FilesCopied)
	}
	got, err := os.ReadFile(filepath.Join(dst, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("content mismatch len=%d want=%d", len(got), len(data))
	}

	// Append so delta reuse path is exercised on the second pass-2 stream.
	data2 := append(append([]byte{}, data...), []byte("TAIL")...)
	writeFile(t, filepath.Join(src, "big.bin"), data2)
	stats2, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Resume: true, Checksum: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1, ChunkAvg: 64 * 1024},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats2.FilesCopied != 1 {
		t.Fatalf("delta copied=%d", stats2.FilesCopied)
	}
	if stats2.ChunksReused < 1 {
		t.Fatalf("expected reuse after append, got %+v", stats2)
	}
	got2, err := os.ReadFile(filepath.Join(dst, "big.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got2, data2) {
		t.Fatal("delta content mismatch")
	}
}

func writeFile(t testing.TB, path string, data []byte) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

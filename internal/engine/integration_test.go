package engine_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
	"github.com/shaneburrell/quiksync/internal/journal"
)

func baseTune() autotune.Config {
	return autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1}
}

func TestDryRun(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), []byte("hello"))
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, DryRun: true, Tune: baseTune(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied != 1 {
		t.Fatalf("copied=%d", stats.FilesCopied)
	}
	if _, err := os.Stat(filepath.Join(dst, "a.txt")); !os.IsNotExist(err) {
		t.Fatal("dry-run must not create dest files")
	}
}

func TestExclude(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "keep.txt"), []byte("k"))
	writeFile(t, filepath.Join(src, "vendor", "lib.go"), []byte("v"))
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Exclude: []string{"vendor/*"}, Tune: baseTune(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied != 1 {
		t.Fatalf("copied=%d", stats.FilesCopied)
	}
	if _, err := os.Stat(filepath.Join(dst, "vendor", "lib.go")); !os.IsNotExist(err) {
		t.Fatal("excluded file present")
	}
}

func TestBandwidthLimit(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	const n = 200_000
	writeFile(t, filepath.Join(src, "big.bin"), bytes.Repeat([]byte("Z"), n))
	const rate = 50_000 // 50KB/s → ~4s for 200KB (with burst)
	start := time.Now()
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, BandwidthLimit: rate, Tune: baseTune(),
	})
	if err != nil {
		t.Fatal(err)
	}
	elapsed := time.Since(start)
	if stats.BytesCopied < int64(n)/2 {
		t.Fatalf("bytes=%d", stats.BytesCopied)
	}
	// Expect roughly bytes/rate after initial burst of ~rate tokens.
	// After initial burst (~rate bytes), remaining ~150KB at 50KB/s ≈ 3s.
	min := 1500 * time.Millisecond
	if elapsed < min {
		t.Fatalf("elapsed %v too fast for bwlimit, want >= %v", elapsed, min)
	}
}

func TestTrueResumeFromJournal(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "one.txt"), []byte("one"))
	writeFile(t, filepath.Join(src, "two.txt"), []byte("two"))
	writeFile(t, filepath.Join(src, "three.txt"), []byte("three"))

	var done atomic.Int32
	ctx, cancel := context.WithCancel(context.Background())
	_, err := engine.Run(ctx, engine.Config{
		Source: src, Dest: dst, Resume: true, Tune: baseTune(),
		TestAfterFile: func(rel, status string) {
			if status == "ok" && done.Add(1) >= 1 {
				cancel()
			}
		},
	})
	_ = err // may be nil; cancel aborts remaining workers

	j, err := journal.Open(dst, "default")
	if err != nil {
		t.Fatal(err)
	}
	completed := 0
	for _, name := range []string{"one.txt", "two.txt", "three.txt"} {
		if j.Completed(name) {
			completed++
		}
	}
	if completed < 1 {
		t.Fatal("expected at least one journal-complete file")
	}

	stats2, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Resume: true, Tune: baseTune(),
	})
	if err != nil {
		t.Fatal(err)
	}
	mismatches, err := engine.Verify(context.Background(), src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) != 0 {
		t.Fatalf("mismatches: %v", mismatches)
	}
	if stats2.FilesSkipped+stats2.FilesCopied < 3 {
		t.Fatalf("stats2=%+v", stats2)
	}
}

func TestLiveChangeBeforeCommit(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	path := filepath.Join(src, "hot.bin")
	writeFile(t, path, bytes.Repeat([]byte("A"), 4096))

	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, SkipUnstable: true, MaxFileAttempts: 1, Tune: baseTune(),
		TestBeforeCommit: func(rel string) {
			_ = os.WriteFile(path, bytes.Repeat([]byte("B"), 4096), 0o644)
			// bump mtime
			now := time.Now().Add(time.Second)
			_ = os.Chtimes(path, now, now)
		},
	})
	if err == nil {
		t.Fatal("expected error when file fails")
	}
	if stats.FilesFailed != 1 {
		t.Fatalf("expected failed unstable file, got %+v", stats)
	}
	if data, err := os.ReadFile(filepath.Join(dst, "hot.bin")); err == nil {
		if bytes.Contains(data, []byte("B")) && bytes.Contains(data, []byte("A")) {
			t.Fatal("torn hybrid dest")
		}
		// If file exists it must match a consistent generation — should not exist after abort.
	}
	if _, err := os.Stat(filepath.Join(dst, "hot.bin")); !os.IsNotExist(err) {
		// Commit aborted; dest should be absent
		t.Fatalf("dest should not be finalized: %v", err)
	}
}

func TestDeltaAppendPayload(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	// Large enough + small CDC avg so append reuses most chunks.
	base := bytes.Repeat([]byte("The quick brown fox jumps over the lazy dog.\n"), 40000) // ~1.8MiB
	writeFile(t, filepath.Join(src, "doc.txt"), base)
	tune := autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1, ChunkAvg: 16 * 1024}

	stats1, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Tune: tune,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats1.BytesPayload < int64(len(base))/2 {
		t.Fatalf("first payload=%d", stats1.BytesPayload)
	}

	appended := append(append([]byte{}, base...), bytes.Repeat([]byte("APPENDIX-ONLY-TAIL\n"), 20)...)
	writeFile(t, filepath.Join(src, "doc.txt"), appended)
	_ = os.Chtimes(filepath.Join(src, "doc.txt"), time.Now(), time.Now())

	stats2, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Checksum: true, Tune: tune,
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats2.ChunksReused < 1 {
		t.Fatalf("expected chunk reuse, stats=%+v", stats2)
	}
	if stats2.BytesPayload > int64(len(appended))/2 {
		t.Fatalf("delta payload too large: %d of %d (reused=%d sent=%d)",
			stats2.BytesPayload, len(appended), stats2.ChunksReused, stats2.ChunksSent)
	}
	mismatches, err := engine.Verify(context.Background(), src, dst)
	if err != nil || len(mismatches) != 0 {
		t.Fatalf("verify: %v %v", err, mismatches)
	}
}

func TestMaxFileAttempts(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	path := filepath.Join(src, "x.dat")
	writeFile(t, path, []byte("v1"))
	attempts := 0
	stats, _ := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, MaxFileAttempts: 2, Tune: baseTune(),
		TestBeforeCommit: func(rel string) {
			attempts++
			_ = os.WriteFile(path, []byte("v2"+string(rune('0'+attempts))), 0o644)
			now := time.Now().Add(time.Duration(attempts) * time.Second)
			_ = os.Chtimes(path, now, now)
		},
	})
	if stats.FilesFailed != 1 {
		t.Fatalf("expected failure after max attempts: %+v attempts=%d", stats, attempts)
	}
}

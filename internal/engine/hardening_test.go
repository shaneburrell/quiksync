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
	"github.com/shaneburrell/quiksync/internal/journal"
)

func TestSourceDeletedMidTransferFails(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	path := filepath.Join(src, "live.txt")
	writeFile(t, path, bytes.Repeat([]byte("x"), 50_000))

	_, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
		TestBeforeCommit: func(rel string) {
			_ = os.Remove(path)
		},
		MaxFileAttempts: 1,
		SkipUnstable:    true,
	})
	if err == nil {
		t.Fatal("expected failure when source deleted before commit")
	}
	if _, err := os.Stat(filepath.Join(dst, "live.txt")); err == nil {
		t.Fatal("dest should not finalize after source delete")
	}
}

func TestJournalCompleteDestDeletedRecopies(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), []byte("payload-a"))
	cfg := engine.Config{
		Source: src, Dest: dst, Resume: true, JobID: "job1",
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	}
	if _, err := engine.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dst, "a.txt")); err != nil {
		t.Fatal(err)
	}
	stats, err := engine.Run(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied != 1 {
		t.Fatalf("expected re-copy, got %+v", stats)
	}
	got, err := os.ReadFile(filepath.Join(dst, "a.txt"))
	if err != nil || string(got) != "payload-a" {
		t.Fatalf("dest content: %q err=%v", got, err)
	}
}

func TestDeleteSkippedWhenFilesFailed(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeFile(t, filepath.Join(src, "ok.txt"), []byte("ok"))
	writeFile(t, filepath.Join(src, "bad.txt"), []byte("bad"))
	writeFile(t, filepath.Join(dst, "extra.txt"), []byte("extra"))

	_, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, SyncMode: true, Delete: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
		TestBeforeCommit: func(rel string) {
			if rel == "bad.txt" {
				_ = os.Remove(filepath.Join(src, "bad.txt"))
			}
		},
		MaxFileAttempts: 1,
		SkipUnstable:    true,
	})
	if err == nil {
		t.Fatal("expected job failure")
	}
	if _, err := os.Stat(filepath.Join(dst, "extra.txt")); err != nil {
		t.Fatal("--delete must not run after failures; extra.txt should remain")
	}
}

func TestChecksumIgnoresStaleIndex(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeFile(t, filepath.Join(src, "f.bin"), []byte("original-content-aaaa"))
	cfg := engine.Config{
		Source: src, Dest: dst, Resume: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	}
	if _, err := engine.Run(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	destPath := filepath.Join(dst, "f.bin")
	st, err := os.Stat(destPath)
	if err != nil {
		t.Fatal(err)
	}
	// Bit-flip while preserving size and mtime so index/mtime skip would be wrong.
	flipped := []byte("original-content-bbbb")
	if err := os.WriteFile(destPath, flipped, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(destPath, st.ModTime(), st.ModTime()); err != nil {
		t.Fatal(err)
	}
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Resume: true, Checksum: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied < 1 {
		t.Fatalf("checksum mode must re-copy stale dest: %+v", stats)
	}
	got, err := os.ReadFile(destPath)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original-content-aaaa" {
		t.Fatalf("got %q", got)
	}
}

func TestCommitSyncDurable(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeFile(t, filepath.Join(src, "d.txt"), []byte("durable"))
	if _, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	}); err != nil {
		t.Fatal(err)
	}
	// Journal entry should exist and be complete (synced Put).
	j, err := journal.Open(dst, "default")
	if err != nil {
		t.Fatal(err)
	}
	e, ok := j.Get("d.txt")
	if !ok || e.Status != journal.StatusComplete {
		t.Fatalf("journal: ok=%v entry=%+v", ok, e)
	}
	if _, err := os.Stat(filepath.Join(dst, "d.txt")); err != nil {
		t.Fatal(err)
	}
}

func TestChunkSizeHintMismatch(t *testing.T) {
	r := bytes.NewReader([]byte("abc"))
	_, err := chunk.ChunkReader(r, 999, chunk.Options{AvgSize: 64 * 1024, KeepData: true})
	if err == nil {
		t.Fatal("expected size hint mismatch error")
	}
}

func TestSleepCtxCancel(t *testing.T) {
	// Ensure unstable retry respects cancel via a short-lived job.
	src := t.TempDir()
	dst := t.TempDir()
	path := filepath.Join(src, "m.txt")
	writeFile(t, path, []byte("v1"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := engine.Run(ctx, engine.Config{
			Source: src, Dest: dst, StableWindow: time.Hour,
			Tune:            autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
			MaxFileAttempts: 10,
		})
		done <- err
	}()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected cancel error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("engine did not respect cancel")
	}
}

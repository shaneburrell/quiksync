package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
	"github.com/shaneburrell/quiksync/internal/journal"
	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestVerifyMismatches(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), []byte("aaa"))
	writeFile(t, filepath.Join(src, "b.txt"), []byte("bbb"))
	// missing b on dest; divergent a
	writeFile(t, filepath.Join(dst, "a.txt"), []byte("XXX"))

	mismatches, err := engine.Verify(context.Background(), src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) < 2 {
		t.Fatalf("want >=2 mismatches, got %v", mismatches)
	}
	joined := strings.Join(mismatches, "\n")
	if !strings.Contains(joined, "a.txt") || !strings.Contains(joined, "b.txt") {
		t.Fatalf("mismatches: %v", mismatches)
	}
}

func TestVerifyFilteredExclude(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "keep.txt"), []byte("keep"))
	writeFile(t, filepath.Join(src, "skip.tmp"), []byte("skip"))
	writeFile(t, filepath.Join(dst, "keep.txt"), []byte("keep"))
	// skip.tmp deliberately missing on dest — exclude should hide it from verify

	mismatches, err := engine.VerifyFiltered(context.Background(), src, dst, transport.OpenOptions{}, []string{"*.tmp"})
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) != 0 {
		t.Fatalf("expected no mismatches with exclude, got %v", mismatches)
	}
	mismatches, err = engine.Verify(context.Background(), src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) == 0 {
		t.Fatal("expected mismatch for skip.tmp without exclude")
	}
}

func TestResumeStaleDigestRecopies(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "x.txt"), []byte("original"))
	if _, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Resume: true, Tune: baseTune(),
	}); err != nil {
		t.Fatal(err)
	}
	// Corrupt dest but leave journal marked complete with old digest mismatch on checksum run
	writeFile(t, filepath.Join(dst, "x.txt"), []byte("CORRUPT!!"))
	j, err := journal.Open(dst, "default")
	if err != nil {
		t.Fatal(err)
	}
	if !j.Completed("x.txt") {
		t.Fatal("expected journal complete")
	}
	writeFile(t, filepath.Join(src, "x.txt"), []byte("original"))
	_ = os.Chtimes(filepath.Join(src, "x.txt"), time.Now(), time.Now())

	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Resume: true, Checksum: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dst, "x.txt"))
	if err != nil || string(got) != "original" {
		t.Fatalf("got %q stats=%+v", got, stats)
	}
}

func TestProgressTickerEmitsProgress(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	// Large enough + bwlimit so ticker fires at least once.
	writeFile(t, filepath.Join(src, "big.bin"), bytesRepeat('P', 120_000))
	logPath := filepath.Join(t.TempDir(), "prog.log")
	_, err := engine.Run(context.Background(), engine.Config{
		Source:           src,
		Dest:             dst,
		LogFile:          logPath,
		ProgressInterval: 30 * time.Millisecond,
		BandwidthLimit:   40_000,
		Tune:             baseTune(),
	})
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), "event=progress") {
		t.Fatalf("expected progress events:\n%s", b)
	}
}

func bytesRepeat(b byte, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = b
	}
	return out
}

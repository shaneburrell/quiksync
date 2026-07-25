package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/shaneburrell/quiksync/internal/engine"
)

func TestChecksumRecopiesMatchingMetadata(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "e.txt"), []byte("payload"))
	if _, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Tune: baseTune(),
	}); err != nil {
		t.Fatal(err)
	}
	dstFile := filepath.Join(dst, "e.txt")
	st, err := os.Stat(dstFile)
	if err != nil {
		t.Fatal(err)
	}
	writeFile(t, dstFile, []byte("CORRUPT"))
	if err := os.Chtimes(dstFile, st.ModTime(), st.ModTime()); err != nil {
		t.Fatal(err)
	}

	// Without checksum, size/mtime match would skip; with checksum, digests diverge.
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Checksum: true, Tune: baseTune(),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dstFile)
	if err != nil || string(got) != "payload" {
		t.Fatalf("got %q stats=%+v", got, stats)
	}
	if stats.FilesCopied < 1 {
		t.Fatalf("expected checksum recopy, stats=%+v", stats)
	}
}

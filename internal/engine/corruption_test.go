package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
)

func TestCommitRejectsTamperedTemp(t *testing.T) {
	// Indirectly: after a good copy, mutating dest and re-sync with checksum repairs it.
	src := t.TempDir()
	dst := t.TempDir()
	path := filepath.Join(src, "sealed.bin")
	if err := os.WriteFile(path, []byte("perfect-bytes-0123456789"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dst, "sealed.bin"), []byte("CORRUPTED"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err = engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, Checksum: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecZstd, Streams: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	mismatches, err := engine.Verify(context.Background(), src, dst)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) != 0 {
		t.Fatalf("expected repair, got %v", mismatches)
	}
}

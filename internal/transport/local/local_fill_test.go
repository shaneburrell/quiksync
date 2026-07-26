package local

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
)

func TestLocalConfinementErrors(t *testing.T) {
	tr, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := tr.Stat(ctx, "../escape"); err == nil {
		t.Fatal("expected Stat confine error")
	}
	if _, err := tr.OpenRead(ctx, ".."); err == nil {
		t.Fatal("expected OpenRead confine error")
	}
	if err := tr.Remove(ctx, "../x"); err == nil {
		t.Fatal("expected Remove confine error")
	}
	if _, err := tr.BeginWrite(ctx, "../out.txt", 1); err == nil {
		t.Fatal("expected BeginWrite confine error")
	}
}

func TestLocalReuseTOCTOUAndDigestMismatch(t *testing.T) {
	root := t.TempDir()
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()

	base := []byte("toctou-base-data!!!!")
	ws, err := tr.BeginWrite(ctx, "t.bin", int64(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.WriteChunk(ctx, 0, compress.CodecNone, len(base), base)
	dig := chunk.Sum(base)
	if err := ws.Commit(ctx, dig, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Digest mismatch path
	wsBad, err := tr.BeginWrite(ctx, "t.bin", int64(len(base))+1)
	if err != nil {
		t.Fatal(err)
	}
	if err := wsBad.ReuseChunk(ctx, 0, 0, chunk.Digest{1}, len(base)); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("digest mismatch: %v", err)
	}
	_ = wsBad.Abort()

	// TOCTOU: mutate dest after BeginWrite opens old handle
	ws2, err := tr.BeginWrite(ctx, "t.bin", int64(len(base))+4)
	if err != nil {
		t.Fatal(err)
	}
	mutated := append(append([]byte{}, base...), []byte("XXXX")...)
	if err := os.WriteFile(filepath.Join(root, "t.bin"), mutated, 0o644); err != nil {
		t.Fatal(err)
	}
	now := time.Now().Add(2 * time.Second)
	_ = os.Chtimes(filepath.Join(root, "t.bin"), now, now)
	if err := ws2.ReuseChunk(ctx, 0, 0, dig, len(base)); err == nil || !strings.Contains(err.Error(), "TOCTOU") {
		t.Fatalf("expected TOCTOU, got %v", err)
	}
	_ = ws2.Abort()
}

func TestLocalWriteReuseAfterCommitAndNoOld(t *testing.T) {
	tr, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	data := []byte("fresh")
	ws, err := tr.BeginWrite(ctx, "n.txt", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.ReuseChunk(ctx, 0, 0, chunk.Sum(data), len(data)); err == nil || !strings.Contains(err.Error(), "no existing") {
		t.Fatalf("reuse without old: %v", err)
	}
	_ = ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data)
	dig := chunk.Sum(data)
	if err := ws.Commit(ctx, dig, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, 1, []byte("z")); err == nil || !strings.Contains(err.Error(), "after commit") {
		t.Fatalf("write after commit: %v", err)
	}
	if err := ws.ReuseChunk(ctx, 0, 0, dig, len(data)); err == nil || !strings.Contains(err.Error(), "after commit") {
		t.Fatalf("reuse after commit: %v", err)
	}
	if err := ws.Abort(); err != nil {
		t.Fatal(err)
	}
}

func TestLocalPrepareStagingNonRegular(t *testing.T) {
	root := t.TempDir()
	tmpDir := filepath.Join(root, ".quiksync.tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Leftover non-regular paths under staging must not block CreateTemp.
	if err := os.Mkdir(filepath.Join(tmpDir, "leftover-dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := tr.BeginWrite(context.Background(), "dir.txt", 3)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.WriteChunk(context.Background(), 0, compress.CodecNone, 3, []byte("abc"))
	if err := ws.Commit(context.Background(), chunk.Sum([]byte("abc")), 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestLocalWalkExclude(t *testing.T) {
	root := t.TempDir()
	_ = os.WriteFile(filepath.Join(root, "keep.txt"), []byte("k"), 0o644)
	_ = os.MkdirAll(filepath.Join(root, "vendor"), 0o755)
	_ = os.WriteFile(filepath.Join(root, "vendor", "x.go"), []byte("x"), 0o644)
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	files, err := tr.Walk(context.Background(), []string{"vendor/*"})
	if err != nil || len(files) != 1 || files[0].RelPath != "keep.txt" {
		t.Fatalf("walk: %v %v", files, err)
	}
}

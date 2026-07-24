package local

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
)

func TestCommitDigestMismatchAborts(t *testing.T) {
	tr, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ws, err := tr.BeginWrite(ctx, "f.bin", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, 5, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	bad := chunk.Digest{1}
	if err := ws.Commit(ctx, bad, 0o644, time.Now()); err == nil {
		t.Fatal("expected digest mismatch")
	}
}

func TestAbortRemovesTemp(t *testing.T) {
	root := t.TempDir()
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ws, err := tr.BeginWrite(ctx, "x.txt", 3)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.WriteChunk(ctx, 0, compress.CodecNone, 3, []byte("abc"))
	if err := ws.Abort(); err != nil {
		t.Fatal(err)
	}
	matches, _ := filepath.Glob(filepath.Join(root, ".quiksync.tmp", "**", "*"))
	for _, m := range matches {
		if st, err := os.Stat(m); err == nil && !st.IsDir() {
			t.Fatalf("temp left behind: %s", m)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "x.txt")); !os.IsNotExist(err) {
		t.Fatal("dest should not exist")
	}
}

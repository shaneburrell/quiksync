package local

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
)

func TestLocalRoundTrip(t *testing.T) {
	root := t.TempDir()
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	ctx := context.Background()
	if !tr.Caps().SupportsDelta || tr.Root() != root {
		t.Fatalf("caps/root: %+v %q", tr.Caps(), tr.Root())
	}
	if err := tr.MkdirAll(ctx, "sub"); err != nil {
		t.Fatal(err)
	}
	ws, err := tr.BeginWrite(ctx, "sub/f.txt", 5)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, 5, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	want := chunk.Sum([]byte("hello"))
	if err := ws.Commit(ctx, want, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	st, err := tr.Stat(ctx, "sub/f.txt")
	if err != nil || st.Size != 5 {
		t.Fatalf("stat: %+v %v", st, err)
	}
	rc, err := tr.OpenRead(ctx, "sub/f.txt")
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(data) != "hello" {
		t.Fatalf("read %q %v", data, err)
	}
	sig, err := tr.GetSignature(ctx, "sub/f.txt")
	if err != nil || sig.Digest != want {
		t.Fatalf("sig %+v %v", sig, err)
	}
	files, err := tr.Walk(ctx, nil)
	if err != nil || len(files) != 1 {
		t.Fatalf("walk %v %v", files, err)
	}
	empty, err := tr.GetSignature(ctx, "missing.txt")
	if err != nil || empty.Size != 0 {
		t.Fatalf("missing sig: %+v %v", empty, err)
	}
	if err := tr.Remove(ctx, "sub/f.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "sub", "f.txt")); !os.IsNotExist(err) {
		t.Fatal("expected removed")
	}
}

func TestLocalReuseChunk(t *testing.T) {
	root := t.TempDir()
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	ctx := context.Background()

	base := []byte("reuse-base-0123456789")
	ws, err := tr.BeginWrite(ctx, "r.bin", int64(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(base), base); err != nil {
		t.Fatal(err)
	}
	dig := chunk.Sum(base)
	if err := ws.Commit(ctx, dig, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
	_ = tr.OnNFS()

	next := append(append([]byte{}, base...), []byte("-MORE")...)
	ws2, err := tr.BeginWrite(ctx, "r.bin", int64(len(next)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws2.ReuseChunk(ctx, 0, 0, dig, len(base)); err != nil {
		t.Fatal(err)
	}
	tail := []byte("-MORE")
	if err := ws2.WriteChunk(ctx, uint64(len(base)), compress.CodecNone, len(tail), tail); err != nil {
		t.Fatal(err)
	}
	if err := ws2.Commit(ctx, chunk.Sum(next), 0o600, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "r.bin"))
	if err != nil || string(got) != string(next) {
		t.Fatalf("got %q err=%v", got, err)
	}
}

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
	if _, err := os.Stat(filepath.Join(root, "x.txt")); !os.IsNotExist(err) {
		t.Fatal("dest should not exist")
	}
}

func TestBeginWriteRejectsStagingDirSymlink(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(root, ".quiksync.tmp")); err != nil {
		t.Skipf("symlink: %v", err)
	}
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tr.BeginWrite(context.Background(), "evil.txt", 4)
	if err == nil || !strings.Contains(err.Error(), "symlink") {
		t.Fatalf("expected staging dir symlink rejection, got %v", err)
	}
}

func TestConcurrentBeginWriteUniqueStaging(t *testing.T) {
	root := t.TempDir()
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ws1, err := tr.BeginWrite(ctx, "same.txt", 4)
	if err != nil {
		t.Fatal(err)
	}
	ws2, err := tr.BeginWrite(ctx, "same.txt", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws1.WriteChunk(ctx, 0, compress.CodecNone, 4, []byte("aaaa")); err != nil {
		t.Fatal(err)
	}
	if err := ws2.WriteChunk(ctx, 0, compress.CodecNone, 4, []byte("bbbb")); err != nil {
		t.Fatal(err)
	}
	digA := chunk.Sum([]byte("aaaa"))
	digB := chunk.Sum([]byte("bbbb"))
	if err := ws1.Commit(ctx, digA, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
	// Second commit may overwrite dest, but must only promote its own verified inode.
	if err := ws2.Commit(ctx, digB, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "same.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "bbbb" {
		t.Fatalf("dest=%q want bbbb", got)
	}
}

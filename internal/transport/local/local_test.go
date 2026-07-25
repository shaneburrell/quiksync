package local

import (
	"context"
	"io"
	"os"
	"path/filepath"
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

func TestBeginWriteRejectsStagingSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("SAFE"), 0o644); err != nil {
		t.Fatal(err)
	}
	tmpDir := filepath.Join(root, ".quiksync.tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		t.Fatal(err)
	}
	// Old nested layout + hashed name: both must not write outside.
	if err := os.Symlink(secret, filepath.Join(tmpDir, "evil.txt.partial")); err != nil {
		t.Skipf("symlink: %v", err)
	}
	hashName := partialTempName("evil.txt")
	if err := os.Symlink(secret, filepath.Join(tmpDir, hashName)); err != nil {
		t.Fatal(err)
	}

	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ws, err := tr.BeginWrite(ctx, "evil.txt", 4)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, 4, []byte("pwn!")); err != nil {
		t.Fatal(err)
	}
	if err := ws.Abort(); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(secret)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "SAFE" {
		t.Fatalf("staging symlink escape: outside became %q", got)
	}
}

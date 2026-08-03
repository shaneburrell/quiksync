package local

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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
	if err != nil || len(files) != 2 {
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

func TestNewRemovesStalePartialFiles(t *testing.T) {
	root := t.TempDir()
	staging := filepath.Join(root, ".quiksync.tmp")
	if err := os.MkdirAll(staging, 0o755); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(staging, "qs-stale.partial")
	if err := os.WriteFile(stale, []byte("stale"), 0o600); err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	if _, err := New(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale partial remains: %v", err)
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

func TestLocalLinksAndDirectoriesPreserveMetadata(t *testing.T) {
	root := t.TempDir()
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := tr.MkdirAll(ctx, "empty/nested"); err != nil {
		t.Fatal(err)
	}
	if err := tr.Chmod(ctx, "empty", 0o711); err != nil {
		t.Fatal(err)
	}
	st, err := os.Stat(filepath.Join(root, "empty"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o711 {
		t.Fatalf("directory mode=%#o want %#o", st.Mode().Perm(), os.FileMode(0o711))
	}
	if err := tr.Symlink(ctx, "../target", "empty/nested/link"); err != nil {
		t.Fatal(err)
	}
	target, err := tr.ReadLink(ctx, "empty/nested/link")
	if err != nil {
		t.Fatal(err)
	}
	if target != "../target" {
		t.Fatalf("target=%q", target)
	}
	// Replacing an existing link must not leave the old target behind.
	if err := tr.Symlink(ctx, "replacement", "empty/nested/link"); err != nil {
		t.Fatal(err)
	}
	target, err = tr.ReadLink(ctx, "empty/nested/link")
	if err != nil || target != "replacement" {
		t.Fatalf("replacement target=%q err=%v", target, err)
	}
}

func TestWriteRejectsDestinationRangeAndCrossDeviceFallback(t *testing.T) {
	root := t.TempDir()
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	ws, err := tr.BeginWrite(ctx, "out.bin", 3)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Abort() }()
	if err := ws.WriteChunk(ctx, 2, compress.CodecNone, 2, []byte("xx")); err == nil {
		t.Fatal("expected write range rejection")
	}

	originalRename := renameFile
	renameFile = func(_, _ string) error { return syscall.EXDEV }
	t.Cleanup(func() { renameFile = originalRename })

	data := []byte("abc")
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data); err != nil {
		t.Fatal(err)
	}
	modTime := time.Unix(1_700_000_000, 0)
	if err := ws.Commit(ctx, chunk.Sum(data), 0o751, modTime); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "out.bin"))
	if err != nil || string(got) != string(data) {
		t.Fatalf("fallback contents=%q err=%v", got, err)
	}
	st, err := os.Stat(filepath.Join(root, "out.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if st.Mode().Perm() != 0o751 {
		t.Fatalf("file mode=%#o want %#o", st.Mode().Perm(), os.FileMode(0o751))
	}
	if !st.ModTime().Equal(modTime) {
		t.Fatalf("mtime=%v want %v", st.ModTime(), modTime)
	}
}

func TestReuseRejectsDestinationRange(t *testing.T) {
	root := t.TempDir()
	data := []byte("abc")
	if err := os.WriteFile(filepath.Join(root, "old.bin"), data, 0o644); err != nil {
		t.Fatal(err)
	}
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := tr.BeginWrite(context.Background(), "old.bin", 2)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Abort() }()
	if err := ws.ReuseChunk(context.Background(), 1, 0, chunk.Sum(data[:2]), 2); err == nil {
		t.Fatal("expected reuse destination range rejection")
	}
}

func TestGCStagingKeepsRecentAndUnrelatedFiles(t *testing.T) {
	dir := t.TempDir()
	stale := filepath.Join(dir, "qs-stale.partial")
	recent := filepath.Join(dir, "qs-recent.partial")
	other := filepath.Join(dir, "other.partial")
	for _, path := range []string{stale, recent, other} {
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	old := time.Now().Add(-25 * time.Hour)
	if err := os.Chtimes(stale, old, old); err != nil {
		t.Fatal(err)
	}
	if err := gcStaging(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale partial was retained: %v", err)
	}
	for _, path := range []string{recent, other} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("%s unexpectedly removed: %v", path, err)
		}
	}
}

func TestNewExistingAndLinkConfinement(t *testing.T) {
	root := t.TempDir()
	if _, err := NewExisting(root); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(root, "missing")
	if _, err := NewExisting(missing); !os.IsNotExist(err) {
		t.Fatalf("missing root error=%v", err)
	}
	file := filepath.Join(root, "file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := NewExisting(file); err == nil {
		t.Fatal("expected non-directory root rejection")
	}
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, rel := range []string{"../link", "nested/../../link"} {
		if _, err := tr.ReadLink(context.Background(), rel); err == nil {
			t.Fatalf("ReadLink(%q) accepted escape", rel)
		}
		if err := tr.Symlink(context.Background(), "target", rel); err == nil {
			t.Fatalf("Symlink(%q) accepted escape", rel)
		}
	}
}

func TestCopyAcrossDevicesDefaultsAndErrors(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "src"), filepath.Join(dir, "dst")
	if err := os.WriteFile(src, []byte("copy"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := copyAcrossDevices(src, dst, 0, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dst, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(dst)
	if err != nil || string(got) != "copy" {
		t.Fatalf("copied=%q err=%v", got, err)
	}
	if err := copyAcrossDevices(filepath.Join(dir, "missing"), dst, 0, time.Time{}); err == nil {
		t.Fatal("expected missing source error")
	}
}

func TestStagingValidationAndWriteDecodeErrors(t *testing.T) {
	root := t.TempDir()
	file := filepath.Join(root, "staging-file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := ensureStagingDir(file); err == nil {
		t.Fatal("expected non-directory staging rejection")
	}
	link := filepath.Join(root, "staging-link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatal(err)
	}
	if err := ensureStagingDir(link); err == nil {
		t.Fatal("expected symlink staging rejection")
	}
	if err := ensureStagingDir(filepath.Join(root, "created")); err != nil {
		t.Fatal(err)
	}

	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tr.ReadLink(context.Background(), "missing"); err == nil {
		t.Fatal("expected missing link error")
	}
	ws, err := tr.BeginWrite(context.Background(), "bad.bin", 1)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Abort() }()
	if err := ws.WriteChunk(context.Background(), 0, compress.CodecZstd, 1, []byte("not-zstd")); err == nil {
		t.Fatal("expected compressed-data decode error")
	}
}

func TestCommitIsIdempotent(t *testing.T) {
	tr, err := New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("x")
	ws, err := tr.BeginWrite(context.Background(), "x", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(context.Background(), 0, compress.CodecNone, 1, data); err != nil {
		t.Fatal(err)
	}
	if err := ws.Commit(context.Background(), chunk.Sum(data), 0o600, time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := ws.Commit(context.Background(), chunk.Digest{}, 0, time.Time{}); err != nil {
		t.Fatalf("second commit: %v", err)
	}
}

func TestLocalOperationErrors(t *testing.T) {
	root := t.TempDir()
	tr, err := New(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := tr.Stat(ctx, "missing"); err == nil {
		t.Fatal("expected missing stat error")
	}
	if err := tr.MkdirAll(ctx, "../escape"); err == nil {
		t.Fatal("expected mkdir confinement error")
	}
	if err := tr.Chmod(ctx, "../escape", 0o600); err == nil {
		t.Fatal("expected chmod confinement error")
	}
	if err := os.WriteFile(filepath.Join(root, "blocked"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.BeginWrite(ctx, "blocked/child", 1); err == nil {
		t.Fatal("expected blocked destination parent error")
	}
	if err := copyAcrossDevices(filepath.Join(root, "blocked"), root, 0o600, time.Time{}); err == nil {
		t.Fatal("expected destination directory error")
	}
}

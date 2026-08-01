package local

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/fsmeta"
	"github.com/shaneburrell/quiksync/internal/transport"
)

type Transport struct {
	root string
	nfs  bool
}

func New(root string) (*Transport, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, err
	}
	return &Transport{root: abs, nfs: isNFS(abs)}, nil
}

func (t *Transport) Caps() transport.Caps {
	return transport.Caps{
		SupportsDelta:      true,
		SupportsMultiplex:  true,
		SupportsResume:     true,
		SupportsReuseChunk: true,
	}
}

// OnNFS reports whether the root appears to be an NFS mount.
func (t *Transport) OnNFS() bool { return t.nfs }

func (t *Transport) Close() error { return nil }

func (t *Transport) Root() string { return t.root }

func (t *Transport) abs(rel string) (string, error) {
	return transport.Confine(t.root, rel)
}

func (t *Transport) Walk(ctx context.Context, exclude []string) ([]transport.FileMeta, error) {
	files, err := fsmeta.Walk(t.root, exclude)
	if err != nil {
		return nil, err
	}
	out := make([]transport.FileMeta, 0, len(files))
	for _, f := range files {
		out = append(out, transport.FileMeta{
			RelPath: f.RelPath,
			Size:    f.Size,
			ModTime: f.ModTime,
			Mode:    f.Mode,
		})
	}
	return out, nil
}

func (t *Transport) Stat(ctx context.Context, rel string) (transport.FileMeta, error) {
	p, err := t.abs(rel)
	if err != nil {
		return transport.FileMeta{}, err
	}
	st, err := os.Stat(p)
	if err != nil {
		return transport.FileMeta{}, err
	}
	return transport.FileMeta{
		RelPath: rel,
		Size:    st.Size(),
		ModTime: st.ModTime(),
		Mode:    st.Mode(),
	}, nil
}

func (t *Transport) OpenRead(ctx context.Context, rel string) (io.ReadCloser, error) {
	return transport.OpenConfined(t.root, rel)
}

func (t *Transport) Remove(ctx context.Context, rel string) error {
	p, err := t.abs(rel)
	if err != nil {
		return err
	}
	return os.Remove(p)
}

func (t *Transport) MkdirAll(ctx context.Context, rel string) error {
	p, err := t.abs(rel)
	if err != nil {
		return err
	}
	return os.MkdirAll(p, 0o755)
}

func (t *Transport) GetSignature(ctx context.Context, rel string) (chunk.FileSignature, error) {
	f, err := transport.OpenConfined(t.root, rel)
	if err != nil {
		if os.IsNotExist(err) {
			return chunk.FileSignature{}, nil
		}
		return chunk.FileSignature{}, err
	}
	defer func() { _ = f.Close() }()
	st, err := f.Stat()
	if err != nil {
		return chunk.FileSignature{}, err
	}
	return chunk.ChunkReader(f, st.Size(), chunk.Options{})
}

type writeSession struct {
	destAbs    string
	tempAbs    string
	f          *os.File
	old        *os.File
	oldSize    int64
	oldModNano int64
	size       int64
	committed  bool
	nfs        bool
}

func (t *Transport) BeginWrite(ctx context.Context, rel string, size int64) (transport.WriteSession, error) {
	dest, err := t.abs(rel)
	if err != nil {
		return nil, err
	}
	// Prefer staging beside dest for same-directory rename (important on NFS).
	tmpDir := filepath.Join(filepath.Dir(dest), ".quiksync.tmp")
	if err := ensureStagingDir(tmpDir); err != nil {
		tmpDir = filepath.Join(t.root, ".quiksync.tmp")
		if err := ensureStagingDir(tmpDir); err != nil {
			return nil, err
		}
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return nil, err
	}
	// Unique per-session name: never unlink a peer writer's staging file.
	f, err := os.CreateTemp(tmpDir, "qs-*.partial")
	if err != nil {
		return nil, err
	}
	tmp := f.Name()
	if size > 0 {
		if err := f.Truncate(size); err != nil {
			_ = f.Close()
			_ = os.Remove(tmp)
			return nil, err
		}
	}
	ws := &writeSession{destAbs: dest, tempAbs: tmp, f: f, size: size, nfs: t.nfs}
	if st, err := os.Lstat(dest); err == nil && st.Mode().IsRegular() {
		old, err := transport.OpenConfined(t.root, rel)
		if err == nil {
			ws.old = old
			ws.oldSize = st.Size()
			// Use Stat for mtime of the regular file (Lstat size matches).
			if st2, err := os.Stat(dest); err == nil {
				ws.oldModNano = st2.ModTime().UnixNano()
				ws.oldSize = st2.Size()
			}
		}
	}
	return ws, nil
}

// ensureStagingDir creates tmpDir as a real directory, rejecting symlink paths.
func ensureStagingDir(tmpDir string) error {
	fi, err := os.Lstat(tmpDir)
	if err != nil {
		if os.IsNotExist(err) {
			return os.MkdirAll(tmpDir, 0o755)
		}
		return err
	}
	if fi.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("staging dir is a symlink: %s", tmpDir)
	}
	if !fi.IsDir() {
		return fmt.Errorf("staging path is not a directory: %s", tmpDir)
	}
	return nil
}

func (w *writeSession) WriteChunk(ctx context.Context, offset uint64, codec compress.Codec, uncompressedLen int, data []byte) error {
	if w.committed {
		return fmt.Errorf("write after commit")
	}
	raw, err := compress.Decode(codec, data, uncompressedLen)
	if err != nil {
		return err
	}
	if _, err := w.f.WriteAt(raw, int64(offset)); err != nil {
		return err
	}
	return nil
}

func (w *writeSession) ReuseChunk(ctx context.Context, newOffset, oldOffset uint64, digest chunk.Digest, length int) error {
	if w.committed {
		return fmt.Errorf("reuse after commit")
	}
	if w.old == nil {
		return fmt.Errorf("reuse chunk: no existing destination file")
	}
	if err := w.checkOldUnchanged(); err != nil {
		return err
	}
	if err := transport.ValidateReuseRange(oldOffset, length, w.oldSize); err != nil {
		return err
	}
	buf := make([]byte, length)
	n, err := w.old.ReadAt(buf, int64(oldOffset))
	if err != nil && err != io.EOF {
		return fmt.Errorf("reuse read: %w", err)
	}
	if n != length {
		return fmt.Errorf("reuse read: short read %d want %d", n, length)
	}
	if got := chunk.Sum(buf); got != digest {
		return fmt.Errorf("reuse digest mismatch at old_offset=%d", oldOffset)
	}
	if _, err := w.f.WriteAt(buf, int64(newOffset)); err != nil {
		return err
	}
	return nil
}

func (w *writeSession) checkOldUnchanged() error {
	st, err := os.Stat(w.destAbs)
	if err != nil {
		return fmt.Errorf("reuse TOCTOU: dest changed: %w", err)
	}
	if st.Size() != w.oldSize || st.ModTime().UnixNano() != w.oldModNano {
		return fmt.Errorf("reuse TOCTOU: dest size/mtime changed during write")
	}
	return nil
}

func (w *writeSession) Commit(ctx context.Context, expected chunk.Digest, mode os.FileMode, modTime time.Time) error {
	if w.committed {
		return nil
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	got, _, err := chunk.HashFile(w.f)
	if err != nil {
		return err
	}
	if got != expected {
		return fmt.Errorf("digest mismatch: got %s want %s", got, expected)
	}
	if err := w.f.Sync(); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f = nil
	if w.old != nil {
		_ = w.old.Close()
		w.old = nil
	}
	if mode != 0 {
		if err := os.Chmod(w.tempAbs, mode.Perm()); err != nil {
			return fmt.Errorf("chmod: %w", err)
		}
	}
	if !modTime.IsZero() {
		if err := os.Chtimes(w.tempAbs, modTime, modTime); err != nil {
			return fmt.Errorf("chtimes: %w", err)
		}
	}
	if err := os.Rename(w.tempAbs, w.destAbs); err != nil {
		return err
	}
	w.committed = true
	if dir, err := os.Open(filepath.Dir(w.destAbs)); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func (w *writeSession) Abort() error {
	if w.committed {
		return nil
	}
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
	if w.old != nil {
		_ = w.old.Close()
		w.old = nil
	}
	err := os.Remove(w.tempAbs)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

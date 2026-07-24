package local

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
}

func New(root string) (*Transport, error) {
	abs, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return nil, err
	}
	return &Transport{root: abs}, nil
}

func (t *Transport) Caps() transport.Caps {
	return transport.Caps{SupportsDelta: true, SupportsMultiplex: true, SupportsResume: true}
}

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
	p, err := t.abs(rel)
	if err != nil {
		return nil, err
	}
	return os.Open(p)
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
	p, err := t.abs(rel)
	if err != nil {
		return chunk.FileSignature{}, err
	}
	f, err := os.Open(p)
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
	destAbs   string
	tempAbs   string
	f         *os.File
	size      int64
	committed bool
}

func partialTempName(rel string) string {
	sum := sha256.Sum256([]byte(filepath.ToSlash(rel)))
	return hex.EncodeToString(sum[:8]) + ".partial"
}

func (t *Transport) BeginWrite(ctx context.Context, rel string, size int64) (transport.WriteSession, error) {
	dest, err := t.abs(rel)
	if err != nil {
		return nil, err
	}
	tmpDir := filepath.Join(t.root, ".quiksync.tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return nil, err
	}
	tmpRel := filepath.ToSlash(rel) + ".partial"
	tmp, err := transport.SafeJoin(tmpDir, tmpRel)
	if err != nil {
		tmp = filepath.Join(tmpDir, partialTempName(rel))
	}
	if err := os.MkdirAll(filepath.Dir(tmp), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	if size > 0 {
		if err := f.Truncate(size); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	return &writeSession{destAbs: dest, tempAbs: tmp, f: f, size: size}, nil
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
	// Best-effort fsync of parent directory.
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
	err := os.Remove(w.tempAbs)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

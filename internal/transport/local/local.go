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

func (t *Transport) abs(rel string) string {
	return filepath.Join(t.root, filepath.FromSlash(rel))
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
	st, err := os.Stat(t.abs(rel))
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
	return os.Open(t.abs(rel))
}

func (t *Transport) Remove(ctx context.Context, rel string) error {
	return os.Remove(t.abs(rel))
}

func (t *Transport) MkdirAll(ctx context.Context, rel string) error {
	return os.MkdirAll(t.abs(rel), 0o755)
}

func (t *Transport) GetSignature(ctx context.Context, rel string) (chunk.FileSignature, error) {
	f, err := os.Open(t.abs(rel))
	if err != nil {
		if os.IsNotExist(err) {
			return chunk.FileSignature{}, nil
		}
		return chunk.FileSignature{}, err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return chunk.FileSignature{}, err
	}
	return chunk.ChunkReader(f, st.Size(), chunk.Options{})
}

type writeSession struct {
	destAbs  string
	tempAbs  string
	f        *os.File
	size     int64
	written  map[uint64]uint32
}

func (t *Transport) BeginWrite(ctx context.Context, rel string, size int64) (transport.WriteSession, error) {
	dest := t.abs(rel)
	tmpDir := filepath.Join(t.root, ".quiksync.tmp")
	if err := os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
		return nil, err
	}
	tmp := filepath.Join(tmpDir, filepath.FromSlash(rel)+".partial")
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
	return &writeSession{destAbs: dest, tempAbs: tmp, f: f, size: size, written: map[uint64]uint32{}}, nil
}

func (w *writeSession) WriteChunk(ctx context.Context, offset uint64, codec compress.Codec, uncompressedLen int, data []byte) error {
	raw, err := compress.Decode(codec, data, uncompressedLen)
	if err != nil {
		return err
	}
	if _, err := w.f.WriteAt(raw, int64(offset)); err != nil {
		return err
	}
	w.written[offset] = uint32(len(raw))
	return nil
}

func (w *writeSession) Commit(ctx context.Context, expected chunk.Digest, mode os.FileMode, modTime time.Time) error {
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
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f = nil
	if mode != 0 {
		_ = os.Chmod(w.tempAbs, mode.Perm())
	}
	if !modTime.IsZero() {
		_ = os.Chtimes(w.tempAbs, modTime, modTime)
	}
	return os.Rename(w.tempAbs, w.destAbs)
}

func (w *writeSession) Abort() error {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
	return os.Remove(w.tempAbs)
}

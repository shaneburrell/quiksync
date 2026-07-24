package transport

import (
	"context"
	"io"
	"os"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
)

// Caps describes transport capabilities.
type Caps struct {
	SupportsDelta     bool
	SupportsMultiplex bool
	SupportsResume    bool
}

// FileMeta is listing metadata.
type FileMeta struct {
	RelPath string
	Size    int64
	ModTime time.Time
	Mode    os.FileMode
}

// WriteSession is an atomic staged write.
type WriteSession interface {
	WriteChunk(ctx context.Context, offset uint64, codec compress.Codec, uncompressedLen int, data []byte) error
	Commit(ctx context.Context, expected chunk.Digest, mode os.FileMode, modTime time.Time) error
	Abort() error
}

// Transport is the common backend interface.
type Transport interface {
	Caps() Caps
	Close() error
	Root() string
	Walk(ctx context.Context, exclude []string) ([]FileMeta, error)
	Stat(ctx context.Context, rel string) (FileMeta, error)
	OpenRead(ctx context.Context, rel string) (io.ReadCloser, error)
	Remove(ctx context.Context, rel string) error
	MkdirAll(ctx context.Context, rel string) error
	BeginWrite(ctx context.Context, rel string, size int64) (WriteSession, error)
	GetSignature(ctx context.Context, rel string) (chunk.FileSignature, error)
}

// Endpoint parses a URI-like source/dest into scheme + path (+ remote).
type Endpoint struct {
	Scheme string // file, ssh, quiksync
	User   string
	Host   string
	Port   string
	Path   string
	Raw    string
}

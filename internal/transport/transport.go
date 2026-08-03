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
	SupportsDelta      bool `json:"supports_delta"`
	SupportsMultiplex  bool `json:"supports_multiplex"`
	SupportsResume     bool `json:"supports_resume"`
	SupportsReuseChunk bool `json:"supports_reuse_chunk"`
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
	// ReuseChunk copies length bytes from the existing dest file at oldOffset into staging at newOffset.
	ReuseChunk(ctx context.Context, newOffset, oldOffset uint64, digest chunk.Digest, length int) error
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

// Linker is implemented by transports that can preserve symbolic links.
// It is optional so existing remote transports can explicitly remain
// unsupported without expanding the base Transport contract.
type Linker interface {
	ReadLink(ctx context.Context, rel string) (string, error)
	Symlink(ctx context.Context, target, rel string) error
}

// ModeSetter is implemented by transports that can apply directory modes.
type ModeSetter interface {
	Chmod(ctx context.Context, rel string, mode os.FileMode) error
}

// Endpoint parses a URI-like source/dest into scheme + path (+ remote).
type Endpoint struct {
	Scheme string // file, ssh, quiksync, s3, nfs
	User   string
	Host   string
	Port   string
	Path   string
	Raw    string
}

// OpenOptions configures transport.Open.
type OpenOptions struct {
	Insecure  bool   // skip QUIC TOFU pin verification (labs only)
	AuthToken string // QUIC daemon shared secret
	// CreateRoot permits creating a local file:// root. Set this only for
	// destinations; sources must already exist.
	CreateRoot bool
	// S3
	S3Endpoint string
	S3Region   string
	// StagingDir is used by S3 sparse assemble (client-adjacent temp files).
	StagingDir string
}

package daemon

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/local"
)

const maxConcurrentStreams = 64

// IsLoopbackListen reports whether listen is a loopback host:port.
// Empty host (":4242") is not loopback — it binds all interfaces.
func IsLoopbackListen(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		return false
	}
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// Serve starts a QUIC listener and serves remote-helper sessions.
func Serve(ctx context.Context, cfg ServeConfig) error {
	if cfg.AuthToken == "" && !IsLoopbackListen(cfg.Listen) {
		return fmt.Errorf("non-loopback listen %q requires --auth-token", cfg.Listen)
	}
	if cfg.AuthToken == "" && !cfg.AllowNoAuth {
		return fmt.Errorf("serve requires --auth-token (or QUIKSYNC_AUTH_TOKEN), or --allow-no-auth for labs")
	}
	tlsConf, err := loadOrCreatePinnedTLS(cfg.CertFile, cfg.KeyFile)
	if err != nil {
		return err
	}
	tlsConf.NextProtos = []string{"quiksync"}
	ln, err := quic.ListenAddr(cfg.Listen, tlsConf, &quic.Config{
		KeepAlivePeriod: 10 * time.Second,
		MaxIdleTimeout:  5 * time.Minute,
		EnableDatagrams: false,
	})
	if err != nil {
		return err
	}
	defer func() { _ = ln.Close() }()

	for {
		conn, err := ln.Accept(ctx)
		if err != nil {
			return err
		}
		go handleConn(ctx, conn, cfg)
	}
}

func handleConn(ctx context.Context, conn *quic.Conn, cfg ServeConfig) {
	defer func() { _ = conn.CloseWithError(0, "bye") }()
	sem := make(chan struct{}, maxConcurrentStreams)
	var wg sync.WaitGroup
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			wg.Wait()
			return
		}
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			_ = stream.Close()
			wg.Wait()
			return
		}
		wg.Add(1)
		go func(s *quic.Stream) {
			defer func() {
				if r := recover(); r != nil {
					fmt.Fprintf(os.Stderr, "quiksync serve: stream panic: %v\n", r)
				}
				_ = s.Close()
				<-sem
				wg.Done()
			}()
			_ = RunRemoteHelperOpts(ctx, s, s, HelperOptions{
				DefaultRoot: cfg.Root,
				AuthToken:   cfg.AuthToken,
			})
		}(stream)
	}
}

// DialOptions configures QUIC client dialing.
type DialOptions struct {
	Insecure  bool   // skip TOFU pin verification (labs only)
	AuthToken string // shared secret matching serve --auth-token
}

// Dial connects to a quiksync:// endpoint over QUIC with TOFU pinning.
func Dial(ctx context.Context, ep transport.Endpoint) (*Client, error) {
	return DialOpts(ctx, ep, DialOptions{})
}

// DialOpts is Dial with options.
func DialOpts(ctx context.Context, ep transport.Endpoint, opts DialOptions) (*Client, error) {
	addr := net.JoinHostPort(ep.Host, ep.Port)
	tlsConf := &tls.Config{
		InsecureSkipVerify: true, // custom VerifyConnection performs TOFU
		VerifyConnection:   verifyTOFU(addr, opts.Insecure),
		NextProtos:         []string{"quiksync"},
	}
	conn, err := quic.DialAddr(ctx, addr, tlsConf, &quic.Config{
		KeepAlivePeriod: 10 * time.Second,
		MaxIdleTimeout:  5 * time.Minute,
	})
	if err != nil {
		return nil, err
	}
	stream, err := conn.OpenStreamSync(ctx)
	if err != nil {
		_ = conn.CloseWithError(1, "open stream failed")
		return nil, err
	}
	c := &Client{ep: ep, conn: conn, stream: stream}
	// Server --root is authoritative; send empty Root + auth token.
	if err := protocol.WriteJSON(c.stream, protocol.MsgHello, protocol.Hello{
		Version: "1", Root: "", AuthToken: opts.AuthToken,
	}); err != nil {
		_ = c.Close()
		return nil, err
	}
	typ, payload, err := protocol.ReadMsg(c.stream)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if typ == protocol.MsgErr {
		_ = c.Close()
		return nil, remoteErr(typ, payload)
	}
	if typ != protocol.MsgHelloOK {
		_ = c.Close()
		return nil, fmt.Errorf("quic hello failed: type=%d", typ)
	}
	return c, nil
}

// Client is a QUIC-backed transport.
type Client struct {
	ep     transport.Endpoint
	conn   *quic.Conn
	stream *quic.Stream
	mu     sync.Mutex // serializes framed RPC on the shared stream
}

func (c *Client) Caps() transport.Caps {
	return transport.Caps{SupportsDelta: true, SupportsMultiplex: true, SupportsResume: true}
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = protocol.WriteMsg(c.stream, protocol.MsgBye, nil)
	_ = c.stream.Close()
	return c.conn.CloseWithError(0, "bye")
}

func (c *Client) Root() string { return c.ep.Path }

func (c *Client) Walk(ctx context.Context, exclude []string) ([]transport.FileMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := protocol.WriteJSON(c.stream, protocol.MsgWalk, protocol.WalkReq{Exclude: exclude}); err != nil {
		return nil, err
	}
	typ, payload, err := protocol.ReadMsg(c.stream)
	if err != nil {
		return nil, err
	}
	if typ != protocol.MsgWalkOK {
		return nil, remoteErr(typ, payload)
	}
	var ok protocol.WalkOK
	if err := protocol.DecodeJSON(payload, &ok); err != nil {
		return nil, err
	}
	out := make([]transport.FileMeta, 0, len(ok.Files))
	for _, f := range ok.Files {
		out = append(out, transport.FileMeta{
			RelPath: f.RelPath, Size: f.Size, ModTime: time.Unix(0, f.ModNano), Mode: os.FileMode(f.Mode),
		})
	}
	return out, nil
}

func (c *Client) Stat(ctx context.Context, rel string) (transport.FileMeta, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := protocol.WriteJSON(c.stream, protocol.MsgStat, protocol.PathReq{Rel: rel}); err != nil {
		return transport.FileMeta{}, err
	}
	typ, payload, err := protocol.ReadMsg(c.stream)
	if err != nil {
		return transport.FileMeta{}, err
	}
	if typ != protocol.MsgStatOK {
		return transport.FileMeta{}, remoteErr(typ, payload)
	}
	var f protocol.FileMeta
	if err := protocol.DecodeJSON(payload, &f); err != nil {
		return transport.FileMeta{}, err
	}
	return transport.FileMeta{RelPath: f.RelPath, Size: f.Size, ModTime: time.Unix(0, f.ModNano), Mode: os.FileMode(f.Mode)}, nil
}

func (c *Client) OpenRead(ctx context.Context, rel string) (io.ReadCloser, error) {
	c.mu.Lock()
	if err := protocol.WriteJSON(c.stream, protocol.MsgOpenRead, protocol.PathReq{Rel: rel}); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	// Hold lock until reader drained via lockedReader.
	return &lockedStreamReader{c: c, locked: true}, nil
}

type lockedStreamReader struct {
	c      *Client
	left   []byte
	eof    bool
	locked bool
}

func (r *lockedStreamReader) Read(p []byte) (int, error) {
	if len(r.left) > 0 {
		n := copy(p, r.left)
		r.left = r.left[n:]
		return n, nil
	}
	if r.eof {
		return 0, io.EOF
	}
	typ, payload, err := protocol.ReadMsg(r.c.stream)
	if err != nil {
		return 0, err
	}
	if typ == protocol.MsgOK {
		r.eof = true
		return 0, io.EOF
	}
	if typ == protocol.MsgErr {
		var em protocol.ErrMsg
		_ = protocol.DecodeJSON(payload, &em)
		r.eof = true
		return 0, fmt.Errorf("%s", em.Error)
	}
	if typ != protocol.MsgReadData {
		return 0, fmt.Errorf("unexpected %d", typ)
	}
	n := copy(p, payload)
	if n < len(payload) {
		r.left = payload[n:]
	}
	return n, nil
}

func (r *lockedStreamReader) Close() error {
	var firstErr error
	for !r.eof {
		buf := make([]byte, 64*1024)
		_, err := r.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			firstErr = err
			break
		}
	}
	if r.locked {
		r.c.mu.Unlock()
		r.locked = false
	}
	return firstErr
}

func (c *Client) Remove(ctx context.Context, rel string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := protocol.WriteJSON(c.stream, protocol.MsgRemove, protocol.PathReq{Rel: rel}); err != nil {
		return err
	}
	return expectOK(c.stream)
}

func (c *Client) MkdirAll(ctx context.Context, rel string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := protocol.WriteJSON(c.stream, protocol.MsgMkdir, protocol.PathReq{Rel: rel}); err != nil {
		return err
	}
	return expectOK(c.stream)
}

func (c *Client) GetSignature(ctx context.Context, rel string) (chunk.FileSignature, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := protocol.WriteJSON(c.stream, protocol.MsgGetSig, protocol.PathReq{Rel: rel}); err != nil {
		return chunk.FileSignature{}, err
	}
	typ, payload, err := protocol.ReadMsg(c.stream)
	if err != nil {
		return chunk.FileSignature{}, err
	}
	if typ != protocol.MsgSigOK {
		return chunk.FileSignature{}, remoteErr(typ, payload)
	}
	var s protocol.SigOK
	if err := protocol.DecodeJSON(payload, &s); err != nil {
		return chunk.FileSignature{}, err
	}
	return chunk.FileSignature{Size: s.Size, Digest: s.Digest, Chunks: s.Chunks}, nil
}

type clientWrite struct {
	c         *Client
	committed bool
	holding   bool // c.mu held for session lifetime
}

func (c *Client) BeginWrite(ctx context.Context, rel string, size int64) (transport.WriteSession, error) {
	c.mu.Lock()
	if err := protocol.WriteJSON(c.stream, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: rel, Size: size}); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	if err := expectOK(c.stream); err != nil {
		c.mu.Unlock()
		return nil, err
	}
	// Hold session lock until Commit/Abort so concurrent workers cannot interleave
	// on the single helper write session.
	return &clientWrite{c: c, holding: true}, nil
}

func (w *clientWrite) WriteChunk(ctx context.Context, offset uint64, codec compress.Codec, uncompressedLen int, data []byte) error {
	if w.committed {
		return fmt.Errorf("write after commit")
	}
	if err := protocol.WriteJSON(w.c.stream, protocol.MsgWriteChunk, protocol.WriteChunkReq{
		Offset: offset, Codec: codec, UncompressedLen: uncompressedLen, Data: data,
	}); err != nil {
		return err
	}
	return expectOK(w.c.stream)
}

func (w *clientWrite) Commit(ctx context.Context, expected chunk.Digest, mode os.FileMode, modTime time.Time) error {
	if w.committed {
		return nil
	}
	if err := protocol.WriteJSON(w.c.stream, protocol.MsgCommit, protocol.CommitReq{
		Digest: expected, Mode: uint32(mode), ModNano: modTime.UnixNano(),
	}); err != nil {
		return err
	}
	if err := expectOK(w.c.stream); err != nil {
		return err
	}
	w.committed = true
	w.release()
	return nil
}

func (w *clientWrite) Abort() error {
	if w.committed {
		return nil
	}
	defer w.release()
	_ = protocol.WriteMsg(w.c.stream, protocol.MsgAbort, nil)
	err := expectOK(w.c.stream)
	w.committed = true
	return err
}

func (w *clientWrite) release() {
	if w.holding {
		w.holding = false
		w.c.mu.Unlock()
	}
}

func expectOK(r io.Reader) error {
	typ, payload, err := protocol.ReadMsg(r)
	if err != nil {
		return err
	}
	if typ == protocol.MsgOK {
		return nil
	}
	return remoteErr(typ, payload)
}

func remoteErr(typ protocol.MsgType, payload []byte) error {
	if typ == protocol.MsgErr {
		var em protocol.ErrMsg
		_ = protocol.DecodeJSON(payload, &em)
		return fmt.Errorf("%s", em.Error)
	}
	return fmt.Errorf("unexpected message type %d", typ)
}

// Ensure local package linkage for root defaulting in serve.
var _ = local.New

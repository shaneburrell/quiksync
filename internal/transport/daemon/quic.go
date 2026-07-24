package daemon

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/local"
)

// Serve starts a QUIC listener and serves remote-helper sessions.
func Serve(ctx context.Context, cfg ServeConfig) error {
	tlsConf, err := loadOrGenerateTLS(cfg.CertFile, cfg.KeyFile)
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
		go handleConn(ctx, conn, cfg.Root)
	}
}

func handleConn(ctx context.Context, conn *quic.Conn, defaultRoot string) {
	defer func() { _ = conn.CloseWithError(0, "bye") }()
	for {
		stream, err := conn.AcceptStream(ctx)
		if err != nil {
			return
		}
		go func(s *quic.Stream) {
			defer func() { _ = s.Close() }()
			_ = RunRemoteHelperRoot(ctx, s, s, defaultRoot)
		}(stream)
	}
}

// Dial connects to a quiksync:// endpoint over QUIC.
func Dial(ctx context.Context, ep transport.Endpoint) (*Client, error) {
	addr := net.JoinHostPort(ep.Host, ep.Port)
	tlsConf := &tls.Config{
		InsecureSkipVerify: true, // TOFU-style for v1; pin later
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
	if err := protocol.WriteJSON(c.stream, protocol.MsgHello, protocol.Hello{Version: "1", Root: ep.Path}); err != nil {
		_ = c.Close()
		return nil, err
	}
	typ, payload, err := protocol.ReadMsg(c.stream)
	if err != nil {
		_ = c.Close()
		return nil, err
	}
	if typ != protocol.MsgHelloOK {
		_ = c.Close()
		return nil, fmt.Errorf("quic hello failed: type=%d", typ)
	}
	_ = payload
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
		return nil, fmt.Errorf("walk failed")
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
		return transport.FileMeta{}, fmt.Errorf("stat failed")
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
	return &lockedStreamReader{c: c}, nil
}

type lockedStreamReader struct {
	c    *Client
	left []byte
	eof  bool
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
	// Drain to MsgOK if needed, then unlock.
	for !r.eof {
		buf := make([]byte, 64*1024)
		_, err := r.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			r.c.mu.Unlock()
			return err
		}
	}
	r.c.mu.Unlock()
	return nil
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
		return chunk.FileSignature{}, fmt.Errorf("sig failed")
	}
	var s protocol.SigOK
	if err := protocol.DecodeJSON(payload, &s); err != nil {
		return chunk.FileSignature{}, err
	}
	return chunk.FileSignature{Size: s.Size, Digest: s.Digest, Chunks: s.Chunks}, nil
}

type clientWrite struct{ c *Client }

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
	return &clientWrite{c: c}, nil
}

func (w *clientWrite) WriteChunk(ctx context.Context, offset uint64, codec compress.Codec, uncompressedLen int, data []byte) error {
	if err := protocol.WriteJSON(w.c.stream, protocol.MsgWriteChunk, protocol.WriteChunkReq{
		Offset: offset, Codec: codec, UncompressedLen: uncompressedLen, Data: data,
	}); err != nil {
		return err
	}
	return expectOK(w.c.stream)
}

func (w *clientWrite) Commit(ctx context.Context, expected chunk.Digest, mode os.FileMode, modTime time.Time) error {
	defer w.c.mu.Unlock()
	if err := protocol.WriteJSON(w.c.stream, protocol.MsgCommit, protocol.CommitReq{
		Digest: expected, Mode: uint32(mode), ModNano: modTime.UnixNano(),
	}); err != nil {
		return err
	}
	return expectOK(w.c.stream)
}

func (w *clientWrite) Abort() error {
	defer w.c.mu.Unlock()
	_ = protocol.WriteMsg(w.c.stream, protocol.MsgAbort, nil)
	return expectOK(w.c.stream)
}

func expectOK(r io.Reader) error {
	typ, payload, err := protocol.ReadMsg(r)
	if err != nil {
		return err
	}
	if typ == protocol.MsgOK {
		return nil
	}
	if typ == protocol.MsgErr {
		var em protocol.ErrMsg
		_ = protocol.DecodeJSON(payload, &em)
		return fmt.Errorf("%s", em.Error)
	}
	return fmt.Errorf("unexpected type %d", typ)
}

func loadOrGenerateTLS(certFile, keyFile string) (*tls.Config, error) {
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
	}
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, priv.Public(), priv)
	if err != nil {
		return nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
}

// Ensure local package linkage for root defaulting in serve.
var _ = local.New

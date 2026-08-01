package ssh

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"sync"
	"time"

	cryptossh "golang.org/x/crypto/ssh"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
)

// Command is the SSH executable (overridable in tests).
var Command = "ssh"

// Transport talks to a remote `quiksync remote-helper` over SSH stdio.
type Transport struct {
	ep      transport.Endpoint
	cmd     *exec.Cmd
	client  *cryptossh.Client
	session *cryptossh.Session
	stdin   io.WriteCloser
	stdout  io.Reader
	mu      sync.Mutex
	caps    transport.Caps
}

func New(ctx context.Context, ep transport.Endpoint) (*Transport, error) {
	var t *Transport
	var err error
	if useNativeSSH() {
		t, err = newNative(ctx, ep)
	} else {
		t, err = newExec(ctx, ep)
	}
	if err != nil {
		return nil, err
	}
	if err := protocol.WriteJSON(t.stdin, protocol.MsgHello, protocol.Hello{Version: protocol.ProtocolVersion, Root: ep.Path}); err != nil {
		_ = t.Close()
		return nil, err
	}
	typ, payload, err := protocol.ReadMsg(t.stdout)
	if err != nil {
		_ = t.Close()
		return nil, err
	}
	if typ == protocol.MsgErr {
		var em protocol.ErrMsg
		_ = protocol.DecodeJSON(payload, &em)
		_ = t.Close()
		return nil, fmt.Errorf("remote: %s", em.Error)
	}
	if typ != protocol.MsgHelloOK {
		_ = t.Close()
		return nil, fmt.Errorf("unexpected hello response %d", typ)
	}
	var ok protocol.HelloOK
	if err := protocol.DecodeJSON(payload, &ok); err != nil {
		_ = t.Close()
		return nil, err
	}
	if err := protocol.CheckPeerVersion(ok.Version); err != nil {
		_ = t.Close()
		return nil, err
	}
	t.caps = mapCaps(ok)
	// SSH is single-stream regardless of remote advertisement.
	t.caps.SupportsMultiplex = false
	return t, nil
}

func newNative(ctx context.Context, ep transport.Endpoint) (*Transport, error) {
	_ = ctx
	client, session, err := dialNative(ep)
	if err != nil {
		return nil, err
	}
	stdin, err := session.StdinPipe()
	if err != nil {
		_ = session.Close()
		_ = client.Close()
		return nil, err
	}
	stdout, err := session.StdoutPipe()
	if err != nil {
		_ = stdin.Close()
		_ = session.Close()
		_ = client.Close()
		return nil, err
	}
	stderr, err := session.StderrPipe()
	if err != nil {
		_ = stdin.Close()
		_ = session.Close()
		_ = client.Close()
		return nil, err
	}
	go func() { _, _ = io.Copy(io.Discard, stderr) }()
	if err := session.Start("quiksync remote-helper"); err != nil {
		_ = stdin.Close()
		_ = session.Close()
		_ = client.Close()
		return nil, fmt.Errorf("ssh start remote-helper: %w", err)
	}
	return &Transport{ep: ep, client: client, session: session, stdin: stdin, stdout: stdout}, nil
}

func newExec(ctx context.Context, ep transport.Endpoint) (*Transport, error) {
	args := execArgs(ep)
	cmd := exec.CommandContext(ctx, Command, args...)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, err
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return nil, err
	}
	// Drain stderr in a goroutine. Sharing os.Stderr can deadlock under nested
	// SSH (e.g. Mac→Windows→Linux) when OpenSSH fills the channel window.
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return nil, err
	}
	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("ssh start: %w", err)
	}
	go func() { _, _ = io.Copy(io.Discard, stderr) }()
	return &Transport{ep: ep, cmd: cmd, stdin: stdin, stdout: stdout}, nil
}

// execArgs builds the OpenSSH argv. "--" stops option parsing so a crafted
// host/user cannot be interpreted as ssh flags (e.g. -oProxyCommand=...).
func execArgs(ep transport.Endpoint) []string {
	target := ep.Host
	if ep.User != "" {
		target = ep.User + "@" + ep.Host
	}
	// -T: no TTY (required for binary remote-helper framing).
	// BatchMode: never block on interactive password/host-key prompts.
	args := []string{"-T", "-o", "BatchMode=yes"}
	if ep.Port != "" {
		args = append(args, "-p", ep.Port)
	}
	args = append(args, "--", target, "quiksync", "remote-helper")
	return args
}

func mapCaps(ok protocol.HelloOK) transport.Caps {
	if ok.Version == "" || ok.Version == "1" {
		// Pre-reuse remotes: full-wire fallback only.
		return transport.Caps{SupportsDelta: true, SupportsMultiplex: false, SupportsResume: true}
	}
	return transport.Caps{
		SupportsDelta:      ok.Caps.SupportsDelta,
		SupportsMultiplex:  ok.Caps.SupportsMultiplex,
		SupportsResume:     ok.Caps.SupportsResume,
		SupportsReuseChunk: ok.Caps.SupportsReuseChunk,
	}
}

func (t *Transport) Caps() transport.Caps { return t.caps }

func (t *Transport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_ = protocol.WriteMsg(t.stdin, protocol.MsgBye, nil)
	_ = t.stdin.Close()
	if c, ok := t.stdout.(io.Closer); ok {
		_ = c.Close()
	}
	var err error
	if t.session != nil {
		_ = t.session.Close()
		t.session = nil
	}
	if t.client != nil {
		err = t.client.Close()
		t.client = nil
	}
	if t.cmd != nil {
		err = t.cmd.Wait()
		t.cmd = nil
	}
	return err
}

func (t *Transport) Root() string { return t.ep.Path }

func (t *Transport) Walk(ctx context.Context, exclude []string) ([]transport.FileMeta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := protocol.WriteJSON(t.stdin, protocol.MsgWalk, protocol.WalkReq{Exclude: exclude}); err != nil {
		return nil, err
	}
	typ, payload, err := protocol.ReadMsg(t.stdout)
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
			RelPath: f.RelPath,
			Size:    f.Size,
			ModTime: time.Unix(0, f.ModNano),
			Mode:    os.FileMode(f.Mode),
		})
	}
	return out, nil
}

func (t *Transport) Stat(ctx context.Context, rel string) (transport.FileMeta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := protocol.WriteJSON(t.stdin, protocol.MsgStat, protocol.PathReq{Rel: rel}); err != nil {
		return transport.FileMeta{}, err
	}
	typ, payload, err := protocol.ReadMsg(t.stdout)
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
	return transport.FileMeta{
		RelPath: f.RelPath,
		Size:    f.Size,
		ModTime: time.Unix(0, f.ModNano),
		Mode:    os.FileMode(f.Mode),
	}, nil
}

func (t *Transport) OpenRead(ctx context.Context, rel string) (io.ReadCloser, error) {
	t.mu.Lock()
	if err := protocol.WriteJSON(t.stdin, protocol.MsgOpenRead, protocol.PathReq{Rel: rel}); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	return &remoteReader{t: t, locked: true}, nil
}

type remoteReader struct {
	t      *Transport
	left   []byte
	eof    bool
	locked bool
}

func (r *remoteReader) Read(p []byte) (int, error) {
	if len(r.left) > 0 {
		n := copy(p, r.left)
		r.left = r.left[n:]
		return n, nil
	}
	if r.eof {
		return 0, io.EOF
	}
	typ, payload, err := protocol.ReadMsg(r.t.stdout)
	if err != nil {
		return 0, err
	}
	if typ == protocol.MsgOK {
		r.eof = true
		return 0, io.EOF
	}
	if typ == protocol.MsgErr {
		r.eof = true
		return 0, remoteErr(typ, payload)
	}
	if typ != protocol.MsgReadData {
		r.eof = true
		return 0, remoteErr(typ, payload)
	}
	n := copy(p, payload)
	if n < len(payload) {
		r.left = payload[n:]
	}
	return n, nil
}

func (r *remoteReader) Close() error {
	for !r.eof {
		buf := make([]byte, 64*1024)
		_, err := r.Read(buf)
		if err == io.EOF {
			break
		}
		if err != nil {
			if r.locked {
				r.t.mu.Unlock()
				r.locked = false
			}
			return err
		}
	}
	if r.locked {
		r.t.mu.Unlock()
		r.locked = false
	}
	return nil
}

func (t *Transport) Remove(ctx context.Context, rel string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := protocol.WriteJSON(t.stdin, protocol.MsgRemove, protocol.PathReq{Rel: rel}); err != nil {
		return err
	}
	return expectOK(t.stdout)
}

func (t *Transport) MkdirAll(ctx context.Context, rel string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := protocol.WriteJSON(t.stdin, protocol.MsgMkdir, protocol.PathReq{Rel: rel}); err != nil {
		return err
	}
	return expectOK(t.stdout)
}

func (t *Transport) GetSignature(ctx context.Context, rel string) (chunk.FileSignature, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := protocol.WriteJSON(t.stdin, protocol.MsgGetSig, protocol.PathReq{Rel: rel}); err != nil {
		return chunk.FileSignature{}, err
	}
	typ, payload, err := protocol.ReadMsg(t.stdout)
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

type writeSession struct {
	t         *Transport
	committed bool
	holding   bool
}

func (t *Transport) BeginWrite(ctx context.Context, rel string, size int64) (transport.WriteSession, error) {
	t.mu.Lock()
	if err := protocol.WriteJSON(t.stdin, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: rel, Size: size}); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	if err := expectOK(t.stdout); err != nil {
		t.mu.Unlock()
		return nil, err
	}
	return &writeSession{t: t, holding: true}, nil
}

func (w *writeSession) WriteChunk(ctx context.Context, offset uint64, codec compress.Codec, uncompressedLen int, data []byte) error {
	if w.committed {
		return fmt.Errorf("write after commit")
	}
	if err := protocol.WriteJSON(w.t.stdin, protocol.MsgWriteChunk, protocol.WriteChunkReq{
		Offset: offset, Codec: codec, UncompressedLen: uncompressedLen, Data: data,
	}); err != nil {
		return err
	}
	return expectOK(w.t.stdout)
}

func (w *writeSession) ReuseChunk(ctx context.Context, newOffset, oldOffset uint64, digest chunk.Digest, length int) error {
	if w.committed {
		return fmt.Errorf("reuse after commit")
	}
	if !w.t.caps.SupportsReuseChunk {
		return fmt.Errorf("remote does not support reuse chunk; upgrade quiksync on the remote host")
	}
	if err := protocol.WriteJSON(w.t.stdin, protocol.MsgReuseChunk, protocol.ReuseChunkReq{
		NewOffset: newOffset, OldOffset: oldOffset, Digest: digest, Length: length,
	}); err != nil {
		return err
	}
	return expectOK(w.t.stdout)
}

// RelayNotify sends a wakeup-only mid-hop notify (control plane).
func (t *Transport) RelayNotify(ctx context.Context, meta protocol.RelayNotifyMeta) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_ = ctx
	if err := protocol.WriteJSON(t.stdin, protocol.MsgRelayNotify, meta); err != nil {
		return err
	}
	return expectOK(t.stdout)
}

// RelayWait asks the peer to acknowledge a wait (paired with store poll).
func (t *Transport) RelayWait(ctx context.Context, meta protocol.RelayNotifyMeta) (protocol.RelayNotifyMeta, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	_ = ctx
	if err := protocol.WriteJSON(t.stdin, protocol.MsgRelayWait, meta); err != nil {
		return protocol.RelayNotifyMeta{}, err
	}
	typ, payload, err := protocol.ReadMsg(t.stdout)
	if err != nil {
		return protocol.RelayNotifyMeta{}, err
	}
	if typ != protocol.MsgRelayWaitOK {
		return protocol.RelayNotifyMeta{}, remoteErr(typ, payload)
	}
	var out protocol.RelayNotifyMeta
	if err := protocol.DecodeJSON(payload, &out); err != nil {
		return protocol.RelayNotifyMeta{}, err
	}
	return out, nil
}

func (w *writeSession) Commit(ctx context.Context, expected chunk.Digest, mode os.FileMode, modTime time.Time) error {
	if w.committed {
		return nil
	}
	if err := protocol.WriteJSON(w.t.stdin, protocol.MsgCommit, protocol.CommitReq{
		Digest: expected, Mode: uint32(mode), ModNano: modTime.UnixNano(),
	}); err != nil {
		return err
	}
	if err := expectOK(w.t.stdout); err != nil {
		return err
	}
	w.committed = true
	w.release()
	return nil
}

func (w *writeSession) Abort() error {
	if w.committed {
		return nil
	}
	defer w.release()
	_ = protocol.WriteMsg(w.t.stdin, protocol.MsgAbort, nil)
	err := expectOK(w.t.stdout)
	w.committed = true
	return err
}

func (w *writeSession) release() {
	if w.holding {
		w.holding = false
		w.t.mu.Unlock()
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

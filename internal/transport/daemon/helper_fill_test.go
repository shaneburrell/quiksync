package daemon

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
)

func helperSession(t *testing.T, root string) (clientR io.Reader, clientW io.WriteCloser, errCh <-chan error, cleanup func()) {
	t.Helper()
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	ch := make(chan error, 1)
	go func() { ch <- RunRemoteHelperRoot(context.Background(), sr, sw, root) }()
	if err := protocol.WriteJSON(cw, protocol.MsgHello, protocol.Hello{Version: protocol.ProtocolVersion, Root: root}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(cr); err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("hello: typ=%v err=%v", typ, err)
	}
	cleanup = func() {
		_ = protocol.WriteMsg(cw, protocol.MsgBye, nil)
		_ = cw.Close()
		select {
		case <-ch:
		case <-time.After(2 * time.Second):
		}
	}
	return cr, cw, ch, cleanup
}

func TestHelperPeerStatsTuneUnknown(t *testing.T) {
	root := t.TempDir()
	r, w, _, cleanup := helperSession(t, root)
	defer cleanup()

	if err := protocol.WriteMsg(w, protocol.MsgPeerStats, nil); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgPeerStats {
		t.Fatalf("peerstats: typ=%v err=%v", typ, err)
	}
	if err := protocol.WriteJSON(w, protocol.MsgTuneOffer, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("tune offer: typ=%v err=%v", typ, err)
	}
	if err := protocol.WriteJSON(w, protocol.MsgTuneApply, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("tune apply: typ=%v err=%v", typ, err)
	}
	if err := protocol.WriteMsg(w, 0xFE, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("unknown: typ=%v err=%v", typ, err)
	}
}

func TestHelperBadJSONAndFailures(t *testing.T) {
	root := t.TempDir()
	r, w, _, cleanup := helperSession(t, root)
	defer cleanup()

	// Bad JSON on walk
	if err := protocol.WriteMsg(w, protocol.MsgWalk, []byte("{")); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("bad walk: typ=%v err=%v", typ, err)
	}

	// BeginWrite then WriteChunk with corrupt compressed payload
	if err := protocol.WriteJSON(w, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "f.bin", Size: 4}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgOK {
		t.Fatalf("begin: typ=%v err=%v", typ, err)
	}
	if err := protocol.WriteJSON(w, protocol.MsgWriteChunk, protocol.WriteChunkReq{
		Offset: 0, Codec: compress.CodecLZ4, UncompressedLen: 4, Data: []byte("not-lz4"),
	}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("bad writechunk: typ=%v err=%v", typ, err)
	}

	// Fresh session + commit digest mismatch
	if err := protocol.WriteJSON(w, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "f.bin", Size: 4}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgOK {
		t.Fatal(err)
	}
	if err := protocol.WriteJSON(w, protocol.MsgWriteChunk, protocol.WriteChunkReq{
		Offset: 0, Codec: compress.CodecNone, UncompressedLen: 4, Data: []byte("abcd"),
	}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgOK {
		t.Fatal(err)
	}
	if err := protocol.WriteJSON(w, protocol.MsgCommit, protocol.CommitReq{Digest: chunk.Digest{9}, Mode: 0o644}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("commit mismatch: typ=%v err=%v", typ, err)
	}

	// Successful write for reuse failure next
	data := []byte("reuse-src")
	if err := protocol.WriteJSON(w, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "r.bin", Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	_, _, _ = protocol.ReadMsg(r)
	_ = protocol.WriteJSON(w, protocol.MsgWriteChunk, protocol.WriteChunkReq{
		Offset: 0, Codec: compress.CodecNone, UncompressedLen: len(data), Data: data,
	})
	_, _, _ = protocol.ReadMsg(r)
	_ = protocol.WriteJSON(w, protocol.MsgCommit, protocol.CommitReq{Digest: chunk.Sum(data), Mode: 0o644, ModNano: time.Now().UnixNano()})
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgOK {
		t.Fatalf("commit ok: typ=%v err=%v", typ, err)
	}

	if err := protocol.WriteJSON(w, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "r.bin", Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	_, _, _ = protocol.ReadMsg(r)
	if err := protocol.WriteJSON(w, protocol.MsgReuseChunk, protocol.ReuseChunkReq{
		NewOffset: 0, OldOffset: 0, Digest: chunk.Digest{1}, Length: len(data),
	}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("reuse mismatch: typ=%v err=%v", typ, err)
	}

	// Abort with no session still OK
	if err := protocol.WriteMsg(w, protocol.MsgAbort, nil); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgOK {
		t.Fatalf("abort: typ=%v err=%v", typ, err)
	}

	// A second begin must not silently abort the in-flight session.
	if err := protocol.WriteJSON(w, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "a.txt", Size: 1}); err != nil {
		t.Fatal(err)
	}
	_, _, _ = protocol.ReadMsg(r)
	if err := protocol.WriteJSON(w, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "b.txt", Size: 1}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("busy begin: typ=%v err=%v", typ, err)
	}
	_ = protocol.WriteMsg(w, protocol.MsgAbort, nil)
	_, _, _ = protocol.ReadMsg(r)
}

func TestHelperNotHelloAndEnvRoot(t *testing.T) {
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- RunRemoteHelper(context.Background(), sr, sw) }()
	if err := protocol.WriteJSON(cw, protocol.MsgWalk, protocol.WalkReq{}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected not-hello error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	_ = cw.Close()
	_ = cr.Close()

	root := t.TempDir()
	t.Setenv("QUIKSYNC_ROOT", root)
	cr2, sw2 := io.Pipe()
	sr2, cw2 := io.Pipe()
	errCh2 := make(chan error, 1)
	go func() { errCh2 <- RunRemoteHelperOpts(context.Background(), sr2, sw2, HelperOptions{}) }()
	if err := protocol.WriteJSON(cw2, protocol.MsgHello, protocol.Hello{Version: protocol.ProtocolVersion}); err != nil {
		t.Fatal(err)
	}
	typ, payload, err := protocol.ReadMsg(cr2)
	if err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("env root hello: typ=%v err=%v", typ, err)
	}
	var ok protocol.HelloOK
	_ = protocol.DecodeJSON(payload, &ok)
	if ok.Root == "" {
		t.Fatal("expected root")
	}
	_ = protocol.WriteMsg(cw2, protocol.MsgBye, nil)
	_ = cw2.Close()
	select {
	case <-errCh2:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
}

func TestHelperBeginWriteConfineError(t *testing.T) {
	root := t.TempDir()
	r, w, _, cleanup := helperSession(t, root)
	defer cleanup()
	if err := protocol.WriteJSON(w, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "../escape", Size: 1}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("confine begin: typ=%v err=%v", typ, err)
	}
	if err := protocol.WriteJSON(w, protocol.MsgRemove, protocol.PathReq{Rel: "missing"}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgErr {
		t.Fatalf("remove missing: typ=%v err=%v", typ, err)
	}
	if err := protocol.WriteJSON(w, protocol.MsgMkdir, protocol.PathReq{Rel: "sub/dir"}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(r); err != nil || typ != protocol.MsgOK {
		t.Fatalf("mkdir: typ=%v err=%v", typ, err)
	}
	if _, err := os.Stat(filepath.Join(root, "sub", "dir")); err != nil {
		t.Fatal(err)
	}
}

func TestDialWrapper(t *testing.T) {
	root := t.TempDir()
	ctx, cancel, addr := startQUIC(t, root, "tok")
	defer cancel()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	ep := transport.Endpoint{Scheme: "quiksync", Host: host, Port: port}
	// Dial() uses empty DialOptions — should fail auth against tokenized server
	_, err = Dial(ctx, ep)
	if err == nil {
		t.Fatal("expected Dial auth failure")
	}
}

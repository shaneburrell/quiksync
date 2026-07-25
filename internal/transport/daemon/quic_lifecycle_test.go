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

func startQUIC(t *testing.T, root, token string) (context.Context, context.CancelFunc, string) {
	t.Helper()
	t.Setenv("QUIKSYNC_CONFIG", t.TempDir())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_ = Serve(ctx, ServeConfig{Listen: addr, Root: root, AuthToken: token})
	}()
	time.Sleep(200 * time.Millisecond)
	return ctx, cancel, addr
}

func TestQUICLifecycleWriteStatRemove(t *testing.T) {
	root := t.TempDir()
	const token = "lifecycle-token"
	ctx, cancel, addr := startQUIC(t, root, token)
	defer cancel()

	ep := transport.Endpoint{Scheme: "quiksync", Host: "127.0.0.1", Port: "", Path: "/"}
	// Host includes port from addr
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	ep.Host, ep.Port = host, port

	c, err := DialOpts(ctx, ep, DialOptions{AuthToken: token})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	_ = c.Root()
	if err := c.MkdirAll(ctx, "sub"); err != nil {
		t.Fatal(err)
	}

	data := []byte("quic-lifecycle-payload-0123456789")
	used, enc, err := compress.Encode(compress.CodecLZ4, data)
	if err != nil {
		t.Fatal(err)
	}
	ws, err := c.BeginWrite(ctx, "sub/a.txt", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, used, len(data), enc); err != nil {
		t.Fatal(err)
	}
	dig := chunk.Sum(data)
	if err := ws.Commit(ctx, dig, 0o640, time.Now()); err != nil {
		t.Fatal(err)
	}

	st, err := c.Stat(ctx, "sub/a.txt")
	if err != nil || st.Size != int64(len(data)) {
		t.Fatalf("stat: %+v %v", st, err)
	}
	sig, err := c.GetSignature(ctx, "sub/a.txt")
	if err != nil || sig.Digest != dig {
		t.Fatalf("sig: %+v %v", sig, err)
	}
	rc, err := c.OpenRead(ctx, "sub/a.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(got) != string(data) {
		t.Fatalf("read %q %v", got, err)
	}

	// Abort a half-started write
	ws2, err := c.BeginWrite(ctx, "sub/abort.txt", 4)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws2.WriteChunk(ctx, 0, compress.CodecNone, 4, []byte("abcd"))
	if err := ws2.Abort(); err != nil {
		t.Fatal(err)
	}

	if err := c.Remove(ctx, "sub/a.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Stat(ctx, "sub/a.txt"); err == nil {
		t.Fatal("expected missing after remove")
	}
}

func TestQUICReuseChunkAndRelay(t *testing.T) {
	root := t.TempDir()
	const token = "reuse-relay-token"
	ctx, cancel, addr := startQUIC(t, root, token)
	defer cancel()
	host, port, _ := net.SplitHostPort(addr)
	ep := transport.Endpoint{Scheme: "quiksync", Host: host, Port: port}
	c, err := DialOpts(ctx, ep, DialOptions{AuthToken: token})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	base := []byte("reuse-base-data-0123456789abcdef")
	ws, err := c.BeginWrite(ctx, "r.bin", int64(len(base)))
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

	next := append(append([]byte{}, base...), []byte("-TAIL")...)
	ws2, err := c.BeginWrite(ctx, "r.bin", int64(len(next)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws2.ReuseChunk(ctx, 0, 0, dig, len(base)); err != nil {
		t.Fatal(err)
	}
	tail := []byte("-TAIL")
	if err := ws2.WriteChunk(ctx, uint64(len(base)), compress.CodecNone, len(tail), tail); err != nil {
		t.Fatal(err)
	}
	if err := ws2.Commit(ctx, chunk.Sum(next), 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "r.bin"))
	if err != nil || string(got) != string(next) {
		t.Fatalf("got %q err=%v", got, err)
	}

	job := "quic-relay-job"
	waitDone := make(chan error, 1)
	go func() {
		_, err := c.RelayWait(ctx, protocol.RelayNotifyMeta{JobID: job})
		waitDone <- err
	}()
	time.Sleep(80 * time.Millisecond)

	c2, err := DialOpts(ctx, ep, DialOptions{AuthToken: token})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c2.Close() }()
	if err := c2.RelayNotify(ctx, protocol.RelayNotifyMeta{JobID: job, Generation: 1}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("RelayWait: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RelayWait hung")
	}
}

func TestQUICDialBadAuth(t *testing.T) {
	root := t.TempDir()
	ctx, cancel, addr := startQUIC(t, root, "good-token")
	defer cancel()
	host, port, _ := net.SplitHostPort(addr)
	ep := transport.Endpoint{Scheme: "quiksync", Host: host, Port: port}
	_, err := DialOpts(ctx, ep, DialOptions{AuthToken: "bad"})
	if err == nil {
		t.Fatal("expected auth failure")
	}
}

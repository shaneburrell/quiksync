package daemon

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestQUICOpenReadPartialThenClose(t *testing.T) {
	root := t.TempDir()
	const token = "openread-token"
	ctx, cancel, addr := startQUIC(t, root, token)
	defer cancel()
	host, port, _ := net.SplitHostPort(addr)
	ep := transport.Endpoint{Scheme: "quiksync", Host: host, Port: port}
	c, err := DialOpts(ctx, ep, DialOptions{AuthToken: token})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = c.Close() }()

	// Large enough to span multiple MsgReadData frames.
	data := make([]byte, 200_000)
	for i := range data {
		data[i] = byte(i)
	}
	ws, err := c.BeginWrite(ctx, "big.bin", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data); err != nil {
		t.Fatal(err)
	}
	if err := ws.Commit(ctx, chunk.Sum(data), 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	rc, err := c.OpenRead(ctx, "big.bin")
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 64)
	n, err := rc.Read(buf)
	if err != nil || n != 64 {
		t.Fatalf("partial read: n=%d err=%v", n, err)
	}
	if err := rc.Close(); err != nil {
		t.Fatal(err)
	}

	// Mutex released: subsequent Walk must succeed.
	files, err := c.Walk(ctx, nil)
	if err != nil || len(files) != 1 {
		t.Fatalf("walk after partial close: %v %v", files, err)
	}

	// Missing path OpenRead error
	rc2, err := c.OpenRead(ctx, "missing.bin")
	if err != nil {
		// Dial may return error immediately or on Read
		return
	}
	_, err = io.ReadAll(rc2)
	_ = rc2.Close()
	if err == nil {
		t.Fatal("expected missing OpenRead error")
	}
}

func TestHelperRunRemoteHelperWrapper(t *testing.T) {
	// Cover RunRemoteHelper thin wrapper (empty opts).
	pr, pw := io.Pipe()
	done := make(chan error, 1)
	go func() {
		done <- RunRemoteHelper(context.Background(), pr, io.Discard)
	}()
	// Close without hello → error path
	_ = pw.Close()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected error without hello")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("helper hung")
	}
}

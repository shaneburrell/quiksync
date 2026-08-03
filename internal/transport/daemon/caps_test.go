package daemon

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestMapHelloCaps(t *testing.T) {
	v1 := mapHelloCaps(protocol.HelloOK{Version: "1"})
	if v1.SupportsReuseChunk || v1.SupportsMultiplex {
		t.Fatalf("v1: %+v", v1)
	}
	v2 := mapHelloCaps(protocol.HelloOK{Version: "2", Caps: protocol.DefaultCaps()})
	if !v2.SupportsReuseChunk || v2.SupportsMultiplex {
		t.Fatalf("v2: %+v", v2)
	}
	c := &Client{ep: transport.Endpoint{Path: "/endpoint"}, caps: v2}
	if !c.Caps().SupportsReuseChunk || c.Root() != "/endpoint" {
		t.Fatalf("client caps/root: %+v %q", c.Caps(), c.Root())
	}
	c.root = "/server/root"
	if c.Root() != "/server/root" {
		t.Fatalf("explicit root %q", c.Root())
	}
}

func TestRemoteErrAndExpectOK(t *testing.T) {
	err := remoteErr(protocol.MsgErr, []byte(`{"error":"nope"}`))
	if err == nil || err.Error() != "nope" {
		t.Fatalf("got %v", err)
	}
	var buf bytes.Buffer
	if err := protocol.WriteJSON(&buf, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
		t.Fatal(err)
	}
	if err := expectOK(&buf); err != nil {
		t.Fatal(err)
	}
	var bad bytes.Buffer
	if err := protocol.WriteJSON(&bad, protocol.MsgErr, protocol.ErrMsg{Error: "x"}); err != nil {
		t.Fatal(err)
	}
	if err := expectOK(&bad); err == nil {
		t.Fatal("expected failure")
	}
	err = remoteErr(protocol.MsgErr, []byte(`not-json`))
	if err == nil || err.Error() != "remote error (invalid or empty error response)" {
		t.Fatalf("invalid remote error: %v", err)
	}
}

func TestClientWriteRejectsOperationsAfterCompletion(t *testing.T) {
	w := &clientWrite{committed: true}
	if err := w.WriteChunk(context.Background(), 0, 0, 0, nil); err == nil {
		t.Fatal("expected completed write rejection")
	}
	if err := w.ReuseChunk(context.Background(), 0, 0, chunk.Digest{}, 0); err == nil {
		t.Fatal("expected completed reuse rejection")
	}
	if err := w.Commit(context.Background(), chunk.Digest{}, os.FileMode(0), time.Time{}); err != nil {
		t.Fatalf("completed commit: %v", err)
	}
	if err := w.Abort(); err != nil {
		t.Fatalf("completed abort: %v", err)
	}
}

func TestLockContextHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var mu sync.Mutex
	if err := lockContext(ctx, &mu); err != context.Canceled {
		t.Fatalf("unlocked cancelled context: %v", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if err := lockContext(ctx, &mu); err != context.Canceled {
		t.Fatalf("locked cancelled context: %v", err)
	}
}

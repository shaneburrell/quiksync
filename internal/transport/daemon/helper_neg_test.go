package daemon

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestHelperNoSessionErrorsThenWalk(t *testing.T) {
	root := t.TempDir()
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperRoot(context.Background(), serverR, serverW, root)
	}()
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: protocol.ProtocolVersion}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("hello: %v", err)
	}

	// WriteChunk without session
	if err := protocol.WriteJSON(clientW, protocol.MsgWriteChunk, protocol.WriteChunkReq{Offset: 0, Data: []byte("x")}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgErr {
		t.Fatalf("want MsgErr for WriteChunk, got %v %v", typ, err)
	}

	// ReuseChunk without session
	if err := protocol.WriteJSON(clientW, protocol.MsgReuseChunk, protocol.ReuseChunkReq{Length: 1}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgErr {
		t.Fatalf("want MsgErr for ReuseChunk, got %v %v", typ, err)
	}

	// Commit without session
	if err := protocol.WriteJSON(clientW, protocol.MsgCommit, protocol.CommitReq{}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgErr {
		t.Fatalf("want MsgErr for Commit, got %v %v", typ, err)
	}

	// Bad JSON
	if err := protocol.WriteMsg(clientW, protocol.MsgStat, []byte(`{`)); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgErr {
		t.Fatalf("want MsgErr for bad JSON, got %v %v", typ, err)
	}

	// Following Walk still works
	if err := protocol.WriteJSON(clientW, protocol.MsgWalk, protocol.WalkReq{}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgWalkOK {
		t.Fatalf("walk after errors: typ=%v err=%v", typ, err)
	}

	_ = protocol.WriteMsg(clientW, protocol.MsgBye, nil)
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

package daemon

import (
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
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

func TestHelperRejectsLongRelayJobID(t *testing.T) {
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
	long := strings.Repeat("j", relayMaxJobIDLen+1)
	if err := protocol.WriteJSON(clientW, protocol.MsgRelayWait, protocol.RelayNotifyMeta{JobID: long}); err != nil {
		t.Fatal(err)
	}
	typ, payload, err := protocol.ReadMsg(clientR)
	if err != nil || typ != protocol.MsgErr {
		t.Fatalf("want MsgErr, got typ=%v err=%v", typ, err)
	}
	var em protocol.ErrMsg
	_ = protocol.DecodeJSON(payload, &em)
	if !strings.Contains(em.Error, "too long") {
		t.Fatalf("err=%q", em.Error)
	}
	if err := protocol.WriteJSON(clientW, protocol.MsgRelayNotify, protocol.RelayNotifyMeta{JobID: long}); err != nil {
		t.Fatal(err)
	}
	typ, payload, err = protocol.ReadMsg(clientR)
	if err != nil || typ != protocol.MsgErr {
		t.Fatalf("notify want MsgErr, got typ=%v err=%v", typ, err)
	}
	_ = protocol.DecodeJSON(payload, &em)
	if !strings.Contains(em.Error, "too long") {
		t.Fatalf("notify err=%q", em.Error)
	}
	_ = protocol.WriteMsg(clientW, protocol.MsgBye, nil)
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

func TestHelperRejectsHugeReuseLength(t *testing.T) {
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
	if err := protocol.WriteJSON(clientW, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "x.bin", Size: 1}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatalf("begin: typ=%v err=%v", typ, err)
	}
	huge := int(chunk.DefaultMaxSize) + 1
	if err := protocol.WriteJSON(clientW, protocol.MsgReuseChunk, protocol.ReuseChunkReq{
		NewOffset: 0, OldOffset: 0, Length: huge,
	}); err != nil {
		t.Fatal(err)
	}
	typ, payload, err := protocol.ReadMsg(clientR)
	if err != nil || typ != protocol.MsgErr {
		t.Fatalf("want MsgErr, got typ=%v err=%v", typ, err)
	}
	var em protocol.ErrMsg
	_ = protocol.DecodeJSON(payload, &em)
	if !strings.Contains(em.Error, "exceeds max") {
		t.Fatalf("err=%q", em.Error)
	}
	_ = protocol.WriteMsg(clientW, protocol.MsgBye, nil)
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

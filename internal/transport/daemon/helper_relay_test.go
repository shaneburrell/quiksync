package daemon

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestHelperRelayWaitNotify(t *testing.T) {
	root := t.TempDir()
	job := "helper-relay-job"

	// Waiter connection
	wClientR, wServerW := io.Pipe()
	wServerR, wClientW := io.Pipe()
	wErr := make(chan error, 1)
	go func() {
		wErr <- RunRemoteHelperRoot(context.Background(), wServerR, wServerW, root)
	}()
	if err := protocol.WriteJSON(wClientW, protocol.MsgHello, protocol.Hello{Version: protocol.ProtocolVersion}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(wClientR); err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("waiter hello: %v typ=%v", err, typ)
	}

	waitDone := make(chan error, 1)
	go func() {
		if err := protocol.WriteJSON(wClientW, protocol.MsgRelayWait, protocol.RelayNotifyMeta{JobID: job}); err != nil {
			waitDone <- err
			return
		}
		typ, _, err := protocol.ReadMsg(wClientR)
		if err != nil {
			waitDone <- err
			return
		}
		if typ != protocol.MsgRelayWaitOK {
			waitDone <- err
			return
		}
		waitDone <- nil
	}()

	time.Sleep(80 * time.Millisecond)

	// Notifier connection
	nClientR, nServerW := io.Pipe()
	nServerR, nClientW := io.Pipe()
	nErr := make(chan error, 1)
	go func() {
		nErr <- RunRemoteHelperRoot(context.Background(), nServerR, nServerW, root)
	}()
	if err := protocol.WriteJSON(nClientW, protocol.MsgHello, protocol.Hello{Version: protocol.ProtocolVersion}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(nClientR); err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("notify hello: %v", err)
	}
	if err := protocol.WriteJSON(nClientW, protocol.MsgRelayNotify, protocol.RelayNotifyMeta{JobID: job}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(nClientR); err != nil || typ != protocol.MsgOK {
		t.Fatalf("notify ack: typ=%v err=%v", typ, err)
	}

	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatalf("wait: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("RelayWait not unblocked by Notify")
	}

	_ = protocol.WriteMsg(wClientW, protocol.MsgBye, nil)
	_ = wClientW.Close()
	_ = protocol.WriteMsg(nClientW, protocol.MsgBye, nil)
	_ = nClientW.Close()
	select {
	case <-wErr:
	case <-time.After(2 * time.Second):
	}
	select {
	case <-nErr:
	case <-time.After(2 * time.Second):
	}
}

func TestHelperRejectsUnknownVersion(t *testing.T) {
	root := t.TempDir()
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperRoot(context.Background(), serverR, serverW, root)
	}()
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: "99"}); err != nil {
		t.Fatal(err)
	}
	typ, _, err := protocol.ReadMsg(clientR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != protocol.MsgErr {
		t.Fatalf("want MsgErr, got %d", typ)
	}
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

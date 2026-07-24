package daemon

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestHelperRejectsBadAuthToken(t *testing.T) {
	root := t.TempDir()
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperOpts(context.Background(), serverR, serverW, HelperOptions{
			DefaultRoot: root,
			AuthToken:   "secret",
		})
	}()
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: "1", AuthToken: "wrong"}); err != nil {
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

func TestHelperAcceptsAuthToken(t *testing.T) {
	root := t.TempDir()
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperOpts(context.Background(), serverR, serverW, HelperOptions{
			DefaultRoot: root,
			AuthToken:   "secret",
		})
	}()
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: "1", AuthToken: "secret"}); err != nil {
		t.Fatal(err)
	}
	typ, _, err := protocol.ReadMsg(clientR)
	if err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("hello typ=%v err=%v", typ, err)
	}
	_ = protocol.WriteMsg(clientW, protocol.MsgBye, nil)
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

func TestHelperRejectsWrongLengthAuthToken(t *testing.T) {
	root := t.TempDir()
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperOpts(context.Background(), serverR, serverW, HelperOptions{
			DefaultRoot: root,
			AuthToken:   "secret",
		})
	}()
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: "1", AuthToken: "x"}); err != nil {
		t.Fatal(err)
	}
	typ, _, err := protocol.ReadMsg(clientR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != protocol.MsgErr {
		t.Fatalf("want MsgErr for wrong-length token, got %d", typ)
	}
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

func TestHelperRejectsEmptyAuthToken(t *testing.T) {
	root := t.TempDir()
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperOpts(context.Background(), serverR, serverW, HelperOptions{
			DefaultRoot: root,
			AuthToken:   "secret",
		})
	}()
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: "1"}); err != nil {
		t.Fatal(err)
	}
	typ, _, err := protocol.ReadMsg(clientR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != protocol.MsgErr {
		t.Fatalf("want MsgErr for empty token, got %d", typ)
	}
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

func TestAuthTokenOK(t *testing.T) {
	if !authTokenOK("secret", "secret") {
		t.Fatal("expected match")
	}
	if authTokenOK("secret", "wrong") {
		t.Fatal("expected mismatch")
	}
	if authTokenOK("", "secret") {
		t.Fatal("expected empty mismatch")
	}
	if authTokenOK("x", "secret") {
		t.Fatal("expected length mismatch")
	}
}

func TestRemoveEmptyRelRejected(t *testing.T) {
	root := t.TempDir()
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperRoot(context.Background(), serverR, serverW, root)
	}()
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: "1"}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgHelloOK {
		t.Fatal(err)
	}
	if err := protocol.WriteJSON(clientW, protocol.MsgRemove, protocol.PathReq{Rel: ""}); err != nil {
		t.Fatal(err)
	}
	typ, _, err := protocol.ReadMsg(clientR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != protocol.MsgErr {
		t.Fatalf("want MsgErr for empty remove, got %d", typ)
	}
	_ = protocol.WriteMsg(clientW, protocol.MsgBye, nil)
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

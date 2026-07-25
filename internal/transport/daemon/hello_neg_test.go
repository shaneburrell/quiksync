package daemon

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestHelperBadHelloJSONAndVersion(t *testing.T) {
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	errCh := make(chan error, 1)
	go func() { errCh <- RunRemoteHelperRoot(context.Background(), sr, sw, t.TempDir()) }()
	if err := protocol.WriteMsg(cw, protocol.MsgHello, []byte("{")); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected bad hello json error")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout")
	}
	_ = cw.Close()
	_ = cr.Close()

	cr2, sw2 := io.Pipe()
	sr2, cw2 := io.Pipe()
	errCh2 := make(chan error, 1)
	go func() { errCh2 <- RunRemoteHelperRoot(context.Background(), sr2, sw2, t.TempDir()) }()
	if err := protocol.WriteJSON(cw2, protocol.MsgHello, protocol.Hello{Version: "99", Root: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	typ, _, err := protocol.ReadMsg(cr2)
	if err != nil || typ != protocol.MsgErr {
		t.Fatalf("bad version: typ=%v err=%v", typ, err)
	}
	_ = cw2.Close()
	select {
	case <-errCh2:
	case <-time.After(2 * time.Second):
	}
}

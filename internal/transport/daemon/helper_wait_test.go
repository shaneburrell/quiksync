package daemon

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestHelperRelayWaitCancel(t *testing.T) {
	root := t.TempDir()
	cr, sw := io.Pipe()
	sr, cw := io.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- RunRemoteHelperOpts(ctx, sr, sw, HelperOptions{DefaultRoot: root}) }()

	if err := protocol.WriteJSON(cw, protocol.MsgHello, protocol.Hello{Version: protocol.ProtocolVersion, Root: root}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(cr); err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("hello: %v %v", typ, err)
	}
	if err := protocol.WriteJSON(cw, protocol.MsgRelayWait, protocol.RelayNotifyMeta{JobID: "cancel-me"}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(50 * time.Millisecond)
	cancel()
	typ, _, err := protocol.ReadMsg(cr)
	if err != nil {
		// helper may close stream on cancel
		return
	}
	if typ != protocol.MsgErr {
		t.Fatalf("want MsgErr on cancel, got %d", typ)
	}
	_ = cw.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
	}
}

func TestHelperBadJSONGetSigRemove(t *testing.T) {
	root := t.TempDir()
	r, w, _, cleanup := helperSession(t, root)
	defer cleanup()
	for _, typ := range []protocol.MsgType{
		protocol.MsgGetSig, protocol.MsgRemove, protocol.MsgMkdir,
		protocol.MsgStat, protocol.MsgOpenRead, protocol.MsgRelayWait,
	} {
		if err := protocol.WriteMsg(w, typ, []byte("{")); err != nil {
			t.Fatal(err)
		}
		got, _, err := protocol.ReadMsg(r)
		if err != nil || got != protocol.MsgErr {
			t.Fatalf("typ %d: got=%d err=%v", typ, got, err)
		}
	}
}

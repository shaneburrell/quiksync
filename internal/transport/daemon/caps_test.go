package daemon

import (
	"bytes"
	"testing"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestMapHelloCaps(t *testing.T) {
	v1 := mapHelloCaps(protocol.HelloOK{Version: "1"})
	if v1.SupportsReuseChunk || v1.SupportsMultiplex {
		t.Fatalf("v1: %+v", v1)
	}
	v2 := mapHelloCaps(protocol.HelloOK{Version: "2", Caps: protocol.DefaultCaps()})
	if !v2.SupportsReuseChunk || !v2.SupportsMultiplex {
		t.Fatalf("v2: %+v", v2)
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
}

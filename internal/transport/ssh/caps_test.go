package ssh

import (
	"bytes"
	"testing"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestMapCaps(t *testing.T) {
	v1 := mapCaps(protocol.HelloOK{Version: "1"})
	if v1.SupportsReuseChunk || v1.SupportsMultiplex {
		t.Fatalf("v1 caps: %+v", v1)
	}
	empty := mapCaps(protocol.HelloOK{})
	if empty.SupportsReuseChunk {
		t.Fatalf("empty version should be v1 fallback: %+v", empty)
	}
	v2 := mapCaps(protocol.HelloOK{Version: "2", Caps: protocol.DefaultCaps()})
	if !v2.SupportsReuseChunk || !v2.SupportsDelta {
		t.Fatalf("v2 caps: %+v", v2)
	}
}

func TestRemoteErrAndExpectOK(t *testing.T) {
	err := remoteErr(protocol.MsgErr, []byte(`{"error":"boom"}`))
	if err == nil || err.Error() != "boom" {
		t.Fatalf("got %v", err)
	}
	err = remoteErr(protocol.MsgOK, []byte(`not-json`))
	if err == nil {
		t.Fatal("expected error")
	}
	var buf bytes.Buffer
	if err := protocol.WriteJSON(&buf, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
		t.Fatal(err)
	}
	if err := expectOK(&buf); err != nil {
		t.Fatal(err)
	}
}

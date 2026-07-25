package protocol

import (
	"bytes"
	"testing"
)

func TestReadWriteMsg(t *testing.T) {
	var buf bytes.Buffer
	payload := []byte(`{"ok":true}`)
	if err := WriteMsg(&buf, MsgOK, payload); err != nil {
		t.Fatal(err)
	}
	typ, got, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if typ != MsgOK || !bytes.Equal(got, payload) {
		t.Fatalf("got type=%d payload=%q", typ, got)
	}
}

func TestWriteJSON(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteJSON(&buf, MsgHello, Hello{Version: "1", Root: "/tmp"}); err != nil {
		t.Fatal(err)
	}
	typ, payload, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if typ != MsgHello {
		t.Fatalf("type %d", typ)
	}
	var h Hello
	if err := DecodeJSON(payload, &h); err != nil {
		t.Fatal(err)
	}
	if h.Root != "/tmp" {
		t.Fatalf("root %q", h.Root)
	}
}

func TestCheckPeerVersion(t *testing.T) {
	for _, v := range []string{"", "1", ProtocolVersion} {
		if err := CheckPeerVersion(v); err != nil {
			t.Fatalf("version %q: %v", v, err)
		}
	}
	if err := CheckPeerVersion("99"); err == nil {
		t.Fatal("expected reject unknown version")
	}
}

func TestDefaultCaps(t *testing.T) {
	c := DefaultCaps()
	if !c.SupportsDelta || !c.SupportsMultiplex || !c.SupportsResume || !c.SupportsReuseChunk {
		t.Fatalf("%+v", c)
	}
}

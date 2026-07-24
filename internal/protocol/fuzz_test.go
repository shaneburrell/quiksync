package protocol

import (
	"bytes"
	"testing"
)

func FuzzReadWriteMsg(f *testing.F) {
	f.Add([]byte("hi"), uint8(MsgOK))
	f.Fuzz(func(t *testing.T, payload []byte, typByte byte) {
		if len(payload) > 1<<16 {
			payload = payload[:1<<16]
		}
		typ := MsgType(typByte)
		if typ == 0 {
			typ = MsgOK
		}
		var buf bytes.Buffer
		if err := WriteMsg(&buf, typ, payload); err != nil {
			t.Fatal(err)
		}
		gotTyp, got, err := ReadMsg(&buf)
		if err != nil {
			t.Fatal(err)
		}
		if gotTyp != typ || !bytes.Equal(got, payload) {
			t.Fatalf("mismatch")
		}
	})
}

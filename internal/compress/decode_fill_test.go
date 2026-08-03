package compress

import (
	"testing"
)

func TestEncodeAutoAndSmall(t *testing.T) {
	used, out, err := Encode(CodecAuto, []byte("tiny"))
	if err != nil || used != CodecNone || string(out) != "tiny" {
		t.Fatalf("auto/small: used=%s out=%q err=%v", used, out, err)
	}
	used, _, err = Encode(Codec(99), []byte("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"))
	if err != nil || used != CodecNone {
		t.Fatalf("unknown codec encode: %v %v", used, err)
	}
}

func TestDecodeEdges(t *testing.T) {
	if _, err := Decode(CodecLZ4, []byte("x"), -1); err == nil {
		t.Fatal("negative len")
	}
	if _, err := Decode(CodecLZ4, []byte("x"), MaxUncompressedChunk+1); err == nil {
		t.Fatal("oversize len")
	}
	if _, err := Decode(Codec(99), []byte("x"), 1); err == nil {
		t.Fatal("unknown decode")
	}
	data := make([]byte, 4096)
	for i := range data {
		data[i] = 'a'
	}
	used, enc, err := Encode(CodecLZ4, data)
	if err != nil || used != CodecLZ4 {
		t.Fatalf("encode: used=%s err=%v", used, err)
	}
	if _, err := Decode(CodecLZ4, enc, len(data)+5); err == nil {
		t.Fatal("expected size mismatch")
	}
	if _, err := Decode(CodecAuto, data, 0); err == nil {
		t.Fatal("CodecAuto must be rejected on decode")
	}
}

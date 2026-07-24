package compress

import (
	"bytes"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	data := bytes.Repeat([]byte("hello compress world\n"), 200)
	for _, c := range []Codec{CodecNone, CodecLZ4, CodecZstd} {
		used, enc, err := Encode(c, data)
		if err != nil {
			t.Fatal(err)
		}
		out, err := Decode(used, enc, len(data))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(out, data) {
			t.Fatalf("%s mismatch", c)
		}
	}
}

func TestEncodeFallbackOnIncompressible(t *testing.T) {
	data := make([]byte, 4096)
	for i := range data {
		data[i] = byte(i * 37)
	}
	used, enc, err := Encode(CodecLZ4, data)
	if err != nil {
		t.Fatal(err)
	}
	// May fall back to none if expansion.
	if used == CodecNone && !bytes.Equal(enc, data) {
		t.Fatal("none should return original")
	}
	out, err := Decode(used, enc, len(data))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatal("decode mismatch")
	}
}

func TestParse(t *testing.T) {
	c, err := Parse("zstd")
	if err != nil || c != CodecZstd {
		t.Fatal(c, err)
	}
	if _, err := Parse("gzip"); err == nil {
		t.Fatal("expected error")
	}
}

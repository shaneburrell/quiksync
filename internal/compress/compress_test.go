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

func TestParseAndString(t *testing.T) {
	for _, s := range []string{"none", "lz4", "zstd", "auto"} {
		c, err := Parse(s)
		if err != nil {
			t.Fatal(err)
		}
		if c.String() == "" {
			t.Fatal("empty string")
		}
	}
	if _, err := Parse("gzip"); err == nil {
		t.Fatal("expected error")
	}
	if Codec(99).String() == "" {
		t.Fatal("unknown codec string")
	}
}

func TestSampleRatio(t *testing.T) {
	text := bytes.Repeat([]byte("aaaa"), 2000)
	if SampleRatio(CodecLZ4, text) < 1.05 {
		t.Fatalf("expected compressible ratio")
	}
	if SampleRatio(CodecNone, text) != 1 {
		t.Fatal("none ratio")
	}
	if SampleRatio(CodecLZ4, nil) != 1 {
		t.Fatal("empty sample")
	}
}

func TestZstdRejectsOversize(t *testing.T) {
	// Compress a payload larger than the declared uncompressedLen cap.
	big := bytes.Repeat([]byte("zstd-bomb-"), 200_000) // ~2MB
	_, enc, err := Encode(CodecZstd, big)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(CodecZstd, enc, 1024); err == nil {
		t.Fatal("expected oversize rejection")
	}
}

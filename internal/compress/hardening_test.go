package compress

import (
	"bytes"
	"testing"
)

func TestDecodeRejectsOversize(t *testing.T) {
	data := bytes.Repeat([]byte("aaaa"), 2000)
	used, enc, err := Encode(CodecLZ4, data)
	if err != nil || used == CodecNone {
		t.Skip("could not compress sample")
	}
	// Claim a tiny uncompressed length — must reject.
	if _, err := Decode(used, enc, 16); err == nil {
		t.Fatal("expected oversize/length mismatch rejection")
	}
	if _, err := Decode(used, enc, MaxUncompressedChunk+1); err == nil {
		t.Fatal("expected hard max rejection")
	}
}

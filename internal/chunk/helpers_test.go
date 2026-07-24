package chunk

import (
	"bytes"
	"testing"
)

func TestDigestStringAndHasher(t *testing.T) {
	d := Sum([]byte("abc"))
	s := d.String()
	if len(s) != 64 {
		t.Fatalf("hex len %d", len(s))
	}
	h := NewHasher()
	_, _ = h.Write([]byte("abc"))
	var got Digest
	copy(got[:], h.Sum(nil))
	if got != d {
		t.Fatal("hasher mismatch")
	}
}

func TestSerializeSignature(t *testing.T) {
	sig := FileSignature{
		Size: 10, Digest: Digest{1, 2, 3},
		Chunks: []Chunk{{Offset: 0, Length: 10, Digest: Digest{9}}},
	}
	b := SerializeSignature(sig)
	if len(b) < 44+40 {
		t.Fatalf("short encode %d", len(b))
	}
	if !bytes.Equal(b[8:11], []byte{1, 2, 3}) {
		t.Fatal("digest bytes missing")
	}
}

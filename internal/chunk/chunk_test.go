package chunk

import (
	"bytes"
	"strings"
	"testing"
)

func TestChunkReaderDeterministic(t *testing.T) {
	data := []byte(strings.Repeat("abcdefghijklmnopqrstuvwxyz0123456789\n", 4000))
	sig1, err := ChunkReader(bytes.NewReader(data), int64(len(data)), Options{KeepData: true})
	if err != nil {
		t.Fatal(err)
	}
	sig2, err := ChunkReader(bytes.NewReader(data), int64(len(data)), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if sig1.Digest != sig2.Digest {
		t.Fatalf("digest mismatch")
	}
	if len(sig1.Chunks) == 0 {
		t.Fatal("expected chunks")
	}
	var rebuilt []byte
	for _, c := range sig1.Chunks {
		rebuilt = append(rebuilt, c.Data...)
		if Sum(c.Data) != c.Digest {
			t.Fatalf("chunk digest mismatch at offset %d", c.Offset)
		}
	}
	if !bytes.Equal(rebuilt, data) {
		t.Fatalf("rebuild mismatch: got %d want %d", len(rebuilt), len(data))
	}
}

func TestDeltaShiftStable(t *testing.T) {
	base := []byte(strings.Repeat("The quick brown fox jumps over the lazy dog. ", 2000))
	shifted := append([]byte("PREFIX-"), base...)
	a, err := ChunkReader(bytes.NewReader(base), int64(len(base)), Options{})
	if err != nil {
		t.Fatal(err)
	}
	b, err := ChunkReader(bytes.NewReader(shifted), int64(len(shifted)), Options{})
	if err != nil {
		t.Fatal(err)
	}
	set := map[Digest]struct{}{}
	for _, c := range a.Chunks {
		set[c.Digest] = struct{}{}
	}
	shared := 0
	for _, c := range b.Chunks {
		if _, ok := set[c.Digest]; ok {
			shared++
		}
	}
	if shared < len(a.Chunks)/2 {
		t.Fatalf("expected substantial chunk reuse after prefix insert, shared=%d/%d", shared, len(a.Chunks))
	}
}

func TestHashFile(t *testing.T) {
	d, n, err := HashFile(strings.NewReader("hello"))
	if err != nil {
		t.Fatal(err)
	}
	if n != 5 {
		t.Fatalf("size %d", n)
	}
	if d == (Digest{}) {
		t.Fatal("empty digest")
	}
}

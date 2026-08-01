package chunk

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestGearTableUnique(t *testing.T) {
	seen := map[uint32]struct{}{}
	for i, v := range gear {
		if v == 0 {
			t.Fatalf("gear[%d] is zero", i)
		}
		if _, ok := seen[v]; ok {
			t.Fatalf("duplicate gear value at %d: %#x", i, v)
		}
		seen[v] = struct{}{}
	}
	if len(seen) != 256 {
		t.Fatalf("want 256 unique, got %d", len(seen))
	}
}

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
	for _, c := range sig2.Chunks {
		if c.Data != nil {
			t.Fatal("KeepData:false must not retain payloads")
		}
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

func TestStreamChunksCallbackErrorAndSizeMismatch(t *testing.T) {
	data := []byte(strings.Repeat("err-path-", 2000))
	_, err := StreamChunks(bytes.NewReader(data), int64(len(data)), Options{}, func(c Chunk) error {
		return fmt.Errorf("stop")
	})
	if err == nil || !strings.Contains(err.Error(), "stop") {
		t.Fatalf("callback err: %v", err)
	}
	_, err = StreamChunks(bytes.NewReader(data), int64(len(data))+10, Options{}, nil)
	if err == nil || !strings.Contains(err.Error(), "size mismatch") {
		t.Fatalf("size mismatch: %v", err)
	}
}

func TestStreamChunksMatchesChunkReader(t *testing.T) {
	data := []byte(strings.Repeat("stream-chunk-payload-0123456789\n", 8000))
	want, err := ChunkReader(bytes.NewReader(data), int64(len(data)), Options{KeepData: false})
	if err != nil {
		t.Fatal(err)
	}
	var gotData []byte
	var n int
	got, err := StreamChunks(bytes.NewReader(data), int64(len(data)), Options{}, func(c Chunk) error {
		n++
		if c.Data == nil || len(c.Data) != int(c.Length) {
			t.Fatalf("stream chunk missing data")
		}
		gotData = append(gotData, c.Data...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Digest != want.Digest || got.Size != want.Size {
		t.Fatalf("sig mismatch stream=%+v want=%+v", got, want)
	}
	if n != len(want.Chunks) {
		t.Fatalf("chunks=%d want %d", n, len(want.Chunks))
	}
	if len(got.Chunks) != 0 {
		t.Fatalf("StreamChunks must not retain chunk list payloads, got %d chunks", len(got.Chunks))
	}
	if !bytes.Equal(gotData, data) {
		t.Fatalf("stream rebuild mismatch")
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

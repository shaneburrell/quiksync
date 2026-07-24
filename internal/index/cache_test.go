package index

import (
	"testing"

	"github.com/shaneburrell/quiksync/internal/chunk"
)

func TestCacheHitMiss(t *testing.T) {
	c, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sig := chunk.FileSignature{
		Size: 100, Digest: chunk.Digest{1, 2, 3},
		Chunks: []chunk.Chunk{{Offset: 0, Length: 100, Digest: chunk.Digest{9}}},
	}
	if err := c.Put("dir/f.bin", 100, 42, sig); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Get("dir/f.bin", 100, 42)
	if !ok || got.Digest != sig.Digest || len(got.Chunks) != 1 {
		t.Fatalf("hit failed: %+v", got)
	}
	if _, ok := c.Get("dir/f.bin", 100, 99); ok {
		t.Fatal("mtime mismatch should miss")
	}
	if _, ok := c.Get("dir/f.bin", 101, 42); ok {
		t.Fatal("size mismatch should miss")
	}
}

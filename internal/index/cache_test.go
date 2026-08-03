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
	if err := c.Put("dir/f.bin", 100, 42, 64*1024, sig); err != nil {
		t.Fatal(err)
	}
	got, ok := c.Get("dir/f.bin", 100, 42, 64*1024)
	if !ok || got.Digest != sig.Digest || len(got.Chunks) != 1 {
		t.Fatalf("hit failed: %+v", got)
	}
	if _, ok := c.Get("dir/f.bin", 100, 99, 64*1024); ok {
		t.Fatal("mtime mismatch should miss")
	}
	if _, ok := c.Get("dir/f.bin", 101, 42, 64*1024); ok {
		t.Fatal("size mismatch should miss")
	}
	if _, ok := c.Get("dir/f.bin", 100, 42, 128*1024); ok {
		t.Fatal("avg size mismatch should miss")
	}
}

func TestCacheRejectsTraversal(t *testing.T) {
	c, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sig := chunk.FileSignature{Size: 1, Digest: chunk.Digest{1}}
	if err := c.Put("../escape", 1, 1, 0, sig); err == nil {
		t.Fatal("expected path rejection")
	}
}

func TestCacheMissWhenStoredAvgZero(t *testing.T) {
	c, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sig := chunk.FileSignature{Size: 10, Digest: chunk.Digest{1}}
	if err := c.Put("f.bin", 10, 1, 0, sig); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Get("f.bin", 10, 1, 64*1024); ok {
		t.Fatal("legacy AvgSize=0 must miss when caller avg is non-zero")
	}
	if _, ok := c.Get("f.bin", 10, 1, 0); !ok {
		t.Fatal("AvgSize=0 caller should still hit legacy entry")
	}
}

func TestCacheDelete(t *testing.T) {
	c, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	sig := chunk.FileSignature{Size: 1, Digest: chunk.Digest{1}}
	if err := c.Put("dir/f.bin", 1, 2, 64, sig); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete("dir/f.bin"); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.Get("dir/f.bin", 1, 2, 64); ok {
		t.Fatal("deleted cache entry was returned")
	}
	if err := c.Delete("dir/f.bin"); err != nil {
		t.Fatalf("deleting missing entry: %v", err)
	}
	if err := c.Delete("../escape"); err == nil {
		t.Fatal("expected traversal rejection")
	}
}

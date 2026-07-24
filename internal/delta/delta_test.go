package delta

import (
	"testing"

	"github.com/shaneburrell/quiksync/internal/chunk"
)

func TestDiffReuse(t *testing.T) {
	d := chunk.Digest{1}
	src := chunk.FileSignature{
		Size: 100,
		Chunks: []chunk.Chunk{
			{Offset: 0, Length: 50, Digest: d},
			{Offset: 50, Length: 50, Digest: chunk.Digest{2}},
		},
	}
	dest := chunk.FileSignature{
		Size: 50,
		Chunks: []chunk.Chunk{
			{Offset: 0, Length: 50, Digest: d},
		},
	}
	p := Diff(src, dest)
	if p.Reuse != 1 || len(p.Missing) != 1 {
		t.Fatalf("reuse=%d missing=%d", p.Reuse, len(p.Missing))
	}
}

func TestNeedsTransfer(t *testing.T) {
	a := chunk.FileSignature{Size: 10, Digest: chunk.Digest{1}}
	b := chunk.FileSignature{Size: 10, Digest: chunk.Digest{1}}
	if NeedsTransfer(a, b, true) {
		t.Fatal("same digest should not need transfer")
	}
	b.Digest = chunk.Digest{2}
	if !NeedsTransfer(a, b, true) {
		t.Fatal("different digest should need transfer")
	}
}

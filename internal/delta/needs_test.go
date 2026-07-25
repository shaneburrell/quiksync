package delta

import (
	"testing"

	"github.com/shaneburrell/quiksync/internal/chunk"
)

func TestNeedsTransferMatrix(t *testing.T) {
	src := chunk.FileSignature{Size: 10, Digest: chunk.Digest{1}}
	if !NeedsTransfer(src, chunk.FileSignature{}, false) {
		t.Fatal("missing dest")
	}
	if !NeedsTransfer(src, chunk.FileSignature{Size: 9, Digest: chunk.Digest{1}}, false) {
		t.Fatal("size mismatch")
	}
	same := chunk.FileSignature{Size: 10, Digest: chunk.Digest{1}}
	if NeedsTransfer(src, same, true) {
		t.Fatal("same digest with checksum")
	}
	if !NeedsTransfer(src, chunk.FileSignature{Size: 10, Digest: chunk.Digest{2}}, true) {
		t.Fatal("digest mismatch")
	}
	emptyDigest := chunk.FileSignature{Size: 10}
	if !NeedsTransfer(emptyDigest, emptyDigest, false) {
		t.Fatal("empty digests without checksum still need transfer")
	}
}

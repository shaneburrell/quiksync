package delta

import "github.com/shaneburrell/quiksync/internal/chunk"

// Plan describes which source chunks are missing at the destination.
type Plan struct {
	Missing []chunk.Chunk
	Reuse   int
}

// Diff returns chunks from src that are not present in dest signature.
func Diff(src, dest chunk.FileSignature) Plan {
	have := make(map[chunk.Digest]struct{}, len(dest.Chunks))
	for _, c := range dest.Chunks {
		have[c.Digest] = struct{}{}
	}
	var missing []chunk.Chunk
	reuse := 0
	for _, c := range src.Chunks {
		if _, ok := have[c.Digest]; ok {
			reuse++
			continue
		}
		missing = append(missing, c)
	}
	return Plan{Missing: missing, Reuse: reuse}
}

// NeedsTransfer reports whether src and dest differ by whole-file digest or size.
func NeedsTransfer(src, dest chunk.FileSignature, checksum bool) bool {
	if dest.Size == 0 && len(dest.Chunks) == 0 && src.Size > 0 {
		return true
	}
	if src.Size != dest.Size {
		return true
	}
	if checksum || src.Digest != (chunk.Digest{}) {
		return src.Digest != dest.Digest
	}
	return true
}

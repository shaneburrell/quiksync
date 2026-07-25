package delta

import "github.com/shaneburrell/quiksync/internal/chunk"

// ReuseEntry maps a source chunk onto bytes already present at the destination.
type ReuseEntry struct {
	NewOffset uint64
	OldOffset uint64
	Digest    chunk.Digest
	Length    int
}

// Plan describes which source chunks are missing vs reusable at the destination.
type Plan struct {
	Missing []chunk.Chunk
	Reuse   []ReuseEntry
}

// Diff returns chunks from src that are not present in dest signature,
// and reuse entries with dest oldOffset for digests that are present.
func Diff(src, dest chunk.FileSignature) Plan {
	type loc struct {
		offset uint64
		length uint32
	}
	have := make(map[chunk.Digest]loc, len(dest.Chunks))
	for _, c := range dest.Chunks {
		if _, ok := have[c.Digest]; ok {
			continue // first occurrence wins
		}
		have[c.Digest] = loc{offset: c.Offset, length: c.Length}
	}
	var missing []chunk.Chunk
	var reuse []ReuseEntry
	for _, c := range src.Chunks {
		if loc, ok := have[c.Digest]; ok && loc.length == c.Length {
			reuse = append(reuse, ReuseEntry{
				NewOffset: c.Offset,
				OldOffset: loc.offset,
				Digest:    c.Digest,
				Length:    int(c.Length),
			})
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

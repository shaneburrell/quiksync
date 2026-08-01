package transport

import (
	"fmt"

	"github.com/shaneburrell/quiksync/internal/chunk"
)

// ValidateReuseRange rejects reuse lengths that would OOM or read past the
// existing destination. oldSize < 0 means the destination size is unknown
// (still enforce the max chunk length before allocating).
func ValidateReuseRange(oldOffset uint64, length int, oldSize int64) error {
	if length <= 0 {
		return fmt.Errorf("reuse: invalid length %d", length)
	}
	if length > int(chunk.DefaultMaxSize) {
		return fmt.Errorf("reuse: length %d exceeds max chunk size %d", length, chunk.DefaultMaxSize)
	}
	if oldSize >= 0 {
		end := int64(oldOffset) + int64(length)
		if int64(oldOffset) < 0 || end < 0 || end > oldSize {
			return fmt.Errorf("reuse: old range out of bounds")
		}
	}
	return nil
}

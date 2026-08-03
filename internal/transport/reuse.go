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
		if end < 0 || end > oldSize {
			return fmt.Errorf("reuse: old range out of bounds")
		}
	}
	return nil
}

// ValidateWriteRange rejects writes that extend past the declared session size.
// sessionSize < 0 disables the bound check.
func ValidateWriteRange(offset uint64, length int, sessionSize int64) error {
	if length < 0 {
		return fmt.Errorf("write: invalid length %d", length)
	}
	if sessionSize < 0 {
		return nil
	}
	end := int64(offset) + int64(length)
	if end < 0 || end > sessionSize {
		return fmt.Errorf("write: range out of bounds")
	}
	return nil
}

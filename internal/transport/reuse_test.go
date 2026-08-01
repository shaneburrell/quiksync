package transport

import (
	"strings"
	"testing"

	"github.com/shaneburrell/quiksync/internal/chunk"
)

func TestValidateReuseRange(t *testing.T) {
	if err := ValidateReuseRange(0, 64, 100); err != nil {
		t.Fatal(err)
	}
	if err := ValidateReuseRange(0, 0, 100); err == nil || !strings.Contains(err.Error(), "invalid length") {
		t.Fatalf("zero: %v", err)
	}
	if err := ValidateReuseRange(0, -1, 100); err == nil {
		t.Fatal("negative")
	}
	huge := int(chunk.DefaultMaxSize) + 1
	if err := ValidateReuseRange(0, huge, 1<<40); err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("huge: %v", err)
	}
	if err := ValidateReuseRange(90, 20, 100); err == nil || !strings.Contains(err.Error(), "out of bounds") {
		t.Fatalf("oob: %v", err)
	}
	// Unknown old size still caps length.
	if err := ValidateReuseRange(0, huge, -1); err == nil {
		t.Fatal("unknown size must still cap length")
	}
	if err := ValidateReuseRange(0, 64, -1); err != nil {
		t.Fatal(err)
	}
}

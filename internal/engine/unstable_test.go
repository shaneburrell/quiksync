package engine

import (
	"fmt"
	"testing"
)

func TestIsUnstableWrapped(t *testing.T) {
	err := fmt.Errorf("wrap: %w", unstableError{fmt.Errorf("not stable")})
	if !isUnstable(err) {
		t.Fatal("expected errors.As to unwrap unstableError")
	}
	if isUnstable(fmt.Errorf("plain")) {
		t.Fatal("plain error should not be unstable")
	}
}

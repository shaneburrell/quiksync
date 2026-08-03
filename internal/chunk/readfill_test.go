package chunk

import (
	"io"
	"strings"
	"testing"
)

type stuckReader struct{}

func (stuckReader) Read([]byte) (int, error) { return 0, nil }

func TestReadFillNoProgress(t *testing.T) {
	buf := make([]byte, 8)
	n, err := readFill(stuckReader{}, buf)
	if err == nil || !strings.Contains(err.Error(), "no progress") {
		t.Fatalf("expected no progress, got n=%d err=%v", n, err)
	}
}

func TestReadFillEOF(t *testing.T) {
	buf := make([]byte, 8)
	n, err := readFill(strings.NewReader("hi"), buf)
	if err != io.EOF || n != 2 {
		t.Fatalf("got n=%d err=%v", n, err)
	}
}

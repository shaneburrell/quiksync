package chunk

import (
	"bytes"
	"strings"
	"testing"
)

func BenchmarkChunkReader(b *testing.B) {
	data := []byte(strings.Repeat("The quick brown fox jumps over the lazy dog.\n", 200_000)) // ~9MiB
	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := ChunkReader(bytes.NewReader(data), int64(len(data)), Options{AvgSize: 64 * 1024})
		if err != nil {
			b.Fatal(err)
		}
	}
}

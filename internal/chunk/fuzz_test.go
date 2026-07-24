package chunk

import (
	"bytes"
	"testing"
)

func FuzzChunkReader(f *testing.F) {
	f.Add([]byte("hello"))
	f.Add(bytes.Repeat([]byte("abc"), 10000))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 1<<20 {
			data = data[:1<<20]
		}
		sig, err := ChunkReader(bytes.NewReader(data), int64(len(data)), Options{KeepData: true})
		if err != nil {
			t.Fatal(err)
		}
		var rebuilt []byte
		for _, c := range sig.Chunks {
			rebuilt = append(rebuilt, c.Data...)
		}
		if !bytes.Equal(rebuilt, data) {
			t.Fatalf("rebuild len %d want %d", len(rebuilt), len(data))
		}
		d, _, err := HashFile(bytes.NewReader(data))
		if err != nil {
			t.Fatal(err)
		}
		if d != sig.Digest {
			t.Fatal("whole-file digest mismatch")
		}
	})
}

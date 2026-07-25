package chunk

import (
	"bytes"
	"testing"
)

func TestNormalizedOptions(t *testing.T) {
	o := Options{AvgSize: 0, MinSize: 0, MaxSize: 0}.normalized()
	if o.AvgSize == 0 || o.MinSize == 0 || o.MaxSize == 0 {
		t.Fatalf("%+v", o)
	}
	o = Options{AvgSize: 4096, MinSize: 100_000, MaxSize: 100}.normalized()
	if o.MinSize > o.AvgSize || o.MaxSize < o.AvgSize {
		t.Fatalf("clamped: %+v", o)
	}
	sig, err := ChunkReader(bytes.NewReader(bytes.Repeat([]byte("n"), 50_000)), 50_000, Options{
		AvgSize: 2048, MinSize: 10_000, MaxSize: 500, KeepData: false,
	})
	if err != nil || sig.Size != 50_000 || len(sig.Chunks) == 0 {
		t.Fatalf("chunk: %+v %v", sig, err)
	}
}

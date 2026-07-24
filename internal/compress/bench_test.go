package compress

import (
	"bytes"
	"testing"
)

func BenchmarkCompressText(b *testing.B) {
	data := bytes.Repeat([]byte("compressible log line with repeated structure\n"), 8000)
	for _, codec := range []Codec{CodecNone, CodecLZ4, CodecZstd} {
		b.Run(codec.String(), func(b *testing.B) {
			b.SetBytes(int64(len(data)))
			for i := 0; i < b.N; i++ {
				used, enc, err := Encode(codec, data)
				if err != nil {
					b.Fatal(err)
				}
				if _, err := Decode(used, enc, len(data)); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkCompressRandom(b *testing.B) {
	data := make([]byte, 256*1024)
	for i := range data {
		data[i] = byte(i * 31)
	}
	b.SetBytes(int64(len(data)))
	for i := 0; i < b.N; i++ {
		_, _, _ = Encode(CodecLZ4, data)
	}
}

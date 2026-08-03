package compress

import (
	"bytes"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

const MaxUncompressedChunk = 64 << 20 // 64 MiB hard cap per chunk

type Codec uint8

const (
	CodecNone Codec = iota
	CodecLZ4
	CodecZstd
	CodecAuto // decision deferred to autotuner
)

func (c Codec) String() string {
	switch c {
	case CodecNone:
		return "none"
	case CodecLZ4:
		return "lz4"
	case CodecZstd:
		return "zstd"
	case CodecAuto:
		return "auto"
	default:
		return fmt.Sprintf("codec(%d)", c)
	}
}

func Parse(s string) (Codec, error) {
	switch s {
	case "none", "":
		return CodecNone, nil
	case "lz4":
		return CodecLZ4, nil
	case "zstd":
		return CodecZstd, nil
	case "auto":
		return CodecAuto, nil
	default:
		return 0, fmt.Errorf("unknown codec %q", s)
	}
}

// Encode compresses data with the given codec. Returns codec actually used
// (may fall back to none if compression expands data).
func Encode(codec Codec, data []byte) (Codec, []byte, error) {
	if codec == CodecNone || codec == CodecAuto || len(data) < 64 {
		return CodecNone, data, nil
	}
	var buf bytes.Buffer
	switch codec {
	case CodecLZ4:
		w := lz4.NewWriter(&buf)
		if _, err := w.Write(data); err != nil {
			_ = w.Close()
			return CodecNone, nil, err
		}
		if err := w.Close(); err != nil {
			return CodecNone, nil, err
		}
	case CodecZstd:
		w, err := zstd.NewWriter(&buf, zstd.WithEncoderLevel(zstd.SpeedFastest))
		if err != nil {
			return CodecNone, nil, err
		}
		if _, err := w.Write(data); err != nil {
			_ = w.Close()
			return CodecNone, nil, err
		}
		if err := w.Close(); err != nil {
			return CodecNone, nil, err
		}
	default:
		return CodecNone, data, nil
	}
	out := buf.Bytes()
	if len(out) >= len(data) {
		return CodecNone, data, nil
	}
	return codec, out, nil
}

var zstdDecPool = sync.Pool{
	New: func() any {
		r, err := zstd.NewReader(nil, zstd.WithDecoderMaxMemory(uint64(MaxUncompressedChunk)))
		if err != nil {
			return nil
		}
		return r
	},
}

// Decode decompresses data. Output is capped to uncompressedLen when > 0,
// and never exceeds MaxUncompressedChunk. CodecAuto is encode-only and rejected.
func Decode(codec Codec, data []byte, uncompressedLen int) ([]byte, error) {
	if codec == CodecAuto {
		return nil, fmt.Errorf("codec auto is encode-only")
	}
	if codec == CodecNone {
		return data, nil
	}
	if uncompressedLen < 0 {
		return nil, fmt.Errorf("negative uncompressed length")
	}
	if uncompressedLen > MaxUncompressedChunk {
		return nil, fmt.Errorf("uncompressed length %d exceeds max %d", uncompressedLen, MaxUncompressedChunk)
	}
	maxOut := MaxUncompressedChunk
	if uncompressedLen > 0 {
		maxOut = uncompressedLen
	}
	switch codec {
	case CodecLZ4:
		r := lz4.NewReader(bytes.NewReader(data))
		return readCapped(r, maxOut, uncompressedLen)
	case CodecZstd:
		raw := zstdDecPool.Get()
		if raw == nil {
			return nil, fmt.Errorf("zstd decoder unavailable")
		}
		dec := raw.(*zstd.Decoder)
		defer zstdDecPool.Put(dec)
		if err := dec.Reset(bytes.NewReader(data)); err != nil {
			return nil, err
		}
		return readCapped(dec, maxOut, uncompressedLen)
	default:
		return nil, fmt.Errorf("unknown codec %d", codec)
	}
}

func readCapped(r io.Reader, maxOut, uncompressedLen int) ([]byte, error) {
	limited := io.LimitReader(r, int64(maxOut)+1)
	out, err := io.ReadAll(limited)
	if err != nil {
		return nil, err
	}
	if len(out) > maxOut {
		return nil, fmt.Errorf("decompressed size exceeds cap %d", maxOut)
	}
	if uncompressedLen > 0 && len(out) != uncompressedLen {
		return nil, fmt.Errorf("decompressed size %d != expected %d", len(out), uncompressedLen)
	}
	return out, nil
}

// SampleRatio returns uncompressed/compressed for a sample (1.0 = no gain).
func SampleRatio(codec Codec, sample []byte) float64 {
	if len(sample) == 0 {
		return 1
	}
	used, out, err := Encode(codec, sample)
	if err != nil || used == CodecNone {
		return 1
	}
	return float64(len(sample)) / float64(len(out))
}

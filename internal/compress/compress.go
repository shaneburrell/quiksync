package compress

import (
	"bytes"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

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

// Decode decompresses data.
func Decode(codec Codec, data []byte, uncompressedLen int) ([]byte, error) {
	if codec == CodecNone || codec == CodecAuto {
		return data, nil
	}
	switch codec {
	case CodecLZ4:
		r := lz4.NewReader(bytes.NewReader(data))
		var buf bytes.Buffer
		if uncompressedLen > 0 {
			buf.Grow(uncompressedLen)
		}
		if _, err := io.Copy(&buf, r); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	case CodecZstd:
		r, err := zstd.NewReader(nil)
		if err != nil {
			return nil, err
		}
		defer r.Close()
		return r.DecodeAll(data, make([]byte, 0, uncompressedLen))
	default:
		return nil, fmt.Errorf("unknown codec %d", codec)
	}
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

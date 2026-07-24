package chunk

import (
	"encoding/binary"
	"hash"
	"io"

	"github.com/zeebo/blake3"
)

const (
	DefaultAvgSize = 64 * 1024
	DefaultMinSize = 16 * 1024
	DefaultMaxSize = 256 * 1024
	SmallFileMax   = 256 * 1024
)

// Digest is a BLAKE3-256 digest.
type Digest [32]byte

func (d Digest) String() string {
	const hexdigits = "0123456789abcdef"
	var b [64]byte
	for i, v := range d {
		b[i*2] = hexdigits[v>>4]
		b[i*2+1] = hexdigits[v&0xf]
	}
	return string(b[:])
}

// Chunk is one content-defined chunk.
type Chunk struct {
	Offset uint64
	Length uint32
	Digest Digest
	Data   []byte // optional; present when hashing with data retained
}

// FileSignature is the ordered chunk digests for a file.
type FileSignature struct {
	Size   int64
	Digest Digest // whole-file BLAKE3
	Chunks []Chunk
}

// Hasher wraps blake3.
func NewHasher() hash.Hash {
	return blake3.New()
}

func Sum(data []byte) Digest {
	return blake3.Sum256(data)
}

// Options controls FastCDC parameters.
type Options struct {
	AvgSize  uint32
	MinSize  uint32
	MaxSize  uint32
	KeepData bool
}

func (o Options) normalized() Options {
	if o.AvgSize == 0 {
		o.AvgSize = DefaultAvgSize
	}
	if o.MinSize == 0 {
		o.MinSize = DefaultMinSize
	}
	if o.MaxSize == 0 {
		o.MaxSize = DefaultMaxSize
	}
	if o.MinSize > o.AvgSize {
		o.MinSize = o.AvgSize / 4
		if o.MinSize < 1024 {
			o.MinSize = 1024
		}
	}
	if o.MaxSize < o.AvgSize {
		o.MaxSize = o.AvgSize * 4
	}
	return o
}

// ChunkReader performs FastCDC over r and returns a signature.
func ChunkReader(r io.Reader, sizeHint int64, opt Options) (FileSignature, error) {
	opt = opt.normalized()
	hFile := blake3.New()
	tr := io.TeeReader(r, hFile)

	mask := maskForAvg(opt.AvgSize)
	buf := make([]byte, opt.MaxSize)
	var chunks []Chunk
	var offset uint64
	var carry []byte

	for {
		n, err := readFill(tr, buf[len(carry):])
		data := append(carry, buf[len(carry):len(carry)+n]...)
		carry = nil
		if len(data) == 0 {
			if err == io.EOF {
				break
			}
			if err != nil {
				return FileSignature{}, err
			}
			continue
		}

		for len(data) > 0 {
			cut := findCut(data, opt.MinSize, opt.MaxSize, mask)
			if cut == 0 {
				// Need more data unless EOF.
				if err == io.EOF || err == nil && n == 0 {
					cut = len(data)
				} else {
					carry = append([]byte(nil), data...)
					break
				}
			}
			piece := data[:cut]
			data = data[cut:]
			d := Sum(piece)
			c := Chunk{Offset: offset, Length: uint32(len(piece)), Digest: d}
			if opt.KeepData {
				c.Data = append([]byte(nil), piece...)
			}
			chunks = append(chunks, c)
			offset += uint64(len(piece))
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return FileSignature{}, err
		}
	}

	var whole Digest
	copy(whole[:], hFile.Sum(nil))
	total := int64(offset)
	if sizeHint > 0 {
		total = sizeHint
	}
	return FileSignature{Size: total, Digest: whole, Chunks: chunks}, nil
}

func readFill(r io.Reader, buf []byte) (int, error) {
	n := 0
	for n < len(buf) {
		nn, err := r.Read(buf[n:])
		n += nn
		if err != nil {
			return n, err
		}
	}
	return n, nil
}

// Gear-based FastCDC cut finder.
func findCut(data []byte, min, max uint32, mask uint32) int {
	n := len(data)
	if uint32(n) <= min {
		return 0
	}
	if uint32(n) > max {
		n = int(max)
	}
	hash := uint32(0)
	i := int(min)
	for ; i < n; i++ {
		hash = (hash << 1) + gear[data[i]]
		if hash&mask == 0 {
			return i + 1
		}
	}
	if uint32(len(data)) >= max {
		return int(max)
	}
	return 0
}

func maskForAvg(avg uint32) uint32 {
	// Highest power-of-two bits set to target avg boundary probability.
	bits := uint32(0)
	for avg > 1 {
		avg >>= 1
		bits++
	}
	if bits < 8 {
		bits = 8
	}
	return (1 << bits) - 1
}

// SerializeSignature encodes digests for wire/cache.
func SerializeSignature(sig FileSignature) []byte {
	buf := make([]byte, 8+32+4+len(sig.Chunks)*40)
	binary.LittleEndian.PutUint64(buf[0:8], uint64(sig.Size))
	copy(buf[8:40], sig.Digest[:])
	binary.LittleEndian.PutUint32(buf[40:44], uint32(len(sig.Chunks)))
	off := 44
	for _, c := range sig.Chunks {
		binary.LittleEndian.PutUint64(buf[off:off+8], c.Offset)
		off += 8
		copy(buf[off:off+32], c.Digest[:])
		off += 32
	}
	return buf[:off]
}

// HashFile hashes an entire reader.
func HashFile(r io.Reader) (Digest, int64, error) {
	h := blake3.New()
	n, err := io.Copy(h, r)
	if err != nil {
		return Digest{}, 0, err
	}
	var d Digest
	copy(d[:], h.Sum(nil))
	return d, n, nil
}

// gear table for FastCDC (precomputed random-ish values).
var gear = [256]uint32{
	0x5c95c078, 0x22408989, 0x5a39d9af, 0x206601a2, 0x5d66d705, 0x52c18365, 0x1fc3c1ec, 0x520e4464,
	0x3e07e9f0, 0x1d70b774, 0x5c57eb9b, 0x379ab867, 0x48be2b76, 0x3f4e4943, 0x2e686a2f, 0x4e6e8f5e,
	0x1f7b6f3c, 0x55a5e3c4, 0x2d6c4b8a, 0x4a3f9d21, 0x16e8c7b5, 0x3b29a6f0, 0x5e714d83, 0x27c0e94a,
	0x49d3b612, 0x31a85fc7, 0x1b4e72d9, 0x56f0a834, 0x3c9d15e6, 0x28b67a01, 0x4fd2c395, 0x15a8e470,
	0x5b3f6912, 0x2c74d8a5, 0x47e19c30, 0x19a5b6f4, 0x3d80c527, 0x51f2e86b, 0x26b49d0e, 0x4c37a891,
	0x18d5f243, 0x35a0c67e, 0x5e2b9147, 0x21f68d0a, 0x4a9c35e2, 0x17e4b859, 0x3b70d1c4, 0x528fa306,
	0x2d45e978, 0x49b0c21f, 0x16c8a5d3, 0x3f27d860, 0x54e19b2c, 0x28a4f715, 0x4d63c8a9, 0x1b90e547,
	0x36f2a81c, 0x5c4d7390, 0x23b8e64d, 0x4871a2f5, 0x15e9c830, 0x3a26d57c, 0x51f0b9a4, 0x2c8d471e,
	0x47a3e569, 0x19f6c2b0, 0x3d4b8157, 0x52e0d9c3, 0x27a5f48e, 0x4c19b672, 0x18d0e3a5, 0x3587c419,
	0x5e2fb86d, 0x21c4a930, 0x4a70d5e2, 0x17e3b84c, 0x3b9f61a7, 0x5248d015, 0x2df5a873, 0x49c1e2b6,
	0x16a07d49, 0x3f58c2e0, 0x54b1972d, 0x28e4d615, 0x4d73a8c9, 0x1b9f0e57, 0x3648c1a2, 0x5cd2e479,
	0x2387b60d, 0x48f1a5c3, 0x15a0d87e, 0x3a59c2b1, 0x51e07d46, 0x2cb8a915, 0x4763f2e8, 0x19d0c5a4,
	0x3d87b160, 0x52f4e829, 0x27a1c573, 0x4c58d0b6, 0x18e3a749, 0x359f1c2e, 0x5e48b0d1, 0x21d5e7a3,
	0x4a70c295, 0x17e8b64c, 0x3b2fd0a1, 0x5249e715, 0x2dc0a873, 0x4987f1b6, 0x16f2d549, 0x3f5ba0e2,
	0x54c1972d, 0x28e4d615, 0x4d73a8c9, 0x1b9f0e57, 0x3648c1a2, 0x5cd2e479, 0x2387b60d, 0x48f1a5c3,
	0x15a0d87e, 0x3a59c2b1, 0x51e07d46, 0x2cb8a915, 0x4763f2e8, 0x19d0c5a4, 0x3d87b160, 0x52f4e829,
	0x27a1c573, 0x4c58d0b6, 0x18e3a749, 0x359f1c2e, 0x5e48b0d1, 0x21d5e7a3, 0x4a70c295, 0x17e8b64c,
	0x3b2fd0a1, 0x5249e715, 0x2dc0a873, 0x4987f1b6, 0x16f2d549, 0x3f5ba0e2, 0x54c1972d, 0x28e4d615,
	0x4d73a8c9, 0x1b9f0e57, 0x3648c1a2, 0x5cd2e479, 0x2387b60d, 0x48f1a5c3, 0x15a0d87e, 0x3a59c2b1,
	0x51e07d46, 0x2cb8a915, 0x4763f2e8, 0x19d0c5a4, 0x3d87b160, 0x52f4e829, 0x27a1c573, 0x4c58d0b6,
	0x18e3a749, 0x359f1c2e, 0x5e48b0d1, 0x21d5e7a3, 0x4a70c295, 0x17e8b64c, 0x3b2fd0a1, 0x5249e715,
	0x2dc0a873, 0x4987f1b6, 0x16f2d549, 0x3f5ba0e2, 0x54c1972d, 0x28e4d615, 0x4d73a8c9, 0x1b9f0e57,
	0x3648c1a2, 0x5cd2e479, 0x2387b60d, 0x48f1a5c3, 0x15a0d87e, 0x3a59c2b1, 0x51e07d46, 0x2cb8a915,
	0x4763f2e8, 0x19d0c5a4, 0x3d87b160, 0x52f4e829, 0x27a1c573, 0x4c58d0b6, 0x18e3a749, 0x359f1c2e,
	0x5e48b0d1, 0x21d5e7a3, 0x4a70c295, 0x17e8b64c, 0x3b2fd0a1, 0x5249e715, 0x2dc0a873, 0x4987f1b6,
	0x16f2d549, 0x3f5ba0e2, 0x54c1972d, 0x28e4d615, 0x4d73a8c9, 0x1b9f0e57, 0x3648c1a2, 0x5cd2e479,
	0x2387b60d, 0x48f1a5c3, 0x15a0d87e, 0x3a59c2b1, 0x51e07d46, 0x2cb8a915, 0x4763f2e8, 0x19d0c5a4,
	0x3d87b160, 0x52f4e829, 0x27a1c573, 0x4c58d0b6, 0x18e3a749, 0x359f1c2e, 0x5e48b0d1, 0x21d5e7a3,
	0x4a70c295, 0x17e8b64c, 0x3b2fd0a1, 0x5249e715, 0x2dc0a873, 0x4987f1b6, 0x16f2d549, 0x3f5ba0e2,
	0x54c1972d, 0x28e4d615, 0x4d73a8c9, 0x1b9f0e57, 0x3648c1a2, 0x5cd2e479, 0x2387b60d, 0x48f1a5c3,
	0x15a0d87e, 0x3a59c2b1, 0x51e07d46, 0x2cb8a915, 0x4763f2e8, 0x19d0c5a4, 0x3d87b160, 0x52f4e829,
	0x27a1c573, 0x4c58d0b6, 0x18e3a749, 0x359f1c2e, 0x5e48b0d1, 0x21d5e7a3, 0x4a70c295, 0x17e8b64c,
}

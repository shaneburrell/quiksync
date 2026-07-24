package index

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/shaneburrell/quiksync/internal/chunk"
)

type cachedSig struct {
	Size    int64         `json:"size"`
	ModNano int64         `json:"mod_nano"`
	Digest  chunk.Digest  `json:"digest"`
	Chunks  []cachedChunk `json:"chunks"`
}

type cachedChunk struct {
	Offset uint64       `json:"offset"`
	Length uint32       `json:"length"`
	Digest chunk.Digest `json:"digest"`
}

// Cache stores destination file signatures under dest/.quiksync/index/.
type Cache struct {
	mu  sync.Mutex
	dir string
}

func Open(destRoot string) (*Cache, error) {
	dir := filepath.Join(destRoot, ".quiksync", "index")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	return &Cache{dir: dir}, nil
}

func (c *Cache) pathFor(rel string) string {
	safe := filepath.FromSlash(rel)
	return filepath.Join(c.dir, safe+".json")
}

func (c *Cache) Get(rel string, size int64, modNano int64) (chunk.FileSignature, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	b, err := os.ReadFile(c.pathFor(rel))
	if err != nil {
		return chunk.FileSignature{}, false
	}
	var cs cachedSig
	if err := json.Unmarshal(b, &cs); err != nil {
		return chunk.FileSignature{}, false
	}
	if cs.Size != size || cs.ModNano != modNano {
		return chunk.FileSignature{}, false
	}
	sig := chunk.FileSignature{Size: cs.Size, Digest: cs.Digest}
	for _, ch := range cs.Chunks {
		sig.Chunks = append(sig.Chunks, chunk.Chunk{Offset: ch.Offset, Length: ch.Length, Digest: ch.Digest})
	}
	return sig, true
}

func (c *Cache) Put(rel string, size int64, modNano int64, sig chunk.FileSignature) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cs := cachedSig{Size: size, ModNano: modNano, Digest: sig.Digest}
	for _, ch := range sig.Chunks {
		cs.Chunks = append(cs.Chunks, cachedChunk{Offset: ch.Offset, Length: ch.Length, Digest: ch.Digest})
	}
	path := c.pathFor(rel)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

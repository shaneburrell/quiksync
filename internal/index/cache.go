package index

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/transport"
)

type cachedSig struct {
	Size    int64         `json:"size"`
	ModNano int64         `json:"mod_nano"`
	Digest  chunk.Digest  `json:"digest"`
	AvgSize uint32        `json:"avg_size,omitempty"`
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

func (c *Cache) pathFor(rel string) (string, error) {
	rel = filepath.ToSlash(rel)
	if strings.Contains(rel, "..") {
		return "", fmt.Errorf("invalid index path: %q", rel)
	}
	return transport.SafeJoin(c.dir, rel+".json")
}

// Get returns a cached signature when size/mtime(/avg) match.
// When requireDigest is true, the caller should still verify content; this only returns metadata hit.
func (c *Cache) Get(rel string, size int64, modNano int64, avgSize uint32) (chunk.FileSignature, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	path, err := c.pathFor(rel)
	if err != nil {
		return chunk.FileSignature{}, false
	}
	b, err := os.ReadFile(path)
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
	// Legacy entries with AvgSize==0 cannot be trusted when the caller has a
	// non-zero CDC average — chunk boundaries may not match.
	if avgSize != 0 && cs.AvgSize != avgSize {
		return chunk.FileSignature{}, false
	}
	sig := chunk.FileSignature{Size: cs.Size, Digest: cs.Digest}
	for _, ch := range cs.Chunks {
		sig.Chunks = append(sig.Chunks, chunk.Chunk{Offset: ch.Offset, Length: ch.Length, Digest: ch.Digest})
	}
	return sig, true
}

func (c *Cache) Put(rel string, size int64, modNano int64, avgSize uint32, sig chunk.FileSignature) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	cs := cachedSig{Size: size, ModNano: modNano, Digest: sig.Digest, AvgSize: avgSize}
	for _, ch := range sig.Chunks {
		cs.Chunks = append(cs.Chunks, cachedChunk{Offset: ch.Offset, Length: ch.Length, Digest: ch.Digest})
	}
	path, err := c.pathFor(rel)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	b, err := json.Marshal(cs)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

// Delete removes a cached signature for rel (best-effort; missing is ok).
func (c *Cache) Delete(rel string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	path, err := c.pathFor(rel)
	if err != nil {
		return err
	}
	err = os.Remove(path)
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

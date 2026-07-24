package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Status string

const (
	StatusPending    Status = "pending"
	StatusInProgress Status = "in_progress"
	StatusComplete   Status = "complete"
	StatusFailed     Status = "failed"
)

// Entry tracks per-file transfer progress for resume.
type Entry struct {
	JobID      string    `json:"job_id"`
	RelPath    string    `json:"rel_path"`
	SrcDigest  string    `json:"src_digest"`
	SrcSize    int64     `json:"src_size"`
	SrcModNano int64     `json:"src_mod_nano"`
	ChunksDone int       `json:"chunks_done"`
	Attempts   int       `json:"attempts"`
	Status     Status    `json:"status"`
	LastError  string    `json:"last_error,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Journal is an append-friendly JSONL store under dest/.quiksync/journal/.
type Journal struct {
	mu   sync.Mutex
	path string
	by   map[string]Entry
}

func Open(destRoot, jobID string) (*Journal, error) {
	dir := filepath.Join(destRoot, ".quiksync", "journal")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, jobID+".jsonl")
	j := &Journal{path: path, by: map[string]Entry{}}
	if err := j.load(); err != nil {
		return nil, err
	}
	return j, nil
}

func (j *Journal) load() error {
	f, err := os.Open(j.path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	sc := bufio.NewScanner(f)
	buf := make([]byte, 64*1024)
	sc.Buffer(buf, 1<<20) // up to 1 MiB per line
	for sc.Scan() {
		var e Entry
		if err := json.Unmarshal(sc.Bytes(), &e); err != nil {
			continue
		}
		j.by[e.RelPath] = e
	}
	return sc.Err()
}

func (j *Journal) Get(rel string) (Entry, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	e, ok := j.by[rel]
	return e, ok
}

func (j *Journal) Put(e Entry) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	e.UpdatedAt = time.Now().UTC()
	j.by[e.RelPath] = e
	f, err := os.OpenFile(j.path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	enc := json.NewEncoder(f)
	if err := enc.Encode(e); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func (j *Journal) Completed(rel string) bool {
	e, ok := j.Get(rel)
	return ok && e.Status == StatusComplete
}

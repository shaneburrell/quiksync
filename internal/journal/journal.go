package journal

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
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
	mu           sync.Mutex
	path         string
	by           map[string]Entry
	CorruptLines int
}

// SanitizeJobID returns a filesystem-safe job id (no path separators / "..").
func SanitizeJobID(jobID string) (string, error) {
	if jobID == "" {
		jobID = "default"
	}
	if strings.Contains(jobID, "..") || strings.ContainsAny(jobID, `/\`) {
		return "", fmt.Errorf("invalid job id %q", jobID)
	}
	b := make([]byte, 0, len(jobID))
	for i := 0; i < len(jobID); i++ {
		c := jobID[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			b = append(b, c)
		} else {
			b = append(b, '_')
		}
	}
	if len(b) == 0 {
		return "", fmt.Errorf("invalid job id %q", jobID)
	}
	return string(b), nil
}

func Open(destRoot, jobID string) (*Journal, error) {
	safe, err := SanitizeJobID(jobID)
	if err != nil {
		return nil, err
	}
	dir := filepath.Join(destRoot, ".quiksync", "journal")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, safe+".jsonl")
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
			j.CorruptLines++
			continue
		}
		j.by[e.RelPath] = e
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if j.CorruptLines > 0 && len(j.by) == 0 {
		return fmt.Errorf("journal corrupt")
	}
	return nil
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
	if err := f.Close(); err != nil {
		return err
	}
	// Update in-memory map only after durable append succeeds.
	j.by[e.RelPath] = e
	return nil
}

func (j *Journal) Completed(rel string) bool {
	e, ok := j.Get(rel)
	return ok && e.Status == StatusComplete
}

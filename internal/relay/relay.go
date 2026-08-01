package relay

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/delta"
	"github.com/shaneburrell/quiksync/internal/journal"
	"github.com/shaneburrell/quiksync/internal/transport"
)

const schemaVersion = 1

// Manifest describes a published mid-hop job.
type Manifest struct {
	SchemaVersion int            `json:"schema_version"`
	JobID         string         `json:"job_id"`
	CreatedAt     time.Time      `json:"created_at"`
	Files         []ManifestFile `json:"files"`
}

// ManifestFile is one file in a mid-hop job.
type ManifestFile struct {
	RelPath string          `json:"rel_path"`
	Size    int64           `json:"size"`
	Digest  chunk.Digest    `json:"digest"`
	Mode    uint32          `json:"mode"`
	ModNano int64           `json:"mod_nano"`
	Chunks  []ManifestChunk `json:"chunks"`
}

// ManifestChunk is a content-addressed chunk reference.
type ManifestChunk struct {
	Offset uint64       `json:"offset"`
	Length uint32       `json:"length"`
	Digest chunk.Digest `json:"digest"`
}

// Lease is written before objects; readers ignore jobs without a manifest.
type Lease struct {
	JobID      string    `json:"job_id"`
	SenderID   string    `json:"sender_id"`
	Generation int64     `json:"generation"`
	TTLSeconds int64     `json:"ttl_seconds"`
	CreatedAt  time.Time `json:"created_at"`
	ExpiresAt  time.Time `json:"expires_at"`
}

// Ack is written by the receiver after successful materialization.
type Ack struct {
	JobID       string    `json:"job_id"`
	CompletedAt time.Time `json:"completed_at"`
	FilesOK     int       `json:"files_ok"`
}

// NotifyMeta is store/control wakeup metadata.
type NotifyMeta struct {
	JobID      string `json:"job_id"`
	Via        string `json:"via,omitempty"`
	Generation int64  `json:"generation,omitempty"`
}

type jobPaths struct {
	prefix string
}

func paths(jobID string) jobPaths {
	return jobPaths{prefix: path.Join(".quiksync/relay", jobID)}
}

func sanitizeJobID(jobID string) (string, error) {
	return journal.SanitizeJobID(jobID)
}

func (p jobPaths) lease() string    { return path.Join(p.prefix, "lease.json") }
func (p jobPaths) manifest() string { return path.Join(p.prefix, "manifest.json") }
func (p jobPaths) notify() string   { return path.Join(p.prefix, "notify") }
func (p jobPaths) ack() string      { return path.Join(p.prefix, "ack.json") }
func (p jobPaths) object(d chunk.Digest) string {
	return path.Join(p.prefix, "objects", d.String())
}

// Signal wakes a receiver (optional). StoreSignal always works via polling.
type Signal interface {
	Notify(ctx context.Context, jobID string, meta NotifyMeta) error
	Wait(ctx context.Context, jobID string) (NotifyMeta, error)
}

// StoreSignal polls notify/manifest objects on the mid transport.
type StoreSignal struct {
	Mid transport.Transport
}

// Notify writes the notify marker.
func (s StoreSignal) Notify(ctx context.Context, jobID string, meta NotifyMeta) error {
	p := paths(jobID)
	body, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return putBytes(ctx, s.Mid, p.notify(), body)
}

const storeSignalMaxHardFails = 5

// Wait polls until notify or manifest appears.
func (s StoreSignal) Wait(ctx context.Context, jobID string) (NotifyMeta, error) {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	hardFails := 0
	for {
		if err := ctx.Err(); err != nil {
			return NotifyMeta{}, err
		}
		meta, ready, err := s.pollOnce(ctx, jobID)
		if ready {
			return meta, nil
		}
		if err != nil {
			hardFails++
			if hardFails >= storeSignalMaxHardFails {
				return NotifyMeta{}, err
			}
		} else {
			hardFails = 0
		}
		select {
		case <-ctx.Done():
			return NotifyMeta{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

// pollOnce checks notify/manifest once. ready=true means the job is published.
// A non-nil err is a hard transport failure (not merely absent).
func (s StoreSignal) pollOnce(ctx context.Context, jobID string) (NotifyMeta, bool, error) {
	p := paths(jobID)
	rc, err := s.Mid.OpenRead(ctx, p.notify())
	if err == nil {
		var meta NotifyMeta
		decErr := json.NewDecoder(rc).Decode(&meta)
		_ = rc.Close()
		if decErr == nil {
			return meta, true, nil
		}
		// Corrupt notify: keep polling; manifest may still be valid.
	} else if !isAbsent(err) {
		return NotifyMeta{}, false, fmt.Errorf("poll notify: %w", err)
	}
	if _, err := s.Mid.Stat(ctx, p.manifest()); err == nil {
		return NotifyMeta{JobID: jobID}, true, nil
	} else if !isAbsent(err) {
		return NotifyMeta{}, false, fmt.Errorf("poll manifest: %w", err)
	}
	return NotifyMeta{}, false, nil
}

func isAbsent(err error) bool {
	if err == nil {
		return false
	}
	if os.IsNotExist(err) || errors.Is(err, os.ErrNotExist) {
		return true
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "notfound") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "no such file") ||
		strings.Contains(msg, "does not exist")
}

// SendOptions configures relay send.
type SendOptions struct {
	JobID    string
	TTL      time.Duration
	SenderID string
	Signal   Signal
	Exclude  []string
	ChunkAvg uint32
}

// Send uploads missing CA objects and publishes lease+manifest to mid.
func Send(ctx context.Context, src, mid transport.Transport, opts SendOptions) error {
	safe, err := sanitizeJobID(opts.JobID)
	if err != nil {
		return err
	}
	opts.JobID = safe
	if opts.TTL <= 0 {
		opts.TTL = 24 * time.Hour
	}
	if opts.SenderID == "" {
		opts.SenderID = "sender"
	}
	p := paths(opts.JobID)
	now := time.Now().UTC()
	lease := Lease{
		JobID: opts.JobID, SenderID: opts.SenderID, Generation: now.UnixNano(),
		TTLSeconds: int64(opts.TTL / time.Second), CreatedAt: now, ExpiresAt: now.Add(opts.TTL),
	}
	body, err := json.Marshal(lease)
	if err != nil {
		return err
	}
	if err := putBytes(ctx, mid, p.lease(), body); err != nil {
		return err
	}

	files, err := src.Walk(ctx, opts.Exclude)
	if err != nil {
		return err
	}
	man := Manifest{SchemaVersion: schemaVersion, JobID: opts.JobID, CreatedAt: now}
	uploaded := map[chunk.Digest]struct{}{}

	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		rc, err := src.OpenRead(ctx, f.RelPath)
		if err != nil {
			return err
		}
		mf := ManifestFile{
			RelPath: f.RelPath, Size: f.Size,
			Mode: uint32(f.Mode), ModNano: f.ModTime.UnixNano(),
		}
		sig, err := chunk.StreamChunks(rc, f.Size, chunk.Options{AvgSize: opts.ChunkAvg}, func(c chunk.Chunk) error {
			mf.Chunks = append(mf.Chunks, ManifestChunk{Offset: c.Offset, Length: c.Length, Digest: c.Digest})
			if _, ok := uploaded[c.Digest]; ok {
				return nil
			}
			if _, err := mid.Stat(ctx, p.object(c.Digest)); err == nil {
				uploaded[c.Digest] = struct{}{}
				return nil
			}
			if c.Data == nil {
				return fmt.Errorf("missing chunk data for %s", f.RelPath)
			}
			if err := putBytes(ctx, mid, p.object(c.Digest), c.Data); err != nil {
				return err
			}
			uploaded[c.Digest] = struct{}{}
			return nil
		})
		_ = rc.Close()
		if err != nil {
			return err
		}
		mf.Digest = sig.Digest
		man.Files = append(man.Files, mf)
	}

	manBody, err := json.Marshal(man)
	if err != nil {
		return err
	}
	if err := putBytes(ctx, mid, p.manifest(), manBody); err != nil {
		return err
	}
	storeSig := StoreSignal{Mid: mid}
	meta := NotifyMeta{JobID: opts.JobID, Generation: lease.Generation}
	_ = storeSig.Notify(ctx, opts.JobID, meta)
	if opts.Signal != nil {
		_ = opts.Signal.Notify(ctx, opts.JobID, meta)
	}
	return nil
}

// RecvOptions configures relay receive.
type RecvOptions struct {
	JobID  string
	Signal Signal
	Wait   time.Duration
}

// Recv waits for a published job and materializes into dest.
func Recv(ctx context.Context, mid, dest transport.Transport, opts RecvOptions) error {
	safe, err := sanitizeJobID(opts.JobID)
	if err != nil {
		return err
	}
	opts.JobID = safe
	if opts.Wait <= 0 {
		opts.Wait = 30 * time.Minute
	}
	waitCtx, cancel := context.WithTimeout(ctx, opts.Wait)
	defer cancel()

	if err := waitForJob(waitCtx, mid, opts.JobID, opts.Signal); err != nil {
		return fmt.Errorf("wait for job: %w", err)
	}

	p := paths(opts.JobID)
	var lease Lease
	if err := getJSON(ctx, mid, p.lease(), &lease); err == nil {
		if !lease.ExpiresAt.IsZero() && time.Now().UTC().After(lease.ExpiresAt) {
			return fmt.Errorf("lease expired for job %s", opts.JobID)
		}
	}
	var man Manifest
	if err := getJSON(ctx, mid, p.manifest(), &man); err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	if err := validateManifest(opts.JobID, &man); err != nil {
		return fmt.Errorf("invalid manifest: %w", err)
	}
	ok := 0
	for _, f := range man.Files {
		if err := materialize(ctx, mid, dest, p, f); err != nil {
			return err
		}
		ok++
	}
	ack := Ack{JobID: opts.JobID, CompletedAt: time.Now().UTC(), FilesOK: ok}
	ackBody, err := json.Marshal(ack)
	if err != nil {
		return err
	}
	return putBytes(ctx, mid, p.ack(), ackBody)
}

// waitForJob polls the mid store and optionally wakes early via Signal.
// Signal success never grants data-plane trust; store state is re-checked.
func waitForJob(ctx context.Context, mid transport.Transport, jobID string, sig Signal) error {
	store := StoreSignal{Mid: mid}
	if meta, ready, err := store.pollOnce(ctx, jobID); ready {
		_ = meta
		return nil
	} else if err != nil {
		return err
	}

	var sigCh <-chan error
	if sig != nil {
		ch := make(chan error, 1)
		sigCh = ch
		go func() {
			_, err := sig.Wait(ctx, jobID)
			ch <- err
		}()
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	hardFails := 0
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-sigCh:
			sigCh = nil // one-shot wakeup
			if err != nil && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
				// Signal dial/wait failed; keep polling the store.
				break
			}
			if meta, ready, err := store.pollOnce(ctx, jobID); ready {
				_ = meta
				return nil
			} else if err != nil {
				hardFails++
				if hardFails >= storeSignalMaxHardFails {
					return err
				}
			} else {
				hardFails = 0
			}
		case <-ticker.C:
			if meta, ready, err := store.pollOnce(ctx, jobID); ready {
				_ = meta
				return nil
			} else if err != nil {
				hardFails++
				if hardFails >= storeSignalMaxHardFails {
					return err
				}
			} else {
				hardFails = 0
			}
		}
	}
}

func validateManifest(jobID string, man *Manifest) error {
	if man.SchemaVersion != schemaVersion {
		return fmt.Errorf("unsupported schema_version %d", man.SchemaVersion)
	}
	if man.JobID != jobID {
		return fmt.Errorf("job_id mismatch: manifest %q want %q", man.JobID, jobID)
	}
	seen := map[string]struct{}{}
	for i, f := range man.Files {
		if err := validateManifestFile(f); err != nil {
			return fmt.Errorf("file[%d]: %w", i, err)
		}
		if _, ok := seen[f.RelPath]; ok {
			return fmt.Errorf("duplicate rel_path %q", f.RelPath)
		}
		seen[f.RelPath] = struct{}{}
	}
	return nil
}

func validateManifestFile(f ManifestFile) error {
	if _, err := transport.SafeJoinFile("/quiksync-relay-confine", f.RelPath); err != nil {
		return fmt.Errorf("rel_path: %w", err)
	}
	if f.Size < 0 {
		return fmt.Errorf("negative size")
	}
	var next uint64
	for i, c := range f.Chunks {
		if c.Offset != next {
			return fmt.Errorf("chunk[%d]: gap or overlap at offset %d (want %d)", i, c.Offset, next)
		}
		if c.Length == 0 {
			return fmt.Errorf("chunk[%d]: zero length", i)
		}
		next += uint64(c.Length)
	}
	if int64(next) != f.Size {
		return fmt.Errorf("chunks cover %d bytes, size is %d", next, f.Size)
	}
	return nil
}

func materialize(ctx context.Context, mid, dest transport.Transport, p jobPaths, f ManifestFile) error {
	var destSig chunk.FileSignature
	if sig, err := dest.GetSignature(ctx, f.RelPath); err == nil {
		destSig = sig
	}
	srcSig := chunk.FileSignature{Size: f.Size, Digest: f.Digest}
	for _, c := range f.Chunks {
		srcSig.Chunks = append(srcSig.Chunks, chunk.Chunk{Offset: c.Offset, Length: c.Length, Digest: c.Digest})
	}
	plan := delta.Diff(srcSig, destSig)
	missing := map[chunk.Digest]struct{}{}
	for _, c := range plan.Missing {
		missing[c.Digest] = struct{}{}
	}
	reuseByNew := map[uint64]delta.ReuseEntry{}
	for _, r := range plan.Reuse {
		reuseByNew[r.NewOffset] = r
	}

	ws, err := dest.BeginWrite(ctx, f.RelPath, f.Size)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = ws.Abort()
		}
	}()

	canReuse := dest.Caps().SupportsReuseChunk && len(destSig.Chunks) > 0
	for _, c := range f.Chunks {
		if _, miss := missing[c.Digest]; miss || len(destSig.Chunks) == 0 {
			data, err := readObject(ctx, mid, p.object(c.Digest), c.Digest)
			if err != nil {
				return err
			}
			if err := ws.WriteChunk(ctx, c.Offset, compress.CodecNone, len(data), data); err != nil {
				return err
			}
			continue
		}
		if canReuse {
			re := reuseByNew[c.Offset]
			if err := ws.ReuseChunk(ctx, re.NewOffset, re.OldOffset, re.Digest, re.Length); err != nil {
				return err
			}
			continue
		}
		data, err := readObject(ctx, mid, p.object(c.Digest), c.Digest)
		if err != nil {
			return err
		}
		if err := ws.WriteChunk(ctx, c.Offset, compress.CodecNone, len(data), data); err != nil {
			return err
		}
	}
	if err := ws.Commit(ctx, f.Digest, os.FileMode(f.Mode), time.Unix(0, f.ModNano)); err != nil {
		return err
	}
	committed = true
	return nil
}

func readObject(ctx context.Context, mid transport.Transport, rel string, want chunk.Digest) ([]byte, error) {
	rc, err := mid.OpenRead(ctx, rel)
	if err != nil {
		return nil, fmt.Errorf("fetch object %s: %w", want, err)
	}
	data, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, err
	}
	if got := chunk.Sum(data); got != want {
		return nil, fmt.Errorf("poison object for digest %s", want)
	}
	return data, nil
}

// GC deletes a job prefix after ack or forced TTL expiry.
func GC(ctx context.Context, mid transport.Transport, jobID string, force bool) error {
	safe, err := sanitizeJobID(jobID)
	if err != nil {
		return err
	}
	jobID = safe
	p := paths(jobID)
	if !force {
		if _, err := mid.Stat(ctx, p.ack()); err != nil {
			var lease Lease
			if err := getJSON(ctx, mid, p.lease(), &lease); err == nil {
				if time.Now().UTC().Before(lease.ExpiresAt) {
					return fmt.Errorf("job %s not acked and lease not expired", jobID)
				}
			} else {
				return fmt.Errorf("job %s not acked", jobID)
			}
		}
	}
	var man Manifest
	if err := getJSON(ctx, mid, p.manifest(), &man); err == nil {
		seen := map[chunk.Digest]struct{}{}
		for _, f := range man.Files {
			for _, c := range f.Chunks {
				if _, ok := seen[c.Digest]; ok {
					continue
				}
				seen[c.Digest] = struct{}{}
				_ = mid.Remove(ctx, p.object(c.Digest))
			}
		}
	}
	for _, rel := range []string{p.lease(), p.manifest(), p.notify(), p.ack()} {
		_ = mid.Remove(ctx, rel)
	}
	return nil
}

func putBytes(ctx context.Context, t transport.Transport, rel string, data []byte) error {
	if err := t.MkdirAll(ctx, path.Dir(rel)); err != nil {
		return err
	}
	ws, err := t.BeginWrite(ctx, rel, int64(len(data)))
	if err != nil {
		return err
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data); err != nil {
		_ = ws.Abort()
		return err
	}
	dig := chunk.Sum(data)
	if err := ws.Commit(ctx, dig, 0o644, time.Now().UTC()); err != nil {
		_ = ws.Abort()
		return err
	}
	return nil
}

func getJSON(ctx context.Context, t transport.Transport, rel string, v any) error {
	rc, err := t.OpenRead(ctx, rel)
	if err != nil {
		return err
	}
	defer func() { _ = rc.Close() }()
	return json.NewDecoder(rc).Decode(v)
}

package s3

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/transport"
)

const (
	metaBlake3 = "quiksync-blake3"
	metaMode   = "quiksync-mode"
	metaMod    = "quiksync-mod-nano"
)

// Options configures the S3 transport.
type Options struct {
	Endpoint   string
	Region     string
	StagingDir string
	// Client, when non-nil, overrides AWS SDK construction (tests).
	Client API
}

// API is the subset of S3 used by this transport.
type API interface {
	ListObjectsV2(ctx context.Context, params *s3.ListObjectsV2Input, optFns ...func(*s3.Options)) (*s3.ListObjectsV2Output, error)
	HeadObject(ctx context.Context, params *s3.HeadObjectInput, optFns ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
	GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error)
	PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error)
	DeleteObject(ctx context.Context, params *s3.DeleteObjectInput, optFns ...func(*s3.Options)) (*s3.DeleteObjectOutput, error)
	CopyObject(ctx context.Context, params *s3.CopyObjectInput, optFns ...func(*s3.Options)) (*s3.CopyObjectOutput, error)
}

// Transport implements transport.Transport against S3-compatible object storage.
type Transport struct {
	bucket     string
	prefix     string
	client     API
	stagingDir string
	reuse      bool // SupportsReuseChunk (Phase 1b)
}

// New opens an S3 transport for s3://bucket/prefix.
func New(ctx context.Context, ep transport.Endpoint, opts Options) (*Transport, error) {
	if ep.Host == "" {
		return nil, fmt.Errorf("s3: missing bucket")
	}
	staging := opts.StagingDir
	if staging == "" {
		if cfg := os.Getenv("QUIKSYNC_CONFIG"); cfg != "" {
			staging = filepath.Join(cfg, "s3-staging")
		} else if h, err := os.UserConfigDir(); err == nil {
			staging = filepath.Join(h, "quiksync", "s3-staging")
		} else {
			staging = filepath.Join(os.TempDir(), "quiksync-s3-staging")
		}
	}
	if err := os.MkdirAll(staging, 0o755); err != nil {
		return nil, err
	}
	client := opts.Client
	if client == nil {
		loadOpts := []func(*config.LoadOptions) error{}
		if opts.Region != "" {
			loadOpts = append(loadOpts, config.WithRegion(opts.Region))
		}
		cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
		if err != nil {
			return nil, fmt.Errorf("s3 config: %w", err)
		}
		var s3opts []func(*s3.Options)
		if opts.Endpoint != "" {
			epURL := opts.Endpoint
			s3opts = append(s3opts, func(o *s3.Options) {
				o.BaseEndpoint = aws.String(epURL)
				o.UsePathStyle = true
			})
		}
		client = s3.NewFromConfig(cfg, s3opts...)
	}
	return &Transport{
		bucket:     ep.Host,
		prefix:     strings.Trim(ep.Path, "/"),
		client:     client,
		stagingDir: staging,
		reuse:      true,
	}, nil
}

func (t *Transport) Caps() transport.Caps {
	return transport.Caps{
		SupportsDelta:      true,
		SupportsMultiplex:  true,
		SupportsResume:     true,
		SupportsReuseChunk: t.reuse,
	}
}

func (t *Transport) Close() error { return nil }

func (t *Transport) Root() string {
	if t.prefix == "" {
		return "s3://" + t.bucket
	}
	return "s3://" + t.bucket + "/" + t.prefix
}

func (t *Transport) key(rel string) string {
	rel = path.Clean("/" + filepath.ToSlash(rel))
	rel = strings.TrimPrefix(rel, "/")
	if t.prefix == "" {
		return rel
	}
	if rel == "" || rel == "." {
		return t.prefix
	}
	return t.prefix + "/" + rel
}

func (t *Transport) sigKey(rel string) string {
	sum := sha256.Sum256([]byte(filepath.ToSlash(rel)))
	name := hex.EncodeToString(sum[:16]) + ".json"
	if t.prefix == "" {
		return ".quiksync/sigs/" + name
	}
	return t.prefix + "/.quiksync/sigs/" + name
}

func (t *Transport) Walk(ctx context.Context, exclude []string) ([]transport.FileMeta, error) {
	prefix := t.prefix
	if prefix != "" && !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}
	var out []transport.FileMeta
	var token *string
	for {
		resp, err := t.client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            aws.String(t.bucket),
			Prefix:            aws.String(prefix),
			ContinuationToken: token,
		})
		if err != nil {
			return nil, err
		}
		for _, obj := range resp.Contents {
			k := aws.ToString(obj.Key)
			if strings.Contains(k, "/.quiksync.tmp/") || strings.Contains(k, "/.quiksync/") ||
				strings.HasPrefix(k, ".quiksync.tmp/") || strings.HasPrefix(k, ".quiksync/") {
				continue
			}
			rel := k
			if t.prefix != "" {
				rel = strings.TrimPrefix(k, t.prefix+"/")
			}
			if rel == "" || strings.HasSuffix(k, "/") {
				continue
			}
			if matchExclude(rel, exclude) {
				continue
			}
			mt := time.Time{}
			if obj.LastModified != nil {
				mt = *obj.LastModified
			}
			out = append(out, transport.FileMeta{
				RelPath: rel,
				Size:    aws.ToInt64(obj.Size),
				ModTime: mt,
				Mode:    0o644,
			})
		}
		if !aws.ToBool(resp.IsTruncated) {
			break
		}
		token = resp.NextContinuationToken
	}
	return out, nil
}

func matchExclude(rel string, patterns []string) bool {
	for _, p := range patterns {
		ok, err := filepath.Match(p, rel)
		if err == nil && ok {
			return true
		}
		ok, err = filepath.Match(p, filepath.Base(rel))
		if err == nil && ok {
			return true
		}
	}
	return false
}

func (t *Transport) Stat(ctx context.Context, rel string) (transport.FileMeta, error) {
	resp, err := t.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(t.key(rel)),
	})
	if err != nil {
		return transport.FileMeta{}, err
	}
	mt := time.Time{}
	if resp.LastModified != nil {
		mt = *resp.LastModified
	}
	if v, ok := resp.Metadata[metaMod]; ok {
		var nano int64
		if _, err := fmt.Sscanf(v, "%d", &nano); err == nil {
			mt = time.Unix(0, nano)
		}
	}
	mode := os.FileMode(0o644)
	if v, ok := resp.Metadata[metaMode]; ok {
		var m uint32
		if _, err := fmt.Sscanf(v, "%d", &m); err == nil {
			mode = os.FileMode(m)
		}
	}
	return transport.FileMeta{
		RelPath: rel,
		Size:    aws.ToInt64(resp.ContentLength),
		ModTime: mt,
		Mode:    mode,
	}, nil
}

func (t *Transport) OpenRead(ctx context.Context, rel string) (io.ReadCloser, error) {
	resp, err := t.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(t.key(rel)),
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

func (t *Transport) Remove(ctx context.Context, rel string) error {
	_, err := t.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(t.key(rel)),
	})
	if err != nil {
		return err
	}
	_, _ = t.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(t.sigKey(rel)),
	})
	return nil
}

func (t *Transport) MkdirAll(ctx context.Context, rel string) error {
	return nil // prefixes are implicit
}

func (t *Transport) GetSignature(ctx context.Context, rel string) (chunk.FileSignature, error) {
	resp, err := t.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(t.sigKey(rel)),
	})
	if err == nil {
		defer func() { _ = resp.Body.Close() }()
		var sig chunk.FileSignature
		if err := json.NewDecoder(resp.Body).Decode(&sig); err == nil && len(sig.Chunks) > 0 {
			if t.sidecarBoundToObject(ctx, rel, sig) {
				return sig, nil
			}
			// Stale/poisoned sidecar: fall through to hash the object body.
		}
	}
	rc, err := t.OpenRead(ctx, rel)
	if err != nil {
		return chunk.FileSignature{}, nil // missing
	}
	defer func() { _ = rc.Close() }()
	st, err := t.Stat(ctx, rel)
	if err != nil {
		return chunk.FileSignature{}, err
	}
	return chunk.ChunkReader(rc, st.Size, chunk.Options{})
}

// sidecarBoundToObject reports whether the sidecar still matches the live object
// size and quiksync-blake3 metadata (set on Commit).
func (t *Transport) sidecarBoundToObject(ctx context.Context, rel string, sig chunk.FileSignature) bool {
	head, err := t.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(t.bucket),
		Key:    aws.String(t.key(rel)),
	})
	if err != nil {
		return false
	}
	if aws.ToInt64(head.ContentLength) != sig.Size {
		return false
	}
	want := sig.Digest.String()
	if dig, ok := head.Metadata[metaBlake3]; ok && dig == want {
		return true
	}
	// AWS SDK returns metadata keys lowercased.
	if dig, ok := head.Metadata[strings.ToLower(metaBlake3)]; ok && dig == want {
		return true
	}
	return false
}

type writeSession struct {
	t         *Transport
	rel       string
	finalKey  string
	size      int64
	f         *os.File
	tempPath  string
	committed bool
	mu        sync.Mutex
}

func (t *Transport) BeginWrite(ctx context.Context, rel string, size int64) (transport.WriteSession, error) {
	tmp, err := os.CreateTemp(t.stagingDir, "qs-*.partial")
	if err != nil {
		return nil, err
	}
	if size > 0 {
		if err := tmp.Truncate(size); err != nil {
			_ = tmp.Close()
			_ = os.Remove(tmp.Name())
			return nil, err
		}
	}
	return &writeSession{
		t:        t,
		rel:      rel,
		finalKey: t.key(rel),
		size:     size,
		f:        tmp,
		tempPath: tmp.Name(),
	}, nil
}

func (w *writeSession) WriteChunk(ctx context.Context, offset uint64, codec compress.Codec, uncompressedLen int, data []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.committed {
		return fmt.Errorf("write after commit")
	}
	raw, err := compress.Decode(codec, data, uncompressedLen)
	if err != nil {
		return err
	}
	_, err = w.f.WriteAt(raw, int64(offset))
	return err
}

func (w *writeSession) ReuseChunk(ctx context.Context, newOffset, oldOffset uint64, digest chunk.Digest, length int) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.committed {
		return fmt.Errorf("reuse after commit")
	}
	if !w.t.reuse {
		return fmt.Errorf("s3 reuse not enabled")
	}
	if err := transport.ValidateReuseRange(oldOffset, length, -1); err != nil {
		return err
	}
	end := oldOffset + uint64(length) - 1
	resp, err := w.t.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(w.t.bucket),
		Key:    aws.String(w.finalKey),
		Range:  aws.String(fmt.Sprintf("bytes=%d-%d", oldOffset, end)),
	})
	if err != nil {
		return fmt.Errorf("s3 reuse get: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	buf := make([]byte, length)
	if _, err := io.ReadFull(resp.Body, buf); err != nil {
		return err
	}
	if got := chunk.Sum(buf); got != digest {
		return fmt.Errorf("s3 reuse digest mismatch")
	}
	_, err = w.f.WriteAt(buf, int64(newOffset))
	return err
}

func (w *writeSession) Commit(ctx context.Context, expected chunk.Digest, mode os.FileMode, modTime time.Time) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.committed {
		return nil
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	sig, err := chunk.ChunkReader(w.f, w.size, chunk.Options{})
	if err != nil {
		return err
	}
	if sig.Digest != expected {
		return fmt.Errorf("digest mismatch: got %s want %s", sig.Digest, expected)
	}
	// Strip inline data before persisting sidecar.
	for i := range sig.Chunks {
		sig.Chunks[i].Data = nil
	}
	if _, err := w.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	meta := map[string]string{
		metaBlake3: expected.String(),
		metaMode:   fmt.Sprintf("%d", uint32(mode)),
	}
	if !modTime.IsZero() {
		meta[metaMod] = fmt.Sprintf("%d", modTime.UnixNano())
	}
	_, err = w.t.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:   aws.String(w.t.bucket),
		Key:      aws.String(w.finalKey),
		Body:     w.f,
		Metadata: meta,
	})
	if err != nil {
		return err
	}
	_ = w.f.Close()
	w.f = nil
	_ = os.Remove(w.tempPath)

	sigBody, err := json.Marshal(sig)
	if err != nil {
		return fmt.Errorf("signature sidecar: %w", err)
	}
	if _, err := w.t.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(w.t.bucket),
		Key:    aws.String(w.t.sigKey(w.rel)),
		Body:   bytes.NewReader(sigBody),
	}); err != nil {
		return fmt.Errorf("signature sidecar: %w", err)
	}

	w.committed = true
	return nil
}

func (w *writeSession) Abort() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.committed {
		return nil
	}
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
	_ = os.Remove(w.tempPath)
	return nil
}

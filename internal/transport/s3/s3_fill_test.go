package s3

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestS3StaleSidecarFallsBackWhenObjectReplaced(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "bucket", Path: "p"}, Options{Client: mem, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	orig := []byte("original-object-body-aaaa")
	ws, err := tr.BeginWrite(ctx, "f.txt", int64(len(orig)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(orig), orig); err != nil {
		t.Fatal(err)
	}
	origDig := chunk.Sum(orig)
	if err := ws.Commit(ctx, origDig, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Replace object body; leave sidecar pointing at old digest.
	replaced := []byte("replaced-object-body-bbbb")
	if len(replaced) != len(orig) {
		t.Fatal("size mismatch")
	}
	_, err = mem.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String("p/f.txt"),
		Body:   strings.NewReader(string(replaced)),
		Metadata: map[string]string{
			metaBlake3: chunk.Sum(replaced).String(),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	sig, err := tr.GetSignature(ctx, "f.txt")
	if err != nil {
		t.Fatal(err)
	}
	if sig.Digest == origDig {
		t.Fatal("stale sidecar digest must not be trusted after object replace")
	}
	if sig.Digest != chunk.Sum(replaced) {
		t.Fatalf("want replaced digest, got %s", sig.Digest)
	}
}

func TestS3CorruptSidecarFallsBackToChunking(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "bucket", Path: "p"}, Options{Client: mem, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	data := []byte("fallback-from-corrupt-sidecar-0123456789")
	ws, err := tr.BeginWrite(ctx, "f.txt", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data); err != nil {
		t.Fatal(err)
	}
	dig := chunk.Sum(data)
	if err := ws.Commit(ctx, dig, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	// Overwrite sidecar with garbage (hashed key layout).
	sum := sha256.Sum256([]byte(filepath.ToSlash("f.txt")))
	sigKey := "p/.quiksync/sigs/" + hex.EncodeToString(sum[:16]) + ".json"
	_, err = mem.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("bucket"),
		Key:    aws.String(sigKey),
		Body:   strings.NewReader("{not-json"),
	})
	if err != nil {
		t.Fatal(err)
	}
	sig, err := tr.GetSignature(ctx, "f.txt")
	if err != nil || sig.Digest != dig || len(sig.Chunks) == 0 {
		t.Fatalf("expected chunked fallback: %+v %v", sig, err)
	}
}

func TestS3WriteReuseAfterCommit(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "b"}, Options{Client: mem, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	data := []byte("committed")
	ws, err := tr.BeginWrite(ctx, "c.txt", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data); err != nil {
		t.Fatal(err)
	}
	dig := chunk.Sum(data)
	if err := ws.Commit(ctx, dig, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, 1, []byte("x")); err == nil || !strings.Contains(err.Error(), "after commit") {
		t.Fatalf("write after commit: %v", err)
	}
	if err := ws.ReuseChunk(ctx, 0, 0, dig, len(data)); err == nil || !strings.Contains(err.Error(), "after commit") {
		t.Fatalf("reuse after commit: %v", err)
	}
	if err := ws.Abort(); err != nil {
		t.Fatal(err)
	}
	// Idempotent second commit
	if err := ws.Commit(ctx, dig, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
}

func TestS3ReuseRejectsHugeLength(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "b", Path: ""}, Options{Client: mem, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	ws, err := tr.BeginWrite(ctx, "h.bin", 8)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ws.Abort() }()
	huge := int(chunk.DefaultMaxSize) + 1
	if err := ws.ReuseChunk(ctx, 0, 0, chunk.Digest{}, huge); err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("expected max reject, got %v", err)
	}
}

func TestS3ReuseDigestMismatchAndWalkExclude(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "b", Path: ""}, Options{Client: mem, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	base := []byte("aabbccddee")
	ws, err := tr.BeginWrite(ctx, "keep.bin", int64(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.WriteChunk(ctx, 0, compress.CodecNone, len(base), base)
	if err := ws.Commit(ctx, chunk.Sum(base), 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}
	skip := []byte("skip-me")
	wsSkip, err := tr.BeginWrite(ctx, "skip.dat", int64(len(skip)))
	if err != nil {
		t.Fatal(err)
	}
	_ = wsSkip.WriteChunk(ctx, 0, compress.CodecNone, len(skip), skip)
	if err := wsSkip.Commit(ctx, chunk.Sum(skip), 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	files, err := tr.Walk(ctx, []string{"skip*"})
	if err != nil || len(files) != 1 || files[0].RelPath != "keep.bin" {
		t.Fatalf("walk exclude: %v %v", files, err)
	}

	ws2, err := tr.BeginWrite(ctx, "keep.bin", int64(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	bad := chunk.Digest{9}
	if err := ws2.ReuseChunk(ctx, 0, 0, bad, len(base)); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("expected reuse digest mismatch, got %v", err)
	}
	_ = ws2.Abort()
}

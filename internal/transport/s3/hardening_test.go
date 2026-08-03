package s3

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/transport"
)

type denyGet struct {
	API
}

func (d denyGet) GetObject(ctx context.Context, params *s3.GetObjectInput, optFns ...func(*s3.Options)) (*s3.GetObjectOutput, error) {
	return nil, fmt.Errorf("AccessDenied: not authorized")
}

func TestGetSignaturePropagatesNonNotFound(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "b", Path: ""}, Options{
		Client: denyGet{API: mem}, StagingDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	_, err = tr.GetSignature(ctx, "f.txt")
	if err == nil || !strings.Contains(err.Error(), "AccessDenied") {
		t.Fatalf("expected AccessDenied, got %v", err)
	}
}

func TestS3ReuseTOCTOUDetectsChange(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "b", Path: ""}, Options{
		Client: mem, StagingDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	tr.reuse = true

	data := []byte("0123456789abcdef")
	ws, err := tr.BeginWrite(ctx, "f.txt", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data); err != nil {
		t.Fatal(err)
	}
	if err := ws.Commit(ctx, chunk.Sum(data), 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	ws2, err := tr.BeginWrite(ctx, "f.txt", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	// Mutate the live object after BeginWrite snapshot.
	_, err = mem.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("b"), Key: aws.String("f.txt"),
		Body: strings.NewReader("xxxxxxxxxxxxxxxx"),
	})
	if err != nil {
		t.Fatal(err)
	}
	err = ws2.ReuseChunk(ctx, 0, 0, chunk.Sum(data), len(data))
	if err == nil || !strings.Contains(err.Error(), "TOCTOU") {
		t.Fatalf("expected TOCTOU, got %v", err)
	}
	_ = ws2.Abort()
}

func TestWriteRangeRejectsPastSessionSize(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "b", Path: ""}, Options{
		Client: mem, StagingDir: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	ws, err := tr.BeginWrite(ctx, "f.txt", 4)
	if err != nil {
		t.Fatal(err)
	}
	err = ws.WriteChunk(ctx, 0, compress.CodecNone, 8, []byte("too-long!"))
	if err == nil || !strings.Contains(err.Error(), "out of bounds") {
		t.Fatalf("expected bounds error, got %v", err)
	}
	_ = ws.Abort()
}

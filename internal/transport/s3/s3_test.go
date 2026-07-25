package s3

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/transport"
)

type failSigPut struct {
	API
}

func (f failSigPut) PutObject(ctx context.Context, params *s3.PutObjectInput, optFns ...func(*s3.Options)) (*s3.PutObjectOutput, error) {
	key := aws.ToString(params.Key)
	if strings.Contains(key, "/.quiksync/sigs/") || strings.HasPrefix(key, ".quiksync/sigs/") {
		return nil, fmt.Errorf("injected sidecar failure")
	}
	return f.API.PutObject(ctx, params, optFns...)
}

func TestS3RoundTripAndSparse(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	ep := transport.Endpoint{Scheme: "s3", Host: "bucket", Path: "pfx"}
	tr, err := New(ctx, ep, Options{Client: mem, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	data := []byte("hello-s3-world-0123456789")
	ws, err := tr.BeginWrite(ctx, "a/file.txt", int64(len(data)))
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

	rc, err := tr.OpenRead(ctx, "a/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(data) {
		t.Fatalf("got %q", got)
	}

	newData := append(append([]byte{}, data...), []byte("-tail")...)
	ws2, err := tr.BeginWrite(ctx, "a/file.txt", int64(len(newData)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws2.ReuseChunk(ctx, 0, 0, dig, len(data)); err != nil {
		t.Fatal(err)
	}
	tail := []byte("-tail")
	if err := ws2.WriteChunk(ctx, uint64(len(data)), compress.CodecNone, len(tail), tail); err != nil {
		t.Fatal(err)
	}
	if err := ws2.Commit(ctx, chunk.Sum(newData), 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	rc2, err := tr.OpenRead(ctx, "a/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	got2, err := io.ReadAll(rc2)
	_ = rc2.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(got2) != string(newData) {
		t.Fatalf("sparse got %q", got2)
	}
}

func TestS3SidecarPutFailure(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	ep := transport.Endpoint{Scheme: "s3", Host: "bucket", Path: "pfx"}
	tr, err := New(ctx, ep, Options{Client: failSigPut{API: mem}, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	data := []byte("sidecar-fail")
	ws, err := tr.BeginWrite(ctx, "x.txt", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data); err != nil {
		t.Fatal(err)
	}
	err = ws.Commit(ctx, chunk.Sum(data), 0o644, time.Now())
	if err == nil || !strings.Contains(err.Error(), "signature sidecar") {
		t.Fatalf("expected sidecar error, got %v", err)
	}
}

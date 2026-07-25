package s3

import (
	"context"
	"fmt"
	"io"
	"os"
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

	if !tr.Caps().SupportsReuseChunk || !strings.Contains(tr.Root(), "bucket") {
		t.Fatalf("caps/root: %+v %q", tr.Caps(), tr.Root())
	}
	_ = tr.MkdirAll(ctx, "a")

	data := []byte("hello-s3-world-0123456789")
	ws, err := tr.BeginWrite(ctx, "a/file.txt", int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(data), data); err != nil {
		t.Fatal(err)
	}
	dig := chunk.Sum(data)
	mod := time.Unix(1_700_000_000, 123)
	if err := ws.Commit(ctx, dig, 0o640, mod); err != nil {
		t.Fatal(err)
	}

	st, err := tr.Stat(ctx, "a/file.txt")
	if err != nil || st.Size != int64(len(data)) || st.Mode != 0o640 {
		t.Fatalf("stat: %+v %v", st, err)
	}
	sig, err := tr.GetSignature(ctx, "a/file.txt")
	if err != nil || sig.Digest != dig || len(sig.Chunks) == 0 {
		t.Fatalf("sig: %+v %v", sig, err)
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

	files, err := tr.Walk(ctx, []string{"skip*"})
	if err != nil || len(files) != 1 || files[0].RelPath != "a/file.txt" {
		t.Fatalf("walk: %v %v", files, err)
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

	if err := tr.Remove(ctx, "a/file.txt"); err != nil {
		t.Fatal(err)
	}
	if _, err := tr.Stat(ctx, "a/file.txt"); err == nil {
		t.Fatal("expected missing after remove")
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

func TestS3AbortAndMissingSig(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	tr, err := New(ctx, transport.Endpoint{Scheme: "s3", Host: "b", Path: ""}, Options{Client: mem, StagingDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	ws, err := tr.BeginWrite(ctx, "tmp.txt", 3)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws.WriteChunk(ctx, 0, compress.CodecNone, 3, []byte("abc"))
	if err := ws.Abort(); err != nil {
		t.Fatal(err)
	}
	sig, err := tr.GetSignature(ctx, "nope.txt")
	if err != nil || sig.Size != 0 {
		t.Fatalf("missing sig: %+v %v", sig, err)
	}
	if tr.Root() != "s3://b" {
		t.Fatalf("root %q", tr.Root())
	}
}

func TestS3NewMissingBucket(t *testing.T) {
	_, err := New(context.Background(), transport.Endpoint{Scheme: "s3"}, Options{Client: NewMemory(), StagingDir: t.TempDir()})
	if err == nil {
		t.Fatal("expected missing bucket")
	}
}

func TestS3CopyObjectMemory(t *testing.T) {
	ctx := context.Background()
	mem := NewMemory()
	_, err := mem.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String("b"), Key: aws.String("src"), Body: strings.NewReader("zz"),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mem.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket: aws.String("b"), Key: aws.String("dst"), CopySource: aws.String("b/src"),
	}); err != nil {
		t.Fatal(err)
	}
	out, err := mem.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("b"), Key: aws.String("dst")})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(out.Body)
	_ = out.Body.Close()
	if string(got) != "zz" {
		t.Fatalf("got %q", got)
	}
}

func TestMatchExclude(t *testing.T) {
	if !matchExclude("vendor/x.go", []string{"vendor/*"}) {
		t.Fatal("expected match")
	}
	if matchExclude("a.txt", []string{"vendor/*"}) {
		t.Fatal("unexpected match")
	}
	_ = os.ErrNotExist
}

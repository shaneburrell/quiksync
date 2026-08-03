package factory

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestOpenFile(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	tr, err := Open(ctx, transport.Endpoint{Scheme: "file", Path: dir, Raw: dir}, transport.OpenOptions{})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	if tr.Root() == "" {
		t.Fatal("empty root")
	}
}

func TestOpenFileDoesNotCreateRootUnlessRequested(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing")
	ep := transport.Endpoint{Scheme: "file", Path: path, Raw: path}
	if _, err := Open(context.Background(), ep, transport.OpenOptions{}); !os.IsNotExist(err) {
		t.Fatalf("got %v, want not-exist", err)
	}
	tr, err := Open(context.Background(), ep, transport.OpenOptions{CreateRoot: true})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
}

func TestOpenS3MissingBucket(t *testing.T) {
	_, err := Open(context.Background(), transport.Endpoint{Scheme: "s3"}, transport.OpenOptions{})
	if err == nil || !strings.Contains(err.Error(), "bucket") {
		t.Fatalf("got %v", err)
	}
}

func TestOpenNFSErrors(t *testing.T) {
	ctx := context.Background()
	if _, err := Open(ctx, transport.Endpoint{Scheme: "nfs", Host: ""}, transport.OpenOptions{}); err == nil {
		t.Fatal("expected missing host")
	}
	if _, err := Open(ctx, transport.Endpoint{Scheme: "nfs", Host: "nas", Port: "9999", Path: "/export"}, transport.OpenOptions{}); err == nil {
		t.Fatal("expected custom port rejection")
	}
}

func TestOpenUnsupported(t *testing.T) {
	_, err := Open(context.Background(), transport.Endpoint{Scheme: "ftp"}, transport.OpenOptions{})
	if err == nil || !strings.Contains(err.Error(), "unsupported") {
		t.Fatalf("got %v", err)
	}
}

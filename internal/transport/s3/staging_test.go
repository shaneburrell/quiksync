package s3

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestS3StagingFromConfigEnv(t *testing.T) {
	cfg := t.TempDir()
	t.Setenv("QUIKSYNC_CONFIG", cfg)
	tr, err := New(context.Background(), transport.Endpoint{Scheme: "s3", Host: "bucket"}, Options{Client: NewMemory()})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()
	want := filepath.Join(cfg, "s3-staging")
	if tr.stagingDir != want {
		t.Fatalf("staging=%q want %q", tr.stagingDir, want)
	}
}

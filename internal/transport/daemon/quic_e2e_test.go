package daemon_test

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
	"github.com/shaneburrell/quiksync/internal/transport/daemon"
)

func TestQUICEngineCopy(t *testing.T) {
	root := t.TempDir()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("quic-e2e-payload"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "n.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const addr = "127.0.0.1:42431"
	errCh := make(chan error, 1)
	go func() {
		errCh <- daemon.Serve(ctx, daemon.ServeConfig{Listen: addr, Root: root})
	}()
	time.Sleep(250 * time.Millisecond)

	destURL := url.URL{Scheme: "quiksync", Host: "127.0.0.1:42431", Path: root}
	dest := destURL.String()
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src,
		Dest:   dest,
		Tune:   autotune.Config{Enabled: false, Compress: compress.CodecLZ4, Streams: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied != 2 {
		t.Fatalf("copied=%d stats=%+v", stats.FilesCopied, stats)
	}

	mismatches, err := engine.Verify(context.Background(), src, root)
	if err != nil {
		t.Fatal(err)
	}
	if len(mismatches) != 0 {
		t.Fatalf("mismatches: %v", mismatches)
	}
	cancel()
}

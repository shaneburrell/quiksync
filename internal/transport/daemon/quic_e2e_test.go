package daemon_test

import (
	"context"
	"net"
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
	t.Setenv("QUIKSYNC_CONFIG", t.TempDir())
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
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	errCh := make(chan error, 1)
	const token = "e2e-test-token"
	cfgDir := t.TempDir()
	go func() {
		errCh <- daemon.Serve(ctx, daemon.ServeConfig{Listen: addr, Root: root, AuthToken: token})
	}()
	time.Sleep(250 * time.Millisecond)

	destURL := url.URL{Scheme: "quiksync", Host: addr, Path: "/"}
	dest := destURL.String()
	stats, err := engine.Run(context.Background(), engine.Config{
		Source:    src,
		Dest:      dest,
		AuthToken: token,
		ConfigDir: cfgDir,
		Resume:    true,
		JobID:     "default",
		Tune:      autotune.Config{Enabled: false, Compress: compress.CodecLZ4, Streams: 2},
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
	if _, err := os.Stat(filepath.Join(cfgDir, "jobs", "default", ".quiksync", "journal", "default.jsonl")); err != nil {
		t.Fatalf("expected remote journal under config dir: %v", err)
	}
	cancel()
}

package relay

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/daemon"
	"github.com/shaneburrell/quiksync/internal/transport/local"
)

type noReuse struct {
	transport.Transport
}

func (n noReuse) Caps() transport.Caps {
	c := n.Transport.Caps()
	c.SupportsReuseChunk = false
	return c
}

func TestRecvReuseAndNoReuse(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	midDir := t.TempDir()
	dstDir := t.TempDir()
	payload := []byte("shared-content-for-relay-reuse-0123456789")
	if err := os.WriteFile(filepath.Join(srcDir, "f.txt"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	// Pre-populate dest with same bytes so reuse can kick in.
	if err := os.WriteFile(filepath.Join(dstDir, "f.txt"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	src, _ := local.New(srcDir)
	mid, _ := local.New(midDir)
	dst, _ := local.New(dstDir)
	if err := Send(ctx, src, mid, SendOptions{JobID: "reuse1", ChunkAvg: 1024}); err != nil {
		t.Fatal(err)
	}
	if err := Recv(ctx, mid, dst, RecvOptions{JobID: "reuse1", Wait: 3 * time.Second}); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(filepath.Join(dstDir, "f.txt"))
	if string(got) != string(payload) {
		t.Fatalf("reuse path got %q", got)
	}

	dst2Dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dst2Dir, "f.txt"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	dst2base, _ := local.New(dst2Dir)
	dst2 := noReuse{Transport: dst2base}
	if err := Recv(ctx, mid, dst2, RecvOptions{JobID: "reuse1", Wait: 3 * time.Second}); err != nil {
		t.Fatal(err)
	}
	got2, _ := os.ReadFile(filepath.Join(dst2Dir, "f.txt"))
	if string(got2) != string(payload) {
		t.Fatalf("no-reuse path got %q", got2)
	}
}

func TestSendSharedChunksAndGC(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	midDir := t.TempDir()
	body := bytesRepeat("CHUNKDATA-", 200)
	_ = os.WriteFile(filepath.Join(srcDir, "a.txt"), body, 0o644)
	_ = os.WriteFile(filepath.Join(srcDir, "b.txt"), body, 0o644)
	src, _ := local.New(srcDir)
	mid, _ := local.New(midDir)
	if err := Send(ctx, src, mid, SendOptions{JobID: "dedup", ChunkAvg: 512}); err != nil {
		t.Fatal(err)
	}
	objDir := filepath.Join(midDir, ".quiksync", "relay", "dedup", "objects")
	entries, err := os.ReadDir(objDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) == 0 {
		t.Fatal("expected objects")
	}
	// Second send should reuse mid objects
	if err := Send(ctx, src, mid, SendOptions{JobID: "dedup", ChunkAvg: 512}); err != nil {
		t.Fatal(err)
	}

	dst, _ := local.New(t.TempDir())
	if err := Recv(ctx, mid, dst, RecvOptions{JobID: "dedup", Wait: 3 * time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := GC(ctx, mid, "dedup", false); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(midDir, ".quiksync", "relay", "dedup", "manifest.json")); !os.IsNotExist(err) {
		t.Fatal("expected gc to remove manifest")
	}
}

func TestGCForceAndExpired(t *testing.T) {
	ctx := context.Background()
	midDir := t.TempDir()
	mid, _ := local.New(midDir)
	job := "force1"
	prefix := filepath.Join(midDir, ".quiksync", "relay", job)
	_ = os.MkdirAll(prefix, 0o755)
	_ = os.WriteFile(filepath.Join(prefix, "lease.json"), []byte(`{"job_id":"force1","expires_at":"2099-01-01T00:00:00Z"}`), 0o644)
	_ = os.WriteFile(filepath.Join(prefix, "manifest.json"), []byte(`{"schema_version":1,"job_id":"force1","files":[]}`), 0o644)
	if err := GC(ctx, mid, job, false); err == nil {
		t.Fatal("expected not acked")
	}
	if err := GC(ctx, mid, job, true); err != nil {
		t.Fatal(err)
	}

	job2 := "expiredgc"
	prefix2 := filepath.Join(midDir, ".quiksync", "relay", job2)
	_ = os.MkdirAll(prefix2, 0o755)
	_ = os.WriteFile(filepath.Join(prefix2, "lease.json"), []byte(`{"job_id":"expiredgc","expires_at":"2000-01-01T00:00:00Z"}`), 0o644)
	_ = os.WriteFile(filepath.Join(prefix2, "manifest.json"), []byte(`{"schema_version":1,"job_id":"expiredgc","files":[]}`), 0o644)
	if err := GC(ctx, mid, job2, false); err != nil {
		t.Fatal(err)
	}
}

func TestQuikSyncSignalRoundTrip(t *testing.T) {
	t.Setenv("QUIKSYNC_CONFIG", t.TempDir())
	root := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	const token = "sig-token"
	go func() { _ = daemon.Serve(ctx, daemon.ServeConfig{Listen: addr, Root: root, AuthToken: token}) }()
	time.Sleep(200 * time.Millisecond)
	host, port, _ := net.SplitHostPort(addr)
	ep := transport.Endpoint{Scheme: "quiksync", Host: host, Port: port}
	sig := &QuikSyncSignal{Endpoint: ep, AuthToken: token}

	waitDone := make(chan error, 1)
	go func() {
		_, err := sig.Wait(ctx, "sigjob")
		waitDone <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if err := sig.Notify(ctx, "sigjob", NotifyMeta{JobID: "sigjob", Generation: 7}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-waitDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("signal wait hung")
	}

	bad := &QuikSyncSignal{Endpoint: transport.Endpoint{Scheme: "ftp"}}
	if err := bad.Notify(ctx, "x", NotifyMeta{}); err == nil || !strings.Contains(err.Error(), "quiksync") {
		t.Fatalf("expected scheme error, got %v", err)
	}
}

func TestCorruptNotifyStillFindsManifest(t *testing.T) {
	ctx := context.Background()
	midDir := t.TempDir()
	dstDir := t.TempDir()
	srcDir := t.TempDir()
	_ = os.WriteFile(filepath.Join(srcDir, "z.txt"), []byte("zz"), 0o644)
	src, _ := local.New(srcDir)
	mid, _ := local.New(midDir)
	dst, _ := local.New(dstDir)
	if err := Send(ctx, src, mid, SendOptions{JobID: "cnotify"}); err != nil {
		t.Fatal(err)
	}
	notifyPath := filepath.Join(midDir, ".quiksync", "relay", "cnotify", "notify")
	_ = os.WriteFile(notifyPath, []byte("not-json"), 0o644)
	if err := Recv(ctx, mid, dst, RecvOptions{JobID: "cnotify", Wait: 3 * time.Second}); err != nil {
		t.Fatal(err)
	}
}

func bytesRepeat(s string, n int) []byte {
	var b []byte
	for i := 0; i < n; i++ {
		b = append(b, s...)
	}
	return b
}

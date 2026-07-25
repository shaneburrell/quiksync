package relay

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/transport/local"
)

func TestSendRecvLocalMid(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	midDir := t.TempDir()
	dstDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "hello.txt"), []byte("relay-hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := local.New(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	mid, err := local.New(midDir)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := local.New(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := Send(ctx, src, mid, SendOptions{JobID: "j1", ChunkAvg: 4 * 1024}); err != nil {
		t.Fatal(err)
	}
	if err := Recv(ctx, mid, dst, RecvOptions{JobID: "j1", Wait: 5 * time.Second}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(dstDir, "hello.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "relay-hello" {
		t.Fatalf("got %q", got)
	}
	if err := GC(ctx, mid, "j1", false); err != nil {
		t.Fatal(err)
	}
}

func TestPoisonObjectRejected(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	midDir := t.TempDir()
	dstDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("abcdef"), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := local.New(srcDir)
	if err != nil {
		t.Fatal(err)
	}
	mid, err := local.New(midDir)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := local.New(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := Send(ctx, src, mid, SendOptions{JobID: "p1"}); err != nil {
		t.Fatal(err)
	}
	objDir := filepath.Join(midDir, ".quiksync", "relay", "p1", "objects")
	entries, err := os.ReadDir(objDir)
	if err != nil {
		t.Fatal(err)
	}
	var objPath string
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		objPath = filepath.Join(objDir, e.Name())
		break
	}
	if objPath == "" {
		t.Fatal("no objects")
	}
	if err := os.WriteFile(objPath, []byte("POISON"), 0o644); err != nil {
		t.Fatal(err)
	}
	err = Recv(ctx, mid, dst, RecvOptions{JobID: "p1", Wait: 2 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "poison") {
		t.Fatalf("expected poison reject, got %v", err)
	}
}

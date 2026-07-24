package daemon

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestQUICRoundTrip(t *testing.T) {
	t.Setenv("QUIKSYNC_CONFIG", t.TempDir())
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "q.txt"), []byte("quic-hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() {
		_ = Serve(ctx2, ServeConfig{Listen: "127.0.0.1:42429", Root: root})
	}()
	time.Sleep(250 * time.Millisecond)

	ep := transport.Endpoint{Scheme: "quiksync", Host: "127.0.0.1", Port: "42429", Path: root}
	client, err := Dial(ctx2, ep)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = client.Close() }()

	files, err := client.Walk(ctx2, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].RelPath != "q.txt" {
		t.Fatalf("files=%v", files)
	}
	rc, err := client.OpenRead(ctx2, "q.txt")
	if err != nil {
		t.Fatal(err)
	}
	all, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		t.Fatal(err)
	}
	if string(all) != "quic-hello" {
		t.Fatalf("got %q", all)
	}
}

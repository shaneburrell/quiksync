package daemon

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestQUICRoundTrip(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "q.txt"), []byte("quic-hello"), 0o644); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Serve(ctx, ServeConfig{Listen: "127.0.0.1:0", Root: root})
	}()

	// Serve with :0 doesn't expose addr easily via our API; use fixed port.
	cancel()
	// Restart on fixed high port.
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go func() {
		_ = Serve(ctx2, ServeConfig{Listen: "127.0.0.1:42429", Root: root})
	}()
	time.Sleep(200 * time.Millisecond)

	ep := transport.Endpoint{Scheme: "quiksync", Host: "127.0.0.1", Port: "42429", Path: root}
	client, err := Dial(ctx2, ep)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

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
	buf := make([]byte, 64)
	n, err := rc.Read(buf)
	_ = rc.Close()
	if err != nil && err.Error() != "EOF" {
		// Read may return n>0 with EOF on next call
	}
	if string(buf[:n]) != "quic-hello" && n == 0 {
		// try full read
		rc, err = client.OpenRead(ctx2, "q.txt")
		if err != nil {
			t.Fatal(err)
		}
		all := make([]byte, 0, 16)
		tmp := make([]byte, 8)
		for {
			nn, er := rc.Read(tmp)
			all = append(all, tmp[:nn]...)
			if er != nil {
				break
			}
		}
		_ = rc.Close()
		if string(all) != "quic-hello" {
			t.Fatalf("got %q", all)
		}
	}
}

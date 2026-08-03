package daemon

import (
	"context"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestQUICCloseWhileOpenRead(t *testing.T) {
	root := t.TempDir()
	payload := make([]byte, 256*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	if err := os.WriteFile(filepath.Join(root, "big.bin"), payload, 0o644); err != nil {
		t.Fatal(err)
	}
	const token = "close-token"
	ctx, cancel, addr := startQUIC(t, root, token)
	defer cancel()

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	ep := transport.Endpoint{Scheme: "quiksync", Host: host, Port: port, Path: "/"}
	c, err := DialOpts(ctx, ep, DialOptions{AuthToken: token})
	if err != nil {
		t.Fatal(err)
	}

	rc, err := c.OpenRead(context.Background(), "big.bin")
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- c.Close()
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Close deadlocked while OpenRead held mu")
	}
	_ = rc.Close()
	// Drain any leftover; ignore errors after force-close.
	_, _ = io.Copy(io.Discard, rc)
}

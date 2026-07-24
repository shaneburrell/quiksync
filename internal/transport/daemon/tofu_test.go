package daemon_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/daemon"
)

func TestTOFUPinMismatch(t *testing.T) {
	cfg := t.TempDir()
	t.Setenv("QUIKSYNC_CONFIG", cfg)
	root := t.TempDir()

	ctx1, cancel1 := context.WithCancel(context.Background())
	errCh := make(chan error, 2)
	const addr1 = "127.0.0.1:42441"
	go func() {
		errCh <- daemon.Serve(ctx1, daemon.ServeConfig{Listen: addr1, Root: root, AllowNoAuth: true})
	}()
	time.Sleep(300 * time.Millisecond)

	ep1, err := transport.ParseEndpoint("quiksync://" + addr1 + "/")
	if err != nil {
		t.Fatal(err)
	}
	c, err := daemon.DialOpts(context.Background(), ep1, daemon.DialOptions{})
	if err != nil {
		cancel1()
		t.Fatal(err)
	}
	_ = c.Close()
	cancel1()
	select {
	case <-errCh:
	case <-time.After(3 * time.Second):
	}
	time.Sleep(100 * time.Millisecond)

	// New cert on a new listen address; keep the old pin keyed by host:port.
	// Reuse same address so pin path matches; regenerate server identity.
	_ = os.Remove(filepath.Join(cfg, "daemon.crt"))
	_ = os.Remove(filepath.Join(cfg, "daemon.key"))

	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	const addr2 = "127.0.0.1:42442"
	go func() {
		errCh <- daemon.Serve(ctx2, daemon.ServeConfig{Listen: addr2, Root: root, AllowNoAuth: true})
	}()
	time.Sleep(300 * time.Millisecond)

	// Copy pin from addr1 to addr2 name so we simulate same host identity change.
	pin1 := filepath.Join(cfg, "pins", "127.0.0.1_42441.pin")
	pin2 := filepath.Join(cfg, "pins", "127.0.0.1_42442.pin")
	b, err := os.ReadFile(pin1)
	if err != nil {
		t.Fatalf("read pin: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(pin2), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pin2, b, 0o644); err != nil {
		t.Fatal(err)
	}

	ep2, err := transport.ParseEndpoint("quiksync://" + addr2 + "/")
	if err != nil {
		t.Fatal(err)
	}
	_, err = daemon.DialOpts(context.Background(), ep2, daemon.DialOptions{})
	if err == nil {
		t.Fatal("expected TOFU pin mismatch")
	}
	c2, err := daemon.DialOpts(context.Background(), ep2, daemon.DialOptions{Insecure: true})
	if err != nil {
		t.Fatalf("insecure dial: %v", err)
	}
	_ = c2.Close()
}

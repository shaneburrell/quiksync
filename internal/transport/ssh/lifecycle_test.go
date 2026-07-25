package ssh_test

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
	sshxfer "github.com/shaneburrell/quiksync/internal/transport/ssh"
)

func setupFakeSSH(t *testing.T) (remoteRoot string, restore func()) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("shell fake ssh script")
	}
	remoteRoot = t.TempDir()
	bin := filepath.Join(t.TempDir(), "quiksync")
	modRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", bin, "./cmd/quiksync")
	build.Dir = modRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	fake := filepath.Join(t.TempDir(), "ssh")
	script := "#!/bin/sh\n" +
		"while [ \"$#\" -gt 0 ] && [ \"$1\" != \"quiksync\" ]; do shift; done\n" +
		"shift\n" +
		"exec \"" + bin + "\" \"$@\"\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	old := sshxfer.Command
	sshxfer.Command = fake
	return remoteRoot, func() { sshxfer.Command = old }
}

func TestSSHLifecycleReuseRelay(t *testing.T) {
	remoteRoot, restore := setupFakeSSH(t)
	defer restore()
	ctx := context.Background()
	ep := transport.Endpoint{Scheme: "ssh", Host: "test@localhost", Path: remoteRoot, Raw: "ssh://test@localhost" + remoteRoot}
	tr, err := sshxfer.New(ctx, ep)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	if err := tr.MkdirAll(ctx, "d"); err != nil {
		t.Fatal(err)
	}
	base := []byte("ssh-reuse-base-0123456789")
	ws, err := tr.BeginWrite(ctx, "d/f.bin", int64(len(base)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws.WriteChunk(ctx, 0, compress.CodecNone, len(base), base); err != nil {
		t.Fatal(err)
	}
	dig := chunk.Sum(base)
	if err := ws.Commit(ctx, dig, 0o644, time.Now()); err != nil {
		t.Fatal(err)
	}

	st, err := tr.Stat(ctx, "d/f.bin")
	if err != nil || st.Size != int64(len(base)) {
		t.Fatalf("stat %+v %v", st, err)
	}
	sig, err := tr.GetSignature(ctx, "d/f.bin")
	if err != nil || sig.Digest != dig {
		t.Fatalf("sig %+v %v", sig, err)
	}
	rc, err := tr.OpenRead(ctx, "d/f.bin")
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil || string(got) != string(base) {
		t.Fatalf("read %q %v", got, err)
	}

	next := append(append([]byte{}, base...), []byte("!!")...)
	ws2, err := tr.BeginWrite(ctx, "d/f.bin", int64(len(next)))
	if err != nil {
		t.Fatal(err)
	}
	if err := ws2.ReuseChunk(ctx, 0, 0, dig, len(base)); err != nil {
		t.Fatal(err)
	}
	if err := ws2.WriteChunk(ctx, uint64(len(base)), compress.CodecNone, 2, []byte("!!")); err != nil {
		t.Fatal(err)
	}
	if err := ws2.Commit(ctx, chunk.Sum(next), 0o600, time.Now()); err != nil {
		t.Fatal(err)
	}

	ws3, err := tr.BeginWrite(ctx, "d/abort.txt", 3)
	if err != nil {
		t.Fatal(err)
	}
	_ = ws3.WriteChunk(ctx, 0, compress.CodecNone, 3, []byte("abc"))
	if err := ws3.Abort(); err != nil {
		t.Fatal(err)
	}

	if err := tr.Remove(ctx, "d/f.bin"); err != nil {
		t.Fatal(err)
	}

	// Each fake-ssh exec is a separate process, so waiters are not shared.
	// Notify still exercises the client RPC path (ack-only wake).
	if err := tr.RelayNotify(ctx, protocol.RelayNotifyMeta{JobID: "ssh-relay"}); err != nil {
		t.Fatal(err)
	}
}

func TestSSHDialBadVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fake ssh")
	}
	fake := filepath.Join(t.TempDir(), "ssh")
	script := "#!/bin/sh\nexit 1\n"
	if err := os.WriteFile(fake, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	old := sshxfer.Command
	sshxfer.Command = fake
	defer func() { sshxfer.Command = old }()
	_, err := sshxfer.New(context.Background(), transport.Endpoint{Scheme: "ssh", Host: "h", Path: "/tmp"})
	if err == nil {
		t.Fatal("expected dial failure")
	}
}

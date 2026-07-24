package ssh_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
	sshxfer "github.com/shaneburrell/quiksync/internal/transport/ssh"
)

func TestFakeSSHCopy(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("shell fake ssh script")
	}
	remoteRoot := t.TempDir()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "via-ssh.txt"), []byte("ssh-payload"), 0o644); err != nil {
		t.Fatal(err)
	}

	bin := filepath.Join(t.TempDir(), "quiksync")
	modRoot, err := filepath.Abs(filepath.Join("..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", bin, "./cmd/quiksync")
	build.Dir = modRoot
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build quiksync: %v\n%s", err, out)
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
	defer func() { sshxfer.Command = old }()

	dest := "ssh://test@localhost" + remoteRoot
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src,
		Dest:   dest,
		Tune:   autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied != 1 {
		t.Fatalf("copied=%d", stats.FilesCopied)
	}
	got, err := os.ReadFile(filepath.Join(remoteRoot, "via-ssh.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "ssh-payload" {
		t.Fatalf("got %q", got)
	}

	// Exercise walk/stat/read/remove via a second engine sync with delete.
	if err := os.WriteFile(filepath.Join(remoteRoot, "extra.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	stats2, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dest, SyncMode: true, Delete: true,
		Tune: autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats2.FilesDeleted != 1 {
		t.Fatalf("deleted=%d", stats2.FilesDeleted)
	}
}

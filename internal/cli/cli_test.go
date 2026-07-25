package cli_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/shaneburrell/quiksync/internal/cli"
)

func TestCLIHelpAndVersion(t *testing.T) {
	if err := cli.ExecuteArgs([]string{"--help"}); err != nil {
		t.Fatal(err)
	}
	if err := cli.ExecuteArgs([]string{"--version"}); err != nil {
		t.Fatal(err)
	}
}

func TestCLICopyDryRunExclude(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	mustWrite(t, filepath.Join(src, "vendor", "b.go"), "b")

	err := cli.ExecuteArgs([]string{
		"copy", src, dst, "--dry-run", "--exclude", "vendor/*", "--auto=false", "--streams=1", "--compress=none",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "a.txt")); !os.IsNotExist(err) {
		t.Fatal("dry-run created file")
	}

	err = cli.ExecuteArgs([]string{
		"copy", src, dst, "--exclude", "vendor/*", "--auto=false", "--streams=1", "--compress=none",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "a.txt")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "vendor", "b.go")); !os.IsNotExist(err) {
		t.Fatal("exclude failed")
	}
}

func TestCLISyncDeleteVerify(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(src, "keep.txt"), "keep")
	mustWrite(t, filepath.Join(dst, "gone.txt"), "gone")

	if err := cli.ExecuteArgs([]string{
		"sync", src, dst, "--delete", "--auto=false", "--streams=1", "--compress=none",
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dst, "gone.txt")); !os.IsNotExist(err) {
		t.Fatal("delete failed")
	}
	if err := cli.ExecuteArgs([]string{"verify", src, dst}); err != nil {
		t.Fatal(err)
	}
}

func TestCLIInvalidCompress(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	err := cli.ExecuteArgs([]string{"copy", src, dst, "--compress=gzip"})
	if err == nil {
		t.Fatal("expected invalid compress error")
	}
}

func TestCLISendRecvLocal(t *testing.T) {
	src, mid, dst := t.TempDir(), t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(src, "hello.txt"), "relay-cli")
	if err := cli.ExecuteArgs([]string{
		"send", src, "--via", mid, "--job-id", "cli1",
	}); err != nil {
		t.Fatal(err)
	}
	if err := cli.ExecuteArgs([]string{
		"recv", "--via", mid, dst, "--job-id", "cli1", "--wait", "5s",
	}); err != nil {
		t.Fatal(err)
	}
	if err := cli.ExecuteArgs([]string{"verify", src, dst}); err != nil {
		t.Fatal(err)
	}
	if err := cli.ExecuteArgs([]string{
		"relay", "gc", "--via", mid, "--job-id", "cli1",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestCLISendRequiresVia(t *testing.T) {
	err := cli.ExecuteArgs([]string{"send", t.TempDir()})
	if err == nil {
		t.Fatal("expected --via required")
	}
}

func mustWrite(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o644); err != nil {
		t.Fatal(err)
	}
}

//go:build unix

package transport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenConfinedOpenatInRootSymlink(t *testing.T) {
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "real.txt"), []byte("ok"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real.txt", filepath.Join(root, "link.txt")); err != nil {
		t.Skipf("symlink: %v", err)
	}
	f, err := OpenConfined(root, "link.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()
	b := make([]byte, 8)
	n, err := f.Read(b)
	if err != nil || string(b[:n]) != "ok" {
		t.Fatalf("read: %q %v", b[:n], err)
	}
}

func TestOpenConfinedOpenatRejectsEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret"), filepath.Join(root, "escape")); err != nil {
		t.Skipf("symlink: %v", err)
	}
	if _, err := OpenConfined(root, "escape"); err == nil {
		t.Fatal("expected escape rejection")
	}
}

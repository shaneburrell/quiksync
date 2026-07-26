package transport

import (
	"os"
	"path/filepath"
	"testing"
)

func TestSafeJoinRejectsEscape(t *testing.T) {
	root := t.TempDir()
	if _, err := SafeJoin(root, "../etc/passwd"); err == nil {
		t.Fatal("expected escape rejection")
	}
	if _, err := SafeJoin(root, "/etc/passwd"); err == nil {
		t.Fatal("expected absolute rejection")
	}
	if _, err := SafeJoin(root, "ok/file.txt"); err != nil {
		t.Fatal(err)
	}
	got, err := SafeJoin(root, "a/../b/c")
	if err != nil {
		t.Fatal(err)
	}
	want, _ := filepath.Abs(filepath.Join(root, "b", "c"))
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
}

func TestSafeJoinFileRejectsEmpty(t *testing.T) {
	root := t.TempDir()
	for _, rel := range []string{"", ".", "./"} {
		if _, err := SafeJoinFile(root, rel); err == nil {
			t.Fatalf("expected rejection for %q", rel)
		}
	}
}

func TestConfineRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "leak")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("symlink: %v", err)
	}
	if _, err := Confine(root, "leak/secret.txt"); err == nil {
		t.Fatal("expected symlink escape rejection")
	}
	// In-root file still works.
	if err := os.WriteFile(filepath.Join(root, "ok.txt"), []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Confine(root, "ok.txt"); err != nil {
		t.Fatal(err)
	}
}

func TestOpenConfinedRejectsSymlinkEscape(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("secret"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "ok.txt"), []byte("in"), 0o644); err != nil {
		t.Fatal(err)
	}
	f, err := OpenConfined(root, "ok.txt")
	if err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	link := filepath.Join(root, "leak")
	if err := os.Symlink(secret, link); err != nil {
		t.Skipf("symlink: %v", err)
	}
	if _, err := OpenConfined(root, "leak"); err == nil {
		t.Fatal("expected OpenConfined to reject symlink escape")
	}
}

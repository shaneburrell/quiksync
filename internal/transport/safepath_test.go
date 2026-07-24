package transport

import (
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

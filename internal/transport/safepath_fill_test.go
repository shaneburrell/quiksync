package transport

import (
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

func TestSafeJoinEmptyRootAndDrive(t *testing.T) {
	root := t.TempDir()
	got, err := SafeJoin(root, "")
	if err != nil {
		t.Fatal(err)
	}
	abs, _ := filepath.Abs(root)
	if got != abs {
		t.Fatalf("empty rel: %s want %s", got, abs)
	}
	if _, err := SafeJoin(root, "C:/windows"); err == nil {
		t.Fatal("expected drive rejection")
	}
	if _, err := SafeJoinFile(root, "ok/../."); err == nil {
		t.Fatal("expected root-identity rejection")
	}
}

func TestConfineMissingPathUnderRoot(t *testing.T) {
	root := t.TempDir()
	got, err := Confine(root, "new/nested/file.txt")
	if err != nil {
		t.Fatal(err)
	}
	absRoot, _ := filepath.Abs(root)
	if r, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = r
	}
	want := filepath.Join(absRoot, "new", "nested", "file.txt")
	if got != want {
		t.Fatalf("got %s want %s", got, want)
	}
	if runtime.GOOS == "windows" {
		return
	}
	// Symlink to file inside root still allowed.
	inner := filepath.Join(root, "inner.txt")
	if err := os.WriteFile(inner, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(root, "link.txt")
	if err := os.Symlink(inner, link); err != nil {
		t.Skip(err)
	}
	if _, err := Confine(root, "link.txt"); err != nil {
		t.Fatal(err)
	}
}

package fsmeta

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMatchExcludeWindowsStyle(t *testing.T) {
	if !matchExclude(`vendor\x.go`, []string{"vendor/*"}) {
		t.Fatal("expected ToSlash match for windows-style rel")
	}
	if matchExclude(`keep.txt`, []string{"vendor/*"}) {
		t.Fatal("keep should not match")
	}
}

func TestWalkExcludeAndSkipQuiksync(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "keep.txt"), "ok")
	mustWrite(t, filepath.Join(root, "vendor", "x.go"), "skip")
	mustWrite(t, filepath.Join(root, ".quiksync", "journal", "x"), "internal")
	mustWrite(t, filepath.Join(root, "logs", "app.log"), "log")

	files, err := Walk(root, []string{"vendor/*", "*.log"})
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 || files[0].RelPath != "keep.txt" {
		t.Fatalf("files=%v", files)
	}
}

func TestUnchangedForAndGeneration(t *testing.T) {
	fi := FileInfo{ModTime: time.Now().Add(-2 * time.Hour), Size: 10}
	if !UnchangedFor(fi, time.Hour) {
		t.Fatal("expected stable")
	}
	fi.ModTime = time.Now()
	if UnchangedFor(fi, time.Hour) {
		t.Fatal("expected unstable")
	}
	g := GenOf(fi)
	if g.Size != 10 || g.ModNano == 0 {
		t.Fatalf("%+v", g)
	}
	path := filepath.Join(t.TempDir(), "f")
	mustWrite(t, path, "hi")
	sg, err := StatGeneration(path)
	if err != nil || sg.Size != 2 {
		t.Fatalf("%+v %v", sg, err)
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

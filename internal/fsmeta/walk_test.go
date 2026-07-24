package fsmeta

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

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

func TestUnchangedFor(t *testing.T) {
	fi := FileInfo{ModTime: time.Now().Add(-2 * time.Hour)}
	if !UnchangedFor(fi, time.Hour) {
		t.Fatal("expected stable")
	}
	fi.ModTime = time.Now()
	if UnchangedFor(fi, time.Hour) {
		t.Fatal("expected unstable")
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

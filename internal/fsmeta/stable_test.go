package fsmeta

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestUnchangedForAndStatGeneration(t *testing.T) {
	if !UnchangedFor(FileInfo{ModTime: time.Now()}, 0) {
		t.Fatal("zero window always unchanged")
	}
	if UnchangedFor(FileInfo{ModTime: time.Now()}, time.Hour) {
		t.Fatal("fresh file should be unstable")
	}
	if !UnchangedFor(FileInfo{ModTime: time.Now().Add(-2 * time.Hour)}, time.Hour) {
		t.Fatal("old file should be stable")
	}
	p := filepath.Join(t.TempDir(), "f.txt")
	if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	g, err := StatGeneration(p)
	if err != nil || g.Size != 1 {
		t.Fatalf("%+v %v", g, err)
	}
	if _, err := StatGeneration(filepath.Join(t.TempDir(), "missing")); err == nil {
		t.Fatal("expected missing")
	}
}

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
	if len(files) != 2 || files[0].RelPath != "keep.txt" || files[1].RelPath != "logs" || !files[1].IsDir {
		t.Fatalf("files=%v", files)
	}
}

func TestWalkSkipsNestedQuiksyncTmp(t *testing.T) {
	root := t.TempDir()
	mustWrite(t, filepath.Join(root, "a", "real.txt"), "keep")
	mustWrite(t, filepath.Join(root, "a", ".quiksync.tmp", "qs-abc.partial"), "staging")
	mustWrite(t, filepath.Join(root, "b", "c", ".quiksync.tmp", "qs-def.partial"), "staging")
	mustWrite(t, filepath.Join(root, "b", "c", ".quiksync", "index"), "internal")
	mustWrite(t, filepath.Join(root, "b", "c", "ok.txt"), "keep")

	files, err := Walk(root, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range files {
		got[f.RelPath] = true
	}
	if !got["a/real.txt"] || !got["b/c/ok.txt"] {
		t.Fatalf("missing expected files: %v", got)
	}
	if len(files) != 5 {
		t.Fatalf("want 2 files and 3 directories, got %v", got)
	}
	for p := range got {
		if filepath.Base(filepath.Dir(p)) == ".quiksync.tmp" || filepath.Base(filepath.Dir(p)) == ".quiksync" {
			t.Fatalf("leaked internal path %q", p)
		}
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

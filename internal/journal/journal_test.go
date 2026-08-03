package journal

import (
	"os"
	"path/filepath"
	"testing"
)

func TestJournalPutGetReload(t *testing.T) {
	root := t.TempDir()
	j, err := Open(root, "resume")
	if err != nil {
		t.Fatal(err)
	}
	e := Entry{
		JobID: "resume", RelPath: "a.txt", Status: StatusComplete,
		SrcSize: 10, SrcModNano: 123, SrcDigest: "abc", ChunksDone: 2,
	}
	if err := j.Put(e); err != nil {
		t.Fatal(err)
	}
	got, ok := j.Get("a.txt")
	if !ok || got.Status != StatusComplete || got.SrcSize != 10 {
		t.Fatalf("get: %+v ok=%v", got, ok)
	}
	if !j.Completed("a.txt") {
		t.Fatal("expected completed")
	}

	j2, err := Open(root, "resume")
	if err != nil {
		t.Fatal(err)
	}
	got2, ok := j2.Get("a.txt")
	if !ok || got2.Status != StatusComplete {
		t.Fatalf("reload: %+v", got2)
	}
	if _, err := filepath.Glob(filepath.Join(root, ".quiksync", "journal", "*.jsonl")); err != nil {
		t.Fatal(err)
	}
}

func TestJournalPutMapOnlyAfterDurableWrite(t *testing.T) {
	root := t.TempDir()
	j, err := Open(root, "dur")
	if err != nil {
		t.Fatal(err)
	}
	// Make the journal path a directory so OpenFile fails.
	j.path = filepath.Join(root, "not-a-file")
	if err := os.MkdirAll(j.path, 0o755); err != nil {
		t.Fatal(err)
	}
	err = j.Put(Entry{JobID: "dur", RelPath: "x.txt", Status: StatusComplete})
	if err == nil {
		t.Fatal("expected put failure")
	}
	if _, ok := j.Get("x.txt"); ok {
		t.Fatal("in-memory map must not update when append fails")
	}
}

func TestJournalReloadReportsCorruptLines(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, ".quiksync", "journal", "resume.jsonl")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	valid := `{"job_id":"resume","rel_path":"ok.txt","status":"complete"}` + "\n"
	if err := os.WriteFile(path, []byte(valid+"not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	j, err := Open(root, "resume")
	if err != nil {
		t.Fatal(err)
	}
	if j.CorruptLines != 1 || !j.Completed("ok.txt") {
		t.Fatalf("corrupt=%d completed=%v", j.CorruptLines, j.Completed("ok.txt"))
	}

	if err := os.WriteFile(path, []byte("not json\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(root, "resume"); err == nil {
		t.Fatal("expected all-corrupt journal error")
	}
}

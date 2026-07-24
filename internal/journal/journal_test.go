package journal

import (
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

package relay

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/transport/local"
)

func TestRecvRequiresLease(t *testing.T) {
	ctx := context.Background()
	midDir := t.TempDir()
	dstDir := t.TempDir()
	mid, err := local.New(midDir)
	if err != nil {
		t.Fatal(err)
	}
	dst, err := local.New(dstDir)
	if err != nil {
		t.Fatal(err)
	}
	job := "nolease"
	prefix := filepath.Join(midDir, ".quiksync", "relay", job)
	if err := os.MkdirAll(prefix, 0o755); err != nil {
		t.Fatal(err)
	}
	man := Manifest{SchemaVersion: schemaVersion, JobID: job}
	mb, _ := json.Marshal(man)
	if err := os.WriteFile(filepath.Join(prefix, "manifest.json"), mb, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prefix, "notify"), []byte(`{"job_id":"nolease"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	err = Recv(ctx, mid, dst, RecvOptions{JobID: job, Wait: 2 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "lease") {
		t.Fatalf("expected missing lease error, got %v", err)
	}
}

func TestValidateManifestChunkLengthCap(t *testing.T) {
	err := validateManifest("j", &Manifest{
		SchemaVersion: schemaVersion,
		JobID:         "j",
		Files: []ManifestFile{{
			RelPath: "a.txt",
			Size:    int64(maxChunkLength) + 1,
			Chunks:  []ManifestChunk{{Offset: 0, Length: maxChunkLength + 1}},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "exceeds max") {
		t.Fatalf("expected length cap error, got %v", err)
	}
}

func TestReadObjectRejectsOversize(t *testing.T) {
	ctx := context.Background()
	midDir := t.TempDir()
	mid, err := local.New(midDir)
	if err != nil {
		t.Fatal(err)
	}
	data := []byte("hello-oversize-object")
	rel := "obj.bin"
	if err := putBytes(ctx, mid, rel, data); err != nil {
		t.Fatal(err)
	}
	want := chunk.Sum(data)
	// Cap below actual size.
	_, err = readObject(ctx, mid, rel, want, 4)
	if err == nil || !strings.Contains(err.Error(), "size") {
		t.Fatalf("expected size mismatch, got %v", err)
	}
}

func TestGetJSONRejectsOversize(t *testing.T) {
	ctx := context.Background()
	midDir := t.TempDir()
	mid, err := local.New(midDir)
	if err != nil {
		t.Fatal(err)
	}
	big := strings.Repeat("x", 100)
	if err := putBytes(ctx, mid, "big.json", []byte(`{"job_id":"`+big+`"}`)); err != nil {
		t.Fatal(err)
	}
	var meta NotifyMeta
	err = getJSONLimited(ctx, mid, "big.json", &meta, 16)
	if err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("expected oversized json error, got %v", err)
	}
}

func TestRecvPartialFailureLeavesDestClean(t *testing.T) {
	ctx := context.Background()
	srcDir := t.TempDir()
	midDir := t.TempDir()
	dstDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(srcDir, "a.txt"), []byte("aaa"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(srcDir, "b.txt"), []byte("bbb"), 0o644); err != nil {
		t.Fatal(err)
	}
	src, _ := local.New(srcDir)
	mid, _ := local.New(midDir)
	dst, _ := local.New(dstDir)
	if err := Send(ctx, src, mid, SendOptions{JobID: "partial1"}); err != nil {
		t.Fatal(err)
	}
	// Poison the second object's bytes so stage fails after first file stages.
	objDir := filepath.Join(midDir, ".quiksync", "relay", "partial1", "objects")
	entries, err := os.ReadDir(objDir)
	if err != nil {
		t.Fatal(err)
	}
	// Corrupt every object so materialize fails; dest finals must stay empty.
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		_ = os.WriteFile(filepath.Join(objDir, e.Name()), []byte("POISON"), 0o644)
	}
	err = Recv(ctx, mid, dst, RecvOptions{JobID: "partial1", Wait: 2 * time.Second})
	if err == nil {
		t.Fatal("expected recv failure")
	}
	if _, err := os.Stat(filepath.Join(dstDir, "a.txt")); !os.IsNotExist(err) {
		t.Fatalf("a.txt should not be promoted on failure, stat=%v", err)
	}
	if _, err := os.Stat(filepath.Join(dstDir, "b.txt")); !os.IsNotExist(err) {
		t.Fatalf("b.txt should not be promoted on failure, stat=%v", err)
	}
}

func TestMaxChunkLengthMatchesCompressCap(t *testing.T) {
	if int(maxChunkLength) != compress.MaxUncompressedChunk {
		t.Fatalf("maxChunkLength %d != MaxUncompressedChunk %d", maxChunkLength, compress.MaxUncompressedChunk)
	}
}

func TestPromoteStagedRejectsInvalidContent(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	dest, err := local.New(root)
	if err != nil {
		t.Fatal(err)
	}
	f := ManifestFile{RelPath: "final.txt", Size: 3, Digest: chunk.Sum([]byte("abc")), Mode: 0o640}
	if err := promoteStaged(ctx, dest, "missing", f); err == nil {
		t.Fatal("expected missing staged file error")
	}
	if err := os.WriteFile(filepath.Join(root, "short"), []byte("ab"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := promoteStaged(ctx, dest, "short", f); err == nil || !strings.Contains(err.Error(), "staged size") {
		t.Fatalf("expected staged size error, got %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "bad"), []byte("xyz"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := promoteStaged(ctx, dest, "bad", f); err == nil || !strings.Contains(err.Error(), "digest mismatch") {
		t.Fatalf("expected staged digest error, got %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "good"), []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := promoteStaged(ctx, dest, "good", f); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(root, "final.txt"))
	if err != nil || string(got) != "abc" {
		t.Fatalf("promoted=%q err=%v", got, err)
	}
}

package relay

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/transport/local"
)

func TestRejectBadJobID(t *testing.T) {
	ctx := context.Background()
	src, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	mid, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	err = Send(ctx, src, mid, SendOptions{JobID: "../escape"})
	if err == nil || !strings.Contains(err.Error(), "invalid job id") {
		t.Fatalf("expected invalid job id, got %v", err)
	}
	err = Recv(ctx, mid, src, RecvOptions{JobID: "a/b", Wait: time.Second})
	if err == nil || !strings.Contains(err.Error(), "invalid job id") {
		t.Fatalf("expected invalid job id on recv, got %v", err)
	}
	err = GC(ctx, mid, "..", true)
	if err == nil || !strings.Contains(err.Error(), "invalid job id") {
		t.Fatalf("expected invalid job id on gc, got %v", err)
	}
}

func TestRejectMalformedManifest(t *testing.T) {
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
	job := "badman"
	prefix := filepath.Join(midDir, ".quiksync", "relay", job)
	if err := os.MkdirAll(prefix, 0o755); err != nil {
		t.Fatal(err)
	}
	man := Manifest{
		SchemaVersion: schemaVersion,
		JobID:         job,
		CreatedAt:     time.Now().UTC(),
		Files: []ManifestFile{{
			RelPath: "../etc/passwd",
			Size:    0,
		}},
	}
	body, _ := json.Marshal(man)
	if err := os.WriteFile(filepath.Join(prefix, "manifest.json"), body, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prefix, "notify"), []byte(`{"job_id":"badman"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	err = Recv(ctx, mid, dst, RecvOptions{JobID: job, Wait: 2 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "invalid manifest") {
		t.Fatalf("expected invalid manifest, got %v", err)
	}
}

func TestValidateManifestChunkCoverage(t *testing.T) {
	err := validateManifest("j", &Manifest{
		SchemaVersion: schemaVersion,
		JobID:         "j",
		Files: []ManifestFile{{
			RelPath: "a.txt",
			Size:    10,
			Chunks:  []ManifestChunk{{Offset: 0, Length: 5}},
		}},
	})
	if err == nil || !strings.Contains(err.Error(), "cover") {
		t.Fatalf("expected coverage error, got %v", err)
	}
}

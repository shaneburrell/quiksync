package relay

import (
	"context"
	"encoding/json"
	"fmt"
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
	lease := Lease{JobID: job, ExpiresAt: time.Now().UTC().Add(time.Hour)}
	lb, _ := json.Marshal(lease)
	if err := os.WriteFile(filepath.Join(prefix, "lease.json"), lb, 0o644); err != nil {
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

func TestIsAbsent(t *testing.T) {
	if !isAbsent(os.ErrNotExist) {
		t.Fatal("ErrNotExist")
	}
	if isAbsent(nil) {
		t.Fatal("nil")
	}
	if !isAbsent(fmt.Errorf("NotFound: key")) {
		t.Fatal("NotFound string")
	}
}

func TestStoreSignalWaitAndPoll(t *testing.T) {
	ctx := context.Background()
	midDir := t.TempDir()
	mid, err := local.New(midDir)
	if err != nil {
		t.Fatal(err)
	}
	sig := StoreSignal{Mid: mid}
	waitCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := sig.Wait(waitCtx, "w1")
		done <- err
	}()
	time.Sleep(100 * time.Millisecond)
	if err := sig.Notify(ctx, "w1", NotifyMeta{JobID: "w1"}); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("wait timeout")
	}
}

type instantSignal struct{}

func (instantSignal) Notify(ctx context.Context, jobID string, meta NotifyMeta) error {
	return nil
}
func (instantSignal) Wait(ctx context.Context, jobID string) (NotifyMeta, error) {
	return NotifyMeta{JobID: jobID}, nil
}

func TestWaitForJobSignalWake(t *testing.T) {
	ctx := context.Background()
	midDir := t.TempDir()
	mid, err := local.New(midDir)
	if err != nil {
		t.Fatal(err)
	}
	// Publish after a short delay while Wait races the instant signal.
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = StoreSignal{Mid: mid}.Notify(ctx, "sig1", NotifyMeta{JobID: "sig1"})
		man := Manifest{SchemaVersion: schemaVersion, JobID: "sig1"}
		body, _ := json.Marshal(man)
		_ = putBytes(ctx, mid, paths("sig1").manifest(), body)
	}()
	waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if err := waitForJob(waitCtx, mid, "sig1", instantSignal{}); err != nil {
		t.Fatal(err)
	}
}

func TestRecvExpiredLease(t *testing.T) {
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
	job := "expired"
	prefix := filepath.Join(midDir, ".quiksync", "relay", job)
	if err := os.MkdirAll(prefix, 0o755); err != nil {
		t.Fatal(err)
	}
	lease := Lease{
		JobID: job, ExpiresAt: time.Now().UTC().Add(-time.Hour),
	}
	lb, _ := json.Marshal(lease)
	if err := os.WriteFile(filepath.Join(prefix, "lease.json"), lb, 0o644); err != nil {
		t.Fatal(err)
	}
	man := Manifest{SchemaVersion: schemaVersion, JobID: job, Files: nil}
	mb, _ := json.Marshal(man)
	if err := os.WriteFile(filepath.Join(prefix, "manifest.json"), mb, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(prefix, "notify"), []byte(`{}`), 0o644); err != nil {
		t.Fatal(err)
	}
	err = Recv(ctx, mid, dst, RecvOptions{JobID: job, Wait: 2 * time.Second})
	if err == nil || !strings.Contains(err.Error(), "expired") {
		t.Fatalf("expected expired lease, got %v", err)
	}
}

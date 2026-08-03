package relay

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/daemon"
	"github.com/shaneburrell/quiksync/internal/transport/local"
)

func TestQuikSyncSignalWaitErrors(t *testing.T) {
	bad := &QuikSyncSignal{Endpoint: transport.Endpoint{Scheme: "http"}}
	if _, err := bad.Wait(context.Background(), "j"); err == nil {
		t.Fatal("expected scheme error on Wait")
	}

	root := t.TempDir()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = daemon.Serve(ctx, daemon.ServeConfig{Listen: addr, Root: root, AuthToken: "good"}) }()
	time.Sleep(150 * time.Millisecond)
	host, port, _ := net.SplitHostPort(addr)
	sig := &QuikSyncSignal{Endpoint: transport.Endpoint{Scheme: "quiksync", Host: host, Port: port}, AuthToken: "bad"}
	if err := sig.Notify(ctx, "j", NotifyMeta{JobID: "j"}); err == nil {
		t.Fatal("expected notify dial auth failure")
	}
	if _, err := sig.Wait(ctx, "j"); err == nil {
		t.Fatal("expected wait dial auth failure")
	}
}

func TestStoreSignalWaitCancel(t *testing.T) {
	mid, err := local.New(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	_, err = StoreSignal{Mid: mid}.Wait(ctx, "missing-job")
	if err == nil {
		t.Fatal("expected wait cancel")
	}
}

func TestValidateManifestDuplicate(t *testing.T) {
	dig := chunk.Sum([]byte("x"))
	err := validateManifest("j", &Manifest{
		SchemaVersion: schemaVersion,
		JobID:         "j",
		Files: []ManifestFile{
			{RelPath: "a.txt", Size: 1, Digest: dig, Chunks: []ManifestChunk{{Offset: 0, Length: 1, Digest: dig}}},
			{RelPath: "a.txt", Size: 1, Digest: dig, Chunks: []ManifestChunk{{Offset: 0, Length: 1, Digest: dig}}},
		},
	})
	if err == nil {
		t.Fatal("expected duplicate")
	}
	err = validateManifest("j", &Manifest{SchemaVersion: 99, JobID: "j"})
	if err == nil {
		t.Fatal("expected schema error")
	}
}

func TestWaitForJobSignalFailureKeepsPolling(t *testing.T) {
	midDir := t.TempDir()
	mid, _ := local.New(midDir)
	ctx, cancel := context.WithTimeout(context.Background(), 800*time.Millisecond)
	defer cancel()
	// Signal that fails immediately; store never publishes → timeout.
	err := waitForJob(ctx, mid, "nopub", failingSignal{})
	if err == nil {
		t.Fatal("expected timeout")
	}
}

func TestSendReturnsNotifyFailure(t *testing.T) {
	srcRoot, midRoot := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(srcRoot, "a.txt"), []byte("data"), 0o644); err != nil {
		t.Fatal(err)
	}
	src, err := local.NewExisting(srcRoot)
	if err != nil {
		t.Fatal(err)
	}
	mid, err := local.New(midRoot)
	if err != nil {
		t.Fatal(err)
	}
	err = Send(context.Background(), src, mid, SendOptions{JobID: "notify-fail", Signal: notifyFailSignal{}})
	if !errors.Is(err, errNotify) {
		t.Fatalf("got %v, want notify failure", err)
	}
}

var errNotify = errors.New("notify failed")

type notifyFailSignal struct{}

func (notifyFailSignal) Notify(context.Context, string, NotifyMeta) error { return errNotify }
func (notifyFailSignal) Wait(context.Context, string) (NotifyMeta, error) {
	return NotifyMeta{}, nil
}

type failingSignal struct{}

func (failingSignal) Notify(context.Context, string, NotifyMeta) error { return nil }
func (failingSignal) Wait(context.Context, string) (NotifyMeta, error) {
	return NotifyMeta{}, context.DeadlineExceeded
}

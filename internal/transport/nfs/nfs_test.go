package nfs

import (
	"context"
	"strings"
	"testing"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestNewValidation(t *testing.T) {
	ctx := context.Background()
	if _, err := New(ctx, transport.Endpoint{Scheme: "nfs"}); err == nil || !strings.Contains(err.Error(), "host") {
		t.Fatalf("missing host: %v", err)
	}
	if _, err := New(ctx, transport.Endpoint{Scheme: "nfs", Host: "nas", Port: "3333", Path: "/export"}); err == nil || !strings.Contains(err.Error(), "custom port") {
		t.Fatalf("custom port: %v", err)
	}
	if _, err := New(ctx, transport.Endpoint{Scheme: "nfs", Host: "nas", Port: "2049", Path: "/"}); err == nil || !strings.Contains(err.Error(), "export") {
		t.Fatalf("missing export: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := New(canceled, transport.Endpoint{Scheme: "nfs", Host: "nas", Path: "/export"}); err == nil {
		t.Fatal("expected canceled context")
	}
}

func TestCapsRootClose(t *testing.T) {
	tr := &Transport{ep: transport.Endpoint{Raw: "nfs://nas/export"}}
	caps := tr.Caps()
	if !caps.SupportsDelta || caps.SupportsMultiplex || !caps.SupportsReuseChunk {
		t.Fatalf("caps %+v", caps)
	}
	if tr.Root() != "nfs://nas/export" {
		t.Fatalf("root %q", tr.Root())
	}
	if err := tr.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestJoinBaseRejectsDotDotName(t *testing.T) {
	tr := &Transport{base: "backup"}
	// After Clean, traversal is removed; remaining ".." in a name is rejected.
	if _, err := tr.joinBase("file..bak"); err == nil {
		t.Fatal("expected reject")
	}
}

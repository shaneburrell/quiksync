package nfs

import (
	"context"
	"strings"
	"testing"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestAuthUnixUsesProcessIdentity(t *testing.T) {
	uid, gid := authUnixUID(), authUnixGID()
	if p := processUID(); p >= 0 && uint32(p) != uid {
		t.Fatalf("AUTH_SYS uid=%d want process uid=%d", uid, p)
	}
	if p := processGID(); p >= 0 && uint32(p) != gid {
		t.Fatalf("AUTH_SYS gid=%d want process gid=%d", gid, p)
	}
	// Windows stubs use nobody, never root.
	if processUID() < 0 && uid == 0 {
		t.Fatal("Windows AUTH_SYS stub must not claim root")
	}
}

func TestUniquePartialName(t *testing.T) {
	a, err := uniquePartialName()
	if err != nil {
		t.Fatal(err)
	}
	b, err := uniquePartialName()
	if err != nil {
		t.Fatal(err)
	}
	if a == b {
		t.Fatalf("expected unique names, got %q", a)
	}
	if !strings.HasSuffix(a, ".partial") {
		t.Fatalf("suffix: %q", a)
	}
}

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

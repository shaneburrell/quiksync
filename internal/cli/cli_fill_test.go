package cli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/shaneburrell/quiksync/internal/cli"
)

func TestCLIInvalidViaAndJobID(t *testing.T) {
	src := t.TempDir()
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	err := cli.ExecuteArgs([]string{"send", src, "--via", "://bad", "--job-id", "ok"})
	if err == nil {
		t.Fatal("expected invalid via")
	}
	mid := t.TempDir()
	err = cli.ExecuteArgs([]string{"send", src, "--via", mid, "--job-id", "../evil"})
	if err == nil || !strings.Contains(err.Error(), "invalid job id") {
		t.Fatalf("expected invalid job id, got %v", err)
	}
	err = cli.ExecuteArgs([]string{"recv", "--via", mid, t.TempDir(), "--job-id", "bad/id", "--wait", "1s"})
	if err == nil || !strings.Contains(err.Error(), "invalid job id") {
		t.Fatalf("expected invalid job id on recv, got %v", err)
	}
}

func TestCLIRejectsUnsafeTransferJobIDAndNegativeBandwidth(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(src, "a.txt"), "a")
	if err := cli.ExecuteArgs([]string{"copy", src, dst, "--no-log", "--job-id", "../escape"}); err == nil {
		t.Fatal("expected unsafe transfer job id to fail")
	}
	if err := cli.ExecuteArgs([]string{"copy", src, dst, "--no-log", "--bwlimit", "-1"}); err == nil {
		t.Fatal("expected negative bandwidth limit to fail")
	}
}

func TestCLIRelayGCForce(t *testing.T) {
	src, mid, dst := t.TempDir(), t.TempDir(), t.TempDir()
	mustWrite(t, filepath.Join(src, "g.txt"), "gc-force")
	if err := cli.ExecuteArgs([]string{"send", src, "--via", mid, "--job-id", "gcforce"}); err != nil {
		t.Fatal(err)
	}
	if err := cli.ExecuteArgs([]string{
		"recv", "--via", mid, dst, "--job-id", "gcforce", "--wait", "5s",
	}); err != nil {
		t.Fatal(err)
	}
	// Without ack, force GC should still clear.
	if err := cli.ExecuteArgs([]string{
		"relay", "gc", "--via", mid, "--job-id", "gcforce", "--force",
	}); err != nil {
		t.Fatal(err)
	}
	prefix := filepath.Join(mid, ".quiksync", "relay", "jobs", "gcforce")
	if _, err := os.Stat(prefix); !os.IsNotExist(err) {
		// GC may leave empty dirs or remove entirely; ensure lease/manifest gone.
		if _, err := os.Stat(filepath.Join(prefix, "manifest.json")); !os.IsNotExist(err) {
			t.Fatalf("manifest still present: %v", err)
		}
	}
}

func TestCLIRecvRequiresVia(t *testing.T) {
	err := cli.ExecuteArgs([]string{"recv", t.TempDir()})
	if err == nil {
		t.Fatal("expected --via required")
	}
}

func TestCLIRelayGCRequiresVia(t *testing.T) {
	err := cli.ExecuteArgs([]string{"relay", "gc"})
	if err == nil {
		t.Fatal("expected --via required")
	}
}

func TestCLIServeHelp(t *testing.T) {
	if err := cli.ExecuteArgs([]string{"serve", "--help"}); err != nil {
		t.Fatal(err)
	}
	if err := cli.ExecuteArgs([]string{"remote-helper", "--help"}); err != nil {
		t.Fatal(err)
	}
}

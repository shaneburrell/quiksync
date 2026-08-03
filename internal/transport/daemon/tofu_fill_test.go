package daemon

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestTOFUPinningLifecycle(t *testing.T) {
	config := t.TempDir()
	t.Setenv("QUIKSYNC_CONFIG", config)

	certFile, keyFile, err := certPaths()
	if err != nil {
		t.Fatal(err)
	}
	conf, err := loadOrCreatePinnedTLS("", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(conf.Certificates) != 1 {
		t.Fatalf("certificates=%d", len(conf.Certificates))
	}
	if _, err := os.Stat(certFile); err != nil {
		t.Fatal(err)
	}
	if st, err := os.Stat(keyFile); err != nil {
		t.Fatalf("key stat: %v", err)
	} else if runtime.GOOS != "windows" && st.Mode().Perm() != 0o600 {
		t.Fatalf("key mode=%v want 0600", st.Mode())
	}
	// Exercise the existing-certificate and explicit-certificate paths too.
	if _, err := loadOrCreatePinnedTLS("", ""); err != nil {
		t.Fatal(err)
	}
	if _, err := loadOrCreatePinnedTLS(certFile, keyFile); err != nil {
		t.Fatal(err)
	}

	cert, err := x509.ParseCertificate(conf.Certificates[0].Certificate[0])
	if err != nil {
		t.Fatal(err)
	}
	state := tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
	verify := verifyTOFU("example.test:443", false)
	if err := verify(state); err != nil {
		t.Fatal(err)
	}
	pin, err := pinPath("example.test:443")
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(pin)
	if err != nil || strings.TrimSpace(string(b)) != fingerprint(cert) {
		t.Fatalf("pin=%q err=%v", b, err)
	}
	if err := verify(state); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(pin, []byte("wrong\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := verify(state); err == nil || !strings.Contains(err.Error(), "mismatch") {
		t.Fatalf("expected pin mismatch, got %v", err)
	}
	if err := verifyTOFU("example.test:443", true)(state); err != nil {
		t.Fatal(err)
	}
	if err := verifyTOFU("example.test:443", false)(tls.ConnectionState{}); err == nil {
		t.Fatal("expected missing peer certificate error")
	}
}

func TestTOFUPathsSanitizeHostPort(t *testing.T) {
	config := t.TempDir()
	t.Setenv("QUIKSYNC_CONFIG", config)
	path, err := pinPath("host:22/../../evil")
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(path) != filepath.Join(config, "pins") {
		t.Fatalf("unsafe pin path %q", path)
	}
	if strings.Contains(filepath.Base(path), "/") {
		t.Fatalf("unsafe pin filename %q", path)
	}
}

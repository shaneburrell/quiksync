package transport

import (
	"strings"
	"testing"
)

func TestParseEndpointMoreSchemes(t *testing.T) {
	ep, err := ParseEndpoint("quiksync://host.example/data")
	if err != nil || ep.Port != "4242" || ep.Path != "/data" {
		t.Fatalf("%+v %v", ep, err)
	}
	ep, err = ParseEndpoint("nfs://filer:/export/share")
	if err != nil || ep.Port != "2049" || ep.Host != "filer" {
		t.Fatalf("nfs: %+v %v", ep, err)
	}
	ep, err = ParseEndpoint("ssh://user@h:2222/")
	if err != nil || ep.User != "user" || ep.Port != "2222" || ep.Path != "/" {
		t.Fatalf("ssh: %+v %v", ep, err)
	}
	ep, err = ParseEndpoint("user@host:")
	if err != nil || ep.Scheme != "ssh" || ep.Path != "." {
		t.Fatalf("scp empty path: %+v %v", ep, err)
	}
	if _, err := ParseEndpoint("ftp://x"); err == nil {
		t.Fatal("unsupported scheme")
	}
	if _, err := ParseEndpoint(""); err == nil {
		t.Fatal("empty")
	}
}

func TestParseEndpointRejectsSSHInjection(t *testing.T) {
	bad := []string{
		"-oProxyCommand=id:/dst",
		"user@-oProxyCommand=id:/dst",
		"-evil@host:/dst",
		"ssh://-oProxyCommand=evil/dst",
		"ssh://user@host:abc/dst",
		"ssh://user@host:0/dst",
		"ssh://user@host:70000/dst",
	}
	for _, in := range bad {
		if _, err := ParseEndpoint(in); err == nil {
			t.Fatalf("expected reject for %q", in)
		} else if !strings.Contains(err.Error(), "ssh endpoint") && !strings.Contains(err.Error(), "invalid") {
			// url.Parse may fail first for some ssh:// forms; either is fine.
			_ = err
		}
	}
}

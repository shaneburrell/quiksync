package ssh

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/pem"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	cryptossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestUseNativeSSHDefault(t *testing.T) {
	old := PreferNative
	t.Cleanup(func() { PreferNative = old })

	PreferNative = false
	t.Setenv("QUIKSYNC_SSH_NATIVE", "")
	if useNativeSSH() {
		t.Fatal("expected false when PreferNative=false and env unset")
	}
	t.Setenv("QUIKSYNC_SSH_NATIVE", "1")
	if !useNativeSSH() {
		t.Fatal("expected true when QUIKSYNC_SSH_NATIVE=1")
	}
	t.Setenv("QUIKSYNC_SSH_NATIVE", "true")
	if !useNativeSSH() {
		t.Fatal("expected true when QUIKSYNC_SSH_NATIVE=true")
	}
	PreferNative = true
	t.Setenv("QUIKSYNC_SSH_NATIVE", "")
	if !useNativeSSH() {
		t.Fatal("expected true when PreferNative=true")
	}
}

func TestPreferNativeMatchesGOOS(t *testing.T) {
	want := runtime.GOOS == "windows"
	if PreferNative != want {
		t.Fatalf("PreferNative=%v want %v for GOOS=%s", PreferNative, want, runtime.GOOS)
	}
}

func TestNativeAuthMethods(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)

	if _, err := nativeAuthMethods(); err == nil {
		t.Fatal("expected error with empty ~/.ssh")
	}

	sshDir := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sshDir, "id_rsa"), []byte("not-a-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := nativeAuthMethods(); err == nil {
		t.Fatal("expected error when only invalid keys exist")
	}

	if _, err := writeTestEd25519Key(filepath.Join(sshDir, "id_ed25519")); err != nil {
		t.Fatal(err)
	}
	auth, err := nativeAuthMethods()
	if err != nil {
		t.Fatal(err)
	}
	if len(auth) != 1 {
		t.Fatalf("auth methods=%d", len(auth))
	}
}

func TestNativeHostKeyCallback(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("QUIKSYNC_SSH_INSECURE", "")

	// Missing known_hosts: create + accept-new pin (not permanent ignore).
	cb, err := nativeHostKeyCallback()
	if err != nil {
		t.Fatal(err)
	}
	khPath := filepath.Join(home, ".ssh", "known_hosts")
	if _, err := os.Stat(khPath); err != nil {
		t.Fatalf("known_hosts should be created: %v", err)
	}
	key := dummyHostKey(t)
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2222}
	if err := cb("[127.0.0.1]:2222", addr, key); err != nil {
		t.Fatalf("accept-new should succeed: %v", err)
	}
	got, err := os.ReadFile(khPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(got, []byte("ssh-ed25519")) {
		t.Fatalf("known_hosts not appended: %q", got)
	}

	// knownhosts.New caches at open time; seed the file before constructing the callback.
	known := dummyHostKey(t)
	line := knownhosts.Line([]string{knownhosts.Normalize("[127.0.0.1]:2223")}, known)
	if err := os.WriteFile(khPath, []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cb, err = nativeHostKeyCallback()
	if err != nil {
		t.Fatal(err)
	}
	addr2 := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 2223}
	if err := cb("[127.0.0.1]:2223", addr2, known); err != nil {
		t.Fatalf("known host should match: %v", err)
	}
	if err := cb("[127.0.0.1]:2223", addr2, dummyHostKey(t)); err == nil {
		t.Fatal("expected host key mismatch error")
	}
}

func TestNativeHostKeyInsecureEnv(t *testing.T) {
	t.Setenv("QUIKSYNC_SSH_INSECURE", "1")
	cb, err := nativeHostKeyCallback()
	if err != nil {
		t.Fatal(err)
	}
	if err := cb("evil.example", &net.TCPAddr{IP: net.ParseIP("1.2.3.4"), Port: 22}, dummyHostKey(t)); err != nil {
		t.Fatalf("insecure should accept: %v", err)
	}
}

func TestDialNativeAndNewNative(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	sshDir := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		t.Fatal(err)
	}
	clientPriv, err := writeTestEd25519Key(filepath.Join(sshDir, "id_ed25519"))
	if err != nil {
		t.Fatal(err)
	}

	ln, hostSigner := startTestSSHServer(t, clientPriv.PublicKey())
	defer ln.Close()
	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}

	line := knownhosts.Line([]string{knownhosts.Normalize(net.JoinHostPort(host, port))}, hostSigner.PublicKey())
	if err := os.WriteFile(filepath.Join(sshDir, "known_hosts"), []byte(line+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	ep := transport.Endpoint{User: "test", Host: host, Port: port, Path: "/tmp"}
	client, session, err := dialNative(ep)
	if err != nil {
		t.Fatalf("dialNative: %v", err)
	}
	_ = session.Close()
	_ = client.Close()

	ep2 := transport.Endpoint{Host: host, Port: port, Path: "/tmp"}
	t.Setenv("USER", "from-env")
	client2, session2, err := dialNative(ep2)
	if err != nil {
		t.Fatalf("dialNative defaults: %v", err)
	}
	_ = session2.Close()
	_ = client2.Close()

	tr, err := newNative(context.Background(), ep)
	if err != nil {
		t.Fatalf("newNative: %v", err)
	}
	if err := tr.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func TestDialNativeAuthFailure(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	if err := os.MkdirAll(filepath.Join(home, ".ssh"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := writeTestEd25519Key(filepath.Join(home, ".ssh", "id_ed25519")); err != nil {
		t.Fatal(err)
	}

	hostKey, err := generateSigner()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &cryptossh.ServerConfig{
		PublicKeyCallback: func(conn cryptossh.ConnMetadata, key cryptossh.PublicKey) (*cryptossh.Permissions, error) {
			return nil, fmt.Errorf("denied")
		},
	}
	cfg.AddHostKey(hostKey)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go acceptSSH(ln, cfg)

	_, port, _ := net.SplitHostPort(ln.Addr().String())
	_, _, err = dialNative(transport.Endpoint{User: "x", Host: "127.0.0.1", Port: port})
	if err == nil {
		t.Fatal("expected dial failure")
	}
}

func writeTestEd25519Key(path string) (cryptossh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	block, err := cryptossh.MarshalPrivateKey(priv, "")
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(path, pem.EncodeToMemory(block), 0o600); err != nil {
		return nil, err
	}
	return cryptossh.NewSignerFromKey(priv)
}

func generateSigner() (cryptossh.Signer, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return cryptossh.NewSignerFromKey(priv)
}

func dummyHostKey(t *testing.T) cryptossh.PublicKey {
	t.Helper()
	s, err := generateSigner()
	if err != nil {
		t.Fatal(err)
	}
	return s.PublicKey()
}

func startTestSSHServer(t *testing.T, clientPub cryptossh.PublicKey) (net.Listener, cryptossh.Signer) {
	t.Helper()
	hostKey, err := generateSigner()
	if err != nil {
		t.Fatal(err)
	}
	cfg := &cryptossh.ServerConfig{
		PublicKeyCallback: func(conn cryptossh.ConnMetadata, key cryptossh.PublicKey) (*cryptossh.Permissions, error) {
			if bytes.Equal(key.Marshal(), clientPub.Marshal()) {
				return nil, nil
			}
			return nil, fmt.Errorf("unknown key")
		},
	}
	cfg.AddHostKey(hostKey)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go acceptSSH(ln, cfg)
	time.Sleep(20 * time.Millisecond)
	return ln, hostKey
}

func acceptSSH(ln net.Listener, cfg *cryptossh.ServerConfig) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		go handleSSHConn(conn, cfg)
	}
}

func handleSSHConn(nConn net.Conn, cfg *cryptossh.ServerConfig) {
	_, chans, reqs, err := cryptossh.NewServerConn(nConn, cfg)
	if err != nil {
		_ = nConn.Close()
		return
	}
	go cryptossh.DiscardRequests(reqs)
	for newCh := range chans {
		if newCh.ChannelType() != "session" {
			_ = newCh.Reject(cryptossh.UnknownChannelType, "unknown")
			continue
		}
		ch, requests, err := newCh.Accept()
		if err != nil {
			continue
		}
		go func(ch cryptossh.Channel, requests <-chan *cryptossh.Request) {
			defer ch.Close()
			for req := range requests {
				switch req.Type {
				case "exec", "shell", "pty-req", "env":
					if req.WantReply {
						_ = req.Reply(true, nil)
					}
				default:
					if req.WantReply {
						_ = req.Reply(false, nil)
					}
				}
			}
		}(ch, requests)
	}
}

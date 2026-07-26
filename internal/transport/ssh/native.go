package ssh

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"time"

	cryptossh "golang.org/x/crypto/ssh"
	"golang.org/x/crypto/ssh/knownhosts"

	"github.com/shaneburrell/quiksync/internal/transport"
)

// PreferNative forces the golang.org/x/crypto/ssh client instead of ssh(1).
// On Windows, native is the default because Win32 OpenSSH does not reliably
// forward piped stdin for binary remote-helper sessions.
var PreferNative = runtime.GOOS == "windows"

func useNativeSSH() bool {
	if PreferNative {
		return true
	}
	switch os.Getenv("QUIKSYNC_SSH_NATIVE") {
	case "1", "true", "yes":
		return true
	}
	return false
}

func sshInsecureHostKey() bool {
	switch os.Getenv("QUIKSYNC_SSH_INSECURE") {
	case "1", "true", "yes":
		return true
	}
	return false
}

func dialNative(ep transport.Endpoint) (*cryptossh.Client, *cryptossh.Session, error) {
	user := ep.User
	if user == "" {
		user = os.Getenv("USER")
		if user == "" {
			user = os.Getenv("USERNAME")
		}
	}
	port := ep.Port
	if port == "" {
		port = "22"
	}
	addr := net.JoinHostPort(ep.Host, port)

	auth, err := nativeAuthMethods()
	if err != nil {
		return nil, nil, err
	}
	hostKeyCB, err := nativeHostKeyCallback()
	if err != nil {
		return nil, nil, err
	}
	cfg := &cryptossh.ClientConfig{
		User:            user,
		Auth:            auth,
		HostKeyCallback: hostKeyCB,
		Timeout:         30 * time.Second,
	}
	client, err := cryptossh.Dial("tcp", addr, cfg)
	if err != nil {
		return nil, nil, fmt.Errorf("ssh dial %s: %w", addr, err)
	}
	session, err := client.NewSession()
	if err != nil {
		_ = client.Close()
		return nil, nil, fmt.Errorf("ssh session: %w", err)
	}
	return client, session, nil
}

func nativeAuthMethods() ([]cryptossh.AuthMethod, error) {
	var signers []cryptossh.Signer
	home, err := os.UserHomeDir()
	if err == nil {
		for _, name := range []string{"id_ed25519", "id_ecdsa", "id_rsa"} {
			path := filepath.Join(home, ".ssh", name)
			key, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			signer, err := cryptossh.ParsePrivateKey(key)
			if err != nil {
				// Encrypted keys need a passphrase; skip for BatchMode-like behavior.
				continue
			}
			signers = append(signers, signer)
		}
	}
	if len(signers) == 0 {
		return nil, fmt.Errorf("ssh: no usable private keys in ~/.ssh/id_*")
	}
	return []cryptossh.AuthMethod{cryptossh.PublicKeys(signers...)}, nil
}

func nativeHostKeyCallback() (cryptossh.HostKeyCallback, error) {
	if sshInsecureHostKey() {
		return cryptossh.InsecureIgnoreHostKey(), nil //nolint:gosec // explicit QUIKSYNC_SSH_INSECURE
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return nil, fmt.Errorf("ssh: cannot resolve home for known_hosts (set QUIKSYNC_SSH_INSECURE=1 for labs): %w", err)
	}
	sshDir := filepath.Join(home, ".ssh")
	if err := os.MkdirAll(sshDir, 0o700); err != nil {
		return nil, fmt.Errorf("ssh: create ~/.ssh: %w", err)
	}
	path := filepath.Join(sshDir, "known_hosts")
	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return nil, fmt.Errorf("ssh: known_hosts: %w", err)
		}
		if err := os.WriteFile(path, nil, 0o600); err != nil {
			return nil, fmt.Errorf("ssh: create known_hosts: %w", err)
		}
	}
	cb, err := knownhosts.New(path)
	if err != nil {
		return nil, fmt.Errorf("known_hosts: %w", err)
	}
	return func(hostname string, remote net.Addr, key cryptossh.PublicKey) error {
		err := cb(hostname, remote, key)
		if err == nil {
			return nil
		}
		// Unknown host: accept once (accept-new style) and pin.
		if keyErr, ok := err.(*knownhosts.KeyError); ok && len(keyErr.Want) == 0 {
			line := knownhosts.Line([]string{knownhosts.Normalize(hostname)}, key)
			f, openErr := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
			if openErr != nil {
				return err
			}
			_, _ = f.WriteString(line + "\n")
			_ = f.Close()
			return nil
		}
		return err
	}, nil
}

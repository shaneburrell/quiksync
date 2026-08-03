package daemon

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func configDir() (string, error) {
	if v := os.Getenv("QUIKSYNC_CONFIG"); v != "" {
		return v, nil
	}
	home, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "quiksync"), nil
}

func certPaths() (certFile, keyFile string, err error) {
	dir, err := configDir()
	if err != nil {
		return "", "", err
	}
	return filepath.Join(dir, "daemon.crt"), filepath.Join(dir, "daemon.key"), nil
}

func pinPath(hostport string) (string, error) {
	dir, err := configDir()
	if err != nil {
		return "", err
	}
	safe := strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '.' || r == '-' || r == '_' {
			return r
		}
		return '_'
	}, hostport)
	return filepath.Join(dir, "pins", safe+".pin"), nil
}

func fingerprint(cert *x509.Certificate) string {
	sum := sha256.Sum256(cert.Raw)
	return hex.EncodeToString(sum[:])
}

func loadOrCreatePinnedTLS(certFile, keyFile string) (*tls.Config, error) {
	if certFile != "" && keyFile != "" {
		cert, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
	}
	cFile, kFile, err := certPaths()
	if err != nil {
		return nil, err
	}
	if _, err := os.Stat(cFile); err == nil {
		cert, err := tls.LoadX509KeyPair(cFile, kFile)
		if err != nil {
			return nil, err
		}
		return &tls.Config{Certificates: []tls.Certificate{cert}}, nil
	}
	tlsConf, certPEM, keyPEM, err := generateTLSMaterial()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(cFile), 0o755); err != nil {
		return nil, err
	}
	if err := os.WriteFile(cFile, certPEM, 0o644); err != nil {
		return nil, err
	}
	if err := os.WriteFile(kFile, keyPEM, 0o600); err != nil {
		return nil, err
	}
	_ = certPEM
	return tlsConf, nil
}

func generateTLSMaterial() (*tls.Config, []byte, []byte, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, nil, err
	}
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, priv.Public(), priv)
	if err != nil {
		return nil, nil, nil, err
	}
	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, nil, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, nil, nil, err
	}
	return &tls.Config{Certificates: []tls.Certificate{cert}}, certPEM, keyPEM, nil
}

func verifyTOFU(hostport string, insecure bool) func(cs tls.ConnectionState) error {
	return func(cs tls.ConnectionState) error {
		if len(cs.PeerCertificates) == 0 {
			return fmt.Errorf("no peer certificate")
		}
		fp := fingerprint(cs.PeerCertificates[0])
		if insecure {
			return nil
		}
		path, err := pinPath(hostport)
		if err != nil {
			return err
		}
		b, err := os.ReadFile(path)
		if err != nil {
			if !os.IsNotExist(err) {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
				return err
			}
			// Atomic first-pin: O_EXCL so concurrent first connects cannot race.
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
			if err != nil {
				if !os.IsExist(err) {
					return err
				}
				// Lost the race — re-read and verify against the winner's pin.
				b, err = os.ReadFile(path)
				if err != nil {
					return err
				}
				stored := strings.TrimSpace(string(b))
				if stored != fp {
					return fmt.Errorf("TOFU pin mismatch for %s: got %s want %s (delete pin or use --insecure)", hostport, fp, stored)
				}
				return nil
			}
			_, werr := f.Write([]byte(fp + "\n"))
			cerr := f.Close()
			if werr != nil {
				return werr
			}
			return cerr
		}
		stored := strings.TrimSpace(string(b))
		if stored != fp {
			return fmt.Errorf("TOFU pin mismatch for %s: got %s want %s (delete pin or use --insecure)", hostport, fp, stored)
		}
		return nil
	}
}

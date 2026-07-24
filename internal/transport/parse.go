package transport

import (
	"fmt"
	"net/url"
	"strings"
)

// ParseEndpoint parses file/ssh/quiksync URIs or plain paths.
func ParseEndpoint(s string) (Endpoint, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Endpoint{}, fmt.Errorf("empty endpoint")
	}

	// scp-like [user@]host:path (but not Windows C:\...)
	if !strings.Contains(s, "://") && strings.Contains(s, ":") && !isWindowsDrive(s) {
		user := ""
		rest := s
		if at := strings.Index(s, "@"); at >= 0 {
			if at > 0 {
				user = s[:at]
			}
			rest = s[at+1:]
		}
		colon := strings.Index(rest, ":")
		if colon > 0 {
			host := rest[:colon]
			// Host must not look like a path component (reject "./file:name", "a/b:c").
			if host != "" && !strings.ContainsAny(host, `/`) {
				path := rest[colon+1:]
				if path == "" {
					path = "."
				}
				return Endpoint{Scheme: "ssh", User: user, Host: host, Path: path, Raw: s}, nil
			}
		}
	}

	if strings.Contains(s, "://") {
		u, err := url.Parse(s)
		if err != nil {
			return Endpoint{}, err
		}
		switch u.Scheme {
		case "file":
			return Endpoint{Scheme: "file", Path: u.Path, Raw: s}, nil
		case "ssh":
			path := u.Path
			if path == "" {
				path = "/"
			}
			port := u.Port()
			return Endpoint{Scheme: "ssh", User: u.User.Username(), Host: u.Hostname(), Port: port, Path: path, Raw: s}, nil
		case "quiksync":
			port := u.Port()
			if port == "" {
				port = "4242"
			}
			path := u.Path
			if path == "" {
				path = "/"
			}
			return Endpoint{Scheme: "quiksync", Host: u.Hostname(), Port: port, Path: path, Raw: s}, nil
		case "s3":
			return Endpoint{Scheme: "s3", Host: u.Host, Path: strings.TrimPrefix(u.Path, "/"), Raw: s}, nil
		default:
			return Endpoint{}, fmt.Errorf("unsupported scheme %q", u.Scheme)
		}
	}

	return Endpoint{Scheme: "file", Path: s, Raw: s}, nil
}

func isWindowsDrive(s string) bool {
	return len(s) >= 2 && s[1] == ':' && ((s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= 'a' && s[0] <= 'z'))
}

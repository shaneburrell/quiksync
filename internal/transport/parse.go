package transport

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
)

// ParseEndpoint parses file/ssh/quiksync URIs or plain paths.
func ParseEndpoint(s string) (Endpoint, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return Endpoint{}, fmt.Errorf("empty endpoint")
	}

	// scp-like [user@]host[:port]:path or [user@][ipv6]:path (but not Windows C:\...)
	if !strings.Contains(s, "://") && strings.Contains(s, ":") && !isWindowsDrive(s) {
		user := ""
		rest := s
		if at := strings.Index(s, "@"); at >= 0 {
			if at > 0 {
				user = s[:at]
			}
			rest = s[at+1:]
		}
		if ep, ok, err := parseScpLike(user, rest, s); err != nil {
			return Endpoint{}, err
		} else if ok {
			return ep, nil
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
			ep := Endpoint{Scheme: "ssh", User: u.User.Username(), Host: u.Hostname(), Port: port, Path: path, Raw: s}
			if err := validateSSHEndpoint(ep); err != nil {
				return Endpoint{}, err
			}
			return ep, nil
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
		case "nfs":
			// nfs://host[:port]/export/path — Host is server; Path is /export/rest
			path := u.Path
			if path == "" {
				path = "/"
			}
			port := u.Port()
			if port == "" {
				port = "2049"
			}
			return Endpoint{Scheme: "nfs", Host: u.Hostname(), Port: port, Path: path, Raw: s}, nil
		default:
			return Endpoint{}, fmt.Errorf("unsupported scheme %q", u.Scheme)
		}
	}

	return Endpoint{Scheme: "file", Path: s, Raw: s}, nil
}

// parseScpLike parses host:path, host:port:path, and [ipv6]:path / [ipv6]:port:path.
func parseScpLike(user, rest, raw string) (Endpoint, bool, error) {
	var host, port, path string
	if strings.HasPrefix(rest, "[") {
		end := strings.Index(rest, "]")
		if end <= 1 {
			return Endpoint{}, false, nil
		}
		host = rest[1:end]
		after := rest[end+1:]
		if !strings.HasPrefix(after, ":") {
			return Endpoint{}, false, fmt.Errorf("ssh endpoint: expected ':' after IPv6 host")
		}
		after = after[1:]
		path = after
		if idx := strings.Index(after, ":"); idx >= 0 {
			if n, err := strconv.Atoi(after[:idx]); err == nil && n >= 1 && n <= 65535 {
				port = after[:idx]
				path = after[idx+1:]
			}
		}
	} else {
		colon := strings.Index(rest, ":")
		if colon <= 0 {
			return Endpoint{}, false, nil
		}
		host = rest[:colon]
		// Host must not look like a path component (reject "./file:name", "a/b:c").
		if host == "" || strings.ContainsAny(host, `/`) {
			return Endpoint{}, false, nil
		}
		after := rest[colon+1:]
		path = after
		if idx := strings.Index(after, ":"); idx >= 0 {
			maybePort := after[:idx]
			if n, err := strconv.Atoi(maybePort); err == nil && n >= 1 && n <= 65535 {
				port = maybePort
				path = after[idx+1:]
			}
		}
	}
	if path == "" {
		path = "."
	}
	ep := Endpoint{Scheme: "ssh", User: user, Host: host, Port: port, Path: path, Raw: raw}
	if err := validateSSHEndpoint(ep); err != nil {
		return Endpoint{}, false, err
	}
	return ep, true, nil
}

func validateSSHEndpoint(ep Endpoint) error {
	if ep.Host == "" {
		return fmt.Errorf("ssh endpoint: empty host")
	}
	if strings.HasPrefix(ep.Host, "-") {
		return fmt.Errorf("ssh endpoint: host must not start with '-'")
	}
	if ep.User != "" && strings.HasPrefix(ep.User, "-") {
		return fmt.Errorf("ssh endpoint: user must not start with '-'")
	}
	if ep.Port != "" {
		n, err := strconv.Atoi(ep.Port)
		if err != nil || n < 1 || n > 65535 {
			return fmt.Errorf("ssh endpoint: invalid port %q", ep.Port)
		}
	}
	return nil
}

func isWindowsDrive(s string) bool {
	return len(s) >= 2 && s[1] == ':' && ((s[0] >= 'A' && s[0] <= 'Z') || (s[0] >= 'a' && s[0] <= 'z'))
}

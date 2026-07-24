# Security Policy

## Supported versions

Security fixes are applied to the latest release on `main` / the newest `v*` tag.

## Transport trust (QUIC)

- Daemon TLS certificates are auto-generated and stored under `~/.config/quiksync/` (override with `$QUIKSYNC_CONFIG`).
- Clients pin the server certificate fingerprint on first connect (**TOFU**). A later fingerprint mismatch fails the dial.
- `--insecure` skips pin verification and is intended for labs only.
- Daemon filesystem access is confined to `serve --root` via path joining that rejects `..` and absolute escapes. Client `Hello.Root` cannot override the server root for the daemon.

## Reporting a vulnerability

Please **do not** open a public issue for security problems that could be exploited.

Prefer:

1. GitHub **Private vulnerability reporting** on [shaneburrell/quiksync](https://github.com/shaneburrell/quiksync/security), or
2. Contact via [github.com/shaneburrell](https://github.com/shaneburrell)

Include QuikSync version, OS, reproduction steps, and impact. You should receive an acknowledgment when possible; coordinated disclosure is appreciated.

# Security Policy

## Supported versions

Security fixes are applied to the latest release on `main` / the newest `v*` tag.

## Transport trust (QUIC)

- Daemon TLS certificates are auto-generated and stored under `~/.config/quiksync/` (override with `$QUIKSYNC_CONFIG`).
- Clients pin the server certificate fingerprint on first connect (**TOFU**). A later fingerprint mismatch fails the dial. First connect silently pins — verify the host on first use.
- `--insecure` skips pin verification and is intended for labs only.
- Serve defaults to `127.0.0.1:4242` and requires a shared **`--auth-token`** (or `QUIKSYNC_AUTH_TOKEN`). Use `--allow-no-auth` only for labs. Non-loopback listen addresses always require a token.
- Concurrent QUIC streams are capped to limit resource exhaustion.
- Protocol Hello negotiates capability bits (including sparse `ReuseChunk`). Older remotes without reuse fall back to full-wire chunk send.

## Filesystem confinement

- Daemon filesystem access is confined to `serve --root` via path joining that rejects `..`, absolute paths, empty/`."` targets, and **symlink escapes** outside the root.
- Client `Hello.Root` cannot override the server root for the daemon.
- Decompress (zstd/lz4) output is capped per chunk to prevent decompression bombs.

## S3 / object storage

- Credentials come from the AWS SDK default chain (env, shared config, instance role). Prefer least-privilege IAM scoped to the destination bucket/prefix.
- Use `--s3-endpoint` only with trusted S3-compatible endpoints (MinIO/R2). Prefer TLS endpoints in production.
- Prefer bucket default encryption (SSE-S3 / SSE-KMS). QuikSync does not yet client-encrypt object payloads.
- `--delete` permanently removes objects under the destination prefix (plus signature sidecars).
- After a successful object put, a failed signature-sidecar write fails the commit (object may already exist; the next successful sync repairs the sidecar).

## NFS

- Mounted NFS paths use the local transport; trust the mount and export options (root squashing, sec=).
- Native `nfs://` is **experimental** (NFSv3 + AUTH_SYS only, unencrypted) and expands the network attack surface versus a kernel mount. Prefer mounts for production. Writes use staged temp files + NFSv3 `RENAME`; mode/mtime are not applied. Custom ports are rejected (portmapper-based dial).
- AUTH_SYS identity is the **local process** uid/gid (`os.Getuid`/`Getgid` on Unix; nobody on Windows stubs), not root. This is not a capability grant — exports with `no_root_squash` still trust the claimed id on the wire.

## Mid-hop relay

- Payload objects are content-addressed; receivers reject bytes that do not match the digest (poison objects fail closed).
- Manifests and leases on a shared writable prefix are a **trusted-peer** channel — anyone who can write the mid prefix can publish jobs. Isolate prefixes per pair with IAM or share ACLs.
- Job IDs are sanitized before path construction (no `..` or separators), so relay state stays under `.quiksync/relay/<job>/`. Use one sender per job id.
- Manifests are validated (schema, job id, relative paths, chunk coverage) before materialize.
- `--signal quiksync://…` / SSH is **wakeup-only** (process-local waiter on the peer daemon); the receiver always re-reads and verifies store state. Signal compromise must not grant data-plane trust.
- Use SSE on the mid bucket; client-side mid-hop encryption is not yet implemented.
- `relay gc` deletes job objects after ack (or forced TTL); do not point `--via` at unrelated prefixes.

## Reporting a vulnerability

Please **do not** open a public issue for security problems that could be exploited.

Prefer:

1. GitHub **Private vulnerability reporting** on [shaneburrell/quiksync](https://github.com/shaneburrell/quiksync/security), or
2. Contact via [github.com/shaneburrell](https://github.com/shaneburrell)

Include QuikSync version, OS, reproduction steps, and impact. You should receive an acknowledgment when possible; coordinated disclosure is appreciated.

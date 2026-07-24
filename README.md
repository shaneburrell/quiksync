# QuikSync

**Resilient, intelligent one-way file copy & sync** — built for flaky links, live data, and maximum bandwidth use.

[![CI](https://github.com/shaneburrell/quiksync/actions/workflows/ci.yml/badge.svg)](https://github.com/shaneburrell/quiksync/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/shaneburrell/quiksync.svg)](https://pkg.go.dev/github.com/shaneburrell/quiksync)

QuikSync moves files **intact and verifiably** from source → destination. It uses content-defined chunking, crash-safe resume, live-change detection, and runtime autotuning (streams, frame size, compression) so you get strong correctness without hand-tuning every transfer.

```bash
quiksync copy ./photos /backup/photos
quiksync sync ./project user@host:/srv/project --delete
quiksync copy ./data quiksync://nas:4242/volume/data
```

## Why QuikSync?

| Problem | What QuikSync does |
|---------|-------------------|
| Half-written files after a drop | Atomic stage + BLAKE3 verify before finalize |
| Interrupted multi-GB jobs | JSONL journal resume under `.quiksync/` |
| Files changing while copying | Detect mutation, requeue (or `--skip-unstable`) |
| Slow / high-RTT / lossy links | Autotune streams, window, compression |
| Re-copying almost-identical trees | FastCDC deltas — only changed chunks move |
| Mixed environments | Local, SSH, and QUIC daemon transports |

## Install

### Prebuilt binaries

Download from [Releases](https://github.com/shaneburrell/quiksync/releases) for:

- macOS (Apple Silicon & Intel)
- Linux (amd64 & arm64)
- Windows (amd64 & arm64)

### From source

Requires [Go 1.22+](https://go.dev/dl/).

```bash
go install github.com/shaneburrell/quiksync/cmd/quiksync@latest
```

Or clone and build:

```bash
git clone https://github.com/shaneburrell/quiksync.git
cd quiksync
make build
./bin/quiksync --version
```

Cross-compile everything:

```bash
make build-all   # → dist/ for darwin/linux/windows × amd64/arm64
```

## Quick start

```bash
# One-shot copy (never deletes at destination)
quiksync copy ./src ./dst

# One-way mirror (optional deletes)
quiksync sync ./src ./dst --delete

# Verify bit-identical digests
quiksync verify ./src ./dst

# Dry run
quiksync copy ./src ./dst --dry-run -v
```

### Monitor a job

Copy/sync write a tailable event log (on by default):

```bash
# Terminal A
quiksync copy ./src ./dst

# Terminal B (or an AI agent watching the file)
tail -f ./dst/.quiksync/logs/latest.log
```

Lines are UTC RFC3339 + logfmt (`event=file_ok path=… bytes=…`). stderr also prints a 1s `progress` ticker and `logging to <path>` at start. Use `--log-file PATH` to override the location, or `--no-log` to disable. Remote destinations log under `~/.config/quiksync/logs/` (or `$QUIKSYNC_CONFIG/logs/`).

### Over SSH

Remote host needs `quiksync` on `PATH`. QuikSync runs `quiksync remote-helper` over SSH stdio (rsync-style).

```bash
quiksync copy ./src user@host:/data/dst
quiksync sync ./src ssh://user@host:22/data/dst --delete
```

### QUIC daemon (tough / WAN links)

The daemon uses a persisted self-signed cert under `~/.config/quiksync/` (or `$QUIKSYNC_CONFIG`). Clients **TOFU-pin** the server fingerprint on first connect; later mismatches fail with a clear error. Use `--insecure` only for labs. The serve `--root` is authoritative — clients cannot escape it via `Hello.Root`.

```bash
# On the server
quiksync serve --listen 0.0.0.0:4242 --root /data

# On the client
quiksync copy ./src quiksync://server.example:4242/
```

## Autotuning

`--auto` is **on by default**. QuikSync probes the path and content, then hill-climbs:

- parallel streams / workers
- chunk / frame size
- compression: `none` · `lz4` · `zstd`

Pin only what you care about; the rest stays automatic:

```bash
quiksync copy ./src ./dst --compress=zstd
quiksync copy ./src ./dst --streams=8 --no-auto
quiksync copy ./src ./dst --chunk-size=64K --bwlimit=10485760
```

| Flag | Default | Purpose |
|------|---------|---------|
| `--auto` / `--no-auto` | on | Probe + continuous tuning |
| `--streams=N` | auto | Pin worker count |
| `--compress=auto\|none\|lz4\|zstd` | auto | Codec selection |
| `--chunk-size=SIZE` | auto | CDC / frame target (`64K`, `1M`, …) |
| `--bwlimit` | 0 | Bytes/sec cap |
| `--stable-window` | 0 | Only transfer files unchanged for this duration |
| `--resume` | true | Resume from `.quiksync/journal` |
| `--checksum` | false | Always compare by content hash |
| `--skip-unstable` | false | Skip mutating files instead of retrying |
| `--exclude` | — | Glob patterns to skip |
| `--delete` | false | (`sync` only) remove dest extras; skipped if any file failed |
| `--insecure` | false | Skip QUIC TOFU pin verification (labs only) |
| `--log-file` | DEST/.quiksync/logs/… | Tailable job event log path |
| `--no-log` | false | Disable event logging / progress ticker |

## How it works

```text
Source ──► Walk / plan ──► FastCDC + BLAKE3 ──► Delta vs dest signatures
                                              │
                     Autotuner ◄──────────────┤ streams / compress / frames
                                              ▼
                         Multiplexed transfer (local / SSH / QUIC)
                                              │
                         Temp write → whole-file verify → atomic rename
                                              │
                         Journal + signature cache under .quiksync/
```

**Correctness invariants**

1. Destination path is replaced only after whole-file hash verification and `fsync` of the temp file.
2. A completed journal entry means the source generation matched at finalize time; resume still checks the dest exists (and re-copies if missing or `--checksum` disagrees).
3. Retries never overwrite a good dest with a partial file (temp + rename only).
4. `--delete` runs only on a clean job (`FilesFailed == 0`, walk completed, context not canceled).

## Project layout

```text
cmd/quiksync/           CLI entrypoint
internal/cli/           Cobra commands & flags
internal/engine/        Sync orchestration
internal/chunk/         FastCDC + BLAKE3
internal/delta/         Chunk-level diff
internal/autotune/      Probe + hill-climb optimizer
internal/compress/      none / lz4 / zstd
internal/journal/       Crash-safe resume (JSONL)
internal/index/         Dest signature cache
internal/progress/      Tailable job event log + progress ticker
internal/transport/     file · ssh · quiksync (QUIC)
internal/protocol/      Framed RPC for remote helper / daemon
```

## Development

```bash
git clone https://github.com/shaneburrell/quiksync.git
cd quiksync
make tools    # install goimports + golangci-lint
make check    # tidy, fmt, vet, lint, race tests, coverage gate
make build
```

### Quality commands

| Make target | What it does |
|-------------|--------------|
| `make fmt` | `gofmt` + `goimports` |
| `make lint` | `golangci-lint run` (see `.golangci.yml`) |
| `make vet` | `go vet ./...` |
| `make tidy` | `go mod tidy` |
| `make test` | Unit + integration |
| `make test-race` | Race detector |
| `make cover` | Coverage HTML + **70%** gate on `./internal/...` |
| `make check` | tidy → fmt → vet → lint → race → cover |
| `make bench` | Benchmarks → `testdata/artifacts/bench.txt` |
| `make test-efficiency` | Soak/efficiency report (not in default CI) |
| `make clean` | Remove `bin/`, `dist/`, and test artifacts |

All generated trees, coverage, benches, and soak output go under **`testdata/artifacts/`** (gitignored). Prefer `t.TempDir()` in tests.

```bash
make check
make bench
make test-efficiency   # → testdata/artifacts/efficiency-report.md
```

Fuzz (optional):

```bash
go test -timeout 3m -fuzz=FuzzChunkReader -fuzztime=30s ./internal/chunk/
go test -timeout 3m -fuzz=FuzzReadWriteMsg -fuzztime=30s ./internal/protocol/
```

See [CONTRIBUTING.md](CONTRIBUTING.md) for PR guidelines. Build: `make build`, `make build-all`, `make release`.

### Release

Tagged releases are built with [GoReleaser](https://goreleaser.com/) (see `.goreleaser.yaml`) and GitHub Actions.

```bash
git tag v0.1.0
git push origin v0.1.0
```

## Status & roadmap

**v0.1** — local + SSH + QUIC daemon, autotune, resume, live-change handling.

Later:

- S3-compatible object storage (`s3://…`) behind the same transport interface
- Host profile tooling / richer TUI (event log + stderr progress ticker shipped in v0.1)
- Optional `quiksync watch` for continuous one-way follow

## License

[MIT](LICENSE) © 2026 Shane Burrell

## Acknowledgments

Inspired by ideas from rsync, rclone, restic, and Syncthing — especially content-defined chunking and careful finalize semantics — implemented as a focused one-way Go CLI.

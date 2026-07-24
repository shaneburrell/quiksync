# Contributing to QuikSync

Thanks for helping improve QuikSync. This project aims to stay **correct first**, then fast, then feature-rich.

## Development setup

1. Install [Go](https://go.dev/dl/) (see `go.mod` for the version).
2. Clone and test:

```bash
git clone https://github.com/shaneburrell/quiksync.git
cd quiksync
go test ./...
make test-race
make build
```

### Test artifacts

Generated coverage, benches, and soak reports land in `testdata/artifacts/` (gitignored). Run `make clean` to remove them. Never commit soak trees or `coverage.out`.

| Command | Purpose |
|---------|---------|
| `make test` | Default suite |
| `make test-race` | Data-race checks |
| `make bench` | Microbenchmarks |
| `make test-efficiency` | Longer goodput/delta/bwlimit soak report |

## Before you open a PR

- [ ] `go test ./...` passes
- [ ] `go test -race ./...` passes (or `make test-race`)
- [ ] `go vet ./...` is clean
- [ ] New behavior has tests when practical (especially integrity / resume / delta paths)
- [ ] README or flag help updated if you change the user-facing CLI
- [ ] No secrets, soak output, or local artifact paths committed

## Design priorities

1. **Never finalize a torn file** — temp write + BLAKE3 verify + atomic rename.
2. **Resume safely** — journal entries must not claim success if the source changed.
3. **Transport-agnostic engine** — local / SSH / QUIC share one interface; cloud later.
4. **Autotune by default** — manual flags pin knobs; they should not be required.

## Code style

- Keep packages small and focused under `internal/`.
- Prefer clear names over clever abstractions.
- Match existing formatting (`gofmt` / `go test` is enough for most changes).
- Avoid drive-by refactors unrelated to your change.

## Commit messages

Short, imperative, explain *why* when it is not obvious:

```text
fix resume when source mtime changes mid-transfer

Journal completion now re-checks generation before marking done.
```

## Reporting bugs

Please include:

- QuikSync version (`quiksync --version`)
- OS / arch
- Exact command
- Whether source was changing during the run
- Transport used (local / SSH / QUIC)
- Logs with `-v` if possible

## Security

If you find a vulnerability, please open a private security advisory on GitHub or email the maintainer via the profile on [github.com/shaneburrell](https://github.com/shaneburrell) rather than filing a public issue with exploit details.

## License

By contributing, you agree that your contributions are licensed under the [MIT License](LICENSE).

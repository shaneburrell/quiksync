VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo 0.1.0)
BINARY  := quiksync
LDFLAGS := -s -w -X github.com/shaneburrell/quiksync/internal/cli.version=$(VERSION)

.PHONY: build build-all test test-race test-cover bench test-efficiency lint release clean

build:
	go build -ldflags "$(LDFLAGS)" -o bin/$(BINARY) ./cmd/quiksync

build-all:
	mkdir -p dist
	GOOS=darwin  GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)_darwin_amd64 ./cmd/quiksync
	GOOS=darwin  GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)_darwin_arm64 ./cmd/quiksync
	GOOS=linux   GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)_linux_amd64 ./cmd/quiksync
	GOOS=linux   GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)_linux_arm64 ./cmd/quiksync
	GOOS=windows GOARCH=amd64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)_windows_amd64.exe ./cmd/quiksync
	GOOS=windows GOARCH=arm64 go build -ldflags "$(LDFLAGS)" -o dist/$(BINARY)_windows_arm64.exe ./cmd/quiksync

test:
	go test ./...

test-race:
	go test -race ./...

test-cover:
	mkdir -p testdata/artifacts
	go test -coverprofile=testdata/artifacts/coverage.out ./...
	go tool cover -html=testdata/artifacts/coverage.out -o testdata/artifacts/coverage.html

bench:
	mkdir -p testdata/artifacts
	go test -bench=. -benchmem -count=1 ./internal/... | tee testdata/artifacts/bench.txt

test-efficiency:
	mkdir -p testdata/artifacts
	go test -tags=efficiency -timeout 20m ./internal/engine/ -run TestEfficiencyReport -v

lint:
	go vet ./...

release: build-all
	cd dist && shasum -a 256 * > checksums.txt

clean:
	rm -rf bin dist testdata/artifacts testdata/soak coverage.out coverage.html

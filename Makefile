VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo 0.1.0)
BINARY  := quiksync
LDFLAGS := -s -w -X github.com/shaneburrell/quiksync/internal/cli.version=$(VERSION)
COVER_PKG := ./internal/...
COVER_MIN ?= 80

.PHONY: build build-all test test-race test-cover bench test-efficiency \
	fmt lint vet tidy check cover release clean tools

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

test-cover cover:
	mkdir -p testdata/artifacts
	# Match CI: exclude experimental NFS (needs a live NFSv3 server) from the gate.
	go test $$(go list $(COVER_PKG) | grep -vE '/transport/nfs$$') \
		-coverprofile=testdata/artifacts/coverage.out -covermode=atomic
	go tool cover -html=testdata/artifacts/coverage.out -o testdata/artifacts/coverage.html
	@total=$$(go tool cover -func=testdata/artifacts/coverage.out | awk '/^total:/{print $$3}' | tr -d '%'); \
	echo "total coverage: $${total}% (min $(COVER_MIN)%)"; \
	awk -v t="$$total" -v m="$(COVER_MIN)" 'BEGIN{ if (t+0 < m+0) { printf("coverage %.1f%% is below %s%% gate\n", t, m); exit 1 } }'

bench:
	mkdir -p testdata/artifacts
	go test -bench=. -benchmem -count=1 ./internal/... | tee testdata/artifacts/bench.txt

test-efficiency:
	mkdir -p testdata/artifacts
	go test -tags=efficiency -timeout 20m ./internal/engine/ -run TestEfficiencyReport -v

fmt:
	gofmt -w $$(go list -f '{{.Dir}}' ./...)
	goimports -w $$(go list -f '{{.Dir}}' ./...) 2>/dev/null || true

vet:
	go vet ./...

lint:
	golangci-lint run ./...

tidy:
	go mod tidy

check: tidy fmt vet lint test-race cover

tools:
	go install golang.org/x/tools/cmd/goimports@latest
	go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.3

release: build-all
	cd dist && shasum -a 256 * > checksums.txt

clean:
	rm -rf bin dist testdata/artifacts testdata/soak coverage.out coverage.html

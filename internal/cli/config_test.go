package cli

import (
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/compress"
)

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want int64
	}{
		{"1024", 1024},
		{"64K", 64 * 1024},
		{"64KiB", 64 * 1024},
		{"1MB", 1024 * 1024},
		{"1MiB", 1024 * 1024},
		{"2G", 2 * 1024 * 1024 * 1024},
	}
	for _, tc := range cases {
		got, err := parseSize(tc.in)
		if err != nil || got != tc.want {
			t.Fatalf("%s: got %d err=%v want %d", tc.in, got, err, tc.want)
		}
	}
	if _, err := parseSize("nope"); err == nil {
		t.Fatal("expected error")
	}
	if _, err := parseSize("-1K"); err == nil {
		t.Fatal("expected negative rejection")
	}
	if _, err := buildConfig("/s", "/d", TransferFlags{Compress: "none", ChunkSize: "1K"}, false); err == nil {
		t.Fatal("expected chunk-size too small")
	}
}

func TestBuildConfig(t *testing.T) {
	cfg, err := buildConfig("/src", "/dst", TransferFlags{
		Resume: true, Compress: "lz4", ChunkSize: "32K", Streams: 4,
		StableWindow: time.Second, Auto: true,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Tune.Compress != compress.CodecLZ4 || cfg.Tune.ChunkAvg != 32*1024 || cfg.Tune.Streams != 4 {
		t.Fatalf("%+v", cfg.Tune)
	}
	if !cfg.SyncMode {
		t.Fatal("expected sync mode")
	}
	if _, err := buildConfig("/s", "/d", TransferFlags{Compress: "gzip"}, false); err == nil {
		t.Fatal("expected invalid compress")
	}
}

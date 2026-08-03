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
	if _, err := parseSize(""); err == nil {
		t.Fatal("expected empty rejection")
	}
	if _, err := parseSize("-1K"); err == nil {
		t.Fatal("expected negative rejection")
	}
	if got, err := parseSize("1GiB"); err != nil || got != 1024*1024*1024 {
		t.Fatalf("GiB: %d %v", got, err)
	}
	if got, err := parseSize("1GB"); err != nil || got != 1024*1024*1024 {
		t.Fatalf("GB: %d %v", got, err)
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
	if cfg.LogFile == "" || cfg.ProgressInterval == 0 {
		t.Fatalf("expected default logging, got log=%q interval=%v", cfg.LogFile, cfg.ProgressInterval)
	}
	if _, err := buildConfig("/s", "/d", TransferFlags{Compress: "gzip"}, false); err == nil {
		t.Fatal("expected invalid compress")
	}
	cfg2, err := buildConfig("/s", "/d", TransferFlags{Compress: "none", NoLog: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg2.LogFile != "" {
		t.Fatalf("expected no log, got %q", cfg2.LogFile)
	}
}

func TestBuildConfigDefaultsAndValidation(t *testing.T) {
	t.Setenv("QUIKSYNC_CONFIG", t.TempDir())
	t.Setenv("QUIKSYNC_AUTH_TOKEN", "from-env")

	cfg, err := buildConfig("/src", "/dst", TransferFlags{
		Compress: "none",
		NoLog:    true,
		Streams:  99,
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.JobID != "default" || cfg.AuthToken != "from-env" || cfg.Tune.Streams != 32 {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
	if _, err := buildConfig("/src", "/dst", TransferFlags{
		Compress: "none", NoLog: true, ChunkSize: "not-a-size",
	}, false); err == nil {
		t.Fatal("expected invalid chunk size")
	}
	if _, err := buildConfig("/src", "/dst", TransferFlags{
		Compress: "none", NoLog: true, BandwidthLimit: -1,
	}, false); err == nil {
		t.Fatal("expected negative bandwidth limit")
	}
}

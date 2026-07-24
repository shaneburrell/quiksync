package engine_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
)

func TestJobEventLog(t *testing.T) {
	src := t.TempDir()
	dst := t.TempDir()
	writeFile(t, filepath.Join(src, "a.txt"), []byte("hello-log"))
	logPath := filepath.Join(t.TempDir(), "job.log")

	stats, err := engine.Run(context.Background(), engine.Config{
		Source:           src,
		Dest:             dst,
		LogFile:          logPath,
		ProgressInterval: 50 * time.Millisecond,
		Tune:             autotune.Config{Enabled: false, Compress: compress.CodecNone, Streams: 1},
	})
	if err != nil {
		t.Fatal(err)
	}
	if stats.FilesCopied != 1 {
		t.Fatalf("copied=%d", stats.FilesCopied)
	}

	b, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	for _, want := range []string{
		"event=job_start",
		"event=probe",
		"event=file_ok",
		"path=a.txt",
		"event=job_end",
		"ok=true",
	} {
		if !strings.Contains(s, want) {
			t.Fatalf("log missing %q:\n%s", want, s)
		}
	}
	latest := filepath.Join(filepath.Dir(logPath), "latest.log")
	lb, err := os.ReadFile(latest)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(lb), "event=job_end") {
		t.Fatalf("latest.log incomplete:\n%s", lb)
	}
}

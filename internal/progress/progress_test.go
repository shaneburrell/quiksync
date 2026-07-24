package progress

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFormatLineQuoting(t *testing.T) {
	line := formatLine(time.Date(2026, 7, 24, 3, 16, 0, 0, time.UTC), "file_ok",
		Str("path", "my file.txt"),
		Int("bytes", 12),
	)
	if !strings.Contains(line, `path="my file.txt"`) {
		t.Fatalf("expected quoted path: %q", line)
	}
	if !strings.HasPrefix(line, "2026-07-24T03:16:00Z event=file_ok") {
		t.Fatalf("prefix: %q", line)
	}
	if !strings.HasSuffix(line, "\n") {
		t.Fatal("missing newline")
	}
}

func TestOpenWritesLatestAndEvents(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "job.log")
	r, err := Open(path, Options{AlsoLatest: true, ProgressToStderr: false})
	if err != nil {
		t.Fatal(err)
	}
	r.JobStart("default", "/src", "/dst", "copy", 2)
	r.FileOK("a.txt", 100, 50, 0, 1, 40*time.Millisecond)
	r.Progress(1, 10, 1, 0, 0, 100, 1000)
	r.JobEnd(true, 1, 0, 0, 0, 100, time.Second)
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}

	for _, p := range []string{path, filepath.Join(dir, "latest.log")} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		s := string(b)
		for _, want := range []string{"event=job_start", "event=file_ok", "event=progress", "event=job_end", `path=a.txt`} {
			if !strings.Contains(s, want) {
				t.Fatalf("%s missing %q in:\n%s", p, want, s)
			}
		}
	}
}

func TestDefaultLogPath(t *testing.T) {
	got := DefaultLogPath("file", "/data/dst", "default", "/cfg")
	if got != filepath.Join("/data/dst", ".quiksync", "logs", "default.log") {
		t.Fatalf("local: %s", got)
	}
	got = DefaultLogPath("ssh", "/remote", "job-1", "/cfg")
	if got != filepath.Join("/cfg", "logs", "job-1.log") {
		t.Fatalf("remote: %s", got)
	}
}

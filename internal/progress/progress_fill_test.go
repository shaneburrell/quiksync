package progress

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReporterPathAndBoolFalse(t *testing.T) {
	var nilR *Reporter
	if nilR.Path() != "" {
		t.Fatal("nil path")
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "j.log")
	var stderr bytes.Buffer
	r, err := Open(path, Options{
		AlsoLatest: true, Stderr: &stderr, MirrorActions: true, ProgressToStderr: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if r.Path() != path {
		t.Fatalf("path %q", r.Path())
	}
	r.Event("probe", Bool("ok", false), ErrField(nil), Str("msg", "hi"))
	r.Progress(1, 2, 0, 0, 0, 1, 2)
	_ = r.Close()
	b, _ := os.ReadFile(path)
	if !strings.Contains(string(b), "ok=false") {
		t.Fatalf("missing bool false: %s", b)
	}
	if stderr.Len() == 0 {
		t.Fatal("expected stderr mirror")
	}
}

func TestOpenEmptyPathAndSanitize(t *testing.T) {
	if _, err := Open("", Options{}); err == nil {
		t.Fatal("expected empty path error")
	}
	got := DefaultLogPath("s3", "bucket/pfx", "../evil", "/cfg")
	base := filepath.Base(got)
	if strings.Contains(base, "/") || strings.Contains(base, `\`) {
		t.Fatalf("path separators in basename: %s", got)
	}
	if !strings.HasSuffix(got, ".log") {
		t.Fatalf("expected .log: %s", got)
	}
}

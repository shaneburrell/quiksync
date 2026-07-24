package progress

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Field is a logfmt key=value pair.
type Field struct {
	Key   string
	Value string
}

func Str(key, v string) Field { return Field{Key: key, Value: v} }
func Int(key string, v int64) Field {
	return Field{Key: key, Value: strconv.FormatInt(v, 10)}
}
func Bool(key string, v bool) Field {
	if v {
		return Field{Key: key, Value: "true"}
	}
	return Field{Key: key, Value: "false"}
}
func ErrField(err error) Field {
	if err == nil {
		return Field{Key: "err", Value: ""}
	}
	return Field{Key: "err", Value: err.Error()}
}

// Reporter writes append-only, tailable job event lines.
type Reporter struct {
	mu       sync.Mutex
	f        *os.File
	latest   *os.File // optional mirror to latest.log
	stderr   io.Writer
	mirror   bool // also write action lines to stderr
	progress bool // write progress lines to stderr
	path     string
}

// Options configures Open.
type Options struct {
	// AlsoLatest opens/replaces latest.log in the same directory as path.
	AlsoLatest bool
	// Stderr receives progress ticker lines and (when MirrorActions) file events.
	Stderr io.Writer
	// MirrorActions mirrors non-progress events to Stderr.
	MirrorActions bool
	// ProgressToStderr prints progress events to Stderr.
	ProgressToStderr bool
}

// Open creates/truncates path for this job run.
func Open(path string, opts Options) (*Reporter, error) {
	if path == "" {
		return nil, fmt.Errorf("empty log path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return nil, err
	}
	r := &Reporter{
		f:        f,
		stderr:   opts.Stderr,
		mirror:   opts.MirrorActions,
		progress: opts.ProgressToStderr,
		path:     path,
	}
	if opts.AlsoLatest {
		latestPath := filepath.Join(filepath.Dir(path), "latest.log")
		// Prefer a real file (portable); truncate each run so tail -f sees a fresh job.
		lf, err := os.OpenFile(latestPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			_ = f.Close()
			return nil, err
		}
		r.latest = lf
	}
	return r, nil
}

// Path returns the primary log file path.
func (r *Reporter) Path() string {
	if r == nil {
		return ""
	}
	return r.path
}

// Event writes one logfmt line. Safe for concurrent use.
func (r *Reporter) Event(event string, fields ...Field) {
	if r == nil {
		return
	}
	line := formatLine(time.Now().UTC(), event, fields...)
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.f == nil {
		return
	}
	_, _ = io.WriteString(r.f, line)
	_ = r.f.Sync() // durable enough for tail -f / AI watchers
	if r.latest != nil {
		_, _ = io.WriteString(r.latest, line)
		_ = r.latest.Sync()
	}
	if r.stderr != nil {
		if event == "progress" {
			if r.progress {
				_, _ = io.WriteString(r.stderr, line)
			}
		} else if r.mirror {
			_, _ = io.WriteString(r.stderr, line)
		}
	}
}

// Close flushes and closes files.
func (r *Reporter) Close() error {
	if r == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var err error
	if r.latest != nil {
		if e := r.latest.Close(); e != nil && err == nil {
			err = e
		}
		r.latest = nil
	}
	if r.f != nil {
		if e := r.f.Close(); e != nil && err == nil {
			err = e
		}
		r.f = nil
	}
	return err
}

func formatLine(ts time.Time, event string, fields ...Field) string {
	var b strings.Builder
	b.WriteString(ts.Format(time.RFC3339))
	b.WriteByte(' ')
	b.WriteString("event=")
	b.WriteString(event)
	for _, f := range fields {
		if f.Key == "" {
			continue
		}
		b.WriteByte(' ')
		b.WriteString(f.Key)
		b.WriteByte('=')
		b.WriteString(quote(f.Value))
	}
	b.WriteByte('\n')
	return b.String()
}

func quote(v string) string {
	if v == "" {
		return `""`
	}
	needs := strings.ContainsAny(v, " \t\"'\\=\n\r")
	if !needs {
		return v
	}
	escaped := strings.ReplaceAll(v, `\`, `\\`)
	escaped = strings.ReplaceAll(escaped, `"`, `\"`)
	escaped = strings.ReplaceAll(escaped, "\n", `\n`)
	escaped = strings.ReplaceAll(escaped, "\r", `\r`)
	return `"` + escaped + `"`
}

// Helpers

func (r *Reporter) JobStart(jobID, src, dest, mode string, streams int) {
	r.Event("job_start",
		Str("job", jobID),
		Str("src", src),
		Str("dest", dest),
		Str("mode", mode),
		Int("streams", int64(streams)),
	)
}

func (r *Reporter) Probe(streams int, compress string, frame, cdc uint32) {
	r.Event("probe",
		Int("streams", int64(streams)),
		Str("compress", compress),
		Int("frame", int64(frame)),
		Int("cdc", int64(cdc)),
	)
}

func (r *Reporter) FileOK(path string, bytes, wired, reused, sent int64, elapsed time.Duration) {
	r.Event("file_ok",
		Str("path", path),
		Int("bytes", bytes),
		Int("wired", wired),
		Int("elapsed_ms", elapsed.Milliseconds()),
		Int("reused", reused),
		Int("sent", sent),
	)
}

func (r *Reporter) FileSkip(path string) {
	r.Event("file_skip", Str("path", path))
}

func (r *Reporter) FileFail(path string, err error, attempts int) {
	r.Event("file_fail",
		Str("path", path),
		ErrField(err),
		Int("attempts", int64(attempts)),
	)
}

func (r *Reporter) Progress(filesDone, filesTotal, copied, skipped, failed, bytes, rateBps int64) {
	r.Event("progress",
		Int("files_done", filesDone),
		Int("files_total", filesTotal),
		Int("copied", copied),
		Int("skipped", skipped),
		Int("failed", failed),
		Int("bytes", bytes),
		Int("rate_bps", rateBps),
	)
}

func (r *Reporter) Delete(path string) {
	r.Event("delete", Str("path", path))
}

func (r *Reporter) DeleteSummary(n int64) {
	r.Event("delete_done", Int("deleted", n))
}

func (r *Reporter) JobEnd(ok bool, copied, skipped, failed, deleted, bytes int64, elapsed time.Duration) {
	r.Event("job_end",
		Bool("ok", ok),
		Int("copied", copied),
		Int("skipped", skipped),
		Int("failed", failed),
		Int("deleted", deleted),
		Int("bytes", bytes),
		Int("elapsed_ms", elapsed.Milliseconds()),
	)
}

func (r *Reporter) Warn(msg string, fields ...Field) {
	fs := append([]Field{Str("msg", msg)}, fields...)
	r.Event("warn", fs...)
}

// DefaultLogPath returns DEST/.quiksync/logs/<jobID>.log for local dest,
// or configDir/logs/<jobID>.log for remote.
func DefaultLogPath(destScheme, destRoot, jobID, configDir string) string {
	if jobID == "" {
		jobID = "default"
	}
	safe := sanitizeJobID(jobID)
	if destScheme == "file" && destRoot != "" {
		return filepath.Join(destRoot, ".quiksync", "logs", safe+".log")
	}
	if configDir == "" {
		configDir = "."
	}
	return filepath.Join(configDir, "logs", safe+".log")
}

func sanitizeJobID(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' {
			b = append(b, c)
		} else {
			b = append(b, '_')
		}
	}
	if len(b) == 0 {
		return "default"
	}
	return string(b)
}

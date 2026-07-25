package engine_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/engine"
)

func TestStableWindowSkipsUnstable(t *testing.T) {
	src, dst := t.TempDir(), t.TempDir()
	writeFile(t, filepath.Join(src, "hot.txt"), []byte("fresh"))
	stats, err := engine.Run(context.Background(), engine.Config{
		Source: src, Dest: dst, StableWindow: time.Hour, MaxFileAttempts: 1, Tune: baseTune(),
	})
	if err == nil {
		t.Fatal("expected unstable failure")
	}
	if stats.FilesFailed != 1 {
		t.Fatalf("stats=%+v", stats)
	}
}

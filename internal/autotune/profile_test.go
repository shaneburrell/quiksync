package autotune

import (
	"path/filepath"
	"testing"

	"github.com/shaneburrell/quiksync/internal/compress"
)

func TestSaveLoadProfile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "host.json")
	p := Profile{Streams: 6, Window: 24, FrameSize: 32 * 1024, Compress: compress.CodecLZ4, Auto: true}
	if err := SaveProfile(path, p); err != nil {
		t.Fatal(err)
	}
	got, err := LoadProfile(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Streams != 6 || got.Compress != compress.CodecLZ4 || got.FrameSize != 32*1024 {
		t.Fatalf("%+v", got)
	}

	tu := New(Config{Enabled: true, Compress: compress.CodecAuto, ProfilePath: path}, "host")
	pr := tu.Profile()
	if pr.Streams != 6 {
		t.Fatalf("warm start streams=%d", pr.Streams)
	}
	tu.profile.Streams = 7
	if err := tu.Save(); err != nil {
		t.Fatal(err)
	}
	loaded, err := LoadProfile(path)
	if err != nil || loaded.Streams != 7 {
		t.Fatalf("save/load: %+v %v", loaded, err)
	}
}

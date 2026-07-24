package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
)

func buildConfig(src, dest string, f TransferFlags, syncMode bool) (engine.Config, error) {
	codec := compress.CodecAuto
	switch strings.ToLower(f.Compress) {
	case "auto", "":
		codec = compress.CodecAuto
	case "none":
		codec = compress.CodecNone
	case "lz4":
		codec = compress.CodecLZ4
	case "zstd":
		codec = compress.CodecZstd
	default:
		return engine.Config{}, fmt.Errorf("invalid --compress %q", f.Compress)
	}

	var chunkAvg uint32
	if f.ChunkSize != "" {
		n, err := parseSize(f.ChunkSize)
		if err != nil {
			return engine.Config{}, err
		}
		chunkAvg = uint32(n)
	}

	return engine.Config{
		Source:          src,
		Dest:            dest,
		SyncMode:        syncMode,
		Delete:          f.Delete,
		Resume:          f.Resume,
		DryRun:          f.DryRun,
		Exclude:         f.Exclude,
		Checksum:        f.Checksum,
		StableWindow:    f.StableWindow,
		BandwidthLimit:  f.BandwidthLimit,
		SkipUnstable:    f.SkipUnstable,
		MaxFileAttempts: f.MaxFileAttempts,
		Tune: autotune.Config{
			Enabled:     f.Auto,
			Streams:     f.Streams,
			Compress:    codec,
			ChunkAvg:    chunkAvg,
			ProfilePath: f.ProfilePath,
		},
		Verbose: verbose,
	}, nil
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "K"), strings.HasSuffix(s, "KB"):
		s = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "K")
		mult = 1024
	case strings.HasSuffix(s, "M"), strings.HasSuffix(s, "MB"):
		s = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "M")
		mult = 1024 * 1024
	case strings.HasSuffix(s, "G"), strings.HasSuffix(s, "GB"):
		s = strings.TrimSuffix(strings.TrimSuffix(s, "B"), "G")
		mult = 1024 * 1024 * 1024
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * mult, nil
}

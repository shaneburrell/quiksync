package cli

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/shaneburrell/quiksync/internal/autotune"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/engine"
	"github.com/shaneburrell/quiksync/internal/progress"
	"github.com/shaneburrell/quiksync/internal/transport"
)

const (
	minChunkSize = 4 * 1024
	maxChunkSize = 16 * 1024 * 1024
)

func buildConfig(src, dest string, f TransferFlags, syncMode bool) (engine.Config, error) {
	var codec compress.Codec
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
		if n < minChunkSize || n > maxChunkSize {
			return engine.Config{}, fmt.Errorf("--chunk-size must be between %d and %d bytes", minChunkSize, maxChunkSize)
		}
		if n > int64(^uint32(0)) {
			return engine.Config{}, fmt.Errorf("--chunk-size too large for uint32")
		}
		chunkAvg = uint32(n)
	}

	streams := f.Streams
	if streams > 32 {
		streams = 32
	}
	authToken := f.AuthToken
	if authToken == "" {
		authToken = os.Getenv("QUIKSYNC_AUTH_TOKEN")
	}
	jobID := f.JobID
	if jobID == "" {
		jobID = "default"
	}
	cfgDir, _ := quiksyncConfigDir()
	cfg := engine.Config{
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
		Insecure:        f.Insecure,
		AuthToken:       authToken,
		JobID:           jobID,
		ConfigDir:       cfgDir,
		Tune: autotune.Config{
			Enabled:     f.Auto,
			Streams:     streams,
			Compress:    codec,
			ChunkAvg:    chunkAvg,
			ProfilePath: f.ProfilePath,
		},
		Verbose:     verbose,
		LogToStderr: verbose,
	}

	if !f.NoLog {
		logPath, err := resolveLogPath(dest, cfg.JobID, f.LogFile)
		if err != nil {
			return engine.Config{}, err
		}
		cfg.LogFile = logPath
		cfg.ProgressInterval = time.Second
	}
	return cfg, nil
}

func resolveLogPath(dest, jobID, override string) (string, error) {
	if override != "" {
		return filepath.Abs(override)
	}
	ep, err := transport.ParseEndpoint(dest)
	if err != nil {
		return "", err
	}
	cfgDir, err := quiksyncConfigDir()
	if err != nil {
		cfgDir = "."
	}
	root := ep.Path
	if ep.Scheme == "file" {
		abs, err := filepath.Abs(root)
		if err == nil {
			root = abs
		}
	}
	return progress.DefaultLogPath(ep.Scheme, root, jobID, cfgDir), nil
}

func quiksyncConfigDir() (string, error) {
	if v := os.Getenv("QUIKSYNC_CONFIG"); v != "" {
		return v, nil
	}
	home, err := os.UserConfigDir()
	if err != nil {
		return "", err
	}
	return filepath.Join(home, "quiksync"), nil
}

func parseSize(s string) (int64, error) {
	s = strings.TrimSpace(strings.ToUpper(s))
	if s == "" {
		return 0, fmt.Errorf("empty size")
	}
	if strings.HasPrefix(s, "-") {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "KIB"):
		s = strings.TrimSuffix(s, "KIB")
		mult = 1024
	case strings.HasSuffix(s, "MIB"):
		s = strings.TrimSuffix(s, "MIB")
		mult = 1024 * 1024
	case strings.HasSuffix(s, "GIB"):
		s = strings.TrimSuffix(s, "GIB")
		mult = 1024 * 1024 * 1024
	case strings.HasSuffix(s, "KB"):
		s = strings.TrimSuffix(s, "KB")
		mult = 1024
	case strings.HasSuffix(s, "MB"):
		s = strings.TrimSuffix(s, "MB")
		mult = 1024 * 1024
	case strings.HasSuffix(s, "GB"):
		s = strings.TrimSuffix(s, "GB")
		mult = 1024 * 1024 * 1024
	case strings.HasSuffix(s, "K"):
		s = strings.TrimSuffix(s, "K")
		mult = 1024
	case strings.HasSuffix(s, "M"):
		s = strings.TrimSuffix(s, "M")
		mult = 1024 * 1024
	case strings.HasSuffix(s, "G"):
		s = strings.TrimSuffix(s, "G")
		mult = 1024 * 1024 * 1024
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("invalid size %q", s)
	}
	return n * mult, nil
}

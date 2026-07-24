package cli

import "time"

// TransferFlags holds shared copy/sync options.
type TransferFlags struct {
	Resume          bool
	DryRun          bool
	Exclude         []string
	Checksum        bool
	StableWindow    time.Duration
	BandwidthLimit  int64
	SkipUnstable    bool
	MaxFileAttempts int
	Auto            bool
	Streams         int
	Compress        string
	ChunkSize       string
	ProfilePath     string
	Delete          bool
	Insecure        bool // labs only: skip QUIC TOFU pin verification
	AuthToken       string
	JobID           string
	LogFile         string
	NoLog           bool
}

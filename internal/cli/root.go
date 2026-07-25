package cli

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

var (
	version = "0.1.0"
	verbose bool
)

func Execute() error {
	return ExecuteArgs(os.Args[1:])
}

// ExecuteArgs runs the CLI with the given arguments (excluding program name).
func ExecuteArgs(args []string) error {
	root := newRoot()
	root.SetArgs(args)
	if err := root.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		return err
	}
	return nil
}

func newRoot() *cobra.Command {
	root := &cobra.Command{
		Use:   "quiksync",
		Short: "Resilient one-way file copy and sync",
		Long: `QuikSync copies and syncs files with content-defined deltas, crash-safe resume,
live-change detection, and runtime autotuning of streams, frame sizes, and compression.`,
		SilenceUsage:  true,
		SilenceErrors: true,
	}

	root.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "verbose logging")
	root.Version = version

	root.AddCommand(newCopyCmd())
	root.AddCommand(newSyncCmd())
	root.AddCommand(newVerifyCmd())
	root.AddCommand(newServeCmd())
	root.AddCommand(newRemoteHelperCmd())
	root.AddCommand(newSendCmd())
	root.AddCommand(newRecvCmd())
	root.AddCommand(newRelayCmd())
	return root
}

func commonTransferFlags(cmd *cobra.Command, opts *TransferFlags) {
	cmd.Flags().BoolVar(&opts.Resume, "resume", true, "resume interrupted transfers from journal")
	cmd.Flags().BoolVar(&opts.DryRun, "dry-run", false, "show what would be transferred")
	cmd.Flags().StringSliceVar(&opts.Exclude, "exclude", nil, "exclude glob patterns")
	cmd.Flags().BoolVar(&opts.Checksum, "checksum", false, "always compare by content checksum")
	cmd.Flags().DurationVar(&opts.StableWindow, "stable-window", 0, "only transfer files unchanged for this duration")
	cmd.Flags().Int64Var(&opts.BandwidthLimit, "bwlimit", 0, "bandwidth limit in bytes/sec (0=unlimited)")
	cmd.Flags().BoolVar(&opts.SkipUnstable, "skip-unstable", false, "skip files that keep changing instead of retrying")
	cmd.Flags().IntVar(&opts.MaxFileAttempts, "max-file-attempts", 5, "max attempts for a mutating file")
	cmd.Flags().BoolVar(&opts.Auto, "auto", true, "autotune streams, frame size, and compression")
	cmd.Flags().IntVar(&opts.Streams, "streams", 0, "pin stream/worker count (0=auto, max 32)")
	cmd.Flags().StringVar(&opts.Compress, "compress", "auto", "compression: auto|none|lz4|zstd")
	cmd.Flags().StringVar(&opts.ChunkSize, "chunk-size", "", "pin CDC average chunk size (e.g. 64K, 1MiB)")
	cmd.Flags().StringVar(&opts.ProfilePath, "profile", "", "path to load/save host autotune profile")
	cmd.Flags().BoolVar(&opts.Insecure, "insecure", false, "skip QUIC TOFU certificate pinning (labs only)")
	cmd.Flags().StringVar(&opts.AuthToken, "auth-token", "", "QUIC daemon auth token (or QUIKSYNC_AUTH_TOKEN)")
	cmd.Flags().StringVar(&opts.JobID, "job-id", "default", "journal/job id for resume isolation")
	cmd.Flags().StringVar(&opts.LogFile, "log-file", "", "job event log path (default: DEST/.quiksync/logs/<job>.log)")
	cmd.Flags().BoolVar(&opts.NoLog, "no-log", false, "disable tailable job event logging")
	cmd.Flags().StringVar(&opts.S3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL (MinIO/R2)")
	cmd.Flags().StringVar(&opts.S3Region, "s3-region", "", "S3 region (or AWS_REGION)")
}

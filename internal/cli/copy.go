package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/shaneburrell/quiksync/internal/engine"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/spf13/cobra"
)

func newCopyCmd() *cobra.Command {
	var flags TransferFlags
	cmd := &cobra.Command{
		Use:   "copy SRC DEST",
		Short: "One-shot copy from source to destination (no deletes)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := buildConfig(args[0], args[1], flags, false)
			if err != nil {
				return err
			}
			if cfg.LogFile != "" {
				fmt.Fprintf(os.Stderr, "logging to %s\n", cfg.LogFile)
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			stats, err := engine.Run(ctx, cfg)
			if err != nil {
				return err
			}
			fmt.Printf("copied %d files (%d bytes), skipped %d, failed %d\n",
				stats.FilesCopied, stats.BytesCopied, stats.FilesSkipped, stats.FilesFailed)
			if stats.FilesFailed > 0 {
				return fmt.Errorf("%d file(s) failed", stats.FilesFailed)
			}
			return nil
		},
	}
	commonTransferFlags(cmd, &flags)
	return cmd
}

func newSyncCmd() *cobra.Command {
	var flags TransferFlags
	cmd := &cobra.Command{
		Use:   "sync SRC DEST",
		Short: "One-way mirror sync (optional --delete)",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg, err := buildConfig(args[0], args[1], flags, true)
			if err != nil {
				return err
			}
			if cfg.LogFile != "" {
				fmt.Fprintf(os.Stderr, "logging to %s\n", cfg.LogFile)
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			stats, err := engine.Run(ctx, cfg)
			if err != nil {
				return err
			}
			fmt.Printf("synced %d files (%d bytes), skipped %d, deleted %d, failed %d\n",
				stats.FilesCopied, stats.BytesCopied, stats.FilesSkipped, stats.FilesDeleted, stats.FilesFailed)
			if stats.FilesFailed > 0 {
				return fmt.Errorf("%d file(s) failed", stats.FilesFailed)
			}
			return nil
		},
	}
	commonTransferFlags(cmd, &flags)
	cmd.Flags().BoolVar(&flags.Delete, "delete", false, "delete destination files absent from source")
	return cmd
}

func newVerifyCmd() *cobra.Command {
	var s3Endpoint, s3Region, authToken string
	var insecure bool
	var exclude []string
	cmd := &cobra.Command{
		Use:   "verify SRC DEST",
		Short: "Compare source and destination digests",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			if authToken == "" {
				authToken = os.Getenv("QUIKSYNC_AUTH_TOKEN")
			}
			opts := transport.OpenOptions{
				Insecure:   insecure,
				AuthToken:  authToken,
				S3Endpoint: s3Endpoint,
				S3Region:   s3Region,
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			mismatches, err := engine.VerifyFiltered(ctx, args[0], args[1], opts, exclude)
			if err != nil {
				return err
			}
			if len(mismatches) == 0 {
				fmt.Println("ok: all compared files match")
				return nil
			}
			for _, m := range mismatches {
				fmt.Println(m)
			}
			return fmt.Errorf("%d mismatch(es)", len(mismatches))
		},
	}
	cmd.Flags().StringSliceVar(&exclude, "exclude", nil, "exclude glob patterns (same as copy/sync)")
	cmd.Flags().StringVar(&s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL (MinIO/R2)")
	cmd.Flags().StringVar(&s3Region, "s3-region", "", "S3 region (or AWS_REGION)")
	cmd.Flags().BoolVar(&insecure, "insecure", false, "skip QUIC TOFU certificate pinning (labs only)")
	cmd.Flags().StringVar(&authToken, "auth-token", "", "QUIC daemon auth token (or QUIKSYNC_AUTH_TOKEN)")
	return cmd
}

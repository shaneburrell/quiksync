package cli

import (
	"context"
	"fmt"

	"github.com/shaneburrell/quiksync/internal/engine"
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
			stats, err := engine.Run(context.Background(), cfg)
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
			stats, err := engine.Run(context.Background(), cfg)
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
	cmd := &cobra.Command{
		Use:   "verify SRC DEST",
		Short: "Compare source and destination digests",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			mismatches, err := engine.Verify(context.Background(), args[0], args[1])
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
	return cmd
}

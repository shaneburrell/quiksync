package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/shaneburrell/quiksync/internal/relay"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/factory"
	"github.com/spf13/cobra"
)

func newSendCmd() *cobra.Command {
	var via, signal, jobID, authToken, s3Endpoint, s3Region string
	var insecure bool
	var ttl time.Duration
	cmd := &cobra.Command{
		Use:   "send SRC --via MID",
		Short: "Publish a tree to a mid-hop store (S3/NFS/file)",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if via == "" {
				return fmt.Errorf("--via is required")
			}
			srcEP, err := transport.ParseEndpoint(args[0])
			if err != nil {
				return err
			}
			midEP, err := transport.ParseEndpoint(via)
			if err != nil {
				return err
			}
			if authToken == "" {
				authToken = os.Getenv("QUIKSYNC_AUTH_TOKEN")
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			opts := transport.OpenOptions{
				Insecure: insecure, AuthToken: authToken,
				S3Endpoint: s3Endpoint, S3Region: s3Region,
			}
			src, err := factory.Open(ctx, srcEP, opts)
			if err != nil {
				return err
			}
			defer func() { _ = src.Close() }()
			mid, err := factory.Open(ctx, midEP, opts)
			if err != nil {
				return err
			}
			defer func() { _ = mid.Close() }()
			sendOpts := relay.SendOptions{JobID: jobID, TTL: ttl}
			if signal != "" {
				sigEP, err := transport.ParseEndpoint(signal)
				if err != nil {
					return err
				}
				sendOpts.Signal = &relay.QuikSyncSignal{Endpoint: sigEP, Insecure: insecure, AuthToken: authToken}
			}
			return relay.Send(ctx, src, mid, sendOpts)
		},
	}
	cmd.Flags().StringVar(&via, "via", "", "mid-hop endpoint (s3://, nfs://, or path)")
	cmd.Flags().StringVar(&signal, "signal", "", "optional quiksync:// or ssh wakeup endpoint")
	cmd.Flags().StringVar(&jobID, "job-id", "default", "relay job id")
	cmd.Flags().DurationVar(&ttl, "ttl", 24*time.Hour, "lease TTL")
	cmd.Flags().BoolVar(&insecure, "insecure", false, "skip QUIC TOFU (labs)")
	cmd.Flags().StringVar(&authToken, "auth-token", "", "QUIC auth token")
	cmd.Flags().StringVar(&s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL (MinIO/R2)")
	cmd.Flags().StringVar(&s3Region, "s3-region", "", "S3 region (or AWS_REGION)")
	_ = cmd.MarkFlagRequired("via")
	return cmd
}

func newRecvCmd() *cobra.Command {
	var via, signal, jobID, authToken, s3Endpoint, s3Region string
	var insecure bool
	var wait time.Duration
	cmd := &cobra.Command{
		Use:   "recv --via MID DEST",
		Short: "Receive a mid-hop job into DEST",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			if via == "" {
				return fmt.Errorf("--via is required")
			}
			dstEP, err := transport.ParseEndpoint(args[0])
			if err != nil {
				return err
			}
			midEP, err := transport.ParseEndpoint(via)
			if err != nil {
				return err
			}
			if authToken == "" {
				authToken = os.Getenv("QUIKSYNC_AUTH_TOKEN")
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			opts := transport.OpenOptions{
				Insecure: insecure, AuthToken: authToken,
				S3Endpoint: s3Endpoint, S3Region: s3Region,
			}
			mid, err := factory.Open(ctx, midEP, opts)
			if err != nil {
				return err
			}
			defer func() { _ = mid.Close() }()
			dst, err := factory.Open(ctx, dstEP, opts)
			if err != nil {
				return err
			}
			defer func() { _ = dst.Close() }()
			recvOpts := relay.RecvOptions{JobID: jobID, Wait: wait}
			if signal != "" {
				sigEP, err := transport.ParseEndpoint(signal)
				if err != nil {
					return err
				}
				recvOpts.Signal = &relay.QuikSyncSignal{Endpoint: sigEP, Insecure: insecure, AuthToken: authToken}
			}
			return relay.Recv(ctx, mid, dst, recvOpts)
		},
	}
	cmd.Flags().StringVar(&via, "via", "", "mid-hop endpoint")
	cmd.Flags().StringVar(&signal, "signal", "", "optional wakeup endpoint")
	cmd.Flags().StringVar(&jobID, "job-id", "default", "relay job id")
	cmd.Flags().DurationVar(&wait, "wait", 30*time.Minute, "max wait for job publish")
	cmd.Flags().BoolVar(&insecure, "insecure", false, "skip QUIC TOFU (labs)")
	cmd.Flags().StringVar(&authToken, "auth-token", "", "QUIC auth token")
	cmd.Flags().StringVar(&s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL (MinIO/R2)")
	cmd.Flags().StringVar(&s3Region, "s3-region", "", "S3 region (or AWS_REGION)")
	_ = cmd.MarkFlagRequired("via")
	return cmd
}

func newRelayCmd() *cobra.Command {
	root := &cobra.Command{
		Use:   "relay",
		Short: "Mid-hop relay maintenance",
	}
	var via, jobID, s3Endpoint, s3Region string
	var force bool
	gc := &cobra.Command{
		Use:   "gc --via MID",
		Short: "Garbage-collect a relay job prefix",
		RunE: func(cmd *cobra.Command, args []string) error {
			if via == "" {
				return fmt.Errorf("--via is required")
			}
			midEP, err := transport.ParseEndpoint(via)
			if err != nil {
				return err
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			mid, err := factory.Open(ctx, midEP, transport.OpenOptions{
				S3Endpoint: s3Endpoint, S3Region: s3Region,
			})
			if err != nil {
				return err
			}
			defer func() { _ = mid.Close() }()
			return relay.GC(ctx, mid, jobID, force)
		},
	}
	gc.Flags().StringVar(&via, "via", "", "mid-hop endpoint")
	gc.Flags().StringVar(&jobID, "job-id", "default", "relay job id")
	gc.Flags().BoolVar(&force, "force", false, "delete even if not acked / lease valid")
	gc.Flags().StringVar(&s3Endpoint, "s3-endpoint", "", "S3-compatible endpoint URL (MinIO/R2)")
	gc.Flags().StringVar(&s3Region, "s3-region", "", "S3 region (or AWS_REGION)")
	_ = gc.MarkFlagRequired("via")
	root.AddCommand(gc)
	return root
}

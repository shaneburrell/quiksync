package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/shaneburrell/quiksync/internal/transport/daemon"
	"github.com/spf13/cobra"
)

func newServeCmd() *cobra.Command {
	var listen string
	var certFile, keyFile, root, authToken string
	var allowNoAuth bool
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run QuikSync QUIC daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			if authToken == "" {
				authToken = os.Getenv("QUIKSYNC_AUTH_TOKEN")
			}
			cfg := daemon.ServeConfig{
				Listen:      listen,
				CertFile:    certFile,
				KeyFile:     keyFile,
				Root:        root,
				AuthToken:   authToken,
				AllowNoAuth: allowNoAuth,
			}
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			fmt.Fprintf(os.Stderr, "quiksync serve starting on %s (root=%s)\n", listen, root)
			return daemon.Serve(ctx, cfg)
		},
	}
	cmd.Flags().StringVar(&listen, "listen", "127.0.0.1:4242", "listen address (default loopback)")
	cmd.Flags().StringVar(&certFile, "cert", "", "TLS certificate file (optional; auto-generated if empty)")
	cmd.Flags().StringVar(&keyFile, "key", "", "TLS key file (optional; auto-generated if empty)")
	cmd.Flags().StringVar(&root, "root", ".", "filesystem root (authoritative; clients cannot override)")
	cmd.Flags().StringVar(&authToken, "auth-token", "", "shared secret required by clients (or QUIKSYNC_AUTH_TOKEN)")
	cmd.Flags().BoolVar(&allowNoAuth, "allow-no-auth", false, "labs only: allow empty auth token")
	return cmd
}

func newRemoteHelperCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "remote-helper",
		Short:  "Stdio remote helper (invoked over SSH)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := cmd.Context()
			if ctx == nil {
				ctx = context.Background()
			}
			opts := daemon.HelperOptions{}
			if tok := os.Getenv("QUIKSYNC_AUTH_TOKEN"); tok != "" {
				opts.AuthToken = tok
			}
			return daemon.RunRemoteHelperOpts(ctx, os.Stdin, os.Stdout, opts)
		},
	}
}

package cli

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/shaneburrell/quiksync/internal/transport/daemon"
)

func newServeCmd() *cobra.Command {
	var listen string
	var certFile, keyFile, root string
	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run QuikSync QUIC daemon",
		RunE: func(cmd *cobra.Command, args []string) error {
			cfg := daemon.ServeConfig{
				Listen:   listen,
				CertFile: certFile,
				KeyFile:  keyFile,
				Root:     root,
			}
			fmt.Fprintf(os.Stderr, "quiksync serve listening on %s (root=%s)\n", listen, root)
			return daemon.Serve(context.Background(), cfg)
		},
	}
	cmd.Flags().StringVar(&listen, "listen", "0.0.0.0:4242", "listen address")
	cmd.Flags().StringVar(&certFile, "cert", "", "TLS certificate file (optional; auto-generated if empty)")
	cmd.Flags().StringVar(&keyFile, "key", "", "TLS key file (optional; auto-generated if empty)")
	cmd.Flags().StringVar(&root, "root", ".", "default filesystem root when client hello omits one")
	return cmd
}

func newRemoteHelperCmd() *cobra.Command {
	return &cobra.Command{
		Use:    "remote-helper",
		Short:  "Stdio remote helper (invoked over SSH)",
		Hidden: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			return daemon.RunRemoteHelper(context.Background(), os.Stdin, os.Stdout)
		},
	}
}

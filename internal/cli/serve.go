package cli

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"

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
			if !isLoopbackListen(listen) && authToken == "" {
				return fmt.Errorf("non-loopback listen %q requires --auth-token", listen)
			}
			if authToken == "" && !allowNoAuth {
				return fmt.Errorf("serve requires --auth-token (or QUIKSYNC_AUTH_TOKEN), or --allow-no-auth for labs")
			}
			cfg := daemon.ServeConfig{
				Listen:      listen,
				CertFile:    certFile,
				KeyFile:     keyFile,
				Root:        root,
				AuthToken:   authToken,
				AllowNoAuth: allowNoAuth,
			}
			fmt.Fprintf(os.Stderr, "quiksync serve listening on %s (root=%s)\n", listen, root)
			return daemon.Serve(context.Background(), cfg)
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

func isLoopbackListen(listen string) bool {
	host, _, err := net.SplitHostPort(listen)
	if err != nil {
		// bare port or invalid — treat as non-loopback
		return false
	}
	host = strings.TrimSpace(host)
	if host == "" || host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
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

package factory

import (
	"context"
	"fmt"

	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/daemon"
	"github.com/shaneburrell/quiksync/internal/transport/local"
	"github.com/shaneburrell/quiksync/internal/transport/nfs"
	"github.com/shaneburrell/quiksync/internal/transport/s3"
	sshxfer "github.com/shaneburrell/quiksync/internal/transport/ssh"
)

// Open dials or opens a Transport for the given endpoint.
func Open(ctx context.Context, ep transport.Endpoint, opts transport.OpenOptions) (transport.Transport, error) {
	switch ep.Scheme {
	case "file":
		return local.New(ep.Path)
	case "ssh":
		return sshxfer.New(ctx, ep)
	case "quiksync":
		return daemon.DialOpts(ctx, ep, daemon.DialOptions{Insecure: opts.Insecure, AuthToken: opts.AuthToken})
	case "s3":
		return s3.New(ctx, ep, s3.Options{
			Endpoint:   opts.S3Endpoint,
			Region:     opts.S3Region,
			StagingDir: opts.StagingDir,
		})
	case "nfs":
		return nfs.New(ctx, ep)
	default:
		return nil, fmt.Errorf("unsupported endpoint scheme %q", ep.Scheme)
	}
}

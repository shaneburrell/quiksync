package relay

import (
	"context"
	"fmt"

	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/daemon"
	sshxfer "github.com/shaneburrell/quiksync/internal/transport/ssh"
)

// QuikSyncSignal sends wakeup-only relay notify over QUIC or SSH helper protocol.
type QuikSyncSignal struct {
	Endpoint  transport.Endpoint
	Insecure  bool
	AuthToken string
}

// Notify dials the peer and sends MsgRelayNotify.
func (q *QuikSyncSignal) Notify(ctx context.Context, jobID string, meta NotifyMeta) error {
	_ = jobID
	switch q.Endpoint.Scheme {
	case "quiksync":
		c, err := daemon.DialOpts(ctx, q.Endpoint, daemon.DialOptions{Insecure: q.Insecure, AuthToken: q.AuthToken})
		if err != nil {
			return err
		}
		defer func() { _ = c.Close() }()
		return c.RelayNotify(ctx, protocol.RelayNotifyMeta{
			JobID: meta.JobID, Via: meta.Via, Generation: meta.Generation,
		})
	case "ssh":
		t, err := sshxfer.New(ctx, q.Endpoint)
		if err != nil {
			return err
		}
		defer func() { _ = t.Close() }()
		return t.RelayNotify(ctx, protocol.RelayNotifyMeta{
			JobID: meta.JobID, Via: meta.Via, Generation: meta.Generation,
		})
	default:
		return fmt.Errorf("signal endpoint must be quiksync:// or ssh")
	}
}

// Wait dials and sends MsgRelayWait (ack only; pair with store poll for truth).
func (q *QuikSyncSignal) Wait(ctx context.Context, jobID string) (NotifyMeta, error) {
	switch q.Endpoint.Scheme {
	case "quiksync":
		c, err := daemon.DialOpts(ctx, q.Endpoint, daemon.DialOptions{Insecure: q.Insecure, AuthToken: q.AuthToken})
		if err != nil {
			return NotifyMeta{}, err
		}
		defer func() { _ = c.Close() }()
		m, err := c.RelayWait(ctx, protocol.RelayNotifyMeta{JobID: jobID})
		if err != nil {
			return NotifyMeta{}, err
		}
		return NotifyMeta{JobID: m.JobID, Via: m.Via, Generation: m.Generation}, nil
	case "ssh":
		t, err := sshxfer.New(ctx, q.Endpoint)
		if err != nil {
			return NotifyMeta{}, err
		}
		defer func() { _ = t.Close() }()
		m, err := t.RelayWait(ctx, protocol.RelayNotifyMeta{JobID: jobID})
		if err != nil {
			return NotifyMeta{}, err
		}
		return NotifyMeta{JobID: m.JobID, Via: m.Via, Generation: m.Generation}, nil
	default:
		return NotifyMeta{}, fmt.Errorf("signal endpoint must be quiksync:// or ssh")
	}
}

package ssh_test

import (
	"context"
	"testing"

	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
	sshxfer "github.com/shaneburrell/quiksync/internal/transport/ssh"
)

func TestSSHRelayWaitAfterNotifyPending(t *testing.T) {
	remoteRoot, restore := setupFakeSSH(t)
	defer restore()

	tr, err := sshxfer.New(context.Background(), transport.Endpoint{
		Scheme: "ssh", User: "u", Host: "h", Path: remoteRoot,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tr.Close() }()

	// Same helper process: notify with no waiters parks a pending wake;
	// subsequent wait returns immediately.
	job := "ssh-pending-wake"
	if err := tr.RelayNotify(context.Background(), protocol.RelayNotifyMeta{JobID: job}); err != nil {
		t.Fatal(err)
	}
	meta, err := tr.RelayWait(context.Background(), protocol.RelayNotifyMeta{JobID: job})
	if err != nil {
		t.Fatal(err)
	}
	if meta.JobID != job {
		t.Fatalf("meta=%+v", meta)
	}
}

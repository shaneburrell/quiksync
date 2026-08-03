package ssh

import (
	"io"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

type discardWC struct{}

func (discardWC) Write(p []byte) (int, error) { return len(p), nil }
func (discardWC) Close() error                { return nil }

func TestCloseWhileReaderHoldsMutex(t *testing.T) {
	pr, pw := io.Pipe()
	tr := &Transport{stdin: discardWC{}, stdout: pr}
	tr.mu.Lock()
	r := &remoteReader{t: tr, locked: true}

	// Keep a large stream open so Close would previously deadlock on mu.
	go func() {
		_ = protocol.WriteMsg(pw, protocol.MsgReadData, []byte("partial-data-chunk"))
		// Never send MsgOK — reader still holds mu until Close drains/errors.
	}()

	done := make(chan error, 1)
	go func() {
		done <- tr.Close()
	}()

	select {
	case <-done:
		// Close must not block forever while reader holds mu.
	case <-time.After(2 * time.Second):
		t.Fatal("Close deadlocked while OpenRead held mu")
	}

	_ = r.Close()
	_ = pw.Close()
}

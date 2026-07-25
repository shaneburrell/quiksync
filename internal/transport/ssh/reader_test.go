package ssh

import (
	"io"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestRemoteReaderPartialThenClose(t *testing.T) {
	pr, pw := io.Pipe()
	tr := &Transport{stdout: pr}
	tr.mu.Lock()
	r := &remoteReader{t: tr, locked: true}

	payload := []byte("abcdefghij")
	go func() {
		_ = protocol.WriteMsg(pw, protocol.MsgReadData, payload)
		_ = protocol.WriteJSON(pw, protocol.MsgOK, protocol.OK{OK: true})
	}()
	buf := make([]byte, 4)
	n, err := r.Read(buf)
	if err != nil || n != 4 || string(buf) != "abcd" {
		t.Fatalf("partial read n=%d err=%v buf=%q", n, err, buf)
	}
	if err := r.Close(); err != nil {
		t.Fatal(err)
	}
	lockDone := make(chan struct{})
	go func() {
		tr.mu.Lock()
		close(lockDone)
		tr.mu.Unlock()
	}()
	select {
	case <-lockDone:
	case <-time.After(2 * time.Second):
		t.Fatal("mutex held after Close")
	}
	_ = pw.Close()
}

func TestRemoteReaderMsgErrSetsEOF(t *testing.T) {
	pr, pw := io.Pipe()
	tr := &Transport{stdout: pr}
	tr.mu.Lock()
	r := &remoteReader{t: tr, locked: true}

	go func() {
		_ = protocol.WriteJSON(pw, protocol.MsgErr, protocol.ErrMsg{Error: "boom"})
	}()
	done := make(chan error, 1)
	go func() {
		_, err := r.Read(make([]byte, 8))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil || err.Error() != "boom" {
			t.Fatalf("want boom, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Read hung on MsgErr")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- r.Close() }()
	select {
	case err := <-closeDone:
		if err != nil && err.Error() != "boom" {
			// Close may return firstErr from prior Read path; nil is also fine.
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close hung after MsgErr")
	}
	// Mutex must be unlocked — next Lock should not block.
	lockDone := make(chan struct{})
	go func() {
		tr.mu.Lock()
		close(lockDone)
		tr.mu.Unlock()
	}()
	select {
	case <-lockDone:
	case <-time.After(2 * time.Second):
		t.Fatal("transport mutex still held after Close")
	}
	_ = pw.Close()
}

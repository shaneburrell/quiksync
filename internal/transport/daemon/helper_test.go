package daemon

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/protocol"
)

func TestOpenReadMissingNoStrayOK(t *testing.T) {
	root := t.TempDir()
	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()
	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperRoot(context.Background(), serverR, serverW, root)
	}()
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: "1", Root: root}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err := protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("hello typ=%v err=%v", typ, err)
	}
	if err := protocol.WriteJSON(clientW, protocol.MsgOpenRead, protocol.PathReq{Rel: "missing.txt"}); err != nil {
		t.Fatal(err)
	}
	typ, _, err := protocol.ReadMsg(clientR)
	if err != nil {
		t.Fatal(err)
	}
	if typ != protocol.MsgErr {
		t.Fatalf("want MsgErr, got %d", typ)
	}
	// Next RPC must still work (no protocol desync / stray MsgOK).
	if err := protocol.WriteJSON(clientW, protocol.MsgWalk, protocol.WalkReq{}); err != nil {
		t.Fatal(err)
	}
	typ, _, err = protocol.ReadMsg(clientR)
	if err != nil || typ != protocol.MsgWalkOK {
		t.Fatalf("walk after err: typ=%d err=%v", typ, err)
	}
	_ = protocol.WriteMsg(clientW, protocol.MsgBye, nil)
	_ = clientW.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}
}

func TestRemoteHelperCopy(t *testing.T) {
	root := t.TempDir()
	srcFile := filepath.Join(root, "in.txt")
	data := bytes.Repeat([]byte("helper-test\n"), 200)
	if err := os.WriteFile(srcFile, data, 0o644); err != nil {
		t.Fatal(err)
	}

	clientR, serverW := io.Pipe()
	serverR, clientW := io.Pipe()

	errCh := make(chan error, 1)
	go func() {
		errCh <- RunRemoteHelperRoot(context.Background(), serverR, serverW, root)
	}()

	// hello
	if err := protocol.WriteJSON(clientW, protocol.MsgHello, protocol.Hello{Version: "1", Root: root}); err != nil {
		t.Fatal(err)
	}
	typ, _, err := protocol.ReadMsg(clientR)
	if err != nil || typ != protocol.MsgHelloOK {
		t.Fatalf("hello: typ=%d err=%v", typ, err)
	}

	// read source via helper
	if err := protocol.WriteJSON(clientW, protocol.MsgOpenRead, protocol.PathReq{Rel: "in.txt"}); err != nil {
		t.Fatal(err)
	}
	var got []byte
	for {
		typ, payload, err := protocol.ReadMsg(clientR)
		if err != nil {
			t.Fatal(err)
		}
		if typ == protocol.MsgOK {
			break
		}
		if typ != protocol.MsgReadData {
			t.Fatalf("typ %d", typ)
		}
		got = append(got, payload...)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("read mismatch")
	}

	// write out.txt
	sig, err := chunk.ChunkReader(bytes.NewReader(data), int64(len(data)), chunk.Options{KeepData: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := protocol.WriteJSON(clientW, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "out.txt", Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatalf("begin write")
	}
	for _, c := range sig.Chunks {
		codec, payload, err := compress.Encode(compress.CodecLZ4, c.Data)
		if err != nil {
			t.Fatal(err)
		}
		if err := protocol.WriteJSON(clientW, protocol.MsgWriteChunk, protocol.WriteChunkReq{
			Offset: c.Offset, Codec: codec, UncompressedLen: len(c.Data), Data: payload,
		}); err != nil {
			t.Fatal(err)
		}
		if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
			t.Fatalf("write chunk")
		}
	}
	if err := protocol.WriteJSON(clientW, protocol.MsgCommit, protocol.CommitReq{
		Digest: sig.Digest, Mode: 0o644, ModNano: time.Now().UnixNano(),
	}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatalf("commit failed typ=%d err=%v", typ, err)
	}

	// mkdir + remove round-trip
	if err := protocol.WriteJSON(clientW, protocol.MsgMkdir, protocol.PathReq{Rel: "tmpdir"}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatalf("mkdir typ=%d err=%v", typ, err)
	}
	if err := protocol.WriteJSON(clientW, protocol.MsgRemove, protocol.PathReq{Rel: "out.txt"}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatalf("remove typ=%d err=%v", typ, err)
	}
	// abort an in-progress write
	if err := protocol.WriteJSON(clientW, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "abort.me", Size: 3}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatal("begin abort")
	}
	_ = protocol.WriteMsg(clientW, protocol.MsgAbort, nil)
	if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatal("abort")
	}
	// rewrite for final assert below
	if err := protocol.WriteJSON(clientW, protocol.MsgBeginWrite, protocol.BeginWriteReq{Rel: "out.txt", Size: int64(len(data))}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatal("begin rewrite")
	}
	for _, c := range sig.Chunks {
		if err := protocol.WriteJSON(clientW, protocol.MsgWriteChunk, protocol.WriteChunkReq{
			Offset: c.Offset, Codec: compress.CodecNone, UncompressedLen: len(c.Data), Data: c.Data,
		}); err != nil {
			t.Fatal(err)
		}
		if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
			t.Fatal("rewrite chunk")
		}
	}
	if err := protocol.WriteJSON(clientW, protocol.MsgCommit, protocol.CommitReq{
		Digest: sig.Digest, Mode: 0o644, ModNano: time.Now().UnixNano(),
	}); err != nil {
		t.Fatal(err)
	}
	if typ, _, err = protocol.ReadMsg(clientR); err != nil || typ != protocol.MsgOK {
		t.Fatal("rewrite commit")
	}

	_ = protocol.WriteMsg(clientW, protocol.MsgBye, nil)
	_ = clientW.Close()
	_ = clientR.Close()
	select {
	case <-errCh:
	case <-time.After(2 * time.Second):
		t.Fatal("helper timeout")
	}

	out, err := os.ReadFile(filepath.Join(root, "out.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(out, data) {
		t.Fatalf("dest mismatch")
	}
}

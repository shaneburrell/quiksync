package daemon

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/local"
)

// RunRemoteHelper serves the framed protocol over r/w against a local root.
// Root is taken from Hello message, falling back to defaultRoot / QUIKSYNC_ROOT / ".".
func RunRemoteHelper(ctx context.Context, r io.Reader, w io.Writer) error {
	return RunRemoteHelperRoot(ctx, r, w, "")
}

// RunRemoteHelperRoot is like RunRemoteHelper with an explicit default root.
func RunRemoteHelperRoot(ctx context.Context, r io.Reader, w io.Writer, defaultRoot string) error {
	typ, payload, err := protocol.ReadMsg(r)
	if err != nil {
		return err
	}
	if typ != protocol.MsgHello {
		return fmt.Errorf("expected hello")
	}
	var hello protocol.Hello
	if err := protocol.DecodeJSON(payload, &hello); err != nil {
		return err
	}
	root := hello.Root
	if root == "" {
		root = defaultRoot
	}
	if root == "" {
		if v := os.Getenv("QUIKSYNC_ROOT"); v != "" {
			root = v
		} else {
			root = "."
		}
	}
	lt, err := local.New(root)
	if err != nil {
		_ = protocol.WriteJSON(w, protocol.MsgErr, protocol.ErrMsg{Error: err.Error()})
		return err
	}
	defer func() { _ = lt.Close() }()
	if err := protocol.WriteJSON(w, protocol.MsgHelloOK, protocol.Hello{Version: "1", Root: lt.Root()}); err != nil {
		return err
	}

	var session transport.WriteSession
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		typ, payload, err := protocol.ReadMsg(r)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		switch typ {
		case protocol.MsgBye:
			return nil
		case protocol.MsgWalk:
			var req protocol.WalkReq
			_ = protocol.DecodeJSON(payload, &req)
			files, err := lt.Walk(ctx, req.Exclude)
			if err != nil {
				_ = writeErr(w, err)
				continue
			}
			out := protocol.WalkOK{}
			for _, f := range files {
				out.Files = append(out.Files, protocol.FileMeta{
					RelPath: f.RelPath, Size: f.Size, ModNano: f.ModTime.UnixNano(), Mode: uint32(f.Mode),
				})
			}
			_ = protocol.WriteJSON(w, protocol.MsgWalkOK, out)
		case protocol.MsgStat:
			var req protocol.PathReq
			_ = protocol.DecodeJSON(payload, &req)
			f, err := lt.Stat(ctx, req.Rel)
			if err != nil {
				_ = writeErr(w, err)
				continue
			}
			_ = protocol.WriteJSON(w, protocol.MsgStatOK, protocol.FileMeta{
				RelPath: f.RelPath, Size: f.Size, ModNano: f.ModTime.UnixNano(), Mode: uint32(f.Mode),
			})
		case protocol.MsgOpenRead:
			var req protocol.PathReq
			_ = protocol.DecodeJSON(payload, &req)
			rc, err := lt.OpenRead(ctx, req.Rel)
			if err != nil {
				_ = writeErr(w, err)
				continue
			}
			buf := make([]byte, 64*1024)
			for {
				n, err := rc.Read(buf)
				if n > 0 {
					if err := protocol.WriteMsg(w, protocol.MsgReadData, buf[:n]); err != nil {
						_ = rc.Close()
						return err
					}
				}
				if err == io.EOF {
					break
				}
				if err != nil {
					_ = rc.Close()
					_ = writeErr(w, err)
					break
				}
			}
			_ = rc.Close()
			_ = protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true})
		case protocol.MsgBeginWrite:
			var req protocol.BeginWriteReq
			_ = protocol.DecodeJSON(payload, &req)
			if session != nil {
				_ = session.Abort()
			}
			session, err = lt.BeginWrite(ctx, req.Rel, req.Size)
			if err != nil {
				_ = writeErr(w, err)
				continue
			}
			_ = protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true})
		case protocol.MsgWriteChunk:
			if session == nil {
				_ = writeErr(w, fmt.Errorf("no write session"))
				continue
			}
			var req protocol.WriteChunkReq
			_ = protocol.DecodeJSON(payload, &req)
			if err := session.WriteChunk(ctx, req.Offset, req.Codec, req.UncompressedLen, req.Data); err != nil {
				_ = writeErr(w, err)
				continue
			}
			_ = protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true})
		case protocol.MsgCommit:
			if session == nil {
				_ = writeErr(w, fmt.Errorf("no write session"))
				continue
			}
			var req protocol.CommitReq
			_ = protocol.DecodeJSON(payload, &req)
			if err := session.Commit(ctx, req.Digest, os.FileMode(req.Mode), time.Unix(0, req.ModNano)); err != nil {
				_ = writeErr(w, err)
				session = nil
				continue
			}
			session = nil
			_ = protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true})
		case protocol.MsgAbort:
			if session != nil {
				_ = session.Abort()
				session = nil
			}
			_ = protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true})
		case protocol.MsgGetSig:
			var req protocol.PathReq
			_ = protocol.DecodeJSON(payload, &req)
			sig, err := lt.GetSignature(ctx, req.Rel)
			if err != nil {
				_ = writeErr(w, err)
				continue
			}
			_ = protocol.WriteJSON(w, protocol.MsgSigOK, protocol.SigOK{Size: sig.Size, Digest: sig.Digest, Chunks: sig.Chunks})
		case protocol.MsgRemove:
			var req protocol.PathReq
			_ = protocol.DecodeJSON(payload, &req)
			if err := lt.Remove(ctx, req.Rel); err != nil {
				_ = writeErr(w, err)
				continue
			}
			_ = protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true})
		case protocol.MsgMkdir:
			var req protocol.PathReq
			_ = protocol.DecodeJSON(payload, &req)
			if err := lt.MkdirAll(ctx, req.Rel); err != nil {
				_ = writeErr(w, err)
				continue
			}
			_ = protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true})
		case protocol.MsgPeerStats:
			_ = protocol.WriteJSON(w, protocol.MsgPeerStats, protocol.PeerStats{At: time.Now().UTC(), CPUPercent: 10})
		case protocol.MsgTuneOffer, protocol.MsgTuneApply:
			_ = protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true})
		default:
			_ = writeErr(w, fmt.Errorf("unknown message %d", typ))
		}
	}
}

func writeErr(w io.Writer, err error) error {
	return protocol.WriteJSON(w, protocol.MsgErr, protocol.ErrMsg{Error: err.Error()})
}

// ServeConfig configures the QUIC daemon.
type ServeConfig struct {
	Listen   string
	CertFile string
	KeyFile  string
	Root     string
}

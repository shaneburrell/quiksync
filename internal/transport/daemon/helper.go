package daemon

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/shaneburrell/quiksync/internal/protocol"
	"github.com/shaneburrell/quiksync/internal/transport"
	"github.com/shaneburrell/quiksync/internal/transport/local"
)

// HelperOptions configures the remote helper session.
type HelperOptions struct {
	DefaultRoot string
	// AuthToken, when non-empty, must match Hello.AuthToken (constant-time).
	AuthToken string
}

// RunRemoteHelper serves the framed protocol over r/w against a local root.
// Root is taken from Hello message, falling back to QUIKSYNC_ROOT / ".".
func RunRemoteHelper(ctx context.Context, r io.Reader, w io.Writer) error {
	return RunRemoteHelperOpts(ctx, r, w, HelperOptions{})
}

// RunRemoteHelperRoot is like RunRemoteHelper with an explicit default root.
func RunRemoteHelperRoot(ctx context.Context, r io.Reader, w io.Writer, defaultRoot string) error {
	return RunRemoteHelperOpts(ctx, r, w, HelperOptions{DefaultRoot: defaultRoot})
}

func authTokenOK(got, want string) bool {
	sumGot := sha256.Sum256([]byte(got))
	sumWant := sha256.Sum256([]byte(want))
	return subtle.ConstantTimeCompare(sumGot[:], sumWant[:]) == 1
}

// RunRemoteHelperOpts serves the framed protocol with options.
func RunRemoteHelperOpts(ctx context.Context, r io.Reader, w io.Writer, opts HelperOptions) error {
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
	if err := protocol.CheckPeerVersion(hello.Version); err != nil {
		_ = writeErr(w, err)
		return err
	}
	if opts.AuthToken != "" {
		if !authTokenOK(hello.AuthToken, opts.AuthToken) {
			_ = writeErr(w, fmt.Errorf("authentication failed"))
			return fmt.Errorf("authentication failed")
		}
	}
	// Daemon (--root / defaultRoot): always use server root; ignore client absolute Root.
	// SSH remote-helper (no defaultRoot): use Hello.Root from client.
	root := opts.DefaultRoot
	if root == "" {
		root = hello.Root
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
	if err := protocol.WriteJSON(w, protocol.MsgHelloOK, protocol.HelloOK{
		Version: protocol.ProtocolVersion,
		Root:    lt.Root(),
		Caps:    protocol.DefaultCaps(),
	}); err != nil {
		return err
	}

	var session transport.WriteSession
	defer func() {
		if session != nil {
			_ = session.Abort()
			session = nil
		}
	}()

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
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			files, err := lt.Walk(ctx, req.Exclude)
			if err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			out := protocol.WalkOK{}
			for _, f := range files {
				out.Files = append(out.Files, protocol.FileMeta{
					RelPath: f.RelPath, Size: f.Size, ModNano: f.ModTime.UnixNano(), Mode: uint32(f.Mode),
				})
			}
			if err := protocol.WriteJSON(w, protocol.MsgWalkOK, out); err != nil {
				return err
			}
		case protocol.MsgStat:
			var req protocol.PathReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			f, err := lt.Stat(ctx, req.Rel)
			if err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := protocol.WriteJSON(w, protocol.MsgStatOK, protocol.FileMeta{
				RelPath: f.RelPath, Size: f.Size, ModNano: f.ModTime.UnixNano(), Mode: uint32(f.Mode),
			}); err != nil {
				return err
			}
		case protocol.MsgOpenRead:
			var req protocol.PathReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			rc, err := lt.OpenRead(ctx, req.Rel)
			if err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			buf := make([]byte, 64*1024)
			readErr := error(nil)
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
					readErr = err
					break
				}
			}
			_ = rc.Close()
			if readErr != nil {
				if err := writeErr(w, readErr); err != nil {
					return err
				}
				continue // do not send MsgOK after MsgErr
			}
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgBeginWrite:
			var req protocol.BeginWriteReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if session != nil {
				_ = session.Abort()
				session = nil
			}
			session, err = lt.BeginWrite(ctx, req.Rel, req.Size)
			if err != nil {
				session = nil
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgWriteChunk:
			if session == nil {
				if err := writeErr(w, fmt.Errorf("no write session")); err != nil {
					return err
				}
				continue
			}
			var req protocol.WriteChunkReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := session.WriteChunk(ctx, req.Offset, req.Codec, req.UncompressedLen, req.Data); err != nil {
				_ = session.Abort()
				session = nil
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgReuseChunk:
			if session == nil {
				if err := writeErr(w, fmt.Errorf("no write session")); err != nil {
					return err
				}
				continue
			}
			var req protocol.ReuseChunkReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			// Early reject before session allocate; oldSize unknown at protocol layer.
			if err := transport.ValidateReuseRange(req.OldOffset, req.Length, -1); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := session.ReuseChunk(ctx, req.NewOffset, req.OldOffset, req.Digest, req.Length); err != nil {
				_ = session.Abort()
				session = nil
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgRelayNotify:
			// Wakeup-only: wake local waiters; receivers still verify mid-store state.
			var meta protocol.RelayNotifyMeta
			_ = protocol.DecodeJSON(payload, &meta)
			if err := validateRelayJobID(meta.JobID); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			relayWake(meta.JobID)
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgRelayWait:
			var meta protocol.RelayNotifyMeta
			if err := protocol.DecodeJSON(payload, &meta); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := validateRelayJobID(meta.JobID); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := relayWaitJob(ctx, meta.JobID); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := protocol.WriteJSON(w, protocol.MsgRelayWaitOK, meta); err != nil {
				return err
			}
		case protocol.MsgCommit:
			if session == nil {
				if err := writeErr(w, fmt.Errorf("no write session")); err != nil {
					return err
				}
				continue
			}
			var req protocol.CommitReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := session.Commit(ctx, req.Digest, os.FileMode(req.Mode), time.Unix(0, req.ModNano)); err != nil {
				_ = session.Abort()
				session = nil
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			session = nil
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgAbort:
			if session != nil {
				_ = session.Abort()
				session = nil
			}
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgGetSig:
			var req protocol.PathReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			sig, err := lt.GetSignature(ctx, req.Rel)
			if err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := protocol.WriteJSON(w, protocol.MsgSigOK, protocol.SigOK{Size: sig.Size, Digest: sig.Digest, Chunks: sig.Chunks}); err != nil {
				return err
			}
		case protocol.MsgRemove:
			var req protocol.PathReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := lt.Remove(ctx, req.Rel); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgMkdir:
			var req protocol.PathReq
			if err := protocol.DecodeJSON(payload, &req); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := lt.MkdirAll(ctx, req.Rel); err != nil {
				if err := writeErr(w, err); err != nil {
					return err
				}
				continue
			}
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		case protocol.MsgPeerStats:
			if err := protocol.WriteJSON(w, protocol.MsgPeerStats, protocol.PeerStats{At: time.Now().UTC(), CPUPercent: 10}); err != nil {
				return err
			}
		case protocol.MsgTuneOffer, protocol.MsgTuneApply:
			if err := protocol.WriteJSON(w, protocol.MsgOK, protocol.OK{OK: true}); err != nil {
				return err
			}
		default:
			if err := writeErr(w, fmt.Errorf("unknown message %d", typ)); err != nil {
				return err
			}
		}
	}
}

func writeErr(w io.Writer, err error) error {
	return protocol.WriteJSON(w, protocol.MsgErr, protocol.ErrMsg{Error: err.Error()})
}

// ServeConfig configures the QUIC daemon.
type ServeConfig struct {
	Listen      string
	CertFile    string
	KeyFile     string
	Root        string
	AuthToken   string
	AllowNoAuth bool // labs only: permit empty AuthToken on loopback
}

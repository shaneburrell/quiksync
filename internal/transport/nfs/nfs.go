// Package nfs provides an experimental NFSv3 (AUTH_SYS) transport.
package nfs

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/shaneburrell/quiksync/internal/chunk"
	"github.com/shaneburrell/quiksync/internal/compress"
	"github.com/shaneburrell/quiksync/internal/transport"
	gonfs "github.com/vmware/go-nfs-client/nfs"
	nfsrpc "github.com/vmware/go-nfs-client/nfs/rpc"
)

// Transport is an experimental NFSv3 (AUTH_SYS) transport.
//
// URI form: nfs://host[:port]/export[/subdir…]
// The longest prefix of Path that the server accepts as an export is mounted;
// any remaining components become the working subdirectory inside that export.
// Example: nfs://nas/export/backup mounts /export with base "backup".
type Transport struct {
	ep     transport.Endpoint
	target *gonfs.Target
	mount  *gonfs.Mount
	auth   nfsrpc.Auth
	export string // mounted export path, e.g. "/export"
	base   string // optional subdir within export, e.g. "backup"
}

// splitExportCandidates returns mount candidates from longest to shortest.
// For path /export/backup → ["/export/backup", "/export"].
func splitExportCandidates(pathStr string) []string {
	full := strings.Trim(pathStr, "/")
	if full == "" {
		return nil
	}
	parts := strings.Split(full, "/")
	out := make([]string, 0, len(parts))
	for i := len(parts); i >= 1; i-- {
		out = append(out, "/"+strings.Join(parts[:i], "/"))
	}
	return out
}

// baseAfterExport returns the subdirectory inside export for the original path.
func baseAfterExport(pathStr, export string) string {
	full := "/" + strings.Trim(pathStr, "/")
	export = path.Clean("/" + strings.Trim(export, "/"))
	if full == export {
		return ""
	}
	prefix := export + "/"
	if !strings.HasPrefix(full, prefix) {
		return ""
	}
	return strings.TrimPrefix(full, prefix)
}

// New dials NFSv3 and mounts the export, using any trailing path as a subdirectory.
func New(ctx context.Context, ep transport.Endpoint) (*Transport, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ep.Host == "" {
		return nil, fmt.Errorf("nfs: missing host")
	}
	// go-nfs-client resolves mount/NFS ports via portmapper on the host; a
	// non-default URI port cannot be honored without a different dial path.
	if ep.Port != "" && ep.Port != "2049" {
		return nil, fmt.Errorf("nfs: custom port %q not supported (client uses portmapper on host)", ep.Port)
	}
	candidates := splitExportCandidates(ep.Path)
	if len(candidates) == 0 {
		return nil, fmt.Errorf("nfs: export path required (nfs://host/export[/subdir])")
	}
	mount, err := gonfs.DialMount(ep.Host)
	if err != nil {
		return nil, fmt.Errorf("nfs dial mount (experimental NFSv3/AUTH_SYS): %w", err)
	}
	auth := nfsrpc.NewAuthUnix("quiksync", 0, 0).Auth()

	var (
		target  *gonfs.Target
		export  string
		lastErr error
	)
	// Try longest path first so nfs://host/export/backup mounts /export when
	// /export/backup is not itself an export.
	for _, tryExport := range candidates {
		if err := ctx.Err(); err != nil {
			_ = mount.Close()
			return nil, err
		}
		t, err := mount.Mount(tryExport, auth)
		if err != nil {
			lastErr = err
			continue
		}
		target = t
		export = tryExport
		break
	}
	if target == nil {
		_ = mount.Close()
		if lastErr == nil {
			lastErr = fmt.Errorf("no export matched")
		}
		return nil, fmt.Errorf("nfs mount %q: %w", ep.Path, lastErr)
	}

	tr := &Transport{
		ep: ep, target: target, mount: mount, auth: auth, export: export,
		base: baseAfterExport(ep.Path, export),
	}
	if err := tr.ensureBase(); err != nil {
		_ = tr.Close()
		return nil, err
	}
	return tr, nil
}

func (t *Transport) ensureBase() error {
	if t.base == "" {
		return nil
	}
	return t.mkdirMount(t.base)
}

func (t *Transport) Caps() transport.Caps {
	return transport.Caps{
		SupportsDelta:      true,
		SupportsMultiplex:  false,
		SupportsResume:     true,
		SupportsReuseChunk: true,
	}
}

func (t *Transport) Close() error {
	if t.mount != nil {
		_ = t.mount.Unmount()
		return t.mount.Close()
	}
	return nil
}

func (t *Transport) Root() string { return t.ep.Raw }

// joinBase maps a user-facing relative path to a path inside the NFS mount.
func (t *Transport) joinBase(rel string) (string, error) {
	rel = filepath.ToSlash(rel)
	rel = path.Clean("/" + rel)
	rel = strings.TrimPrefix(rel, "/")
	if rel == "." {
		rel = ""
	}
	if strings.Contains(rel, "..") {
		return "", fmt.Errorf("nfs: invalid path %q", rel)
	}
	if t.base == "" {
		return rel, nil
	}
	if rel == "" {
		return t.base, nil
	}
	return t.base + "/" + rel, nil
}

// userRel strips the base prefix from a mount-relative path.
func (t *Transport) userRel(mountRel string) string {
	if t.base == "" {
		return mountRel
	}
	if mountRel == t.base {
		return ""
	}
	prefix := t.base + "/"
	if strings.HasPrefix(mountRel, prefix) {
		return strings.TrimPrefix(mountRel, prefix)
	}
	return mountRel
}

func (t *Transport) Walk(ctx context.Context, exclude []string) ([]transport.FileMeta, error) {
	_ = ctx
	var out []transport.FileMeta
	start := t.base
	err := t.walkDir(start, &out, exclude)
	return out, err
}

func (t *Transport) walkDir(dir string, out *[]transport.FileMeta, exclude []string) error {
	entries, err := t.target.ReadDirPlus(dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		name := e.FileName
		if name == "." || name == ".." || name == ".quiksync.tmp" {
			continue
		}
		mountRel := name
		if dir != "" {
			mountRel = dir + "/" + name
		}
		rel := t.userRel(mountRel)
		if rel == "" {
			continue
		}
		if strings.HasPrefix(rel, ".quiksync/") || strings.HasPrefix(filepath.Base(rel), ".quiksync") {
			continue
		}
		if matchExclude(rel, exclude) {
			continue
		}
		if e.IsDir() {
			if err := t.walkDir(mountRel, out, exclude); err != nil {
				return err
			}
			continue
		}
		attr := e.Attr.Attr
		*out = append(*out, transport.FileMeta{
			RelPath: rel,
			Size:    int64(attr.Filesize),
			ModTime: time.Unix(int64(attr.Mtime.Seconds), 0),
			Mode:    0o644,
		})
	}
	return nil
}

func matchExclude(rel string, patterns []string) bool {
	for _, p := range patterns {
		ok, err := filepath.Match(p, rel)
		if err == nil && ok {
			return true
		}
	}
	return false
}

func (t *Transport) Stat(ctx context.Context, rel string) (transport.FileMeta, error) {
	_ = ctx
	userRel := rel
	mountRel, err := t.joinBase(rel)
	if err != nil {
		return transport.FileMeta{}, err
	}
	fi, _, err := t.target.Lookup(mountRel)
	if err != nil {
		return transport.FileMeta{}, err
	}
	return transport.FileMeta{
		RelPath: userRel,
		Size:    fi.Size(),
		ModTime: fi.ModTime(),
		Mode:    0o644,
	}, nil
}

func (t *Transport) OpenRead(ctx context.Context, rel string) (io.ReadCloser, error) {
	_ = ctx
	mountRel, err := t.joinBase(rel)
	if err != nil {
		return nil, err
	}
	return t.target.Open(mountRel)
}

func (t *Transport) Remove(ctx context.Context, rel string) error {
	_ = ctx
	mountRel, err := t.joinBase(rel)
	if err != nil {
		return err
	}
	return t.target.Remove(mountRel)
}

func (t *Transport) MkdirAll(ctx context.Context, rel string) error {
	_ = ctx
	mountRel, err := t.joinBase(rel)
	if err != nil {
		return err
	}
	return t.mkdirMount(mountRel)
}

func (t *Transport) mkdirMount(mountRel string) error {
	if mountRel == "" || mountRel == "." {
		return nil
	}
	parts := strings.Split(mountRel, "/")
	cur := ""
	for _, p := range parts {
		if p == "" {
			continue
		}
		if cur == "" {
			cur = p
		} else {
			cur = cur + "/" + p
		}
		if _, _, err := t.target.Lookup(cur); err == nil {
			continue
		}
		if _, err := t.target.Mkdir(cur, 0o755); err != nil {
			if _, _, err2 := t.target.Lookup(cur); err2 != nil {
				return err
			}
		}
	}
	return nil
}

func (t *Transport) GetSignature(ctx context.Context, rel string) (chunk.FileSignature, error) {
	rc, err := t.OpenRead(ctx, rel)
	if err != nil {
		return chunk.FileSignature{}, nil
	}
	defer func() { _ = rc.Close() }()
	st, err := t.Stat(ctx, rel)
	if err != nil {
		return chunk.FileSignature{}, err
	}
	return chunk.ChunkReader(rc, st.Size, chunk.Options{})
}

type writeSession struct {
	t         *Transport
	userRel   string
	mountRel  string
	tmpRel    string
	staging   *gonfs.File
	old       *gonfs.File
	oldSize   int64
	oldMod    int64
	size      int64
	committed bool
}

func partialName(rel string) string {
	sum := sha256.Sum256([]byte(filepath.ToSlash(rel)))
	return hex.EncodeToString(sum[:8]) + ".partial"
}

func (t *Transport) BeginWrite(ctx context.Context, rel string, size int64) (transport.WriteSession, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	mountRel, err := t.joinBase(rel)
	if err != nil {
		return nil, err
	}
	if dir := path.Dir(rel); dir != "." && dir != "" {
		if err := t.MkdirAll(ctx, dir); err != nil {
			return nil, err
		}
	}
	tmpDirUser := ".quiksync.tmp"
	if err := t.MkdirAll(ctx, tmpDirUser); err != nil {
		return nil, err
	}
	tmpRel, err := t.joinBase(tmpDirUser + "/" + partialName(rel))
	if err != nil {
		return nil, err
	}
	_ = t.target.Remove(tmpRel)
	staging, err := t.target.OpenFile(tmpRel, 0o644)
	if err != nil {
		return nil, fmt.Errorf("nfs staging create: %w", err)
	}
	ws := &writeSession{
		t: t, userRel: rel, mountRel: mountRel, tmpRel: tmpRel,
		staging: staging, size: size,
	}
	if st, err := t.Stat(ctx, rel); err == nil {
		if old, err := t.target.Open(mountRel); err == nil {
			ws.old = old
			ws.oldSize = st.Size
			ws.oldMod = st.ModTime.UnixNano()
		}
	}
	return ws, nil
}

func (w *writeSession) WriteChunk(ctx context.Context, offset uint64, codec compress.Codec, uncompressedLen int, data []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.committed {
		return fmt.Errorf("write after commit")
	}
	if w.staging == nil {
		return fmt.Errorf("write: no staging file")
	}
	raw, err := compress.Decode(codec, data, uncompressedLen)
	if err != nil {
		return err
	}
	if _, err := w.staging.Seek(int64(offset), io.SeekStart); err != nil {
		return err
	}
	if _, err := w.staging.Write(raw); err != nil {
		return err
	}
	return nil
}

func (w *writeSession) ReuseChunk(ctx context.Context, newOffset, oldOffset uint64, digest chunk.Digest, length int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.committed {
		return fmt.Errorf("reuse after commit")
	}
	if w.old == nil {
		return fmt.Errorf("reuse: no existing file")
	}
	if length < 0 {
		return fmt.Errorf("reuse: negative length")
	}
	st, err := w.t.Stat(ctx, w.userRel)
	if err != nil {
		return fmt.Errorf("reuse TOCTOU: %w", err)
	}
	if st.Size != w.oldSize || st.ModTime.UnixNano() != w.oldMod {
		return fmt.Errorf("reuse TOCTOU: dest changed")
	}
	if int64(oldOffset)+int64(length) > w.oldSize {
		return fmt.Errorf("reuse: old range out of bounds")
	}
	piece := make([]byte, length)
	if _, err := w.old.Seek(int64(oldOffset), io.SeekStart); err != nil {
		return err
	}
	if _, err := io.ReadFull(w.old, piece); err != nil {
		return fmt.Errorf("reuse read: %w", err)
	}
	if chunk.Sum(piece) != digest {
		return fmt.Errorf("reuse digest mismatch")
	}
	if _, err := w.staging.Seek(int64(newOffset), io.SeekStart); err != nil {
		return err
	}
	if _, err := w.staging.Write(piece); err != nil {
		return err
	}
	return nil
}

func (w *writeSession) Commit(ctx context.Context, expected chunk.Digest, mode os.FileMode, modTime time.Time) error {
	// Mode/mtime are not applied: NFSv3 SETATTR is not wired in this experimental client.
	_ = mode
	_ = modTime
	if err := ctx.Err(); err != nil {
		return err
	}
	if w.committed {
		return nil
	}
	if w.staging == nil {
		return fmt.Errorf("commit: no staging file")
	}
	if _, err := w.staging.Seek(0, io.SeekStart); err != nil {
		return err
	}
	got, _, err := chunk.HashFile(w.staging)
	if err != nil {
		return err
	}
	if got != expected {
		return fmt.Errorf("digest mismatch: got %s want %s", got, expected)
	}
	if err := w.staging.Close(); err != nil {
		return err
	}
	w.staging = nil
	if w.old != nil {
		_ = w.old.Close()
		w.old = nil
	}
	// Atomic promote: prior final remains until RENAME succeeds.
	if err := w.t.rename(w.tmpRel, w.mountRel); err != nil {
		return err
	}
	w.committed = true
	return nil
}

func (w *writeSession) Abort() error {
	if w.committed {
		return nil
	}
	if w.staging != nil {
		_ = w.staging.Close()
		w.staging = nil
	}
	if w.old != nil {
		_ = w.old.Close()
		w.old = nil
	}
	_ = w.t.target.Remove(w.tmpRel)
	return nil
}

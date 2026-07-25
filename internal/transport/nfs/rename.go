package nfs

import (
	"fmt"
	"path"
	"strings"

	gonfs "github.com/vmware/go-nfs-client/nfs"
	nfsrpc "github.com/vmware/go-nfs-client/nfs/rpc"
	"github.com/vmware/go-nfs-client/nfs/xdr"
)

// NFSv3 RENAME is proc 11 (not exposed by go-nfs-client).
const nfsProc3Rename = 11

// rename replaces toRel with fromRel via NFSv3 RENAME (atomic on the server).
func (t *Transport) rename(fromRel, toRel string) error {
	fromDir, fromName := splitNFSPath(fromRel)
	toDir, toName := splitNFSPath(toRel)
	if fromName == "" || toName == "" || fromName == "." || toName == "." {
		return fmt.Errorf("nfs rename: invalid paths %q -> %q", fromRel, toRel)
	}
	_, fromFH, err := t.target.Lookup(fromDir)
	if err != nil {
		return fmt.Errorf("nfs rename lookup from dir: %w", err)
	}
	_, toFH, err := t.target.Lookup(toDir)
	if err != nil {
		return fmt.Errorf("nfs rename lookup to dir: %w", err)
	}

	type renameArgs struct {
		nfsrpc.Header
		From gonfs.Diropargs3
		To   gonfs.Diropargs3
	}
	res, err := t.target.Call(&renameArgs{
		Header: nfsrpc.Header{
			Rpcvers: 2,
			Prog:    gonfs.Nfs3Prog,
			Vers:    gonfs.Nfs3Vers,
			Proc:    nfsProc3Rename,
			Cred:    t.auth,
			Verf:    nfsrpc.AuthNull,
		},
		From: gonfs.Diropargs3{FH: fromFH, Filename: fromName},
		To:   gonfs.Diropargs3{FH: toFH, Filename: toName},
	})
	if err != nil {
		return fmt.Errorf("nfs rename: %w", err)
	}
	status, err := xdr.ReadUint32(res)
	if err != nil {
		return err
	}
	return gonfs.NFS3Error(status)
}

func splitNFSPath(p string) (dir, name string) {
	p = strings.Trim(p, "/")
	p = path.Clean("/" + p)
	p = strings.TrimPrefix(p, "/")
	if p == "" || p == "." {
		return "", ""
	}
	dir = path.Dir(p)
	name = path.Base(p)
	if dir == "." {
		dir = ""
	}
	return dir, name
}

package nfs

import "testing"

func TestNFSProc3RenameIsRFC1813(t *testing.T) {
	// RFC 1813 §3.3: MKNOD=11, REMOVE=12, RMDIR=13, RENAME=14.
	// Using 11 (MKNOD) produces RPC GARBAGE_ARGS against real nfs-kernel-server.
	if nfsProc3Rename != 14 {
		t.Fatalf("nfsProc3Rename=%d want 14 (NFSv3 RENAME)", nfsProc3Rename)
	}
}

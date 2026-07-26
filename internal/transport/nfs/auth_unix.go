//go:build unix

package nfs

import "os"

func authUnixUID() uint32 {
	uid := os.Getuid()
	if uid < 0 {
		return 65534 // nobody
	}
	return uint32(uid)
}

func authUnixGID() uint32 {
	gid := os.Getgid()
	if gid < 0 {
		return 65534
	}
	return uint32(gid)
}

func processUID() int { return os.Getuid() }
func processGID() int { return os.Getgid() }

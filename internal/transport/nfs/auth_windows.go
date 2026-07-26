//go:build windows

package nfs

// Windows has no POSIX uid/gid; AUTH_SYS still needs numeric ids.
// Prefer nobody over claiming root (0,0).
func authUnixUID() uint32 { return 65534 }
func authUnixGID() uint32 { return 65534 }

func processUID() int { return -1 }
func processGID() int { return -1 }

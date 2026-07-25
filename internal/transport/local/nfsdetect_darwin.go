//go:build darwin

package local

import (
	"strings"
	"syscall"
)

func isNFS(path string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false
	}
	var name [16]byte
	for i, c := range st.Fstypename {
		if c == 0 {
			break
		}
		name[i] = byte(c)
	}
	s := strings.TrimRight(string(name[:]), "\x00")
	return strings.HasPrefix(s, "nfs")
}

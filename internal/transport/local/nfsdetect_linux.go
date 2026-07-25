//go:build linux

package local

import "syscall"

func isNFS(path string) bool {
	var st syscall.Statfs_t
	if err := syscall.Statfs(path, &st); err != nil {
		return false
	}
	const nfsSuperMagic = 0x6969
	return st.Type == nfsSuperMagic
}

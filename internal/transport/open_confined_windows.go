//go:build windows

package transport

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/windows"
)

func openConfinedPath(absRoot, cleanedRel string) (*os.File, error) {
	full := filepath.Join(absRoot, cleanedRel)
	return os.Open(full)
}

func openedPath(f *os.File) (string, error) {
	h := windows.Handle(f.Fd())
	buf := make([]uint16, 4096)
	n, err := windows.GetFinalPathNameByHandle(h, &buf[0], uint32(len(buf)), 0)
	if err != nil {
		return "", err
	}
	if n == 0 || int(n) > len(buf) {
		return "", fmt.Errorf("GetFinalPathNameByHandle failed")
	}
	p := windows.UTF16ToString(buf[:n])
	p = strings.TrimPrefix(p, `\\?\UNC\`)
	p = strings.TrimPrefix(p, `\\?\`)
	p = strings.TrimPrefix(p, `\??\`)
	return filepath.Clean(p), nil
}

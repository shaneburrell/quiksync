//go:build unix

package transport

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

// openConfinedPath opens cleanedRel under absRoot using openat + O_NOFOLLOW
// for each path component. That shrinks the classic check-then-open TOCTOU
// window versus a single os.Open of the joined path.
func openConfinedPath(absRoot, cleanedRel string) (*os.File, error) {
	rootFD, err := unix.Open(absRoot, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	defer func() { _ = unix.Close(rootFD) }()

	parts := strings.Split(cleanedRel, string(filepath.Separator))
	var comps []string
	for _, p := range parts {
		if p == "" || p == "." {
			continue
		}
		comps = append(comps, p)
	}
	if len(comps) == 0 {
		fd, err := unix.Openat(rootFD, ".", unix.O_RDONLY|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, err
		}
		return os.NewFile(uintptr(fd), absRoot), nil
	}

	curFD := rootFD
	closeCur := false
	logical := absRoot
	for i, name := range comps {
		last := i == len(comps)-1
		flags := unix.O_RDONLY | unix.O_CLOEXEC | unix.O_NOFOLLOW
		if !last {
			flags |= unix.O_DIRECTORY
		}
		fd, err := unix.Openat(curFD, name, flags, 0)
		if err != nil {
			if err != unix.ELOOP && err != unix.ENOTDIR {
				if closeCur {
					_ = unix.Close(curFD)
				}
				return nil, err
			}
			// Symlink component: resolve, require it stay under root, then reopen.
			buf := make([]byte, 4096)
			n, lerr := unix.Readlinkat(curFD, name, buf)
			if lerr != nil {
				if closeCur {
					_ = unix.Close(curFD)
				}
				return nil, lerr
			}
			target := string(buf[:n])
			var resolved string
			if filepath.IsAbs(target) {
				resolved = filepath.Clean(target)
			} else {
				resolved = filepath.Clean(filepath.Join(logical, target))
			}
			if r, e := filepath.EvalSymlinks(resolved); e == nil {
				resolved = r
			}
			if err := mustUnder(absRoot, resolved); err != nil {
				if closeCur {
					_ = unix.Close(curFD)
				}
				return nil, fmt.Errorf("symlink escapes root: %q", cleanedRel)
			}
			if closeCur {
				_ = unix.Close(curFD)
			}
			if last {
				f, oerr := os.Open(resolved)
				if oerr != nil {
					return nil, oerr
				}
				return f, nil
			}
			nfd, oerr := unix.Open(resolved, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
			if oerr != nil {
				return nil, oerr
			}
			curFD = nfd
			closeCur = true
			logical = resolved
			continue
		}
		if closeCur {
			_ = unix.Close(curFD)
		}
		curFD = fd
		closeCur = true
		logical = filepath.Join(logical, name)
	}
	return os.NewFile(uintptr(curFD), filepath.Join(absRoot, cleanedRel)), nil
}

func openedPath(f *os.File) (string, error) {
	name := f.Name()
	if name == "" {
		return "", fmt.Errorf("empty file name")
	}
	abs, err := filepath.Abs(name)
	if err != nil {
		return "", err
	}
	if r, err := filepath.EvalSymlinks(abs); err == nil {
		return r, nil
	}
	// Dangling or race: still return abs for mustUnder against root.
	return abs, nil
}

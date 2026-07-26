//go:build unix

package transport

import (
	"fmt"
	"os"
	"path/filepath"
)

func openConfinedPath(absRoot, cleanedRel string) (*os.File, error) {
	full := filepath.Join(absRoot, cleanedRel)
	return os.Open(full)
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

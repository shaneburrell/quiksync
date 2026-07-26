package transport

import (
	"fmt"
	"os"
	"path/filepath"
)

// OpenConfined opens rel under root for reading, rejecting symlink escapes.
// After open, the resolved path of the opened file is re-checked under root.
func OpenConfined(root, rel string) (*os.File, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if r, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = r
	}
	if _, err := Confine(root, rel); err != nil {
		return nil, err
	}
	cleaned := filepath.Clean(filepath.FromSlash(rel))
	f, err := openConfinedPath(absRoot, cleaned)
	if err != nil {
		return nil, err
	}
	resolved, err := openedPath(f)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if r, err := filepath.EvalSymlinks(resolved); err == nil {
		resolved = r
	}
	if err := mustUnder(absRoot, resolved); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("symlink escapes root: %q", rel)
	}
	return f, nil
}

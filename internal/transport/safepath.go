package transport

import (
	"fmt"
	"path/filepath"
	"strings"
)

// SafeJoin joins root and a relative path, rejecting escapes outside root.
func SafeJoin(root, rel string) (string, error) {
	if rel == "" || rel == "." {
		return root, nil
	}
	rel = filepath.ToSlash(rel)
	if strings.HasPrefix(rel, "/") || filepath.IsAbs(rel) {
		return "", fmt.Errorf("absolute path rejected: %q", rel)
	}
	// Reject Windows drive paths and volume names.
	if len(rel) >= 2 && rel[1] == ':' {
		return "", fmt.Errorf("absolute path rejected: %q", rel)
	}
	cleaned := filepath.Clean(filepath.FromSlash(rel))
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root: %q", rel)
	}
	full := filepath.Join(root, cleaned)
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	absFull, err := filepath.Abs(full)
	if err != nil {
		return "", err
	}
	relOut, err := filepath.Rel(absRoot, absFull)
	if err != nil {
		return "", err
	}
	if relOut == ".." || strings.HasPrefix(relOut, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("path escapes root: %q", rel)
	}
	return absFull, nil
}

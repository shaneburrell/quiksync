package transport

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// SafeJoin joins root and a relative path, rejecting escapes outside root.
// Empty rel or "." maps to root (identity). Prefer SafeJoinFile for file ops.
func SafeJoin(root, rel string) (string, error) {
	if rel == "" || rel == "." {
		return filepath.Abs(root)
	}
	return safeJoinClean(root, rel)
}

// SafeJoinFile is like SafeJoin but rejects "", ".", and cleaned paths that
// resolve to the root itself (so Remove/OpenRead cannot target the serve root).
func SafeJoinFile(root, rel string) (string, error) {
	if rel == "" || rel == "." {
		return "", fmt.Errorf("empty path rejected")
	}
	full, err := safeJoinClean(root, rel)
	if err != nil {
		return "", err
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if full == absRoot {
		return "", fmt.Errorf("empty path rejected: %q", rel)
	}
	return full, nil
}

func safeJoinClean(root, rel string) (string, error) {
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
	if cleaned == "." {
		return "", fmt.Errorf("empty path rejected: %q", rel)
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
	if err := mustUnder(absRoot, absFull); err != nil {
		return "", fmt.Errorf("path escapes root: %q", rel)
	}
	return absFull, nil
}

// Confine resolves rel under root and rejects symlink escapes.
// Returns an absolute path suitable for OS operations.
func Confine(root, rel string) (string, error) {
	if _, err := SafeJoinFile(root, rel); err != nil {
		return "", err
	}
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return "", err
	}
	if r, err := filepath.EvalSymlinks(absRoot); err == nil {
		absRoot = r
	}
	cleaned := filepath.Clean(filepath.FromSlash(rel))
	parts := strings.Split(cleaned, string(filepath.Separator))
	cur := absRoot
	for i, p := range parts {
		if p == "" || p == "." {
			continue
		}
		next := filepath.Join(cur, p)
		fi, err := os.Lstat(next)
		if err != nil {
			if os.IsNotExist(err) {
				rest := filepath.Join(parts[i:]...)
				out := filepath.Join(cur, rest)
				if err := mustUnder(absRoot, cur); err != nil {
					return "", fmt.Errorf("path escapes root: %q", rel)
				}
				return out, nil
			}
			return "", err
		}
		if fi.Mode()&os.ModeSymlink != 0 {
			resolved, err := filepath.EvalSymlinks(next)
			if err != nil {
				return "", err
			}
			if err := mustUnder(absRoot, resolved); err != nil {
				return "", fmt.Errorf("symlink escapes root: %q", rel)
			}
			cur = resolved
			continue
		}
		if err := mustUnder(absRoot, next); err != nil {
			return "", fmt.Errorf("path escapes root: %q", rel)
		}
		cur = next
	}
	return cur, nil
}

func mustUnder(absRoot, absPath string) error {
	relOut, err := filepath.Rel(absRoot, absPath)
	if err != nil {
		return err
	}
	if relOut == ".." || strings.HasPrefix(relOut, ".."+string(filepath.Separator)) {
		return fmt.Errorf("path escapes root")
	}
	return nil
}

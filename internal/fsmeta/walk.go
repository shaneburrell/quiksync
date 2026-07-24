package fsmeta

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// FileInfo is portable metadata for a source/dest path.
type FileInfo struct {
	RelPath   string
	AbsPath   string
	Size      int64
	ModTime   time.Time
	Mode      os.FileMode
	IsDir     bool
	IsSymlink bool
}

// Walk lists regular files under root, applying exclude globs.
func Walk(root string, exclude []string) ([]FileInfo, error) {
	root = filepath.Clean(root)
	var out []FileInfo
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		// Skip internal state dirs.
		parts := strings.Split(rel, string(os.PathSeparator))
		if parts[0] == ".quiksync" || parts[0] == ".quiksync.tmp" {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if matchExclude(rel, exclude) {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		fi := FileInfo{
			RelPath:   filepath.ToSlash(rel),
			AbsPath:   path,
			Size:      info.Size(),
			ModTime:   info.ModTime(),
			Mode:      info.Mode(),
			IsDir:     d.IsDir(),
			IsSymlink: info.Mode()&os.ModeSymlink != 0,
		}
		if d.IsDir() {
			return nil
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		out = append(out, fi)
		return nil
	})
	return out, err
}

func matchExclude(rel string, patterns []string) bool {
	// Normalize separators so Windows-style paths match Unix-style globs.
	rel = filepath.ToSlash(strings.ReplaceAll(rel, "\\", "/"))
	for _, p := range patterns {
		p = filepath.ToSlash(strings.ReplaceAll(p, "\\", "/"))
		if ok, _ := filepath.Match(p, rel); ok {
			return true
		}
		if ok, _ := filepath.Match(p, filepath.Base(rel)); ok {
			return true
		}
		// prefix dir match: "vendor/*"
		if strings.HasSuffix(p, "/*") {
			prefix := strings.TrimSuffix(p, "/*")
			if rel == prefix || strings.HasPrefix(rel, prefix+"/") {
				return true
			}
		}
	}
	return false
}

// Generation is used for live-change detection.
type Generation struct {
	Size    int64
	ModNano int64
}

func GenOf(fi FileInfo) Generation {
	return Generation{Size: fi.Size, ModNano: fi.ModTime.UnixNano()}
}

func StatGeneration(path string) (Generation, error) {
	st, err := os.Stat(path)
	if err != nil {
		return Generation{}, err
	}
	return Generation{Size: st.Size(), ModNano: st.ModTime().UnixNano()}, nil
}

func UnchangedFor(fi FileInfo, window time.Duration) bool {
	if window <= 0 {
		return true
	}
	return time.Since(fi.ModTime) >= window
}

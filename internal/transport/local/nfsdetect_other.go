//go:build !linux && !darwin

package local

func isNFS(path string) bool { return false }

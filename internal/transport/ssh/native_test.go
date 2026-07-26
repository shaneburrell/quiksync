package ssh

import (
	"runtime"
	"testing"
)

func TestUseNativeSSHDefault(t *testing.T) {
	old := PreferNative
	t.Cleanup(func() { PreferNative = old })

	PreferNative = false
	t.Setenv("QUIKSYNC_SSH_NATIVE", "")
	if useNativeSSH() {
		t.Fatal("expected false when PreferNative=false and env unset")
	}
	t.Setenv("QUIKSYNC_SSH_NATIVE", "1")
	if !useNativeSSH() {
		t.Fatal("expected true when QUIKSYNC_SSH_NATIVE=1")
	}
	PreferNative = true
	t.Setenv("QUIKSYNC_SSH_NATIVE", "")
	if !useNativeSSH() {
		t.Fatal("expected true when PreferNative=true")
	}
}

func TestPreferNativeMatchesGOOS(t *testing.T) {
	want := runtime.GOOS == "windows"
	// PreferNative is initialized at package load from GOOS.
	if PreferNative != want {
		t.Fatalf("PreferNative=%v want %v for GOOS=%s", PreferNative, want, runtime.GOOS)
	}
}

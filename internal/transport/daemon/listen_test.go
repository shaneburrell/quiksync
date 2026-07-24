package daemon

import (
	"context"
	"testing"
)

func TestIsLoopbackListen(t *testing.T) {
	cases := []struct {
		listen string
		want   bool
	}{
		{":4242", false},
		{"0.0.0.0:4242", false},
		{"[::]:4242", false},
		{"192.168.1.1:4242", false},
		{"127.0.0.1:4242", true},
		{"localhost:4242", true},
		{"[::1]:4242", true},
		{"not-a-hostport", false},
	}
	for _, tc := range cases {
		if got := IsLoopbackListen(tc.listen); got != tc.want {
			t.Fatalf("%q: got %v want %v", tc.listen, got, tc.want)
		}
	}
}

func TestServeRejectsNoAuthOnNonLoopback(t *testing.T) {
	err := Serve(context.Background(), ServeConfig{
		Listen:      ":42421",
		Root:        t.TempDir(),
		AllowNoAuth: true,
	})
	if err == nil {
		t.Fatal("expected error for :port with AllowNoAuth and empty token")
	}
	err = Serve(context.Background(), ServeConfig{
		Listen:      "0.0.0.0:42422",
		Root:        t.TempDir(),
		AllowNoAuth: true,
	})
	if err == nil {
		t.Fatal("expected error for 0.0.0.0 with AllowNoAuth and empty token")
	}
}

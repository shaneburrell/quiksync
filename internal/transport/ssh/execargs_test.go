package ssh

import (
	"testing"

	"github.com/shaneburrell/quiksync/internal/transport"
)

func TestExecArgsStopsOptionParsing(t *testing.T) {
	args := execArgs(transport.Endpoint{User: "u", Host: "h", Port: "2222"})
	want := []string{"-T", "-o", "BatchMode=yes", "-p", "2222", "--", "u@h", "quiksync", "remote-helper"}
	if len(args) != len(want) {
		t.Fatalf("args=%v", args)
	}
	for i := range want {
		if args[i] != want[i] {
			t.Fatalf("args[%d]=%q want %q (full=%v)", i, args[i], want[i], args)
		}
	}
	// Ensure -- appears before the target so a dash-prefixed host cannot become a flag.
	dashDash := -1
	target := -1
	for i, a := range args {
		if a == "--" {
			dashDash = i
		}
		if a == "u@h" {
			target = i
		}
	}
	if dashDash < 0 || target != dashDash+1 {
		t.Fatalf("-- must immediately precede target: %v", args)
	}
}

package fsmeta

import "testing"

func TestMatchExclude(t *testing.T) {
	if !MatchExclude("vendor/x.go", []string{"vendor/*"}) {
		t.Fatal("vendor/*")
	}
	if !MatchExclude("a.txt", []string{"*.txt"}) {
		t.Fatal("*.txt")
	}
	if MatchExclude("a.go", []string{"*.txt"}) {
		t.Fatal("no match")
	}
}

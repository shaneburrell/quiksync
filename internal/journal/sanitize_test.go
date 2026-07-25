package journal

import "testing"

func TestSanitizeJobIDEdges(t *testing.T) {
	got, err := SanitizeJobID("")
	if err != nil || got != "default" {
		t.Fatalf("empty: %q %v", got, err)
	}
	if _, err := SanitizeJobID("../x"); err == nil {
		t.Fatal("dotdot")
	}
	if _, err := SanitizeJobID("a/b"); err == nil {
		t.Fatal("slash")
	}
	got, err = SanitizeJobID("ok-job_1.2")
	if err != nil || got != "ok-job_1.2" {
		t.Fatalf("ok: %q %v", got, err)
	}
	got, err = SanitizeJobID("weird job!")
	if err != nil || got != "weird_job_" {
		t.Fatalf("sanitize: %q %v", got, err)
	}
}

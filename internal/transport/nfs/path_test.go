package nfs

import "testing"

func TestSplitExportCandidates(t *testing.T) {
	got := splitExportCandidates("/export/backup/data")
	want := []string{"/export/backup/data", "/export/backup", "/export"}
	if len(got) != len(want) {
		t.Fatalf("got %v want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v want %v", got, want)
		}
	}
}

func TestBaseAfterExport(t *testing.T) {
	cases := []struct {
		path, export, want string
	}{
		{"/export/backup", "/export", "backup"},
		{"/export/backup/data", "/export", "backup/data"},
		{"/export", "/export", ""},
		{"export/backup", "/export", "backup"},
	}
	for _, tc := range cases {
		if got := baseAfterExport(tc.path, tc.export); got != tc.want {
			t.Fatalf("baseAfterExport(%q,%q)=%q want %q", tc.path, tc.export, got, tc.want)
		}
	}
}

func TestJoinBase(t *testing.T) {
	tr := &Transport{base: "backup"}
	got, err := tr.joinBase("a/b.txt")
	if err != nil || got != "backup/a/b.txt" {
		t.Fatalf("got %q err=%v", got, err)
	}
	got, err = tr.joinBase(".")
	if err != nil || got != "backup" {
		t.Fatalf("root got %q err=%v", got, err)
	}
	if tr.userRel("backup/a/b.txt") != "a/b.txt" {
		t.Fatalf("userRel=%q", tr.userRel("backup/a/b.txt"))
	}
}

func TestSplitNFSPath(t *testing.T) {
	cases := []struct {
		in, dir, name string
	}{
		{"a/b.txt", "a", "b.txt"},
		{"file.txt", "", "file.txt"},
		{".quiksync.tmp/ab.partial", ".quiksync.tmp", "ab.partial"},
		{"/export/x", "export", "x"},
	}
	for _, tc := range cases {
		dir, name := splitNFSPath(tc.in)
		if dir != tc.dir || name != tc.name {
			t.Fatalf("splitNFSPath(%q)=(%q,%q) want (%q,%q)", tc.in, dir, name, tc.dir, tc.name)
		}
	}
}

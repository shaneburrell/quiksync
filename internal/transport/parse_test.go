package transport

import "testing"

func TestParseEndpoints(t *testing.T) {
	cases := []struct {
		in, scheme, path, host string
	}{
		{"/tmp/a", "file", "/tmp/a", ""},
		{"file:///tmp/a", "file", "/tmp/a", ""},
		{"user@host:/data", "ssh", "/data", "host"},
		{"myserver:/data", "ssh", "/data", "myserver"},
		{"myserver:relative", "ssh", "relative", "myserver"},
		{"./file:name", "file", "./file:name", ""},
		{"a/b:c", "file", "a/b:c", ""},
		{"C:\\data", "file", "C:\\data", ""},
		{"ssh://user@host:22/data", "ssh", "/data", "host"},
		{"quiksync://host:4242/data", "quiksync", "/data", "host"},
		{"s3://bucket/prefix", "s3", "prefix", "bucket"},
	}
	for _, tc := range cases {
		ep, err := ParseEndpoint(tc.in)
		if err != nil {
			t.Fatalf("%s: %v", tc.in, err)
		}
		if ep.Scheme != tc.scheme || ep.Path != tc.path || ep.Host != tc.host {
			t.Fatalf("%s: got scheme=%s host=%s path=%s", tc.in, ep.Scheme, ep.Host, ep.Path)
		}
	}
}

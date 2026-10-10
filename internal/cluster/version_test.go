package cluster

import "testing"

func TestParseCephVersion(t *testing.T) {
	for output, want := range map[string]string{
		"ceph version 20.2.4 (abc) tentacle (stable)\n":             "20.2.4",
		"ceph version 19.2.6 (f9fd95b) squid (stable)":              "19.2.6",
		"ceph version 21.0.0-1234-gdeadbeef (abc) umbrella (dev)\n": "21.0.0",
	} {
		if got, err := parseCephVersion(output); err != nil || got != want {
			t.Fatalf("parseCephVersion(%q) = %q, %v; want %q", output, got, err, want)
		}
	}
	for _, output := range []string{"", "ceph version unknown", "rados 20.2.4"} {
		if got, err := parseCephVersion(output); err == nil {
			t.Fatalf("parseCephVersion(%q) = %q, want error", output, got)
		}
	}
}

func TestCephBeforeTreatsUnknownAsCurrent(t *testing.T) {
	for version, want := range map[string]bool{"": false, "19.2.6": true, "20.2.4": false, "21.0.0": false} {
		c := &Container{cephVersion: version}
		if got := c.cephBefore(20); got != want {
			t.Fatalf("cephBefore(20) with %q = %v, want %v", version, got, want)
		}
	}
	var missing *Container
	if missing.CephVersion() != "" {
		t.Fatal("nil cluster reported a version")
	}
}

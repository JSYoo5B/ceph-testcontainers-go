package naming

import (
	"strings"
	"testing"
)

// TestValidMirrorDaemonName runs the names on which the two mirror packages
// differ through both rules. cephfs/mirror admits dots and a leading hyphen;
// rbd/mirror admits neither.
func TestValidMirrorDaemonName(t *testing.T) {
	cephfs := MirrorDaemonRule{Dots: true, LeadingHyphen: true}
	rbd := MirrorDaemonRule{}
	for _, c := range []struct {
		name        string
		cephfs, rbd bool
	}{
		{"a", true, true},
		{"node-27", true, true},
		{"receiver_B", true, true},
		{"_a", true, true},
		{"a-", true, true},
		{strings.Repeat("a", 63), true, true},
		{"a.b", true, false},
		{".a", true, false},
		{"-a", true, false},
		{"-.", true, false},
		{"", false, false},
		{strings.Repeat("a", 64), false, false},
		{"a b", false, false},
		{"a/b", false, false},
		{"a+b", false, false},
		{"ä", false, false},
	} {
		if got := ValidMirrorDaemonName(c.name, cephfs); got != c.cephfs {
			t.Errorf("cephfs rule, %q: %v", c.name, got)
		}
		if got := ValidMirrorDaemonName(c.name, rbd); got != c.rbd {
			t.Errorf("rbd rule, %q: %v", c.name, got)
		}
	}
}

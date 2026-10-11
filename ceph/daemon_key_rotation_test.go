package ceph

import (
	"strings"
	"testing"
)

func TestRotateDaemonKeyRefusesBeforeRunningCommands(t *testing.T) {
	for name, test := range map[string]struct {
		edit   func(*Container)
		daemon string
		want   string
	}{
		"MON":            {nil, "mon.a", "share the mon. key"},
		"invalid name":   {nil, "osd.01", "must be"},
		"MDS":            {nil, "mds.a", "must be"},
		"unknown OSD":    {nil, "osd.7", "not owned"},
		"purged OSD":     {nil, "osd.1", "not owned"},
		"terminated MGR": {nil, "mgr.b", "not owned"},
		"closed cluster": {func(c *Container) { c.closed = true }, "osd.0", "terminated"},
	} {
		t.Run(name, func(t *testing.T) {
			c, control := adminSocketCluster()
			if test.edit != nil {
				test.edit(c)
			}
			err := c.RotateDaemonKey(t.Context(), test.daemon)
			if err == nil || !strings.Contains(err.Error(), test.want) || len(control.Calls) != 0 {
				t.Fatalf("err=%v calls=%v", err, control.Calls)
			}
		})
	}
}

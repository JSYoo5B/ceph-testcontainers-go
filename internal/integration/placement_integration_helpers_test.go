//go:build all || integration

package integration_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func waitPlacementOSDDown(t *testing.T, parent context.Context, cluster *ceph.Container, id int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		output, err := cluster.Ceph(ctx, "osd", "dump", "--format", "json")
		if err != nil {
			t.Fatal(err)
		}
		var dump struct {
			OSDs []struct {
				ID int `json:"osd"`
				Up int `json:"up"`
			} `json:"osds"`
		}
		if err := json.Unmarshal(output, &dump); err != nil {
			t.Fatal(err)
		}
		for _, osd := range dump.OSDs {
			if osd.ID == id && osd.Up == 0 {
				return
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("osd.%d did not become down in the OSD map: %s", id, output)
		case <-ticker.C:
		}
	}
}

const hostPlacementClientScript = `import rados, sys
pool, phase = sys.argv[1:]
original = bytes(range(256)) * 256
patch = b"write-while-one-host-is-down" * 23
offset = 4093
expected = original[:offset] + patch + original[offset + len(patch):]
with rados.Rados(conffile="/etc/ceph/ceph.conf", conf={"rados_mon_op_timeout": "30", "rados_osd_op_timeout": "30"}) as cluster:
    with cluster.open_ioctx(pool) as ioctx:
        for index in range(16):
            name = "object-%02d" % index
            if phase == "seed":
                ioctx.write_full(name, original)
            elif phase == "outage":
                assert ioctx.read(name, len(original) + 1) == original, "outage read lost seeded bytes"
                ioctx.write(name, patch, offset)
                ioctx.write_full("outage-%02d" % index, patch)
            wanted = original if phase == "seed" else expected
            assert ioctx.read(name, len(wanted) + 1) == wanted, "host-placement object bytes differ"
            if phase != "seed":
                assert ioctx.read("outage-%02d" % index, len(patch) + 1) == patch, "outage-created bytes differ"
            if phase == "verify":
                ioctx.remove_object(name)
                ioctx.remove_object("outage-%02d" % index)
        if phase == "verify":
            assert list(ioctx.list_objects()) == [], "host-placement objects remain after cleanup"
`

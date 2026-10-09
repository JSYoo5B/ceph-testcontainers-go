//go:build all || (integration && features)

package integration_test

import (
	"context"
	"testing"
	"time"

	ceph "github.com/jsyoo5b/ceph-testcontainers-go/ceph"
)

func waitOSDPolicyState(t *testing.T, ctx context.Context, cluster *ceph.Container, id int, matches func(ceph.OSDState) bool) ceph.OSDState {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	var last ceph.OSDState
	for {
		states, err := cluster.OSDStates(ctx)
		if err != nil {
			t.Fatal(err)
		}
		for _, state := range states {
			if state.ID == id {
				last = state
				if matches(state) {
					return state
				}
			}
		}
		select {
		case <-ctx.Done():
			t.Fatalf("OSD state not observed: %+v: %v", last, ctx.Err())
		case <-time.After(500 * time.Millisecond):
		}
	}
}

const osdPolicyProbe = `import rados,sys
phase=sys.argv[1]
payload=bytes(range(256))*64
c=rados.Rados(conffile="/etc/ceph/ceph.conf")
c.conf_set("rados_osd_op_timeout","10")
c.connect(timeout=10)
try:
    with c.open_ioctx("tc-osd-policy") as io:
        for n in range(64):
            name="seed-"+str(n)
            if phase=="seed": io.write_full(name,payload)
            assert io.read(name,len(payload))==payload
        io.write_full("after-"+phase,payload[::-1])
        assert io.read("after-"+phase,len(payload))==payload[::-1]
    print("OSD native payload phase passed:",phase)
finally: c.shutdown()
`

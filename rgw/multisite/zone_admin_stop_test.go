package multisite

import (
	"context"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// ZoneAdmin runs radosgw-admin in the zone's CLI client supervised, as
// rgw.Gateway.Admin does in a gateway: a canceled call stops the command
// before it returns, or returns a NativeCommandUnstoppedError whose Stop
// stops it.
func TestRGWZoneAdminStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	cli := &rgwScriptCLI{}
	f := newPhaseFixture(cli, cli)
	hang := clustertest.NewHangingCommand(cli, func(argv []string) bool {
		return argv[0] == "radosgw-admin" && slices.Contains(argv, "bucket") && slices.Contains(argv, "stats")
	})
	f.realm.sourceClient = hang
	clustertest.AssertSupervisedStop[*ceph.NativeCommandUnstoppedError](t, hang, func(ctx context.Context) error {
		_, err := f.ZoneAdmin(ctx, "a", "bucket", "stats")
		return err
	})
}

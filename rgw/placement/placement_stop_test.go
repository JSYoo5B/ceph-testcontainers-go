package placement

import (
	"context"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// A placement change runs radosgw-admin supervised, as rgw.Gateway.Admin
// does: a canceled call stops the command before it returns, or returns a
// NativeCommandUnstoppedError whose Stop stops it.
func TestRGWPlacementStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	g, f := newRGWPlacementFixture()
	hang := clustertest.NewHangingCommand(f, func(argv []string) bool {
		return argv[0] == "radosgw-admin" && slices.Contains(argv, "zonegroup") && slices.Contains(argv, "placement") && slices.Contains(argv, "add")
	})
	g.Replace(hang)
	clustertest.AssertSupervisedStop[*ceph.NativeCommandUnstoppedError](t, hang, func(ctx context.Context) error {
		_, err := create(ctx, g.Core, testConfig())
		return err
	})
}

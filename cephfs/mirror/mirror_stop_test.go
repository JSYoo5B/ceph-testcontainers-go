package mirror

import (
	"context"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
)

// A CephFS mirror status read runs the daemon's admin-socket command
// supervised: a canceled call stops the command before it returns, or
// returns a NativeCommandUnstoppedError whose Stop stops it.
func TestCephFSMirrorStatusReadStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	mirror, _, _, daemon, _ := newCephFSObserverDraft()
	hang := clustertest.NewHangingCommand(daemon, func(argv []string) bool {
		return slices.Equal(argv, []string{"ceph", "--admin-daemon", "/var/run/ceph/cephfs-mirror.asok", "fs", "mirror", "status", "source@1"})
	})
	mirror.roster.daemons[0].Container = hang
	clustertest.AssertSupervisedStop[*ceph.NativeCommandUnstoppedError](t, hang, func(ctx context.Context) error {
		_, err := mirror.DirectoryStatus(ctx, "/owned")
		return err
	})
}

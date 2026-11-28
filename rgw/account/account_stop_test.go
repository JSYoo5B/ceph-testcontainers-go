package account

import (
	"context"
	"slices"
	"testing"

	"github.com/JSYoo5B/ceph-testcontainers-go/ceph"
	"github.com/JSYoo5B/ceph-testcontainers-go/internal/clustertest"
	"github.com/JSYoo5B/ceph-testcontainers-go/rgw"
)

// An account quota change runs radosgw-admin supervised, as rgw.Gateway.Admin
// does: a canceled call stops the command before it returns, or returns a
// NativeCommandUnstoppedError whose Stop stops it.
func TestAccountQuotaStopsItsCommandWhenTheCallerCancels(t *testing.T) {
	g, f := newAccountTestGateway()
	created, err := create(t.Context(), g.Core, Config{Name: "account", Tenant: "team"})
	if err != nil {
		t.Fatal(err)
	}
	a := newAccount(created)
	hang := clustertest.NewHangingCommand(f, func(argv []string) bool {
		return argv[0] == "radosgw-admin" && slices.Contains(argv, "quota") && slices.Contains(argv, "set")
	})
	g.Replace(hang)
	clustertest.AssertSupervisedStop[*ceph.NativeCommandUnstoppedError](t, hang, func(ctx context.Context) error {
		return a.SetQuota(ctx, rgw.Quota{Enabled: true, MaxSizeBytes: 1024, MaxObjects: 1})
	})
}
